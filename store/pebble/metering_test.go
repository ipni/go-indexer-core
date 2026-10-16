package pebble

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

func TestBatchSleep(t *testing.T) {
	require.Equal(t, 3*time.Second, batchSleep(time.Second, 0.25))
	require.Equal(t, time.Second, batchSleep(time.Second, 0.5))
	require.EqualValues(t, 0, batchSleep(time.Second, 1))
	require.Equal(t, minBatchElapsed, batchSleep(0, 0.5))
	require.Equal(t, maxBatchSleep, batchSleep(maxBatchSleep, 0.01))
}

func registerMeteringViews(t *testing.T) {
	t.Helper()
	view.Unregister(metrics.MeteringViews...)
	require.NoError(t, view.Register(metrics.MeteringViews...))
	t.Cleanup(func() { view.Unregister(metrics.MeteringViews...) })
}

func meteringSum(t *testing.T, name string) float64 {
	t.Helper()
	rows, err := view.RetrieveData(name)
	require.NoError(t, err)
	if len(rows) == 0 {
		return 0
	}
	require.Len(t, rows, 1)
	sum, ok := rows[0].Data.(*view.SumData)
	require.True(t, ok)
	return sum.Value
}

func meteringLastValue(t *testing.T, name string) float64 {
	t.Helper()
	rows, err := view.RetrieveData(name)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	last, ok := rows[0].Data.(*view.LastValueData)
	require.True(t, ok)
	return last.Value
}

func openMetered(t *testing.T, cfg MeteringConfig) (indexer.Interface, indexer.StatsMeter) {
	t.Helper()
	s, err := New(t.TempDir(), nil, WithMetering(cfg))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	pm, ok := s.(indexer.StatsMeter)
	require.True(t, ok, "store does not implement StatsMeter")
	return s, pm
}

func waitScanDone(t *testing.T, pm indexer.StatsMeter, timeout time.Duration) *indexer.AllStatsReport {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := pm.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		require.Empty(t, st.Error, "scan failed: %s", st.Error)
		if st.State == indexer.ScanStateDone {
			report, err := pm.MeteringAllStats(context.Background(), nil)
			require.NoError(t, err)
			if report != nil {
				return report
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.FailNow(t, "timeout waiting for scan to complete")
	return nil
}

// waitPastScanID waits until the wall clock is past scanID, so a scan started
// afterwards gets a new ID. Scan IDs are the start time in Unix microseconds.
func waitPastScanID(t *testing.T, scanID uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return uint64(time.Now().UnixMicro()) > scanID
	}, time.Second, 10*time.Microsecond)
}

// triggerWhenIdle starts a scan. Status can show done while the runner is still
// leaving the previous scan, and trigger then returns ErrScanInProgress.
func triggerWhenIdle(t *testing.T, pm indexer.StatsMeter) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := pm.MeteringTriggerScan(context.Background())
		if err == nil {
			return
		}
		require.ErrorIs(t, err, indexer.ErrScanInProgress)
		require.False(t, time.Now().After(deadline), "timed out waiting to trigger: %v", err)
		time.Sleep(5 * time.Millisecond)
	}
}

func putValue(t *testing.T, s indexer.Interface, pid peer.ID, ctxID, meta []byte, mhs ...multihash.Multihash) {
	t.Helper()
	err := s.Put(indexer.Value{
		ProviderID:    pid,
		ContextID:     ctxID,
		MetadataBytes: meta,
	}, mhs...)
	require.NoError(t, err)
}

func TestMeteringBasicCounts(t *testing.T) {
	s, pm := openMetered(t, MeteringConfig{
		BatchSize: 100,
		Interval:  0, // manual only
		TimeFill:  1,
	})

	peers := random.Peers(2)
	p1, p2 := peers[0], peers[1]
	mhs := random.Multihashes(5)

	// p1: 3 multihashes under ctx A, 2 under ctx B (one shared with A)
	putValue(t, s, p1, []byte("ctxA"), []byte("metaA"), mhs[0], mhs[1], mhs[2])
	putValue(t, s, p1, []byte("ctxB"), []byte("metaBB"), mhs[2], mhs[3])
	// p2: 2 multihashes under ctx C
	putValue(t, s, p2, []byte("ctxC"), []byte("m"), mhs[3], mhs[4])
	// Duplicate put should not increase slot count after merge.
	putValue(t, s, p1, []byte("ctxA"), []byte("metaA"), mhs[0])

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	report := waitScanDone(t, pm, 5*time.Second)

	require.EqualValues(t, 5, report.Totals.Active.Entries)
	require.Zero(t, report.Totals.Deleted.Entries)
	// slots: p1 has 3+2=5, p2 has 2 → 7
	require.EqualValues(t, 7, report.Totals.Active.Slots)

	st, err := pm.MeteringScanStatus(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, indexer.ScanStateDone, st.State)
	require.Empty(t, st.Error)
	require.Equal(t, report.Totals, st.Current.Totals)
	require.Equal(t, 100.0, st.EstimatedPercentDone)
	require.NotNil(t, st.EstimatedFinish)
	byID := map[peer.ID]indexer.ProviderStats{}
	for _, ps := range report.Providers {
		byID[ps.ProviderID] = ps
	}
	require.EqualValues(t, 4, byID[p1].Multihashes)
	require.EqualValues(t, 5, byID[p1].Slots)
	require.EqualValues(t, 2, byID[p2].Multihashes)
	require.EqualValues(t, 2, byID[p2].Slots)

	filtered, err := pm.MeteringAllStats(context.Background(), []peer.ID{p1})
	require.NoError(t, err)
	require.NotNil(t, filtered)
	require.Equal(t, report.Totals.Active.Entries, filtered.Totals.Active.Entries)
	require.Len(t, filtered.Providers, 1)
	require.Equal(t, p1, filtered.Providers[0].ProviderID)
	require.EqualValues(t, 4, filtered.Providers[0].Multihashes)

	none, err := pm.MeteringAllStats(context.Background(), []peer.ID{})
	require.NoError(t, err)
	require.NotNil(t, none)
	require.Equal(t, report.Totals.Active.Entries, none.Totals.Active.Entries)
	require.Empty(t, none.Providers)

	missing, err := pm.MeteringAllStats(context.Background(), []peer.ID{peer.ID("missing-provider")})
	require.NoError(t, err)
	require.NotNil(t, missing)
	require.Empty(t, missing.Providers)
}

func TestMeteringOrphanSlotsAfterRemoveProviderContext(t *testing.T) {
	s, pm := openMetered(t, MeteringConfig{BatchSize: 50, Interval: 0, TimeFill: 1})
	p1 := random.Peers(1)[0]
	mhs := random.Multihashes(3)
	putValue(t, s, p1, []byte("ctx"), []byte("meta"), mhs...)

	require.NoError(t, s.RemoveProviderContext(p1, []byte("ctx")))

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	report := waitScanDone(t, pm, 5*time.Second)

	// Removing a context deletes its value record and leaves the multihash slots.
	// No provider value remains, so the three keys are deleted entries.
	require.Zero(t, report.Totals.Active.Entries)
	require.Zero(t, report.Totals.Active.Slots)
	require.EqualValues(t, 3, report.Totals.Deleted.Entries)
	require.EqualValues(t, 3, report.Totals.Deleted.Slots)
	require.Empty(t, report.Providers)
}

func TestMeteringDeletedProvider(t *testing.T) {
	s, pm := openMetered(t, MeteringConfig{BatchSize: 50, Interval: 0, TimeFill: 1})
	p1 := random.Peers(1)[0]
	mhs := random.Multihashes(3)
	putValue(t, s, p1, []byte("ctx"), []byte("meta"), mhs...)

	require.NoError(t, s.RemoveProvider(context.Background(), p1))

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	report := waitScanDone(t, pm, 5*time.Second)

	require.Zero(t, report.Totals.Active.Entries)
	require.EqualValues(t, 3, report.Totals.Deleted.Entries)
	require.EqualValues(t, 3, report.Totals.Deleted.Slots)
	require.Empty(t, report.Providers)
}

func TestMeteringDeletedContextKeptProvider(t *testing.T) {
	s, pm := openMetered(t, MeteringConfig{BatchSize: 50, Interval: 0, TimeFill: 1})
	p1 := random.Peers(1)[0]
	mhs := random.Multihashes(3)
	// mh0 and mh1 keep ctxA. mh1 and mh2 lose ctxB.
	putValue(t, s, p1, []byte("ctxA"), []byte("metaA"), mhs[0], mhs[1])
	putValue(t, s, p1, []byte("ctxB"), []byte("metaB"), mhs[1], mhs[2])
	require.NoError(t, s.RemoveProviderContext(p1, []byte("ctxB")))

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	report := waitScanDone(t, pm, 5*time.Second)

	require.EqualValues(t, 2, report.Totals.Active.Entries)
	// Only mh2 has no remaining value record.
	require.EqualValues(t, 1, report.Totals.Deleted.Entries)
	require.Len(t, report.Providers, 1)
	require.Equal(t, p1, report.Providers[0].ProviderID)
	require.EqualValues(t, 2, report.Providers[0].Multihashes)
	require.EqualValues(t, 2, report.Providers[0].Slots)
	require.EqualValues(t, 2, report.Providers[0].DeletedContexts)
}

func TestMeteringCleanupRemovesDeadSlots(t *testing.T) {
	registerMeteringViews(t)
	s, pm := openMetered(t, MeteringConfig{BatchSize: 50, Interval: 0, TimeFill: 1, Cleanup: true})
	p1 := random.Peers(1)[0]
	mhs := random.Multihashes(2)
	putValue(t, s, p1, []byte("ctxA"), []byte("metaA"), mhs[0], mhs[1])
	putValue(t, s, p1, []byte("ctxB"), []byte("metaB"), mhs[1])
	require.NoError(t, s.RemoveProviderContext(p1, []byte("ctxB")))
	// A later advertisement of a different context must survive the cleanup.
	putValue(t, s, p1, []byte("ctxC"), []byte("metaC"), mhs[1])

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	report := waitScanDone(t, pm, 5*time.Second)

	require.EqualValues(t, 2, report.Totals.Active.Entries)
	require.EqualValues(t, 3, report.Totals.Active.Slots)
	require.Zero(t, report.Totals.Deleted.Entries)
	require.Len(t, report.Providers, 1)
	require.EqualValues(t, 2, report.Providers[0].Multihashes)
	require.EqualValues(t, 3, report.Providers[0].Slots)
	require.Zero(t, report.Providers[0].DeletedContexts)
	require.EqualValues(t, 1, meteringSum(t, "core/metering/cleanup_slots"))
	require.Zero(t, meteringSum(t, "core/metering/cleanup_multihashes"))
	require.Zero(t, meteringSum(t, "core/metering/cleanup_key_bytes"))
	require.EqualValues(t, marshalledValueKeyLength, meteringSum(t, "core/metering/cleanup_value_bytes"))
	// The last batch has committed, so no batch is running.
	require.Zero(t, meteringLastValue(t, "core/metering/batch_started_at"))

	contextIDs := func(values []indexer.Value) [][]byte {
		out := make([][]byte, len(values))
		for i, v := range values {
			out[i] = v.ContextID
		}
		return out
	}

	values, found, err := s.Get(mhs[0])
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, [][]byte{[]byte("ctxA")}, contextIDs(values))

	values, found, err = s.Get(mhs[1])
	require.NoError(t, err)
	require.True(t, found)
	require.ElementsMatch(t, [][]byte{[]byte("ctxA"), []byte("ctxC")}, contextIDs(values))
}

// A provider removes a context while a cleanup scan is part way through, and
// advertises it again after the scan has cached the old value record as gone.
// The new advertisement must survive this scan, the next one, compaction, and
// a reopen.
func TestMeteringCleanupReAddMidScan(t *testing.T) {
	for _, tc := range []struct {
		name   string
		remove func(s indexer.Interface, pid peer.ID, ctxID []byte) error
	}{
		{"RemoveProviderContext", func(s indexer.Interface, pid peer.ID, ctxID []byte) error {
			return s.RemoveProviderContext(pid, ctxID)
		}},
		{"RemoveProvider", func(s indexer.Interface, pid peer.ID, _ []byte) error {
			return s.RemoveProvider(context.Background(), pid)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registerMeteringViews(t)
			dir := t.TempDir()
			// Interval zero keeps the runner idle, so the test drives the scan
			s, err := New(dir, nil, WithMetering(MeteringConfig{BatchSize: 1, Interval: 0, TimeFill: 1, Cleanup: true}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			r := s.(*store).metering

			// The scan reads multihash keys in byte order. The first key holds
			// only another provider's value, so the first batch does not look
			// up the context under test
			all := random.Multihashes(5)
			slices.SortFunc(all, func(a, b multihash.Multihash) int { return bytes.Compare(a, b) })
			first, mhs := all[0], all[1:]

			peers := random.Peers(2)
			pid := peers[0]
			ctxID := []byte("ctx")
			putValue(t, s, peers[1], []byte("other"), []byte("other"), first)
			putValue(t, s, pid, ctxID, []byte("old"), mhs...)

			progress, err := r.createMultihashScanProgress()
			require.NoError(t, err)
			counters := &multihashScanCounters{providers: make(map[string]*indexer.ProviderStats)}
			providerIDByHash := make(map[string]peer.ID)
			cleaned := func() float64 { return meteringSum(t, "core/metering/cleanup_slots") }
			cleanedBefore := cleaned()
			runBatch := func() bool {
				done, err := r.runMultihashScanBatch(t.Context(), progress, counters, providerIDByHash)
				require.NoError(t, err)
				return done
			}

			require.False(t, runBatch())
			require.Zero(t, cleaned()-cleanedBefore)

			// The context is removed while the scan is part way through
			require.NoError(t, tc.remove(s, pid, ctxID))

			// The next batch removes one dead slot and caches its value record
			// as gone
			require.False(t, runBatch())
			require.EqualValues(t, 1, cleaned()-cleanedBefore)

			want := indexer.Value{ProviderID: pid, ContextID: ctxID, MetadataBytes: []byte("new")}
			require.NoError(t, s.Put(want, mhs...))

			for i := 0; !runBatch(); i++ {
				// Each batch reads one key, so the rest of the scan needs at most
				// one batch per key
				require.Less(t, i, len(all), "scan did not finish after %d batches", i+1)
			}

			require.NoError(t, r.finishMultihashScan(progress, counters))
			// Each multihash had one dead slot; the new slots are kept
			require.EqualValues(t, len(mhs), cleaned()-cleanedBefore)

			requireWant := func(s indexer.Interface) {
				t.Helper()
				for _, mh := range mhs {
					values, found, err := s.Get(mh)
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, []indexer.Value{want}, values)
				}
			}
			requireWant(s)

			// A second cleanup scan finds only live slots
			require.NoError(t, r.runMultihashScan(t.Context(), nil))
			report, err := s.(indexer.StatsMeter).MeteringAllStats(t.Context(), nil)
			require.NoError(t, err)
			require.EqualValues(t, len(all), report.Totals.Active.Entries)
			require.EqualValues(t, len(all), report.Totals.Active.Slots)
			require.Zero(t, report.Totals.Deleted.Entries)
			require.EqualValues(t, len(mhs), cleaned()-cleanedBefore)

			require.NoError(t, s.(*store).db.Compact(t.Context(), []byte{0}, []byte{0xff}, false))
			requireWant(s)
			require.NoError(t, s.Close())

			reopened, err := New(dir, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			requireWant(reopened)
		})
	}
}

func TestMeteringCleanupRemovesDeadProvider(t *testing.T) {
	registerMeteringViews(t)
	s, pm := openMetered(t, MeteringConfig{BatchSize: 50, Interval: 0, TimeFill: 1, Cleanup: true})
	p1 := random.Peers(1)[0]
	mhs := random.Multihashes(2)
	putValue(t, s, p1, []byte("ctx"), []byte("meta"), mhs...)
	require.NoError(t, s.RemoveProvider(context.Background(), p1))

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	report := waitScanDone(t, pm, 5*time.Second)

	require.Zero(t, report.Totals.Active.Entries)
	require.Empty(t, report.Providers)
	require.EqualValues(t, 2, meteringSum(t, "core/metering/cleanup_slots"))
	require.EqualValues(t, 2*marshalledValueKeyLength, meteringSum(t, "core/metering/cleanup_value_bytes"))
	// Removed slots count as cleaned. Each multihash counts as cleaned when the
	// merge deletes its key, and as deleted when the emptied key stays.
	require.Zero(t, report.Totals.Deleted.Slots)
	require.Zero(t, report.Totals.Deleted.ValueBytes)
	if deleteEmptyKeys {
		require.Zero(t, report.Totals.Deleted.Entries)
		require.EqualValues(t, 2, meteringSum(t, "core/metering/cleanup_multihashes"))
		require.Greater(t, meteringSum(t, "core/metering/cleanup_key_bytes"), float64(0))
	} else {
		require.EqualValues(t, 2, report.Totals.Deleted.Entries)
		require.Greater(t, report.Totals.Deleted.KeyBytes, uint64(0))
		require.Zero(t, meteringSum(t, "core/metering/cleanup_multihashes"))
		require.Zero(t, meteringSum(t, "core/metering/cleanup_key_bytes"))
	}
	for _, mh := range mhs {
		_, found, err := s.Get(mh)
		require.NoError(t, err)
		require.False(t, found)
	}

	// The merge result is an empty value. The next scan counts those keys as
	// deleted multihashes.
	first, err := pm.MeteringScanStatus(context.Background(), nil)
	require.NoError(t, err)
	waitPastScanID(t, first.ScanID)
	triggerWhenIdle(t, pm)
	deadline := time.Now().Add(5 * time.Second)
	var second *indexer.AllStatsReport
	for time.Now().Before(deadline) {
		st, err := pm.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		require.Empty(t, st.Error)
		if st.ScanID != first.ScanID && st.State == indexer.ScanStateDone {
			second, err = pm.MeteringAllStats(context.Background(), nil)
			require.NoError(t, err)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.NotNil(t, second)
	require.EqualValues(t, 2, second.Totals.Deleted.Entries)
	require.Zero(t, second.Totals.Deleted.Slots)
	require.Zero(t, second.Totals.Active.Entries)
	require.Greater(t, second.Totals.Deleted.KeyBytes, uint64(0))
	// The scan that emptied the keys already counted them as deleted
	require.Equal(t, report.Totals, second.Totals)
}

// The deleted gauges keep the latest completed scan while the next scan
// runs. The scan_deleted gauges show what the running scan has counted so far
// and read zero once it finishes.
func TestMeteringDeletedGauges(t *testing.T) {
	registerMeteringViews(t)
	// Interval zero keeps the runner idle, so the test drives the scans
	s, _ := openMetered(t, MeteringConfig{BatchSize: 1, Interval: 0, TimeFill: 1})
	r := s.(*store).metering
	pid := random.Peers(1)[0]
	putValue(t, s, pid, []byte("ctx"), []byte("meta"), random.Multihashes(2)...)
	require.NoError(t, s.RemoveProvider(context.Background(), pid))

	require.NoError(t, r.runMultihashScan(t.Context(), nil))
	require.EqualValues(t, 2, meteringLastValue(t, "core/metering/total_deleted"))
	require.Zero(t, meteringLastValue(t, "core/metering/scan_deleted"))

	progress, err := r.createMultihashScanProgress()
	require.NoError(t, err)
	counters := &multihashScanCounters{providers: make(map[string]*indexer.ProviderStats)}
	done, err := r.runMultihashScanBatch(t.Context(), progress, counters, make(map[string]peer.ID))
	require.NoError(t, err)
	require.False(t, done)
	require.EqualValues(t, 2, meteringLastValue(t, "core/metering/total_deleted"))
	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/scan_deleted"))
	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/scan_deleted_slots"))
}

// A store reopened in the middle of a scan publishes the gauges of the latest
// completed scan before it resumes the unfinished one.
func TestMeteringResumePublishesCompletedScan(t *testing.T) {
	registerMeteringViews(t)
	dir := t.TempDir()
	// Interval zero keeps the runner idle, so the test drives the first scans
	s1, err := New(dir, nil, WithMetering(MeteringConfig{BatchSize: 1, Interval: 0, TimeFill: 1}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s1.Close() })
	r := s1.(*store).metering
	pid := random.Peers(1)[0]
	putValue(t, s1, pid, []byte("ctxA"), []byte("meta"), random.Multihashes(2)...)
	require.NoError(t, r.runMultihashScan(t.Context(), nil))

	putValue(t, s1, pid, []byte("ctxB"), []byte("meta"), random.Multihashes(3)...)
	progress, err := r.createMultihashScanProgress()
	require.NoError(t, err)
	counters := &multihashScanCounters{providers: make(map[string]*indexer.ProviderStats)}
	_, err = r.runMultihashScanBatch(t.Context(), progress, counters, make(map[string]peer.ID))
	require.NoError(t, err)
	require.NoError(t, s1.Close())

	// A new process starts without gauge values
	registerMeteringViews(t)
	totalMultihashes := func() (float64, bool) {
		rows, err := view.RetrieveData("core/metering/total_multihashes")
		require.NoError(t, err)
		if len(rows) == 0 {
			return 0, false
		}
		return rows[0].Data.(*view.LastValueData).Value, true
	}
	_, ok := totalMultihashes()
	require.False(t, ok)

	// The resumed scan pauses for about a second after each batch, so it
	// cannot finish and republish the gauges before the check below
	s2, err := New(dir, nil, WithMetering(MeteringConfig{BatchSize: 1, Interval: 0, TimeFill: 0.001}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })
	require.Eventually(t, func() bool {
		v, ok := totalMultihashes()
		return ok && v == 2
	}, 500*time.Millisecond, time.Millisecond)
	st, err := s2.(indexer.StatsMeter).MeteringScanStatus(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, indexer.ScanStateInProgress, st.State)
}

// Active and undecodable entries are exported for the latest completed scan,
// and as counted so far while a scan runs.
func TestMeteringActiveAndInvalidGauges(t *testing.T) {
	registerMeteringViews(t)
	// Interval zero keeps the runner idle, so the test drives the scans
	s, _ := openMetered(t, MeteringConfig{BatchSize: 100, Interval: 0, TimeFill: 1})
	r := s.(*store).metering
	putValue(t, s, random.Peers(1)[0], []byte("ctx"), []byte("meta"), random.Multihashes(2)...)
	invalidKey := append([]byte{byte(multihashKeyPrefix)}, random.Multihashes(1)[0]...)
	invalidValue := []byte{1, 2, 3}
	require.NoError(t, s.(*store).db.Set(invalidKey, invalidValue, pebble.Sync))

	require.NoError(t, r.runMultihashScan(t.Context(), nil))
	require.EqualValues(t, 2, meteringLastValue(t, "core/metering/total_multihashes"))
	require.Positive(t, meteringLastValue(t, "core/metering/active_key_bytes"))
	require.EqualValues(t, 2*marshalledValueKeyLength, meteringLastValue(t, "core/metering/active_value_bytes"))
	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/total_invalid"))
	require.EqualValues(t, len(invalidKey)+len(invalidValue), meteringLastValue(t, "core/metering/invalid_bytes"))
	require.Zero(t, meteringLastValue(t, "core/metering/scan_active"))
	require.Zero(t, meteringLastValue(t, "core/metering/scan_invalid"))

	progress, err := r.createMultihashScanProgress()
	require.NoError(t, err)
	counters := &multihashScanCounters{providers: make(map[string]*indexer.ProviderStats)}
	_, err = r.runMultihashScanBatch(t.Context(), progress, counters, make(map[string]peer.ID))
	require.NoError(t, err)
	require.EqualValues(t, 2, meteringLastValue(t, "core/metering/scan_active"))
	require.EqualValues(t, 2, meteringLastValue(t, "core/metering/scan_active_slots"))
	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/scan_invalid"))
}

// A value whose packed keys all have an unexpected prefix decodes to no value
// keys. The scan counts it as deleted, with its key and value bytes.
func TestMeteringUnknownPrefixValueCountedDeleted(t *testing.T) {
	// Interval zero keeps the runner idle, so the test drives the scan
	s, pm := openMetered(t, MeteringConfig{BatchSize: 100, Interval: 0, TimeFill: 1})
	st := s.(*store)
	keygen := st.p.leaseBlake3Keyer()
	vk, err := keygen.valueKey(value1, false)
	require.NoError(t, err)
	unknown := bytes.Clone(vk.buf)
	unknown[0] = 0xee
	require.NoError(t, vk.Close())
	require.NoError(t, keygen.Close())
	value, closer, err := st.vcodec.marshalValueKeys([][]byte{unknown})
	require.NoError(t, err)
	key := append([]byte{byte(multihashKeyPrefix)}, random.Multihashes(1)[0]...)
	require.NoError(t, st.db.Set(key, value, pebble.Sync))
	// Set copies the value, so its pooled buffer can go back
	require.NoError(t, closer.Close())

	require.NoError(t, st.metering.runMultihashScan(t.Context(), nil))
	report, err := pm.MeteringAllStats(t.Context(), nil)
	require.NoError(t, err)
	require.Zero(t, report.Totals.Active.Entries)
	require.Zero(t, report.Totals.Invalid.Entries)
	require.Equal(t, indexer.EntryMeters{
		Entries:    1,
		KeyBytes:   uint64(len(key)),
		ValueBytes: marshalledValueKeyLength,
	}, report.Totals.Deleted)
}

// With ExportProviderMetrics, each provider of the latest completed scan has
// a multihash gauge tagged with its base58 peer ID.
func TestMeteringProviderGauge(t *testing.T) {
	registerMeteringViews(t)
	view.Unregister(metrics.MeteringProviderViews...)
	require.NoError(t, view.Register(metrics.MeteringProviderViews...))
	t.Cleanup(func() { view.Unregister(metrics.MeteringProviderViews...) })

	// Interval zero keeps the runner idle, so the test drives the scan
	s, _ := openMetered(t, MeteringConfig{BatchSize: 100, Interval: 0, TimeFill: 1, ExportProviderMetrics: true})
	peers := random.Peers(2)
	putValue(t, s, peers[0], []byte("ctx"), []byte("meta"), random.Multihashes(2)...)
	putValue(t, s, peers[1], []byte("ctx"), []byte("meta"), random.Multihashes(3)...)
	require.NoError(t, s.(*store).metering.runMultihashScan(t.Context(), nil))

	rows, err := view.RetrieveData("core/metering/provider_multihashes")
	require.NoError(t, err)
	got := make(map[string]float64, len(rows))
	for _, row := range rows {
		require.Len(t, row.Tags, 1)
		got[row.Tags[0].Value] = row.Data.(*view.LastValueData).Value
	}
	require.Equal(t, map[string]float64{peers[0].String(): 2, peers[1].String(): 3}, got)
}

func TestMeteringResume(t *testing.T) {
	dir := t.TempDir()
	cfg := MeteringConfig{BatchSize: 2, Interval: 0, TimeFill: 1}

	s1, err := New(dir, nil, WithMetering(cfg))
	require.NoError(t, err)
	p1 := random.Peers(1)[0]
	mhs := random.Multihashes(20)
	putValue(t, s1, p1, []byte("ctx"), []byte("meta"), mhs...)

	pm1 := s1.(indexer.StatsMeter)
	require.NoError(t, pm1.MeteringTriggerScan(context.Background()))
	// Wait until some progress has been made, then close mid-scan.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, err := pm1.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		if st.State == indexer.ScanStateInProgress && st.Current.Totals.Active.Entries > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	require.NoError(t, s1.Close())

	// Reopen: should resume and finish.
	s2, err := New(dir, nil, WithMetering(cfg))
	require.NoError(t, err)
	defer s2.Close()
	pm2 := s2.(indexer.StatsMeter)
	report := waitScanDone(t, pm2, 10*time.Second)
	require.EqualValues(t, 20, report.Totals.Active.Entries)
	require.EqualValues(t, 20, report.Totals.Active.Slots)
}

// cancelAfterCtx reports cancellation once Err has returned nil n times.
// context.Cause falls back to Err for this type, so a scan batch, which checks
// before each key, stops after reading n keys.
type cancelAfterCtx struct {
	context.Context
	n int
}

func (c *cancelAfterCtx) Err() error {
	if c.n == 0 {
		return context.Canceled
	}
	c.n--
	return nil
}

// A cleanup scan stops part way through a batch, after it has added
// merge-deletes and counted keys that are never committed. On reopen the scan
// resumes from the last committed cursor and removes every dead slot exactly
// once.
func TestMeteringCleanupResumeAfterPartialBatch(t *testing.T) {
	registerMeteringViews(t)
	dir := t.TempDir()
	cfg := MeteringConfig{BatchSize: 4, Interval: 0, TimeFill: 1, Cleanup: true}
	// Interval zero keeps the runner idle, so the test drives the first scan
	s1, err := New(dir, nil, WithMetering(cfg))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s1.Close() })
	r := s1.(*store).metering

	pid := random.Peers(1)[0]
	mhs := random.Multihashes(8)
	live := indexer.Value{ProviderID: pid, ContextID: []byte("ctxA"), MetadataBytes: []byte("metaA")}
	require.NoError(t, s1.Put(live, mhs...))
	putValue(t, s1, pid, []byte("ctxB"), []byte("metaB"), mhs...)
	require.NoError(t, s1.RemoveProviderContext(pid, []byte("ctxB")))

	cleaned := func() float64 { return meteringSum(t, "core/metering/cleanup_slots") }
	cleanedBefore := cleaned()

	progress, err := r.createMultihashScanProgress()
	require.NoError(t, err)
	counters := &multihashScanCounters{providers: make(map[string]*indexer.ProviderStats)}
	providerIDByHash := make(map[string]peer.ID)

	done, err := r.runMultihashScanBatch(t.Context(), progress, counters, providerIDByHash)
	require.NoError(t, err)
	require.False(t, done)
	require.EqualValues(t, cfg.BatchSize, cleaned()-cleanedBefore)

	// The batch reads two keys and stops before it commits
	ctx := &cancelAfterCtx{Context: t.Context(), n: 2}
	_, err = r.runMultihashScanBatch(ctx, progress, counters, providerIDByHash)
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, cfg.BatchSize, cleaned()-cleanedBefore)
	require.NoError(t, s1.Close())

	// The runner resumes the stored scan on open
	s2, err := New(dir, nil, WithMetering(cfg))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })
	report := waitScanDone(t, s2.(indexer.StatsMeter), 5*time.Second)

	require.EqualValues(t, len(mhs), report.Totals.Active.Entries)
	require.EqualValues(t, len(mhs), report.Totals.Active.Slots)
	require.Zero(t, report.Totals.Deleted.Entries)
	require.Len(t, report.Providers, 1)
	require.EqualValues(t, len(mhs), report.Providers[0].Multihashes)
	require.Zero(t, report.Providers[0].DeletedContexts)
	require.EqualValues(t, len(mhs), cleaned()-cleanedBefore)
	for _, mh := range mhs {
		values, found, err := s2.Get(mh)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []indexer.Value{live}, values)
	}
}

func TestMeteringTriggerWhileInProgress(t *testing.T) {
	s, pm := openMetered(t, MeteringConfig{BatchSize: 1, Interval: 0, TimeFill: 0.05})
	p1 := random.Peers(1)[0]
	putValue(t, s, p1, []byte("ctx"), []byte("meta"), random.Multihashes(50)...)

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	// Second trigger while first is running.
	deadline := time.Now().Add(2 * time.Second)
	var gotErr error
	for time.Now().Before(deadline) {
		st, err := pm.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		if st.State == indexer.ScanStateInProgress {
			gotErr = pm.MeteringTriggerScan(context.Background())
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	require.ErrorIs(t, gotErr, indexer.ErrScanInProgress)
	_ = waitScanDone(t, pm, 10*time.Second)
}

func TestMeteringCancelScan(t *testing.T) {
	s, pm := openMetered(t, MeteringConfig{BatchSize: 1, Interval: 0, TimeFill: 0.05})
	p1 := random.Peers(1)[0]
	putValue(t, s, p1, []byte("ctx"), []byte("meta"), random.Multihashes(80)...)

	require.ErrorIs(t, pm.MeteringCancelScan(context.Background(), ""), indexer.ErrScanNotInProgress)

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := pm.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		if st.State == indexer.ScanStateInProgress && st.Current.Totals.Active.Entries > 0 {
			break
		}
		require.False(t, time.Now().After(deadline), "scan did not start")
		time.Sleep(time.Millisecond)
	}

	require.NoError(t, pm.MeteringCancelScan(context.Background(), "paused for gc"))
	// A second cancel while the same scan is stopping is a no-op.
	_ = pm.MeteringCancelScan(context.Background(), "ignored")

	wantCancel := indexer.ScanCancelledError("paused for gc")
	deadline = time.Now().Add(5 * time.Second)
	var st *indexer.ScanStatus
	for {
		var err error
		st, err = pm.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		if st.State == indexer.ScanStateError {
			break
		}
		require.False(t, time.Now().After(deadline), "scan did not stop after cancel")
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, wantCancel.Error(), st.Error)
	// Status can show the error before the scan goroutine clears its cancel func.
	deadline = time.Now().Add(5 * time.Second)
	for {
		err := pm.MeteringCancelScan(context.Background(), "")
		if errors.Is(err, indexer.ErrScanNotInProgress) {
			break
		}
		require.NoError(t, err)
		require.False(t, time.Now().After(deadline), "scan did not leave the in-progress state")
		time.Sleep(time.Millisecond)
	}

	empty, err := pm.MeteringAllStats(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, empty)

	// A new cancel cause is used for the next scan.
	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	deadline = time.Now().Add(10 * time.Second)
	var report *indexer.AllStatsReport
	for time.Now().Before(deadline) {
		st, err = pm.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		if st.Error == wantCancel.Error() {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		require.Empty(t, st.Error, "scan failed: %s", st.Error)
		if st.State == indexer.ScanStateDone {
			report, err = pm.MeteringAllStats(context.Background(), nil)
			require.NoError(t, err)
			if report != nil {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.NotNil(t, report, "timeout waiting for scan after cancel")
	require.GreaterOrEqual(t, report.Totals.Active.Entries, uint64(80))
}

func TestMeteringRetentionOnlyCurrent(t *testing.T) {
	s, pm := openMetered(t, MeteringConfig{BatchSize: 100, Interval: 0, TimeFill: 1})
	store := s.(*store)
	p1 := random.Peers(1)[0]
	putValue(t, s, p1, []byte("ctx"), []byte("meta"), random.Multihashes(3)...)

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	r1 := waitScanDone(t, pm, 5*time.Second)
	cur1, err := store.metering.loadCompletedScan(store.db)
	require.NoError(t, err)
	require.NotNil(t, cur1)
	firstScanID := cur1.ScanID

	waitPastScanID(t, firstScanID)
	triggerWhenIdle(t, pm)
	deadline := time.Now().Add(5 * time.Second)
	var r2 *indexer.AllStatsReport
	for time.Now().Before(deadline) {
		st, err := pm.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		report, err := pm.MeteringAllStats(context.Background(), nil)
		require.NoError(t, err)
		if report != nil && !report.MeasuredAt.Equal(r1.MeasuredAt) && st.State == indexer.ScanStateDone {
			r2 = report
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.NotNil(t, r2, "timeout waiting for second scan")

	// Old scan stats must be gone.
	_, closer, err := store.db.Get(meteringTotalsKey(firstScanID))
	if closer != nil {
		_ = closer.Close()
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)

	cur2, err := store.metering.loadCompletedScan(store.db)
	require.NoError(t, err)
	require.NotNil(t, cur2)
	_, closer, err = store.db.Get(meteringTotalsKey(cur2.ScanID))
	require.NoError(t, err)
	_ = closer.Close()
}

func TestMeteringConcurrentPut(t *testing.T) {
	s, pm := openMetered(t, MeteringConfig{BatchSize: 10, Interval: 0, TimeFill: 1})
	p1 := random.Peers(1)[0]
	base := random.Multihashes(100)
	putValue(t, s, p1, []byte("ctx"), []byte("meta"), base...)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			putValue(t, s, p1, []byte("ctx2"), []byte("meta2"), random.Multihashes(5)...)
			time.Sleep(time.Millisecond)
		}
	}()

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	report := waitScanDone(t, pm, 10*time.Second)
	wg.Wait()

	require.GreaterOrEqual(t, report.Totals.Active.Entries, uint64(100))
}

func TestMeteringNotSupportedWithoutOption(t *testing.T) {
	s, err := New(t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	pm, ok := s.(indexer.StatsMeter)
	require.True(t, ok, "store should still implement StatsMeter")
	_, err = pm.MeteringAllStats(context.Background(), nil)
	require.ErrorIs(t, err, indexer.ErrMeteringNotSupported)
	err = pm.MeteringCancelScan(context.Background(), "")
	require.ErrorIs(t, err, indexer.ErrMeteringNotSupported)
}

func TestScanStatusCurrent(t *testing.T) {
	// A small fill pauses after the first one-key batch long enough to read that status.
	s, pm := openMetered(t, MeteringConfig{BatchSize: 1, Interval: 0, TimeFill: 0.001})
	p1 := random.Peers(1)[0]
	putValue(t, s, p1, []byte("ctx"), []byte("meta"), random.Multihashes(2)...)

	require.NoError(t, pm.MeteringTriggerScan(context.Background()))
	deadline := time.Now().Add(5 * time.Second)
	var st *indexer.ScanStatus
	for time.Now().Before(deadline) {
		got, err := pm.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		if got.State == indexer.ScanStateInProgress && got.Current.Totals.Active.Entries == 1 {
			st = got
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.NotNil(t, st, "scan did not report partial progress")
	require.EqualValues(t, 1, st.Current.Totals.Active.Entries)
	emptySel, err := pm.MeteringScanStatus(context.Background(), []peer.ID{})
	require.NoError(t, err)
	require.Empty(t, emptySel.Current.Providers)
	require.EqualValues(t, 1, emptySel.Current.Totals.Active.Entries)
	_ = waitScanDone(t, pm, 15*time.Second)
}

func TestMeteringKeysRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	p := multihashScanProgressRecord{
		ScanID:    12345,
		StartedAt: now,
		KeysRead:  99,
		BytesRead: 1000,
		Cursor:    []byte{1, 2},
		DoneAt:    now,
		Error:     "scan failed",
	}
	raw, err := encodeMultihashScanProgress(&p)
	require.NoError(t, err)
	got, err := decodeMultihashScanProgress(raw)
	require.NoError(t, err)
	require.Equal(t, p.ScanID, got.ScanID)
	require.Equal(t, p.KeysRead, got.KeysRead)
	require.Equal(t, p.BytesRead, got.BytesRead)
	require.True(t, got.DoneAt.Equal(p.DoneAt))
	require.Equal(t, p.Cursor, got.Cursor)
	require.Equal(t, p.Error, got.Error)
	require.True(t, got.StartedAt.Equal(p.StartedAt))

	open, err := encodeMultihashScanProgress(&multihashScanProgressRecord{ScanID: 1, StartedAt: now})
	require.NoError(t, err)
	require.NotContains(t, string(open), "DoneAt")

	cur := multihashScanResults{
		ScanID:      99,
		StartedAt:   now,
		CompletedAt: now.Add(time.Second),
		Totals: indexer.StoreTotals{
			Active: indexer.EntryMeters{Entries: 1, KeyBytes: 3, ValueBytes: 4, Slots: 2},
		},
	}
	rawCur, err := encodeMultihashScanResults(&cur)
	require.NoError(t, err)
	gotCur, err := decodeMultihashScanResults(rawCur)
	require.NoError(t, err)
	require.Equal(t, cur, *gotCur)

	ps := indexer.ProviderStats{
		ProviderID:  random.Peers(1)[0],
		Multihashes: 2,
		Slots:       3,
	}
	rawPS, err := encodeProviderStats(&ps)
	require.NoError(t, err)
	gotPS, err := decodeProviderStats(rawPS)
	require.NoError(t, err)
	require.Equal(t, ps, *gotPS)
}

func TestMeteringPersistScanError(t *testing.T) {
	s, pm := openMetered(t, MeteringConfig{BatchSize: 100, Interval: 0, TimeFill: 1})
	store := s.(*store)
	// Stop the runner so startup cannot resume the injected progress.
	store.metering.stop()

	progress := &multihashScanProgressRecord{
		ScanID:    42,
		StartedAt: time.Now().UTC(),
		KeysRead:  7,
	}
	raw, err := encodeMultihashScanProgress(progress)
	require.NoError(t, err)
	require.NoError(t, store.db.Set(meteringProgressKey, raw, pebble.Sync))

	store.metering.persistScanError(progress, context.Canceled)
	st, err := pm.MeteringScanStatus(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, st.Error)
	// Unfinished progress without Error is reported as in progress.
	require.Equal(t, indexer.ScanStateInProgress, st.State)

	store.metering.persistScanError(progress, indexer.ErrScanCancelled)
	st, err = pm.MeteringScanStatus(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, indexer.ErrScanCancelled.Error(), st.Error)
	require.Equal(t, indexer.ScanStateError, st.State)

	progress.Error = ""
	raw, err = encodeMultihashScanProgress(progress)
	require.NoError(t, err)
	require.NoError(t, store.db.Set(meteringProgressKey, raw, pebble.Sync))

	store.metering.persistScanError(progress, errors.New("boom"))
	st, err = pm.MeteringScanStatus(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, "boom", st.Error)
	require.EqualValues(t, 42, st.ScanID)
	require.Equal(t, indexer.ScanStateError, st.State)

	store.metering.persistScanError(progress, errors.New("second"))
	st, err = pm.MeteringScanStatus(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, "boom", st.Error)
}

func TestMeteringFailedScanNotResumed(t *testing.T) {
	dir := t.TempDir()
	cfg := MeteringConfig{BatchSize: 100, Interval: 0, TimeFill: 1}

	s1, err := New(dir, nil, WithMetering(cfg))
	require.NoError(t, err)
	p1 := random.Peers(1)[0]
	putValue(t, s1, p1, []byte("ctx"), []byte("meta"), random.Multihashes(5)...)

	store1 := s1.(*store)
	progress := &multihashScanProgressRecord{
		ScanID:    99,
		StartedAt: time.Now().UTC(),
		KeysRead:  1,
		Cursor:    []byte{byte(multihashKeyPrefix)},
		Error:     "injected failure",
	}
	raw, err := encodeMultihashScanProgress(progress)
	require.NoError(t, err)
	require.NoError(t, store1.db.Set(meteringProgressKey, raw, pebble.Sync))
	require.NoError(t, s1.Close())

	s2, err := New(dir, nil, WithMetering(cfg))
	require.NoError(t, err)
	defer s2.Close()
	pm2 := s2.(indexer.StatsMeter)

	// Failed progress must not resume on startup.
	time.Sleep(50 * time.Millisecond)
	st, err := pm2.MeteringScanStatus(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, "injected failure", st.Error)
	require.Equal(t, indexer.ScanStateError, st.State)
	report, err := pm2.MeteringAllStats(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, report)

	// A manual trigger starts a fresh scan that completes.
	require.NoError(t, pm2.MeteringTriggerScan(context.Background()))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, err = pm2.MeteringScanStatus(context.Background(), nil)
		require.NoError(t, err)
		report, err = pm2.MeteringAllStats(context.Background(), nil)
		require.NoError(t, err)
		if report != nil && st.State == indexer.ScanStateDone {
			require.Empty(t, st.Error)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.NotNil(t, report)
	require.EqualValues(t, 5, report.Totals.Active.Entries)
}

func TestSha256ScanEstimate(t *testing.T) {
	key := make([]byte, 1+2+32)
	key[0] = byte(multihashKeyPrefix)
	key[1] = byte(multihash.SHA2_256)
	key[2] = 32
	key[3] = 0x80
	frac := sha256MultihashKeyFraction(key)
	require.InDelta(t, 0.5, frac, 0.01)
	require.EqualValues(t, 0, sha256MultihashKeyFraction([]byte{byte(multihashKeyPrefix)}))
	after := []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256), 33}
	require.EqualValues(t, 1, sha256MultihashKeyFraction(after))
	now := time.Now()
	percent, finish := scanEstimate(now.Add(-time.Hour), key, now)
	require.InDelta(t, 50, percent, 1)
	require.NotNil(t, finish)
	require.False(t, finish.Before(now.Add(50*time.Minute)))
	require.False(t, finish.After(now.Add(70*time.Minute)))
	percent, finish = scanEstimate(now, []byte{0x01, 0x00}, now)
	require.EqualValues(t, 0, percent)
	require.Nil(t, finish)
}
