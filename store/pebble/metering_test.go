package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/multiformats/go-varint"
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

// openStoppedMetered opens a metered store in dir and stops its runner, so the
// test drives every scan. A running runner resumes on startup any scan it
// finds unfinished, which races with a scan the test creates.
func openStoppedMetered(t *testing.T, dir string, cfg MeteringConfig) *store {
	t.Helper()
	// With an interval the runner could start a scheduled scan before stop,
	// and stopping it part way leaves an unfinished scan behind
	cfg.Interval = 0
	s, err := New(dir, nil, WithMetering(cfg))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	st := s.(*store)
	st.metering.stop()
	return st
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
			s := openStoppedMetered(t, dir, MeteringConfig{BatchSize: 1, TimeFill: 1, Cleanup: true})
			r := s.metering

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

			progress, err := r.createMultihashScanProgress(t.Context())
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
			report, err := s.MeteringAllStats(t.Context(), nil)
			require.NoError(t, err)
			require.EqualValues(t, len(all), report.Totals.Active.Entries)
			require.EqualValues(t, len(all), report.Totals.Active.Slots)
			require.Zero(t, report.Totals.Deleted.Entries)
			require.EqualValues(t, len(mhs), cleaned()-cleanedBefore)

			require.NoError(t, s.db.Compact(t.Context(), []byte{0}, []byte{0xff}, false))
			requireWant(s)
			require.NoError(t, s.Close())

			reopened, err := New(dir, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			requireWant(reopened)
		})
	}
}

// A cleanup batch reads a slot, finds its value record gone, and queues a
// merge-delete. The provider advertises the same context again before that
// batch commits. The merge-delete is newer than the re-add, so it must name
// only the removed value key and leave the new slot in place.
func TestMeteringCleanupPutBeforeBatchCommit(t *testing.T) {
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
			s := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 1, TimeFill: 1, Cleanup: true})
			r := s.metering

			pid := random.Peers(1)[0]
			ctxID := []byte("ctx")
			mh := random.Multihashes(1)[0]
			putValue(t, s, pid, ctxID, []byte("old"), mh)

			keygen := s.p.leaseBlake3Keyer()
			mhk, err := keygen.multihashKey(mh)
			require.NoError(t, err)
			key := slices.Clone(mhk.buf)
			require.NoError(t, mhk.Close())
			require.NoError(t, keygen.Close())

			// The scan iterator reads the slot of the old advertisement
			raw, closer, err := s.db.Get(key)
			require.NoError(t, err)
			value := slices.Clone(raw)
			require.NoError(t, closer.Close())

			require.NoError(t, tc.remove(s, pid, ctxID))

			// The scan looks up the value record, finds it gone, and queues
			// the merge-delete in its uncommitted batch
			b := s.db.NewBatch()
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			counters := &multihashScanCounters{providers: make(map[string]*indexer.ProviderStats)}
			var cleaned cleanupTally
			require.NoError(t, r.countMultihashScanRecord(
				counters, key, value,
				make(map[string]struct{}),
				make(map[string]peer.ID),
				b, &cleaned,
			))
			require.EqualValues(t, 1, cleaned.slots)

			// The provider advertises the context again, then the scan
			// batch commits
			want := indexer.Value{ProviderID: pid, ContextID: ctxID, MetadataBytes: []byte("new")}
			require.NoError(t, s.Put(want, mh))
			require.NoError(t, b.Commit(pebble.Sync))

			requireWant := func() {
				t.Helper()
				values, found, err := s.Get(mh)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, []indexer.Value{want}, values)
			}
			requireWant()
			require.NoError(t, s.db.Compact(t.Context(), []byte{0}, []byte{0xff}, false))
			requireWant()
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
	s := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 1, TimeFill: 1})
	r := s.metering
	pid := random.Peers(1)[0]
	putValue(t, s, pid, []byte("ctx"), []byte("meta"), random.Multihashes(2)...)
	require.NoError(t, s.RemoveProvider(context.Background(), pid))

	require.NoError(t, r.runMultihashScan(t.Context(), nil))
	require.EqualValues(t, 2, meteringLastValue(t, "core/metering/total_deleted"))
	require.Zero(t, meteringLastValue(t, "core/metering/scan_deleted"))

	progress, err := r.createMultihashScanProgress(t.Context())
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
	s1 := openStoppedMetered(t, dir, MeteringConfig{BatchSize: 1, TimeFill: 1})
	r := s1.metering
	pid := random.Peers(1)[0]
	putValue(t, s1, pid, []byte("ctxA"), []byte("meta"), random.Multihashes(2)...)
	require.NoError(t, r.runMultihashScan(t.Context(), nil))

	putValue(t, s1, pid, []byte("ctxB"), []byte("meta"), random.Multihashes(3)...)
	progress, err := r.createMultihashScanProgress(t.Context())
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
	s := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
	r := s.metering
	putValue(t, s, random.Peers(1)[0], []byte("ctx"), []byte("meta"), random.Multihashes(2)...)
	invalidKey := append([]byte{byte(multihashKeyPrefix)}, random.Multihashes(1)[0]...)
	invalidValue := []byte{1, 2, 3}
	require.NoError(t, s.db.Set(invalidKey, invalidValue, pebble.Sync))

	require.NoError(t, r.runMultihashScan(t.Context(), nil))
	require.EqualValues(t, 2, meteringLastValue(t, "core/metering/total_multihashes"))
	require.Positive(t, meteringLastValue(t, "core/metering/active_key_bytes"))
	require.EqualValues(t, 2*marshalledValueKeyLength, meteringLastValue(t, "core/metering/active_value_bytes"))
	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/total_invalid"))
	require.EqualValues(t, len(invalidKey)+len(invalidValue), meteringLastValue(t, "core/metering/invalid_bytes"))
	require.Zero(t, meteringLastValue(t, "core/metering/scan_active"))
	require.Zero(t, meteringLastValue(t, "core/metering/scan_invalid"))

	progress, err := r.createMultihashScanProgress(t.Context())
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
	st := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
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
	report, err := st.MeteringAllStats(t.Context(), nil)
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

	s := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1, ExportProviderMetrics: true})
	peers := random.Peers(2)
	putValue(t, s, peers[0], []byte("ctx"), []byte("meta"), random.Multihashes(2)...)
	putValue(t, s, peers[1], []byte("ctx"), []byte("meta"), random.Multihashes(3)...)
	require.NoError(t, s.metering.runMultihashScan(t.Context(), nil))

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
	s1 := openStoppedMetered(t, dir, cfg)
	r := s1.metering

	pid := random.Peers(1)[0]
	mhs := random.Multihashes(8)
	live := indexer.Value{ProviderID: pid, ContextID: []byte("ctxA"), MetadataBytes: []byte("metaA")}
	require.NoError(t, s1.Put(live, mhs...))
	putValue(t, s1, pid, []byte("ctxB"), []byte("metaB"), mhs...)
	require.NoError(t, s1.RemoveProviderContext(pid, []byte("ctxB")))

	cleaned := func() float64 { return meteringSum(t, "core/metering/cleanup_slots") }
	cleanedBefore := cleaned()

	progress, err := r.createMultihashScanProgress(t.Context())
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
		Ranges: []multihashScanRange{{
			Start:   []byte{1, 0x12, 32},
			End:     []byte{1, 0x12, 33},
			Weight:  0.75,
			Uniform: true,
		}},
		DoneAt: now,
		Error:  "scan failed",
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
	require.Equal(t, p.Ranges, got.Ranges)
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

func TestScanProgressFraction(t *testing.T) {
	shaHeader := []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256), 32}
	blakeHeader := []byte{
		byte(multihashKeyPrefix), 0xa0, 0xe4, 0x02, 32, // blake2b-256
	}
	ranges := []multihashScanRange{
		{Start: shaHeader, End: incrementBytes(shaHeader), Weight: 400, Uniform: true},
		{Start: blakeHeader, End: incrementBytes(blakeHeader), Weight: 600, Uniform: true},
	}
	midBlake := append(slices.Clone(blakeHeader), 0x80, 0, 0, 0)
	progress := &multihashScanProgressRecord{
		StartedAt: time.Now().Add(-time.Hour),
		Ranges:    ranges,
		Cursor:    midBlake,
	}
	require.InDelta(t, 0.4+0.6*0.5, scanProgressFraction(progress), 0.01)

	percent, finish := scanEstimate(progress, time.Now())
	require.InDelta(t, 70, percent, 1)
	require.NotNil(t, finish)

	progress.Cursor = nil
	require.EqualValues(t, 0, scanProgressFraction(progress))
	percent, finish = scanEstimate(progress, time.Now())
	require.EqualValues(t, 0, percent)
	require.Nil(t, finish)

	progress.Cursor = incrementBytes(blakeHeader)
	require.EqualValues(t, 1, scanProgressFraction(progress))
}

func TestSampleMultihashRangesWeights(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
	p1 := random.Peers(1)[0]

	// 1024 sha2-256 keys whose last digest prefix is ~0.5 → weight ≈ 2048.
	shaMHs := make([]multihash.Multihash, rangeSampleSize)
	for i := range shaMHs {
		digest := make([]byte, 32)
		binary.BigEndian.PutUint32(digest, uint32(float64(i)/float64(rangeSampleSize-1)*0x80000000))
		mh, err := multihash.Encode(digest, multihash.SHA2_256)
		require.NoError(t, err)
		shaMHs[i] = mh
	}
	putValue(t, store, p1, []byte("sha"), []byte("meta"), shaMHs...)

	// Few blake2b-256 keys: prefix ends before the sample size → weight = count.
	blakeMHs := make([]multihash.Multihash, 100)
	for i := range blakeMHs {
		digest := make([]byte, 32)
		binary.BigEndian.PutUint32(digest, uint32(i)<<24)
		mh, err := multihash.Encode(digest, multihash.BLAKE2B_MIN+31) // blake2b-256
		require.NoError(t, err)
		blakeMHs[i] = mh
	}
	putValue(t, store, p1, []byte("blake"), []byte("meta"), blakeMHs...)

	ranges, err := store.metering.sampleMultihashRanges(t.Context())
	require.NoError(t, err)
	require.Len(t, ranges, 2)

	// sha2-256 sorts before blake2b-256.
	require.InEpsilon(t, 2048, ranges[0].Weight, 0.01)
	require.EqualValues(t, 100, ranges[1].Weight)

	mid := append(slices.Clone(ranges[1].Start), 0x80, 0, 0, 0)
	progress := &multihashScanProgressRecord{Ranges: ranges, Cursor: mid}
	require.InDelta(t, (2048+100*0.5)/(2048+100.0), scanProgressFraction(progress), 0.01)
}

func TestSampleMultihashRangesShortPrefix(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
	p1 := random.Peers(1)[0]
	mhs := make([]multihash.Multihash, 10)
	for i := range mhs {
		digest := make([]byte, 32)
		digest[0] = byte(i)
		mh, err := multihash.Encode(digest, multihash.SHA2_256)
		require.NoError(t, err)
		mhs[i] = mh
	}
	putValue(t, store, p1, []byte("ctx"), []byte("meta"), mhs...)

	ranges, err := store.metering.sampleMultihashRanges(t.Context())
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	require.EqualValues(t, 10, ranges[0].Weight)
}

func TestSampleMultihashRangesShortDigestAndIdentity(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
	p1 := random.Peers(1)[0]

	// Identity is not uniform: its weight is capped at the sample size however
	// many keys it has.
	idMHs := make([]multihash.Multihash, 2*rangeSampleSize)
	for i := range idMHs {
		digest := make([]byte, 8)
		binary.BigEndian.PutUint64(digest, uint64(i))
		mh, err := multihash.Encode(digest, multihash.IDENTITY)
		require.NoError(t, err)
		idMHs[i] = mh
	}
	putValue(t, store, p1, []byte("id"), []byte("meta"), idMHs...)

	// sha2-256 with a 16-byte digest is still uniform. The last sampled key
	// sits at 1/16 of the digest space, so the weight is 1024*16.
	shaMHs := make([]multihash.Multihash, rangeSampleSize)
	for i := range shaMHs {
		digest := make([]byte, 16)
		binary.BigEndian.PutUint32(digest, uint32(float64(i)/float64(rangeSampleSize-1)*0x10000000))
		mh, err := multihash.Encode(digest, multihash.SHA2_256)
		require.NoError(t, err)
		shaMHs[i] = mh
	}
	putValue(t, store, p1, []byte("sha"), []byte("meta"), shaMHs...)

	ranges, err := store.metering.sampleMultihashRanges(t.Context())
	require.NoError(t, err)
	require.Len(t, ranges, 2)

	// Identity (code 0) sorts before sha2-256.
	require.EqualValues(t, rangeSampleSize, ranges[0].Weight)
	require.InEpsilon(t, rangeSampleSize*16, ranges[1].Weight, 0.01)

	// Inside the identity range the cursor reports no progress until it leaves.
	progress := &multihashScanProgressRecord{Ranges: ranges, Cursor: append([]byte{byte(multihashKeyPrefix)}, idMHs[len(idMHs)-1]...)}
	require.EqualValues(t, 0, scanProgressFraction(progress))
}

func TestDigestPrefixFractionShortDigest(t *testing.T) {
	require.InDelta(t, 0.5, digestPrefixFraction([]byte{0x80}), 1e-9)
	require.InDelta(t, 0.75, digestPrefixFraction([]byte{0xc0, 0x00}), 1e-9)
	require.EqualValues(t, 0, digestPrefixFraction(nil))

	header := []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256), 2}
	rng := multihashScanRange{Start: header, End: incrementBytes(header), Uniform: true}
	require.InDelta(t, 0.5, rangeContainsFraction(append(slices.Clone(header), 0x80), rng), 1e-9)
	rng.Uniform = false
	require.EqualValues(t, 0, rangeContainsFraction(append(slices.Clone(header), 0x80), rng))
}

func TestRangesSampledOnlyForNewScan(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
	p1 := random.Peers(1)[0]
	putValue(t, store, p1, []byte("ctx"), []byte("meta"), random.Multihashes(5)...)

	progress, err := store.metering.createMultihashScanProgress(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, progress.Ranges)

	loaded, err := store.metering.loadMultihashScanProgress(store.db)
	require.NoError(t, err)
	require.Equal(t, progress.Ranges, loaded.Ranges)

	require.NoError(t, store.metering.runMultihashScan(t.Context(), loaded))
	done, err := store.metering.loadMultihashScanProgress(store.db)
	require.NoError(t, err)
	require.False(t, done.DoneAt.IsZero())
	require.Equal(t, progress.Ranges, done.Ranges)

	// A resumed record without ranges is not probed: its estimate stays 0.
	progress, err = store.metering.createMultihashScanProgress(t.Context())
	require.NoError(t, err)
	progress.Ranges = nil
	require.NoError(t, store.metering.runMultihashScan(t.Context(), progress))
	done, err = store.metering.loadMultihashScanProgress(store.db)
	require.NoError(t, err)
	require.False(t, done.DoneAt.IsZero())
	require.Empty(t, done.Ranges)
}

func TestSampleMultihashRangesTail(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
	p1 := random.Peers(1)[0]

	// More distinct headers than the cap. Unknown codes are not uniform, so
	// each recorded range weighs its one key.
	mhs := make([]multihash.Multihash, rangeSpanCap+10)
	for i := range mhs {
		mh, err := multihash.Encode([]byte{1}, uint64(0x300000+i))
		require.NoError(t, err)
		mhs[i] = mh
	}
	putValue(t, store, p1, []byte("ctx"), []byte("meta"), mhs...)

	ranges, err := store.metering.sampleMultihashRanges(t.Context())
	require.NoError(t, err)
	require.Len(t, ranges, rangeSpanCap)
	for _, rng := range ranges[:rangeSpanCap-1] {
		require.EqualValues(t, 1, rng.Weight)
		require.False(t, rng.Tail)
	}
	tail := ranges[rangeSpanCap-1]
	require.EqualValues(t, rangeTailWeight, tail.Weight)
	require.Equal(t, multihashScanUpper, tail.End)
	require.True(t, tail.Tail)
}

func TestSampleMultihashRangesSampleSizeEdges(t *testing.T) {
	// Key i sits at digest position i/(2*rangeSampleSize), so a full sample
	// ends on key rangeSampleSize-1 at a known position.
	const step = 1 << 32 / (2 * rangeSampleSize)
	fullWeight := float64(rangeSampleSize) / (float64(rangeSampleSize-1) / (2 * rangeSampleSize))

	for _, tc := range []struct {
		name   string
		count  int
		weight float64
	}{
		{"single", 1, 1},
		{"one less", rangeSampleSize - 1, rangeSampleSize - 1},
		{"exact", rangeSampleSize, fullWeight},
		{"one more", rangeSampleSize + 1, fullWeight},
	} {
		for _, followed := range []bool{false, true} {
			name := tc.name
			if followed {
				name += " followed by another header"
			}
			t.Run(name, func(t *testing.T) {
				store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
				p1 := random.Peers(1)[0]

				shaMHs := make([]multihash.Multihash, tc.count)
				for i := range shaMHs {
					digest := make([]byte, 32)
					binary.BigEndian.PutUint32(digest, uint32(i*step))
					mh, err := multihash.Encode(digest, multihash.SHA2_256)
					require.NoError(t, err)
					shaMHs[i] = mh
				}
				putValue(t, store, p1, []byte("sha"), []byte("meta"), shaMHs...)

				// A following header checks that sampling does not run into it.
				// Without one, the sample ends at the end of the key space.
				if followed {
					blakeMHs := make([]multihash.Multihash, 3)
					for i := range blakeMHs {
						digest := make([]byte, 32)
						digest[0] = byte(i)
						mh, err := multihash.Encode(digest, multihash.BLAKE2B_MIN+31)
						require.NoError(t, err)
						blakeMHs[i] = mh
					}
					putValue(t, store, p1, []byte("blake"), []byte("meta"), blakeMHs...)
				}

				ranges, err := store.metering.sampleMultihashRanges(t.Context())
				require.NoError(t, err)
				if followed {
					require.Len(t, ranges, 2)
				} else {
					require.Len(t, ranges, 1)
				}

				shaHeader := []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256), 32}
				require.Equal(t, shaHeader, ranges[0].Start)
				require.Equal(t, incrementBytes(shaHeader), ranges[0].End)
				require.InDelta(t, tc.weight, ranges[0].Weight, 1e-9)
				require.True(t, ranges[0].Uniform)

				if followed {
					blakeHeader := []byte{byte(multihashKeyPrefix), 0xa0, 0xe4, 0x02, 32}
					require.Equal(t, blakeHeader, ranges[1].Start)
					require.EqualValues(t, 3, ranges[1].Weight)
				}
			})
		}
	}
}

// putShaAtSteps stores n sha2-256 multihashes, key i at digest position
// i/(2*rangeSampleSize). A full sample then weighs sampleWeightAtSteps.
func putShaAtSteps(t *testing.T, s *store, n int) {
	const step = 1 << 32 / (2 * rangeSampleSize)
	mhs := make([]multihash.Multihash, n)
	for i := range mhs {
		digest := make([]byte, 32)
		binary.BigEndian.PutUint32(digest, uint32(i*step))
		mh, err := multihash.Encode(digest, multihash.SHA2_256)
		require.NoError(t, err)
		mhs[i] = mh
	}
	putValue(t, s, random.Peers(1)[0], []byte("sha"), []byte("meta"), mhs...)
}

const sampleWeightAtSteps = float64(rangeSampleSize) / (float64(rangeSampleSize-1) / (2 * rangeSampleSize))

func TestSampleMultihashRangesUndecodableKey(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})

	// A key holding only the sha2-256 code byte does not decode, and every
	// sha2-256 key extends it.
	bad := []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256)}
	require.NoError(t, store.db.Set(bad, []byte{0}, pebble.Sync))
	putShaAtSteps(t, store, 2000)

	ranges, err := store.metering.sampleMultihashRanges(t.Context())
	require.NoError(t, err)
	require.Len(t, ranges, 2)

	require.Equal(t, bad, ranges[0].Start)
	require.Equal(t, append(bad, 0), ranges[0].End)
	require.EqualValues(t, 1, ranges[0].Weight)
	require.False(t, ranges[0].Uniform)

	shaHeader := []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256), 32}
	require.Equal(t, shaHeader, ranges[1].Start)
	require.Equal(t, incrementBytes(shaHeader), ranges[1].End)
	require.InDelta(t, sampleWeightAtSteps, ranges[1].Weight, 1e-9)
	require.True(t, ranges[1].Uniform)
}

func TestSampleMultihashRangesUniformTail(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})

	// sha1 headers with lengths 1 to rangeSpanCap-1 sort before sha2-256 and
	// fill every slot but the last: sha1 is uniform, so each length is its
	// own range. The header decodes without a digest.
	for n := 1; n < rangeSpanCap; n++ {
		key := append([]byte{byte(multihashKeyPrefix), byte(multihash.SHA1)}, varint.ToUvarint(uint64(n))...)
		require.NoError(t, store.db.Set(key, []byte{0}, nil))
	}
	putShaAtSteps(t, store, 2000)
	blake := []byte{byte(multihashKeyPrefix), 0xa0, 0xe4, 0x02, 32, 0x10}
	require.NoError(t, store.db.Set(blake, []byte{0}, pebble.Sync))

	ranges, err := store.metering.sampleMultihashRanges(t.Context())
	require.NoError(t, err)
	require.Len(t, ranges, rangeSpanCap)

	// The last range starts on the sha2-256 header but also holds blake2b.
	tail := ranges[rangeSpanCap-1]
	shaHeader := []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256), 32}
	require.Equal(t, shaHeader, tail.Start)
	require.Equal(t, multihashScanUpper, tail.End)
	require.False(t, tail.Uniform)
	require.True(t, tail.Tail)

	// Moving from late in sha2-256 to blake2b inside the tail never lowers
	// the estimate.
	progress := &multihashScanProgressRecord{Ranges: ranges}
	progress.Cursor = append(slices.Clone(shaHeader), 0xf0, 0, 0, 0)
	inSha := scanProgressFraction(progress)
	progress.Cursor = blake
	require.GreaterOrEqual(t, scanProgressFraction(progress), inSha)
}

func TestSampleMultihashRangesCancelled(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
	putShaAtSteps(t, store, 10)

	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(indexer.ErrScanCancelled)
	_, err := store.metering.sampleMultihashRanges(ctx)
	require.ErrorIs(t, err, indexer.ErrScanCancelled)

	// A scan cancelled while sampling writes no progress record.
	_, err = store.metering.createMultihashScanProgress(ctx)
	require.ErrorIs(t, err, indexer.ErrScanCancelled)
	progress, err := store.metering.loadMultihashScanProgress(store.db)
	require.NoError(t, err)
	require.Nil(t, progress)
}

func TestSampleMultihashRangesNonUniformCodeOneRange(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})

	// Identity keys with 300 digest lengths, plus one with no length at all,
	// share one range that counts every key.
	identity := []byte{byte(multihashKeyPrefix), byte(multihash.IDENTITY)}
	require.NoError(t, store.db.Set(identity, []byte{0}, nil))
	for n := 1; n <= 300; n++ {
		key := slices.Concat(identity, varint.ToUvarint(uint64(n)), make([]byte, n))
		require.NoError(t, store.db.Set(key, []byte{0}, nil))
	}
	putShaAtSteps(t, store, 2000)

	// An unknown code with two lengths is one range as well.
	unknown := append([]byte{byte(multihashKeyPrefix)}, varint.ToUvarint(0x300000)...)
	require.NoError(t, store.db.Set(slices.Concat(unknown, []byte{1, 0xaa}), []byte{0}, nil))
	require.NoError(t, store.db.Set(slices.Concat(unknown, []byte{2, 0xaa, 0xbb}), []byte{0}, pebble.Sync))

	ranges, err := store.metering.sampleMultihashRanges(t.Context())
	require.NoError(t, err)
	require.Len(t, ranges, 3)

	require.Equal(t, identity, ranges[0].Start)
	require.Equal(t, incrementBytes(identity), ranges[0].End)
	require.EqualValues(t, 301, ranges[0].Weight)
	require.False(t, ranges[0].Uniform)

	shaHeader := []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256), 32}
	require.Equal(t, shaHeader, ranges[1].Start)
	require.InDelta(t, sampleWeightAtSteps, ranges[1].Weight, 1e-9)
	require.True(t, ranges[1].Uniform)

	require.Equal(t, unknown, ranges[2].Start)
	require.EqualValues(t, 2, ranges[2].Weight)
	require.False(t, ranges[2].Uniform)
}

func TestSampleMultihashRangesShortKeys(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})

	// The prefix alone holds an empty multihash, and 0x80 starts a code
	// varint that never ends. Neither decodes.
	prefixOnly := []byte{byte(multihashKeyPrefix)}
	truncated := []byte{byte(multihashKeyPrefix), 0x80}
	require.NoError(t, store.db.Set(prefixOnly, []byte{0}, nil))
	require.NoError(t, store.db.Set(truncated, []byte{0}, nil))
	putShaAtSteps(t, store, 2000)

	ranges, err := store.metering.sampleMultihashRanges(t.Context())
	require.NoError(t, err)
	require.Len(t, ranges, 3)

	require.Equal(t, multihashScanRange{Start: prefixOnly, End: append(prefixOnly, 0), Weight: 1}, ranges[0])

	shaHeader := []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256), 32}
	require.Equal(t, shaHeader, ranges[1].Start)
	require.InDelta(t, sampleWeightAtSteps, ranges[1].Weight, 1e-9)
	require.True(t, ranges[1].Uniform)

	require.Equal(t, multihashScanRange{Start: truncated, End: append(truncated, 0), Weight: 1}, ranges[2])

	// The short keys also work as the cursor.
	progress := &multihashScanProgressRecord{Ranges: ranges, Cursor: prefixOnly}
	require.EqualValues(t, 0, scanProgressFraction(progress))
	progress.Cursor = truncated
	require.InDelta(t, (1+sampleWeightAtSteps)/(2+sampleWeightAtSteps), scanProgressFraction(progress), 1e-9)
}

func TestSummarizeRanges(t *testing.T) {
	ranges := []multihashScanRange{
		{Start: []byte{1, 0}, End: []byte{1, 1}, Weight: 300},
		{Start: []byte{1, 0x12, 32}, End: []byte{1, 0x12, 33}, Weight: 2e9, Uniform: true},
		{Start: []byte{1, 0x32}, End: []byte{1, 0x33}, Weight: 1024},
		{Start: []byte{1, 0xa0, 0xe4, 2, 32}, End: []byte{1, 0xa0, 0xe4, 2, 33}, Weight: 1e8, Uniform: true},
	}
	require.Equal(t, rangeSummary{
		uniformRanges:    2,
		nonUniformRanges: 2,
		uniformWeight:    2e9 + 1e8,
		nonUniformWeight: 300 + 1024,
	}, summarizeRanges(ranges))

	// The capped last range only sets capped.
	ranges = append(ranges, multihashScanRange{Start: []byte{1, 0xb0}, End: multihashScanUpper, Weight: rangeTailWeight, Tail: true})
	s := summarizeRanges(ranges)
	require.True(t, s.capped)
	require.EqualValues(t, 2, s.nonUniformRanges)
	require.EqualValues(t, 300+1024, s.nonUniformWeight)
}

func TestScanRangeGauges(t *testing.T) {
	registerMeteringViews(t)
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})

	// One uniform range, one non-uniform range, and enough unknown codes to
	// reach the cap.
	putShaAtSteps(t, store, 2000)
	require.NoError(t, store.db.Set([]byte{byte(multihashKeyPrefix), byte(multihash.IDENTITY), 1, 0}, []byte{0}, nil))
	for i := range rangeSpanCap {
		key := append([]byte{byte(multihashKeyPrefix)}, varint.ToUvarint(uint64(0x300000+i))...)
		require.NoError(t, store.db.Set(append(key, 1, 0), []byte{0}, nil))
	}

	ranges, err := store.metering.sampleMultihashRanges(t.Context())
	require.NoError(t, err)
	recordScanRanges(ranges)

	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/scan_uniform_ranges"))
	require.InDelta(t, sampleWeightAtSteps, meteringLastValue(t, "core/metering/scan_uniform_weight"), 1e-6)
	// Identity plus the unknown codes that fit before the last slot.
	require.EqualValues(t, rangeSpanCap-2, meteringLastValue(t, "core/metering/scan_non_uniform_ranges"))
	require.EqualValues(t, rangeSpanCap-2, meteringLastValue(t, "core/metering/scan_non_uniform_weight"))
	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/scan_ranges_capped"))

	clearInProgressScanMetrics()
	for _, name := range []string{
		"core/metering/scan_uniform_ranges",
		"core/metering/scan_non_uniform_ranges",
		"core/metering/scan_uniform_weight",
		"core/metering/scan_non_uniform_weight",
		"core/metering/scan_ranges_capped",
	} {
		require.Zero(t, meteringLastValue(t, name), name)
	}
}

func TestScanRangeGaugesRestoredOnResume(t *testing.T) {
	registerMeteringViews(t)
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
	putShaAtSteps(t, store, 2000)
	require.NoError(t, store.db.Set([]byte{byte(multihashKeyPrefix), byte(multihash.IDENTITY), 1, 0}, []byte{0}, pebble.Sync))

	_, err := store.metering.createMultihashScanProgress(t.Context())
	require.NoError(t, err)
	progress, err := store.metering.loadMultihashScanProgress(store.db)
	require.NoError(t, err)
	clearInProgressScanMetrics()

	// The resumed scan publishes the stored ranges, then stops at its first
	// context check. Cancellation does not clear the in-progress gauges.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, store.metering.runMultihashScan(ctx, progress), context.Canceled)

	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/scan_uniform_ranges"))
	require.InDelta(t, sampleWeightAtSteps, meteringLastValue(t, "core/metering/scan_uniform_weight"), 1e-6)
	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/scan_non_uniform_ranges"))
	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/scan_non_uniform_weight"))
	require.Zero(t, meteringLastValue(t, "core/metering/scan_ranges_capped"))
}

func TestScanRangeGaugesRecordedEveryBatch(t *testing.T) {
	store := openStoppedMetered(t, t.TempDir(), MeteringConfig{BatchSize: 100, TimeFill: 1})
	putShaAtSteps(t, store, 2000)
	progress, err := store.metering.createMultihashScanProgress(t.Context())
	require.NoError(t, err)

	// Views registered after the scan started still get the range gauges
	// from the next batch.
	registerMeteringViews(t)
	counters := &multihashScanCounters{providers: make(map[string]*indexer.ProviderStats)}
	done, err := store.metering.runMultihashScanBatch(t.Context(), progress, counters, make(map[string]peer.ID))
	require.NoError(t, err)
	require.False(t, done)

	require.EqualValues(t, 1, meteringLastValue(t, "core/metering/scan_uniform_ranges"))
	require.InDelta(t, sampleWeightAtSteps, meteringLastValue(t, "core/metering/scan_uniform_weight"), 1e-6)
	require.Zero(t, meteringLastValue(t, "core/metering/scan_ranges_capped"))
}
