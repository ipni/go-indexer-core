package pebble

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestBatchSleep(t *testing.T) {
	require.Equal(t, 3*time.Second, batchSleep(time.Second, 0.25))
	require.Equal(t, time.Second, batchSleep(time.Second, 0.5))
	require.EqualValues(t, 0, batchSleep(time.Second, 1))
	require.Equal(t, minBatchElapsed, batchSleep(0, 0.5))
	require.Equal(t, maxBatchSleep, batchSleep(maxBatchSleep, 0.01))
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

	// Second scan after a distinct microsecond. The runner may still be
	// leaving the previous scan, so wait out ErrScanInProgress.
	time.Sleep(2 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := pm.MeteringTriggerScan(context.Background())
		if err == nil {
			break
		}
		require.ErrorIs(t, err, indexer.ErrScanInProgress)
		require.False(t, time.Now().After(deadline), "timed out waiting to trigger: %v", err)
		time.Sleep(5 * time.Millisecond)
	}
	deadline = time.Now().Add(5 * time.Second)
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
