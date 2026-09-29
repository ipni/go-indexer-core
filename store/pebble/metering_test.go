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
		if !st.InProgress {
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

	require.EqualValues(t, 5, report.Totals.Multihashes)
	// slots: p1 has 3+2=5, p2 has 2 → 7
	require.EqualValues(t, 7, report.Totals.Slots)
	byID := map[peer.ID]indexer.ProviderStats{}
	for _, ps := range report.Providers {
		byID[ps.ProviderID] = ps
	}
	require.EqualValues(t, 4, byID[p1].Multihashes)
	require.EqualValues(t, 2, byID[p2].Multihashes)

	filtered, err := pm.MeteringAllStats(context.Background(), []peer.ID{p1})
	require.NoError(t, err)
	require.NotNil(t, filtered)
	require.Equal(t, report.Totals.Multihashes, filtered.Totals.Multihashes)
	require.Len(t, filtered.Providers, 1)
	require.Equal(t, p1, filtered.Providers[0].ProviderID)
	require.EqualValues(t, 4, filtered.Providers[0].Multihashes)

	none, err := pm.MeteringAllStats(context.Background(), []peer.ID{})
	require.NoError(t, err)
	require.NotNil(t, none)
	require.Equal(t, report.Totals.Multihashes, none.Totals.Multihashes)
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
	// Provider IDs are read from value records, so these slots stay in the store totals.
	require.EqualValues(t, 3, report.Totals.Slots)
	require.Empty(t, report.Providers)
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
		if st.InProgress && st.KeysRead > 0 {
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
	require.EqualValues(t, 20, report.Totals.Multihashes)
	require.EqualValues(t, 20, report.Totals.Slots)
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
		if st.InProgress {
			gotErr = pm.MeteringTriggerScan(context.Background())
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	require.ErrorIs(t, gotErr, indexer.ErrScanInProgress)
	_ = waitScanDone(t, pm, 10*time.Second)
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
		if report != nil && !report.MeasuredAt.Equal(r1.MeasuredAt) && !st.InProgress {
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

	require.GreaterOrEqual(t, report.Totals.Multihashes, uint64(100))
}

func TestMeteringNotSupportedWithoutOption(t *testing.T) {
	s, err := New(t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	pm, ok := s.(indexer.StatsMeter)
	require.True(t, ok, "store should still implement StatsMeter")
	_, err = pm.MeteringAllStats(context.Background(), nil)
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
		if got.InProgress && got.Current.Totals.Multihashes == 1 {
			st = got
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.NotNil(t, st, "scan did not report partial progress")
	require.EqualValues(t, 1, st.Current.Totals.Multihashes)
	emptySel, err := pm.MeteringScanStatus(context.Background(), []peer.ID{})
	require.NoError(t, err)
	require.Empty(t, emptySel.Current.Providers)
	require.EqualValues(t, 1, emptySel.Current.Totals.Multihashes)
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
		Done:      true,
		Error:     "scan failed",
	}
	raw, err := encodeMultihashScanProgress(&p)
	require.NoError(t, err)
	got, err := decodeMultihashScanProgress(raw)
	require.NoError(t, err)
	require.Equal(t, p.ScanID, got.ScanID)
	require.Equal(t, p.KeysRead, got.KeysRead)
	require.Equal(t, p.BytesRead, got.BytesRead)
	require.Equal(t, p.Done, got.Done)
	require.Equal(t, p.Cursor, got.Cursor)
	require.Equal(t, p.Error, got.Error)
	require.True(t, got.StartedAt.Equal(p.StartedAt))

	cur := multihashScanResults{
		ScanID:      99,
		StartedAt:   now,
		CompletedAt: now.Add(time.Second),
		Totals: indexer.StoreTotals{
			Multihashes: 1, Slots: 2, MultihashKeyBytes: 3,
			MultihashValueBytes: 4,
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
	require.True(t, st.InProgress)

	store.metering.persistScanError(progress, errors.New("boom"))
	st, err = pm.MeteringScanStatus(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, "boom", st.Error)
	require.EqualValues(t, 42, st.ScanID)
	require.EqualValues(t, 7, st.KeysRead)
	require.False(t, st.InProgress)

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
	require.False(t, st.InProgress)
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
		if report != nil && !st.InProgress {
			require.Empty(t, st.Error)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.NotNil(t, report)
	require.EqualValues(t, 5, report.Totals.Multihashes)
}
