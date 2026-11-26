package pebble

import (
	"bufio"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// testReadFailStore is a metered store reopened over a file system whose
// table reads fail. The store holds one completed scan, flushed to tables.
type testReadFailStore struct {
	store *store
	// scanID is the completed scan
	scanID uint64
	// providerHash is the hash of the provider whose values the store holds
	providerHash []byte
	// failing makes reads of table files fail while it is on
	failing *errorfs.Toggle
}

// openTestReadFailStore fills a store, runs a scan to completion, flushes and
// reopens the store with table reads already failing, and with the metering
// runner stopped so the test drives every scan.
func openTestReadFailStore(t *testing.T) *testReadFailStore {
	t.Helper()
	failing := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
		if strings.HasSuffix(op.Path, ".sst") && op.Kind.ReadOrWrite() == errorfs.OpIsRead {
			return errorfs.ErrInjected
		}
		return nil
	})}
	fs := errorfs.Wrap(vfs.NewMem(), failing)
	cfg := MeteringConfig{BatchSize: 100, Interval: 0, TimeFill: 1}

	s, err := New("/db", &pebble.Options{FS: fs}, WithMetering(cfg))
	require.NoError(t, err)
	st := s.(*store)
	st.metering.stop()
	putValue(t, s, random.Peers(1)[0], []byte("ctx"), []byte("meta"), random.Multihashes(10)...)
	require.NoError(t, st.metering.runMultihashScan(t.Context(), nil))
	status, err := st.MeteringScanStatus(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, indexer.ScanStateDone, status.State)

	var providerHash []byte
	for kv, err := range pebbleKVs(t.Context(), st.db, &pebble.IterOptions{
		LowerBound: []byte{byte(valueKeyPrefix)},
		UpperBound: []byte{byte(valueKeyPrefix) + 1},
	}) {
		require.NoError(t, err)
		providerHash = append([]byte(nil), providerHashFromValueKey(kv.key)...)
		break
	}
	require.NotEmpty(t, providerHash)

	require.NoError(t, s.Flush())
	require.NoError(t, s.Close())

	// Opening the store checks its tables, so reads fail only after it is
	// open. The runner starts after that, so that no table block is cached
	// by a read made while reads still worked.
	s, err = New("/db", &pebble.Options{FS: fs})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	st = s.(*store)
	failing.On()
	st.startMetering(cfg)
	st.metering.stop()
	return &testReadFailStore{store: st, scanID: status.ScanID, providerHash: providerHash, failing: failing}
}

// A read error in a scan batch fails the scan, which is recorded as failed
// instead of completing with partial counts.
func TestMeteringScanReadError(t *testing.T) {
	f := openTestReadFailStore(t)
	r := f.store.metering

	// A new scan needs progress; a hand-made record avoids reading tables.
	progress := &multihashScanProgressRecord{ScanID: f.scanID + 1, StartedAt: time.Now().UTC()}
	raw, err := encodeMultihashScanProgress(progress)
	require.NoError(t, err)
	require.NoError(t, f.store.db.Set(meteringProgressKey, raw, pebble.NoSync))

	done, err := r.runMultihashScanBatch(t.Context(), progress, &multihashScanCounters{
		providers: map[string]*indexer.ProviderStats{},
	}, map[string]peer.ID{})
	require.ErrorIs(t, err, errorfs.ErrInjected)
	require.False(t, done)
	require.Zero(t, progress.KeysRead)
	require.True(t, progress.DoneAt.IsZero())

	err = r.runMultihashScan(t.Context(), progress)
	require.ErrorIs(t, err, errorfs.ErrInjected)
	persisted, err := r.loadMultihashScanProgress(f.store.db)
	require.NoError(t, err)
	require.Contains(t, persisted.Error, errorfs.ErrInjected.Error())
	require.True(t, persisted.DoneAt.IsZero())
}

// A read error while loading provider rows is returned instead of a partial
// list.
func TestMeteringLoadProvidersReadError(t *testing.T) {
	f := openTestReadFailStore(t)
	r := f.store.metering

	_, err := r.loadProviderStats(f.store.db, f.scanID, nil)
	require.ErrorIs(t, err, errorfs.ErrInjected)

	_, err = r.loadMultihashScanCounters(f.store.db, f.scanID)
	require.ErrorIs(t, err, errorfs.ErrInjected)

	// With reads working again, the same rows load
	f.failing.Off()
	providers, err := r.loadProviderStats(f.store.db, f.scanID, nil)
	require.NoError(t, err)
	require.Len(t, providers, 1)
	counters, err := r.loadMultihashScanCounters(f.store.db, f.scanID)
	require.NoError(t, err)
	require.Len(t, counters.providers, 1)
}

// A read error while looking up a provider's ID is logged, and the lookup
// finds no ID.
func TestMeteringReadProviderIDReadError(t *testing.T) {
	f := openTestReadFailStore(t)

	pipe := logging.NewPipeReader(logging.PipeFormat(logging.PlaintextOutput), logging.PipeLevel(logging.LevelError))
	var mu sync.Mutex
	var logged strings.Builder
	var wg sync.WaitGroup
	wg.Go(func() {
		sc := bufio.NewScanner(pipe)
		for sc.Scan() {
			mu.Lock()
			logged.WriteString(sc.Text() + "\n")
			mu.Unlock()
		}
	})

	require.Empty(t, f.store.metering.readProviderID(f.providerHash))
	require.NoError(t, pipe.Close())
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, logged.String(), "provider lookup failed")
	require.Contains(t, logged.String(), errorfs.ErrInjected.Error())
}
