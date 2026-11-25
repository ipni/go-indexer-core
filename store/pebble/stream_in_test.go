package pebble

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importertest"
	"github.com/stretchr/testify/require"
)

func TestStreamPebbleToPebble(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	src := openPebbleStore(t)
	f := fillTestStreamStore(t, rng, src)
	source, ok := any(src).(importer.Source)
	require.True(t, ok)

	// The source's contract, and its entries against the raw store
	for _, kind := range Kinds {
		parts := importertest.ReadParts(t, rng, source, kind, 32<<10)
		got := importertest.Entries(parts)
		require.Equal(t, testExpectedWire(t, src, kind), got, kind)
		requireTestPartSizes(t, parts)
		for _, e := range got {
			switch kind {
			case importer.KindRecords:
				require.Len(t, e.Key, 20)
			case importer.KindContextMap:
				require.Len(t, e.Key, 20)
				require.Len(t, e.Value, 10)
			default:
				require.NotEmpty(t, e.Value)
				require.Zero(t, len(e.Value)%20)
			}
		}
		if kind == importer.KindMultihashes {
			require.Greater(t, len(parts), 5, "multihashes are cut in parts")
			byKey := make(map[string][]byte, len(got))
			for _, e := range got {
				byKey[string(e.Key)] = e.Value
			}
			for _, mh := range f.emptied {
				require.NotContains(t, byKey, string(mh), "an emptied multihash is sent")
			}
			liveKey := valueKeyForContext(t, src, f.live.ProviderID, f.live.ContextID)
			require.Equal(t, liveKey[1:], byKey[string(f.withDeleteSlot)], "a merge-delete slot is sent")
		}
	}
	// The raw store holds what the source skips
	require.Less(t, len(testExpectedWire(t, src, importer.KindMultihashes)), len(testRawEntries(t, src, multihashKeyPrefix)))

	dst := openPebbleStore(t)
	target, ok := any(dst).(importer.Target)
	require.True(t, ok)
	for _, kind := range Kinds {
		importertest.Copy(t, rng, source, target, kind, 32<<10, 4)
	}

	for _, mh := range f.mhs {
		want, wantFound, err := src.Get(mh)
		require.NoError(t, err)
		got, found, err := dst.Get(mh)
		require.NoError(t, err)
		require.Equal(t, wantFound, found)
		// Where Get finds nothing, the source may return an empty slice
		// and the destination nil
		if found {
			require.Equal(t, want, got)
		}
	}
	vals, found, err := dst.Get(f.withDeleteSlot)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []indexer.Value{f.live}, vals)

	require.Equal(t, testRawEntries(t, src, valueKeyPrefix), testRawEntries(t, dst, valueKeyPrefix))
	require.Equal(t, testRawEntries(t, src, contextMapKeyPrefix), testRawEntries(t, dst, contextMapKeyPrefix))
	wantMultihashes := testExpectedWire(t, src, importer.KindMultihashes)
	require.Equal(t, wantMultihashes, testExpectedWire(t, dst, importer.KindMultihashes))
	// The destination has no key that the source skipped
	require.Len(t, testRawEntries(t, dst, multihashKeyPrefix), len(wantMultihashes))

	// Every iterator is closed, including those of parts broken out of
	require.NoError(t, src.Close())
	require.NoError(t, dst.Close())
}

// testEntriesOf yields entries, then err when it is not nil
func testEntriesOf(err error, entries ...importer.Entry) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		for _, e := range entries {
			if !yield(e, nil) {
				return
			}
		}
		if err != nil {
			yield(importer.Entry{}, err)
		}
	}
}

func TestStreamTargetRefuses(t *testing.T) {
	s := openPebbleStore(t)
	target, ok := any(s).(importer.Target)
	require.True(t, ok)
	ctx := t.Context()
	record, closer, err := s.vcodec.marshalValue(&indexer.Value{ProviderID: random.Peers(1)[0], ContextID: []byte("c"), MetadataBytes: []byte("m")})
	require.NoError(t, err)
	record = bytes.Clone(record)
	_ = closer.Close()
	id := bytes.Repeat([]byte{7}, recordIDLength)

	for _, tc := range []struct {
		name  string
		kind  importer.Kind
		entry importer.Entry
	}{
		{"record key too short", importer.KindRecords, importer.Entry{Key: id[1:], Value: record}},
		{"record key too long", importer.KindRecords, importer.Entry{Key: append(id, 0), Value: record}},
		{"record does not decode", importer.KindRecords, importer.Entry{Key: id, Value: []byte{0xff}}},
		{"record empty", importer.KindRecords, importer.Entry{Key: id, Value: nil}},
		{"context map key too short", importer.KindContextMap, importer.Entry{Key: id[1:], Value: id[:10]}},
		{"context token too short", importer.KindContextMap, importer.Entry{Key: id, Value: id[:9]}},
		{"multihash key empty", importer.KindMultihashes, importer.Entry{Key: nil, Value: id}},
		{"multihash value empty", importer.KindMultihashes, importer.Entry{Key: []byte{1}, Value: nil}},
		{"multihash value not whole IDs", importer.KindMultihashes, importer.Entry{Key: []byte{1}, Value: append(id, 1)}},
		{"unknown kind", "pinfo", importer.Entry{Key: id, Value: record}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := target.Write(ctx, tc.kind, testEntriesOf(nil, tc.entry))
			require.True(t, importer.IsPermanent(err), "%v", err)
		})
	}
	// Nothing was written
	for _, p := range []keyPrefix{valueKeyPrefix, contextMapKeyPrefix, multihashKeyPrefix} {
		require.Empty(t, testRawEntries(t, s, p))
	}

	t.Run("entries error", func(t *testing.T) {
		boom := errors.New("boom")
		err := target.Write(ctx, importer.KindRecords, testEntriesOf(boom, importer.Entry{Key: id, Value: record}))
		require.ErrorIs(t, err, boom)
		require.False(t, importer.IsPermanent(err))
	})
	t.Run("cancelled part way", func(t *testing.T) {
		// Cancel before the second entry. The open batch is committed only
		// after the entries, so that commit sees the cancelled context and
		// writes nothing.
		cctx, cancel := context.WithCancel(ctx)
		entries := func(yield func(importer.Entry, error) bool) {
			if yield(importer.Entry{Key: id, Value: record}, nil) {
				cancel()
				yield(importer.Entry{Key: id, Value: record}, nil)
			}
		}
		err := target.Write(cctx, importer.KindRecords, entries)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, importer.IsPermanent(err))
		require.Empty(t, testRawEntries(t, s, valueKeyPrefix))
	})
}

func TestStreamTargetRewrites(t *testing.T) {
	s := openPebbleStore(t)
	target, ok := any(s).(importer.Target)
	require.True(t, ok)
	ctx := t.Context()
	a := bytes.Repeat([]byte{1}, recordIDLength)
	b := bytes.Repeat([]byte{2}, recordIDLength)
	mh := []byte{0x12, 0x01, 0xaa}
	write := func(ids ...[]byte) []importer.Entry {
		require.NoError(t, target.Write(ctx, importer.KindMultihashes, testEntriesOf(nil, importer.Entry{Key: mh, Value: slices.Concat(ids...)})))
		return testRawEntries(t, s, multihashKeyPrefix)
	}

	first := write(a, b)
	require.Equal(t, first, write(a, b), "writing a part again changes the store")
	require.Equal(t, testExpectedWire(t, s, importer.KindMultihashes), []importer.Entry{{Key: mh, Value: slices.Concat(a, b)}})
	// A later write replaces the multihash's value
	write(b)
	require.Equal(t, testExpectedWire(t, s, importer.KindMultihashes), []importer.Entry{{Key: mh, Value: b}})
}

func TestStreamTargetSyncSurvivesCrash(t *testing.T) {
	fs := vfs.NewCrashableMem()
	vs, err := New("/db", &pebble.Options{FS: fs})
	require.NoError(t, err)
	target, ok := any(vs).(importer.Target)
	require.True(t, ok)
	ctx := t.Context()
	ids := bytes.Repeat([]byte{1}, recordIDLength)
	synced := importer.Entry{Key: []byte{0x12, 0x01, 0xaa}, Value: ids}
	unsynced := importer.Entry{Key: []byte{0x12, 0x01, 0xbb}, Value: ids}

	require.NoError(t, target.Write(ctx, importer.KindMultihashes, testEntriesOf(nil, synced)))
	require.NoError(t, target.Sync())
	require.NoError(t, target.Write(ctx, importer.KindMultihashes, testEntriesOf(nil, unsynced)))
	crashed := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	require.NoError(t, vs.Close())

	reopened, err := New("/db", &pebble.Options{FS: crashed})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.Equal(t, []importer.Entry{synced}, testExpectedWire(t, reopened.(*store), importer.KindMultihashes))
}
