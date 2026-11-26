package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/stretchr/testify/require"
)

// testRecordKeys returns the keys of the value records in s
func testRecordKeys(t *testing.T, s *store) map[string]bool {
	t.Helper()
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(valueKeyPrefix)},
		UpperBound: []byte{byte(valueKeyPrefix) + 1},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	keys := make(map[string]bool)
	for ok := it.First(); ok; ok = it.Next() {
		keys[string(it.Key())] = true
	}
	return keys
}

func TestValidateRecords(t *testing.T) {
	vs, err := New(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })
	s := vs.(*store)
	keygen := s.p.leaseBlake3Keyer()
	defer func() { _ = keygen.Close() }()
	set := func(key, value []byte) { require.NoError(t, s.db.Set(key, value, pebble.Sync)) }
	del := func(key []byte) { require.NoError(t, s.db.Delete(key, pebble.Sync)) }
	value := func(v indexer.Value) []byte {
		b, closer, err := s.vcodec.marshalValue(&v)
		require.NoError(t, err)
		defer func() { _ = closer.Close() }()
		return append([]byte(nil), b...)
	}
	pidHash := func(v indexer.Value) []byte {
		h, err := keygen.hashPeerID(v.ProviderID)
		require.NoError(t, err)
		return h
	}
	ctxHash := func(v indexer.Value) []byte {
		h, err := keygen.hashContextID(v.ContextID)
		require.NoError(t, err)
		return h
	}
	recordKey := func(pidHash, token []byte) []byte {
		return append(append([]byte{byte(valueKeyPrefix)}, pidHash...), token...)
	}
	mapKey := func(v indexer.Value) []byte {
		return append(append([]byte{byte(contextMapKeyPrefix)}, pidHash(v)...), ctxHash(v)...)
	}
	// tokenRecord returns the key of v's record that its map entry leads to
	tokenRecord := func(v indexer.Value) []byte {
		token, closer, err := s.db.Get(mapKey(v))
		require.NoError(t, err)
		defer func() { _ = closer.Close() }()
		return recordKey(pidHash(v), token)
	}

	providers := random.Peers(3)
	newValue := func(p int, ctx string) indexer.Value {
		return indexer.Value{ProviderID: providers[p], ContextID: []byte(ctx), MetadataBytes: []byte("md-" + ctx)}
	}
	want := make(map[string]bool)

	// Contexts put as the store puts them: a map entry and its token record
	for p := range providers {
		for _, c := range []string{"a", "b", "c"} {
			v := newValue(p, c)
			require.NoError(t, s.Put(v, random.Multihashes(2)...))
			want[string(tokenRecord(v))] = true
		}
	}
	// A legacy record, with no map entry, is kept
	legacy := newValue(0, "legacy")
	set(recordKey(pidHash(legacy), ctxHash(legacy)), value(legacy))
	want[string(recordKey(pidHash(legacy), ctxHash(legacy)))] = true

	// A token record whose map entry is gone is deleted
	orphan := newValue(1, "orphan")
	require.NoError(t, s.Put(orphan, random.Multihashes(1)...))
	del(mapKey(orphan))
	// A record of a context whose map entry leads to another token is
	// deleted, as is a legacy record of a mapped context
	stale := newValue(2, "a")
	set(recordKey(pidHash(stale), []byte("0123456789")), value(stale))
	set(recordKey(pidHash(stale), ctxHash(stale)), value(stale))
	// A record under another provider's hash is deleted
	moved := newValue(0, "moved")
	set(recordKey(pidHash(newValue(1, "x")), ctxHash(moved)), value(moved))
	// A record whose key is not a record ID, or whose value does not
	// decode, is deleted
	set([]byte{byte(valueKeyPrefix), 1, 2, 3}, value(newValue(0, "short")))
	set(recordKey(pidHash(legacy), []byte("9876543210")), []byte("not a value"))

	// Validate in small steps, resuming from each position
	var from []byte
	var deleted, calls int
	for {
		next, done, n, err := s.ValidateRecords(t.Context(), from, 3)
		require.NoError(t, err)
		deleted += n
		calls++
		if done {
			break
		}
		require.NotEqual(t, from, next, "the position moves")
		from = next
	}
	require.Equal(t, 6, deleted)
	require.Greater(t, calls, 3, "the records are checked in several steps")
	require.Equal(t, want, testRecordKeys(t, s))

	// One record at a time, each call checks the record after the last
	from, calls = nil, 0
	for calls < 1000 {
		next, done, n, err := s.ValidateRecords(t.Context(), from, 1)
		require.NoError(t, err)
		require.Zero(t, n)
		calls++
		if done {
			break
		}
		from = next
	}
	require.Equal(t, len(want), calls)

	// A store that is valid stays as it is
	next, done, n, err := s.ValidateRecords(t.Context(), nil, 100)
	require.NoError(t, err)
	require.True(t, done)
	require.Zero(t, n)
	require.NotNil(t, next)

	_, _, _, err = s.ValidateRecords(t.Context(), nil, 0)
	require.Error(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, _, err = s.ValidateRecords(ctx, nil, 100)
	require.ErrorIs(t, err, context.Canceled)
}
