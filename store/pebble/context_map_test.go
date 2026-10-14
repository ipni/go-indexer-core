package pebble

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func openPebbleStore(t *testing.T) *store {
	t.Helper()
	s, err := New(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, s.Close())
	})
	return s.(*store)
}

// seedLegacyValue writes a provider-context value using the pre-mapping layout:
// multihashes point at the hash-based value key, and no context-map entry is stored.
func seedLegacyValue(t *testing.T, s *store, v indexer.Value, mhs ...multihash.Multihash) {
	t.Helper()
	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()

	vk, err := keygen.valueKey(&v, false)
	require.NoError(t, err)
	defer vk.Close()

	vs, closer, err := s.vcodec.marshalValue(&v)
	require.NoError(t, err)
	defer closer.Close()

	b := s.db.NewBatch()
	defer b.Close()
	require.NoError(t, b.Set(vk.buf, vs, pebble.NoSync))
	for _, mh := range mhs {
		mhk, err := keygen.multihashKey(mh)
		require.NoError(t, err)
		require.NoError(t, b.Merge(mhk.buf, vk.buf, pebble.NoSync))
		_ = mhk.Close()
	}
	require.NoError(t, b.Commit(pebble.NoSync))
}

func contextMapBytes(t *testing.T, s *store, pid peer.ID, contextID []byte) ([]byte, bool) {
	t.Helper()
	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()
	mk, err := keygen.contextMapKey(pid, contextID)
	require.NoError(t, err)
	defer mk.Close()
	val, closer, err := s.db.Get(mk.buf)
	if err == pebble.ErrNotFound {
		return nil, false
	}
	require.NoError(t, err)
	defer closer.Close()
	return bytes.Clone(val), true
}

func legacyValueKeyPresent(t *testing.T, s *store, v indexer.Value) bool {
	t.Helper()
	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()
	vk, err := keygen.valueKey(&v, false)
	require.NoError(t, err)
	defer vk.Close()
	_, closer, err := s.db.Get(vk.buf)
	if err == pebble.ErrNotFound {
		return false
	}
	require.NoError(t, err)
	_ = closer.Close()
	return true
}

func valueKeyForContext(t *testing.T, s *store, pid peer.ID, contextID []byte) []byte {
	t.Helper()
	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()
	vk, err := s.findContextValueKey(keygen, pid, contextID)
	require.NoError(t, err)
	defer vk.Close()
	return bytes.Clone(vk.buf)
}

func requireOnly(t *testing.T, s *store, mh multihash.Multihash, want indexer.Value) {
	t.Helper()
	vals, found, err := s.Get(mh)
	require.NoError(t, err)
	require.True(t, found)
	require.ElementsMatch(t, []indexer.Value{want}, vals)
}

func requireAbsent(t *testing.T, s *store, mh multihash.Multihash) {
	t.Helper()
	vals, found, err := s.Get(mh)
	require.NoError(t, err)
	require.False(t, found, "multihash still resolves: %+v", vals)
}

func TestLegacySeed_GetWithoutMap(t *testing.T) {
	s := openPebbleStore(t)
	pid := random.Peers(1)[0]
	mhs := random.Multihashes(2)
	v := indexer.Value{
		ProviderID:    pid,
		ContextID:     []byte("legacy-ctx"),
		MetadataBytes: []byte("meta"),
	}
	seedLegacyValue(t, s, v, mhs...)

	requireOnly(t, s, mhs[0], v)
	requireOnly(t, s, mhs[1], v)
	_, found := contextMapBytes(t, s, pid, v.ContextID)
	require.False(t, found)
	require.True(t, legacyValueKeyPresent(t, s, v))
}

func TestLegacySeed_PutKeepsHashKeyWithoutBackfill(t *testing.T) {
	s := openPebbleStore(t)
	pid := random.Peers(1)[0]
	mhs := random.Multihashes(3)
	old := indexer.Value{
		ProviderID:    pid,
		ContextID:     []byte("legacy-ctx"),
		MetadataBytes: []byte("old-meta"),
	}
	seedLegacyValue(t, s, old, mhs[0], mhs[1])

	updated := indexer.Value{
		ProviderID:    pid,
		ContextID:     []byte("legacy-ctx"),
		MetadataBytes: []byte("new-meta"),
	}
	require.NoError(t, s.Put(updated, mhs[1], mhs[2]))

	requireOnly(t, s, mhs[0], updated)
	requireOnly(t, s, mhs[1], updated)
	requireOnly(t, s, mhs[2], updated)
	_, found := contextMapBytes(t, s, pid, updated.ContextID)
	require.False(t, found)
	require.True(t, legacyValueKeyPresent(t, s, updated))
}

func TestLegacySeed_RemoveUsesHashKey(t *testing.T) {
	s := openPebbleStore(t)
	pid := random.Peers(1)[0]
	mhs := random.Multihashes(2)
	v := indexer.Value{
		ProviderID:    pid,
		ContextID:     []byte("legacy-ctx"),
		MetadataBytes: []byte("meta"),
	}
	seedLegacyValue(t, s, v, mhs...)

	require.NoError(t, s.Remove(v, mhs[0]))
	requireAbsent(t, s, mhs[0])
	requireOnly(t, s, mhs[1], v)
	_, found := contextMapBytes(t, s, pid, v.ContextID)
	require.False(t, found)
}

func TestLegacySeed_RemoveContextThenPutMintsToken(t *testing.T) {
	s := openPebbleStore(t)
	pid := random.Peers(1)[0]
	mhs := random.Multihashes(3)
	oldOnly, overlap, newOnly := mhs[0], mhs[1], mhs[2]
	contextID := []byte("legacy-ctx")
	old := indexer.Value{
		ProviderID:    pid,
		ContextID:     contextID,
		MetadataBytes: []byte("old-meta"),
	}
	seedLegacyValue(t, s, old, oldOnly, overlap)

	require.NoError(t, s.RemoveProviderContext(pid, contextID))
	requireAbsent(t, s, oldOnly)
	requireAbsent(t, s, overlap)
	require.False(t, legacyValueKeyPresent(t, s, old))

	newVal := indexer.Value{
		ProviderID:    pid,
		ContextID:     contextID,
		MetadataBytes: []byte("new-meta"),
	}
	require.NoError(t, s.Put(newVal, overlap, newOnly))

	requireOnly(t, s, overlap, newVal)
	requireOnly(t, s, newOnly, newVal)
	requireAbsent(t, s, oldOnly)

	token, found := contextMapBytes(t, s, pid, contextID)
	require.True(t, found)
	require.Len(t, token, contextTokenLength)
	require.False(t, legacyValueKeyPresent(t, s, old))
}

func TestContextMap_ReuseAfterRemoveGetsNewToken(t *testing.T) {
	s := openPebbleStore(t)
	pid := random.Peers(1)[0]
	mhs := random.Multihashes(3)
	contextID := []byte("ctx")
	first := indexer.Value{
		ProviderID:    pid,
		ContextID:     contextID,
		MetadataBytes: []byte("first"),
	}
	require.NoError(t, s.Put(first, mhs[0], mhs[1]))
	token1, found := contextMapBytes(t, s, pid, contextID)
	require.True(t, found)
	vk1 := valueKeyForContext(t, s, pid, contextID)

	require.NoError(t, s.Put(first, mhs[1]))
	token2, found := contextMapBytes(t, s, pid, contextID)
	require.True(t, found)
	require.Equal(t, token1, token2)
	require.Equal(t, vk1, valueKeyForContext(t, s, pid, contextID))

	require.NoError(t, s.RemoveProviderContext(pid, contextID))
	_, found = contextMapBytes(t, s, pid, contextID)
	require.False(t, found)

	second := indexer.Value{
		ProviderID:    pid,
		ContextID:     contextID,
		MetadataBytes: []byte("second"),
	}
	require.NoError(t, s.Put(second, mhs[1], mhs[2]))
	token3, found := contextMapBytes(t, s, pid, contextID)
	require.True(t, found)
	require.NotEqual(t, token1, token3)
	require.NotEqual(t, vk1, valueKeyForContext(t, s, pid, contextID))

	requireAbsent(t, s, mhs[0])
	requireOnly(t, s, mhs[1], second)
	requireOnly(t, s, mhs[2], second)
}

func TestContextMap_RemoveProviderClearsMap(t *testing.T) {
	s := openPebbleStore(t)
	pids := random.Peers(2)
	mhs := random.Multihashes(2)
	contextID := []byte("ctx")
	dropped := indexer.Value{
		ProviderID:    pids[0],
		ContextID:     contextID,
		MetadataBytes: []byte("dropped"),
	}
	kept := indexer.Value{
		ProviderID:    pids[1],
		ContextID:     contextID,
		MetadataBytes: []byte("kept"),
	}
	require.NoError(t, s.Put(dropped, mhs[0]))
	require.NoError(t, s.Put(kept, mhs[1]))
	_, found := contextMapBytes(t, s, pids[0], contextID)
	require.True(t, found)

	require.NoError(t, s.RemoveProvider(context.Background(), pids[0]))
	requireAbsent(t, s, mhs[0])
	requireOnly(t, s, mhs[1], kept)
	_, found = contextMapBytes(t, s, pids[0], contextID)
	require.False(t, found)

	// Re-adding the same context for the removed provider must not revive old multihashes.
	again := indexer.Value{
		ProviderID:    pids[0],
		ContextID:     contextID,
		MetadataBytes: []byte("again"),
	}
	require.NoError(t, s.Put(again, mhs[0]))
	requireOnly(t, s, mhs[0], again)
	requireOnly(t, s, mhs[1], kept)
}

func TestContextMap_SameContextDifferentProviders(t *testing.T) {
	s := openPebbleStore(t)
	pids := random.Peers(2)
	mh := random.Multihashes(1)[0]
	contextID := []byte("shared-ctx")
	first := indexer.Value{
		ProviderID:    pids[0],
		ContextID:     contextID,
		MetadataBytes: []byte("a"),
	}
	second := indexer.Value{
		ProviderID:    pids[1],
		ContextID:     contextID,
		MetadataBytes: []byte("b"),
	}
	require.NoError(t, s.Put(first, mh))
	require.NoError(t, s.Put(second, mh))

	tokenA, foundA := contextMapBytes(t, s, pids[0], contextID)
	tokenB, foundB := contextMapBytes(t, s, pids[1], contextID)
	require.True(t, foundA)
	require.True(t, foundB)
	require.NotEqual(t, tokenA, tokenB)
	require.NotEqual(t, valueKeyForContext(t, s, pids[0], contextID), valueKeyForContext(t, s, pids[1], contextID))

	vals, found, err := s.Get(mh)
	require.NoError(t, err)
	require.True(t, found)
	require.ElementsMatch(t, []indexer.Value{first, second}, vals)
}
