package test

import (
	"context"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// conformanceTestSuite is the indexer.Interface behavior every store must implement.
// NewStore is called for each test and must return an open store.
type conformanceTestSuite struct {
	suite.Suite
	NewStore func(t *testing.T) indexer.Interface
	store    indexer.Interface
}

func (c *conformanceTestSuite) SetupTest() {
	c.store = c.NewStore(c.T())
}

func (c *conformanceTestSuite) TearDownTest() {
	t := c.T()
	require.NoError(t, c.store.Close())
}

// RunConformance runs the conformance suite against stores from newStore.
func RunConformance(t *testing.T, newStore func(t *testing.T) indexer.Interface) {
	suite.Run(t, &conformanceTestSuite{NewStore: newStore})
}

func (c *conformanceTestSuite) TestPutGetAndRemove() {
	t := c.T()
	s := c.store

	p := random.Peers(1)[0]
	mhs := random.Multihashes(15)

	ctxid1 := []byte(mhs[0])
	metadata1 := []byte("test-meta-1")
	ctxid2 := []byte(mhs[1])
	metadata2 := []byte("test-meta-2")

	value1 := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxid1,
		MetadataBytes: metadata1,
	}
	value2 := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxid2,
		MetadataBytes: metadata2,
	}

	single := mhs[2]
	noadd := mhs[3]
	batch := mhs[4:]
	remove := mhs[4]

	// Check for err when putting a multihash with nil metadata
	t.Log("Put bad value")
	badValue := indexer.Value{
		ProviderID: p,
		ContextID:  ctxid1,
	}
	require.Error(t, s.Put(badValue, single))

	// Put a single multihash
	t.Log("Put/Get a single multihash")
	require.NoError(t, s.Put(value1, single))

	// Put same value again.
	t.Log("Put/Get single multihash again")
	require.NoError(t, s.Put(value1, single))

	require.NoError(t, s.Flush())
	vals, found, err := s.Get(single)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value1, vals[0])

	// Put a batch of multihashes
	t.Log("Put/Get a batch of multihashes")
	require.NoError(t, s.Put(value1, batch...))

	require.NoError(t, s.Flush())
	vals, found, err = s.Get(mhs[5])
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value1, vals[0])

	// Put on an existing key
	t.Log("Put/Get on existing key")
	require.NoError(t, s.Put(value2, single))
	require.NoError(t, s.Flush())
	vals, found, err = s.Get(single)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 2)
	require.Equal(t, value2, vals[1])

	// Get a key that is not set
	t.Log("Get non-existing key")
	_, found, err = s.Get(noadd)
	require.NoError(t, err)
	require.False(t, found)

	// Check that a short v1 CID hash can be stored.
	v1cid, err := cid.Decode("baguqeeqqskyz3yh4jxnsdj57v5blazexyy")
	require.NoError(t, err)
	v1mh := v1cid.Hash()
	require.NoError(t, s.Put(value2, v1mh))
	require.NoError(t, s.Flush())

	vals, found, err = s.Get(v1mh)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value2, vals[0])

	// Update a value's metadata
	metadata3 := []byte("test-meta-3")
	value1a := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxid1,
		MetadataBytes: metadata3,
	}
	require.NoError(t, s.Put(value1a, v1mh))
	require.NoError(t, s.Flush())

	// Retrieve value using different multihash
	vals, found, err = s.Get(single)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value1a, vals[0])

	// Remove a key
	t.Log("Remove key")
	require.NoError(t, s.Remove(value1, remove))

	_, found, err = s.Get(remove)
	require.NoError(t, err)
	require.False(t, found)

	// Remove a value from the key
	require.NoError(t, s.Remove(value1, single))

	vals, found, err = s.Get(single)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 1)
}

func (c *conformanceTestSuite) TestSize() {
	t := c.T()
	s := c.store

	p := random.Peers(1)[0]

	mhs := random.Multihashes(151)

	value := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("test-metadata"),
	}
	for _, mh := range mhs[1:] {
		require.NoError(t, s.Put(value, mh))
	}

	// Flush out all changes to assure the size returned is representative of persisted data.
	require.NoError(t, s.Flush())

	size, err := s.Size()
	require.NoError(t, err)
	// A store with no persistent files, such as the memory store, reports 0.
	require.GreaterOrEqual(t, size, int64(0))
}

func (c *conformanceTestSuite) TestRemove() {
	t := c.T()
	s := c.store

	p := random.Peers(1)[0]
	mhs := random.Multihashes(15)

	value := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("test-metadata"),
	}
	batch := mhs[1:]

	// Put a batch of multihashes
	t.Log("Put a batch of multihashes")
	require.NoError(t, s.Put(value, batch...))

	vals, found, err := s.Get(batch[2])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 1)

	t.Log("Remove indexes")
	require.NoError(t, s.Remove(value, batch[1:]...))

	vals, found, err = s.Get(batch[0])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 1)

	_, found, err = s.Get(batch[2])
	require.NoError(t, err)
	require.False(t, found)

	mhs = random.Multihashes(5)
	require.NoError(t, s.Put(value, mhs...))

	vals, found, err = s.Get(mhs[0])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 1)
}

func (c *conformanceTestSuite) TestRemoveProviderContextValues() {
	t := c.T()
	s := c.store

	pids := random.Peers(2)
	prov1, prov2 := pids[0], pids[1]

	mhs := random.Multihashes(2)

	ctx1id := []byte(mhs[0])
	ctx2id := []byte(mhs[1])
	value1 := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctx1id,
		MetadataBytes: []byte("ctx1-metadata"),
	}
	value2 := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctx2id,
		MetadataBytes: []byte("ctx2-metadata"),
	}
	value3 := indexer.Value{
		ProviderID:    prov2,
		ContextID:     ctx1id,
		MetadataBytes: []byte("ctx3-metadata"),
	}

	mhs = random.Multihashes(15)

	batch1 := mhs[:5]
	batch2 := mhs[5:10]
	batch3 := mhs[10:15]

	// Put a batches of multihashes
	t.Log("Put batch1 value (provider1 context1)")
	require.NoError(t, s.Put(value1, batch1...))
	t.Log("Put batch2 values (provider1 context1), (provider1 context2)")
	require.NoError(t, s.Put(value1, batch2...))
	require.NoError(t, s.Put(value2, batch2...))
	t.Log("Put batch3 values (provider1 context2), (provider2 context1)")
	require.NoError(t, s.Put(value2, batch3...))
	require.NoError(t, s.Put(value3, batch3...))

	// Verify starting with correct values
	vals, found, err := s.Get(mhs[0])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 1)
	vals, found, err = s.Get(mhs[5])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 2)
	vals, found, err = s.Get(mhs[10])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 2)

	t.Log("Removing provider1 context1")
	require.NoError(t, s.RemoveProviderContext(prov1, ctx1id))
	_, found, err = s.Get(mhs[0])
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = s.Get(mhs[1])
	require.NoError(t, err)
	require.False(t, found)
	vals, found, err = s.Get(mhs[5])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 1)
	require.Equal(t, value2, vals[0])
	vals, found, err = s.Get(mhs[10])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 2)

	t.Log("Removing provider1 context2")
	require.NoError(t, s.RemoveProviderContext(prov1, ctx2id))
	_, found, err = s.Get(mhs[5])
	require.NoError(t, err)
	require.False(t, found)
	vals, found, err = s.Get(mhs[10])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 1)
	require.Equal(t, value3, vals[0])

	t.Log("Removing provider2 context1")
	require.NoError(t, s.RemoveProviderContext(prov2, ctx1id))
	_, found, err = s.Get(mhs[10])
	require.NoError(t, err)
	require.False(t, found)
}

func (c *conformanceTestSuite) TestRemoveProviderValues() {
	t := c.T()
	s := c.store

	pids := random.Peers(2)
	prov1, prov2 := pids[0], pids[1]

	ctx1id := []byte("ctxid-1")
	ctx2id := []byte("ctxid-2")
	value1 := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctx1id,
		MetadataBytes: []byte("ctx1-metadata"),
	}
	value2 := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctx2id,
		MetadataBytes: []byte("ctx2-metadata"),
	}
	value3 := indexer.Value{
		ProviderID:    prov2,
		ContextID:     ctx1id,
		MetadataBytes: []byte("ctx3-metadata"),
	}

	mhs := random.Multihashes(15)

	batch1 := mhs[:5]
	batch2 := mhs[5:10]
	batch3 := mhs[10:15]

	// Put a batches of multihashes
	t.Log("Put batch1 value (provider1 context1)")
	require.NoError(t, s.Put(value1, batch1...))
	t.Log("Put batch2 values (provider1 context1), (provider1 context2)")
	require.NoError(t, s.Put(value1, batch2...))
	require.NoError(t, s.Put(value2, batch2...))
	t.Log("Put batch3 values (provider1 context2), (provider2 context1)")
	require.NoError(t, s.Put(value2, batch3...))
	require.NoError(t, s.Put(value3, batch3...))

	t.Log("Removing provider1")
	require.NoError(t, s.RemoveProvider(context.Background(), prov1))
	_, found, err := s.Get(mhs[0])
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = s.Get(mhs[1])
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = s.Get(mhs[5])
	require.NoError(t, err)
	require.False(t, found)
	vals, found, err := s.Get(mhs[10])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vals, 1)

	t.Log("Removing provider2")
	require.NoError(t, s.RemoveProvider(context.Background(), prov2))
	_, found, err = s.Get(mhs[10])
	require.NoError(t, err)
	require.False(t, found)
}

func (c *conformanceTestSuite) TestParallelUpdate() {
	t := c.T()
	s := c.store

	mhs := random.Multihashes(15)

	p := random.Peers(1)[0]
	single := mhs[14]
	metadata := []byte("test-metadata")

	wg := new(sync.WaitGroup)

	// Test parallel writes over same multihash
	for i := range 5 {
		wg.Go(func() {
			value := indexer.Value{
				ProviderID:    p,
				ContextID:     []byte(mhs[i]),
				MetadataBytes: metadata,
			}
			assert.NoError(t, s.Put(value, single))
		})
	}
	wg.Wait()
	require.False(t, t.Failed())

	x, found, err := s.Get(single)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, x, 5)

	// Test remove for all except one.
	for i := range 4 {
		wg.Go(func() {
			value := indexer.Value{
				ProviderID:    p,
				ContextID:     []byte(mhs[i]),
				MetadataBytes: metadata,
			}
			assert.NoError(t, s.Remove(value, single))
		})
	}
	wg.Wait()
	require.False(t, t.Failed())

	x, found, err = s.Get(single)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, x, 1)
}

func (c *conformanceTestSuite) TestPutUpdatesMetadata() {
	t := c.T()
	pid := random.Peers(1)[0]
	mhs := random.Multihashes(2)
	require.NoError(t, c.store.Put(valueOf(pid, "ctx", "old-meta"), mhs...))

	updated := valueOf(pid, "ctx", "new-meta")
	require.NoError(t, c.store.Put(updated))

	for _, mh := range mhs {
		c.requireOnly(t, mh, updated)
	}
	c.requireAbsent(t, random.Multihashes(1)[0])
}

func (c *conformanceTestSuite) TestTwoProviders() {
	t := c.T()
	pids := random.Peers(2)
	mh := random.Multihashes(1)[0]
	first := valueOf(pids[0], "ctx-a", "meta-a")
	second := valueOf(pids[1], "ctx-b", "meta-b")
	require.NoError(t, c.store.Put(first, mh))
	require.NoError(t, c.store.Put(second, mh))

	c.requireValues(t, mh, first, second)
}

func (c *conformanceTestSuite) TestRemoveProviderContext() {
	t := c.T()
	pid := random.Peers(1)[0]
	mhs := random.Multihashes(2)
	kept := valueOf(pid, "kept", "kept-meta")
	dropped := valueOf(pid, "dropped", "dropped-meta")
	require.NoError(t, c.store.Put(kept, mhs[0]))
	require.NoError(t, c.store.Put(dropped, mhs[0], mhs[1]))

	require.NoError(t, c.store.RemoveProviderContext(pid, dropped.ContextID))

	c.requireOnly(t, mhs[0], kept)
	c.requireAbsent(t, mhs[1])
}

func (c *conformanceTestSuite) TestRemoveProvider() {
	t := c.T()
	pids := random.Peers(2)
	mh := random.Multihashes(1)[0]
	dropped := valueOf(pids[0], "ctx", "dropped-meta")
	kept := valueOf(pids[1], "ctx", "kept-meta")
	require.NoError(t, c.store.Put(dropped, mh))
	require.NoError(t, c.store.Put(kept, mh))

	require.NoError(t, c.store.RemoveProvider(context.Background(), pids[0]))

	c.requireOnly(t, mh, kept)
}

func (c *conformanceTestSuite) requireAbsent(t *testing.T, mh multihash.Multihash) {
	t.Helper()
	vals, found, err := c.store.Get(mh)
	require.NoError(t, err)
	require.False(t, found, "multihash still resolves: %+v", vals)
}

func (c *conformanceTestSuite) requireOnly(t *testing.T, mh multihash.Multihash, want indexer.Value) {
	t.Helper()
	c.requireValues(t, mh, want)
}

func (c *conformanceTestSuite) requireValues(t *testing.T, mh multihash.Multihash, want ...indexer.Value) {
	t.Helper()
	vals, found, err := c.store.Get(mh)
	require.NoError(t, err)
	require.True(t, found)
	require.ElementsMatch(t, want, vals)
}

func valueOf(pid peer.ID, contextID, meta string) indexer.Value {
	return indexer.Value{
		ProviderID:    pid,
		ContextID:     []byte(contextID),
		MetadataBytes: []byte(meta),
	}
}
