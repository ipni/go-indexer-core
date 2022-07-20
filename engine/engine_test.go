package engine

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/go-indexer-core/store/test"
	"github.com/libp2p/go-libp2p-core/peer"
)

func initEngine(t *testing.T, withCache, cacheOnPut bool) *Engine {
	valueStore, err := storethehash.New(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var resultCache cache.Interface
	if withCache {
		resultCache = radixcache.New(100000)
	}
	return New(resultCache, valueStore, CacheOnPut(cacheOnPut))
}

func TestPassthrough(t *testing.T) {
	eng := initEngine(t, true, false)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := test.RandomMultihashes(5)

	value1 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("mtadata-1"),
	}
	value2 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[1]),
		MetadataBytes: []byte("mtadata-2"),
	}

	single := mhs[2]

	// First put should go to value store
	err = eng.Put(value1, single)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	_, found, _ := eng.valueStore.Get(single)
	if !found {
		t.Fatal("single put did not go to value store")
	}
	_, found = eng.resultCache.Get(single)
	if found {
		t.Fatal("single put went to result cache")
	}

	// Getting the value should put it in cache
	v, found, _ := eng.Get(single)
	if !found || !v[0].Equal(value1) {
		t.Fatal("value not found in combined storage")
	}
	_, found = eng.resultCache.Get(single)
	if !found {
		t.Fatal("multihash not moved to cache after miss get")
	}

	// Updating an existing multihash should also update cache
	err = eng.Put(value2, single)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	values, _, _ := eng.valueStore.Get(single)
	if len(values) != 2 {
		t.Fatal("values not updated in value store")
	}
	values, _ = eng.resultCache.Get(single)
	if len(values) != 2 {
		t.Fatal("values not updated in resutl cache")
	}

	// Remove should apply to both storages
	err = eng.Remove(value1, single)
	if err != nil {
		t.Fatal(err)
	}
	values, _, _ = eng.valueStore.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from value store")
	}
	values, _ = eng.resultCache.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from result cache")
	}

	// Putting many should only update in cache the ones
	// already stored, adding all to value store.
	err = eng.Put(value1, mhs[2:]...)
	if err != nil {
		t.Fatal("Error putting multiple multihashes:", err)
	}
	_, found = eng.resultCache.Get(mhs[4])
	if found {
		t.Fatal("mhs[4] should not be in result cache")
	}

	values, _, _ = eng.valueStore.Get(single)
	if len(values) != 2 {
		t.Fatal("value not updated in value store after PutMany")
	}
	values, _ = eng.resultCache.Get(single)
	if len(values) != 2 {
		t.Fatal("value not updated in result cache after PutMany")
	}

	// This multihash should only be found in value store
	_, found, _ = eng.valueStore.Get(mhs[4])
	if !found {
		t.Fatal("single put did not go to value store")
	}
	_, found = eng.resultCache.Get(mhs[4])
	if found {
		t.Fatal("single put went to result cache")
	}

	// RemoveMany should remove the corresponding from both storages
	err = eng.Remove(value1, mhs[2:]...)
	if err != nil {
		t.Fatal("Error removing multiple multihashes:", err)
	}
	values, _, _ = eng.valueStore.Get(single)
	if len(values) != 1 {
		t.Fatal("values not removed from value store")
	}
	values, _ = eng.resultCache.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from result cache after RemoveMany")
	}

	_, found, _ = eng.valueStore.Get(mhs[4])
	if found {
		t.Fatal("remove many did not remove values from value store")
	}
	_, found = eng.resultCache.Get(mhs[4])
	if found {
		t.Fatal("remove many did not remove value from result cache")
	}

	err = eng.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProvider(t *testing.T) {
	eng := initEngine(t, true, true)

	prov1, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}
	prov2, err := peer.Decode("12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV")
	if err != nil {
		t.Fatal(err)
	}
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

	mhs := test.RandomMultihashes(15)

	batch1 := mhs[:5]
	batch2 := mhs[5:10]
	batch3 := mhs[10:15]

	// Put a batches of multihashes
	t.Log("Put batch1 value (provider1 context1)")
	if err = eng.Put(value1, batch1...); err != nil {
		t.Fatal(err)
	}
	t.Log("Put batch2 values (provider1 context1), (provider1 context2)")
	if err = eng.Put(value1, batch2...); err != nil {
		t.Fatal(err)
	}
	if err = eng.Put(value2, batch2...); err != nil {
		t.Fatal(err)
	}
	t.Log("Put batch3 values (provider1 context2), (provider2 context1)")
	if err = eng.Put(value2, batch3...); err != nil {
		t.Fatal(err)
	}
	if err = eng.Put(value3, batch3...); err != nil {
		t.Fatal(err)
	}

	stats := eng.resultCache.Stats()
	if stats.Values != 3 {
		t.Fatalf("Wrong number of values; expected 3, got %d", stats.Values)
	}

	t.Log("Removing provider1")
	if err = eng.RemoveProvider(prov1); err != nil {
		t.Fatalf("Error removing provider: %s", err)
	}
	_, found, err := eng.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	_, found, err = eng.Get(mhs[1])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	_, found, err = eng.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	vals, found, err := eng.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 1 {
		t.Fatalf("wrong number of values removed for batch3, expected 1 got %d", len(vals))
	}

	stats = eng.resultCache.Stats()
	if stats.Values != 1 {
		t.Fatalf("Wrong number of values; expected 1, got %d", stats.Values)
	}

	t.Log("Removing provider2")
	if err = eng.RemoveProvider(prov2); err != nil {
		t.Fatalf("Error removing provider: %s", err)
	}
	_, found, err = eng.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}

	stats = eng.resultCache.Stats()
	if stats.Values != 0 {
		t.Fatalf("Wrong number of values; expected 0, got %d", stats.Values)
	}

	err = eng.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCacheOnPut(t *testing.T) {
	eng := initEngine(t, true, true)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := test.RandomMultihashes(3)

	value1 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("mtadata-1"),
	}
	value2 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[1]),
		MetadataBytes: []byte("mtadata-2"),
	}

	single := mhs[2]

	// First put should go to value store and cache
	err = eng.Put(value1, single)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	_, found, _ := eng.valueStore.Get(single)
	if !found {
		t.Fatal("single put did not go to value store")
	}
	_, found = eng.resultCache.Get(single)
	if !found {
		t.Fatal("single put did not go to result cache")
	}

	// Updating an existing multihash should also update cache
	err = eng.Put(value2, single)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	values, _, _ := eng.valueStore.Get(single)
	if len(values) != 2 {
		t.Fatal("values not updated in value store")
	}
	values, _ = eng.resultCache.Get(single)
	if len(values) != 2 {
		t.Fatal("values not updated in resutl cache")
	}

	err = eng.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProviderContext(t *testing.T) {
	eng := initEngine(t, true, true)

	// Create new valid peer.ID
	prov1, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}
	prov2, err := peer.Decode("12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV")
	if err != nil {
		t.Fatal(err)
	}

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

	mhs := test.RandomMultihashes(15)

	batch1 := mhs[:5]
	batch2 := mhs[5:10]
	batch3 := mhs[10:15]

	// Put a batches of multihashes
	t.Log("Put batch1 value (provider1 context1)")
	if err = eng.Put(value1, batch1...); err != nil {
		t.Fatal(err)
	}
	t.Log("Put batch2 values (provider1 context1), (provider1 context2)")
	if err = eng.Put(value1, batch2...); err != nil {
		t.Fatal(err)
	}
	if err = eng.Put(value2, batch2...); err != nil {
		t.Fatal(err)
	}
	t.Log("Put batch3 values (provider1 context2), (provider2 context1)")
	if err = eng.Put(value2, batch3...); err != nil {
		t.Fatal(err)
	}
	if err = eng.Put(value3, batch3...); err != nil {
		t.Fatal(err)
	}

	// Verify starting with correct values
	vals, found, err := eng.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 1 {
		t.Fatalf("wrong number of multihashes, expected 1 got %d", len(vals))
	}
	vals, found, err = eng.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 2 {
		t.Fatalf("wrong number of multihashes, expected 2 got %d", len(vals))
	}
	vals, found, err = eng.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 2 {
		t.Fatalf("wrong number of multihashes, expected 2 got %d", len(vals))
	}

	t.Log("Removing provider1 context1")
	if err = eng.RemoveProviderContext(prov1, ctx1id); err != nil {
		t.Fatalf("Error removing provider context: %s", err)
	}
	_, found, err = eng.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	_, found, err = eng.Get(mhs[1])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	vals, found, err = eng.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 1 {
		t.Fatalf("wrong number of multihashes removed for bathc2, expected 2 got %d", len(vals))
	}
	if !vals[0].Equal(value2) {
		t.Fatal("Wrong value removed")
	}
	vals, found, err = eng.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 2 {
		t.Fatalf("wrong number of multihashes removed for batch3, expected 2 got %d", len(vals))
	}

	t.Log("Removing provider1 context2")
	if err = eng.RemoveProviderContext(prov1, ctx2id); err != nil {
		t.Fatalf("Error removing provider context: %s", err)
	}
	_, found, err = eng.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	vals, found, err = eng.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 1 {
		t.Fatal("wrong number of multihashes removed")
	}
	if !vals[0].Equal(value3) {
		t.Fatal("Wrong value removed")
	}

	t.Log("Removing provider2 context1")
	if err = eng.RemoveProviderContext(prov2, ctx1id); err != nil {
		t.Fatalf("Error removing provider context: %s", err)
	}
	_, found, err = eng.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should not have been found")
	}

	err = eng.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOnlyValueStore(t *testing.T) {
	eng := initEngine(t, false, false)
	e2e(t, eng)
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoth(t *testing.T) {
	eng := initEngine(t, true, false)
	e2e(t, eng)
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
}

func e2e(t *testing.T, eng *Engine) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := test.RandomMultihashes(15)

	value1 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("mtadata-1"),
	}
	value2 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[1]),
		MetadataBytes: []byte("mtadata-2"),
	}

	single := mhs[2]
	noadd := mhs[3]
	batch := mhs[4:]
	remove := mhs[4]

	// Put a single multihash
	t.Logf("Put/Get a single multihash in storage")
	err = eng.Put(value1, single)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}

	i, found, err := eng.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding single multihash")
	}
	if !i[0].Equal(value1) {
		t.Errorf("Got wrong value for single multihash")
	}

	// Put a batch of multihashes
	t.Logf("Put/Get a batch of multihashes in storage")
	err = eng.Put(value1, batch...)
	if err != nil {
		t.Fatal("Error putting batch of multihashse:", err)
	}

	i, found, err = eng.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding a multihash from the batch")
	}
	if !i[0].Equal(value1) {
		t.Errorf("Got wrong value for single multihash")
	}

	// Put on an existing key
	t.Logf("Put/Get on existing key")
	err = eng.Put(value2, single)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	i, found, err = eng.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding a multihash from the batch")
	}
	if len(i) != 2 {
		t.Fatal("Update over existing key not correct")
	}
	if !i[1].Equal(value2) {
		t.Errorf("Got wrong value for single multihash")
	}

	// Get a key that is not set
	t.Logf("Get non-existing key")
	_, found, err = eng.Get(noadd)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("Error, the key for the multihash shouldn't be set")
	}

	// Remove a key
	t.Logf("Remove key")
	err = eng.Remove(value1, remove)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}

	_, found, err = eng.Get(remove)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("multihash should have been removed")
	}

	// Remove a value from the key
	err = eng.Remove(value1, single)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	i, found, err = eng.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("multihash should still have one value")
	}
	if len(i) != 1 {
		t.Errorf("wrong number of values after remove")
	}

}

func SizeTest(t *testing.T) {
	eng := initEngine(t, true, false)
	// Init storage
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := test.RandomMultihashes(151)

	value := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("mtadata"),
	}

	err = eng.Put(value, mhs[1:]...)
	if err != nil {
		t.Fatal(err)
	}

	size, err := eng.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size == int64(0) {
		t.Error("failed to compute storage size")
	}

	err = eng.Close()
	if err != nil {
		t.Fatal(err)
	}
}
