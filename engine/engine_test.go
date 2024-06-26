package engine

import (
	"context"
	"testing"

	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/cache"
	"github.com/ipni/go-indexer-core/cache/radixcache"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-indexer-core/store/pebble"
	"github.com/ipni/go-indexer-core/store/vsinfo"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func initEngine(t *testing.T, withCache, cacheOnPut bool) *Engine {
	valueStore, err := pebble.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var resultCache cache.Interface
	if withCache {
		resultCache = radixcache.New(100000)
	}
	eng := New(valueStore, WithCache(resultCache), WithCacheOnPut(cacheOnPut))
	t.Cleanup(func() {
		if err := eng.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return eng
}

func TestPassthrough(t *testing.T) {
	eng := initEngine(t, true, false)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := random.Multihashes(5)

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

	// First Put should go to value store.
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

	// Getting the value should put it in cache.
	v, found, _ := eng.Get(single)
	if !found || !v[0].Equal(value1) {
		t.Fatal("value not found in combined storage")
	}
	_, found = eng.resultCache.Get(single)
	if !found {
		t.Fatal("multihash not moved to cache after miss get")
	}

	// Updating an existing multihash should also update cache.
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

	// Remove should apply to both storage and cache.
	err = eng.Remove(value1, single)
	if err != nil {
		t.Fatal(err)
	}
	eng.valueStore.Flush()
	values, _, _ = eng.valueStore.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from value store")
	}
	values, _ = eng.resultCache.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from result cache")
	}

	// Put should only update in cache the ones already stored, adding all to
	// value store.
	//
	// Make copy so that tests are not affected by Put reordering multihashes.
	mhsCpy := make([]multihash.Multihash, len(mhs)-2)
	copy(mhsCpy, mhs[2:])
	err = eng.Put(value1, mhsCpy...)
	if err != nil {
		t.Fatal("Error putting multiple multihashes:", err)
	}
	eng.valueStore.Flush()
	_, found = eng.resultCache.Get(mhs[4])
	if found {
		t.Fatal("mhs[4] should not be in result cache")
	}

	values, _, _ = eng.valueStore.Get(single)
	if len(values) != 2 {
		t.Fatal("value not updated in value store after Put")
	}
	values, _ = eng.resultCache.Get(single)
	if len(values) != 2 {
		t.Fatal("value not updated in result cache after Put")
	}

	// This multihash should only be found in value store.
	_, found, _ = eng.valueStore.Get(mhs[4])
	if !found {
		t.Fatal("single put did not go to value store")
	}
	_, found = eng.resultCache.Get(mhs[4])
	if found {
		t.Fatal("single put went to result cache")
	}

	// Remove should remove indexes from both storage and cache.
	err = eng.Remove(value1, mhsCpy...)
	if err != nil {
		t.Fatal("Error removing multiple multihashes:", err)
	}
	values, _, _ = eng.valueStore.Get(single)
	if len(values) != 1 {
		t.Fatal("values not removed from value store")
	}
	values, _ = eng.resultCache.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from result cache after Remove")
	}

	_, found, _ = eng.valueStore.Get(mhs[4])
	if found {
		t.Fatal("remove did not remove value from value store")
	}
	_, found = eng.resultCache.Get(mhs[4])
	if found {
		t.Fatal("remove did not remove value from result cache")
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

	mhs := random.Multihashes(15)

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
	if err = eng.RemoveProvider(context.Background(), prov1); err != nil {
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
	if err = eng.RemoveProvider(context.Background(), prov2); err != nil {
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

	mhs := random.Multihashes(3)

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

	mhs := random.Multihashes(15)

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
}

func TestOnlyValueStore(t *testing.T) {
	eng := initEngine(t, false, false)
	e2e(t, eng)
}

func TestBoth(t *testing.T) {
	eng := initEngine(t, true, false)
	e2e(t, eng)
}

func TestMultiCodec(t *testing.T) {
	const vsType = "pebble"

	// Force the codec to be json and create engine instance.
	tempDir := t.TempDir()
	vsi, err := vsinfo.Load(tempDir, vsType)
	if err != nil {
		t.Fatal(err)
	}
	vsi.Codec = vsinfo.JsonCodec
	if err = vsi.Save(tempDir); err != nil {
		t.Fatal(err)
	}
	vsi, err = vsinfo.Load(tempDir, vsType)
	if err != nil {
		t.Fatal(err)
	}
	if vsi.Codec != vsinfo.JsonCodec {
		t.Fatal("Codec should be", vsinfo.JsonCodec, "got", vsi.Codec)
	}
	var valueStore indexer.Interface
	switch vsType {
	case "memmory":
		valueStore = memory.New()
	case "pebble":
		valueStore, err = pebble.New(tempDir, nil)
	default:
		t.Fatal("vsType must be memory or pebble")
	}
	if err != nil {
		t.Fatal(err)
	}
	eng := New(valueStore)

	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := random.Multihashes(5)

	value1 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("0{metadata-1}"),
	}
	value2 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[1]),
		MetadataBytes: []byte("{metadata-2}"),
	}

	key := mhs[2]

	// Store value1 using the json codec
	err = eng.Put(value1, key)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}

	err = eng.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Change codec from Json to BinaryJson and start new engine.
	vsi, err = vsinfo.Load(tempDir, vsType)
	if err != nil {
		t.Fatal(err)
	}
	vsi.Codec = vsinfo.BinaryJsonCodec
	if err = vsi.Save(tempDir); err != nil {
		t.Fatal(err)
	}
	switch vsType {
	case "memory":
		valueStore = memory.New()
	case "pebble":
		valueStore, err = pebble.New(tempDir, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	eng = New(valueStore)
	t.Cleanup(func() {
		if err := eng.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Confirm that codec is BinaryJson after starting engine.
	vsi, err = vsinfo.Load(tempDir, vsType)
	if err != nil {
		t.Fatal(err)
	}
	if vsi.Codec != vsinfo.BinaryJsonCodec {
		t.Fatal("Codec should be", vsinfo.BinaryJsonCodec, "got", vsi.Codec)
	}

	// Store value2 using Binary codec
	err = eng.Put(value2, key)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}

	// Get both values, and confirm they are retrieved.
	vals, found, err := eng.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding multihash")
	}
	if len(vals) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(vals))
	}
	if !vals[0].Equal(value1) {
		t.Errorf("Got wrong first value")
	}
	if !vals[1].Equal(value2) {
		t.Errorf("Got wrong second value")
	}
}

func e2e(t *testing.T, eng *Engine) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := random.Multihashes(15)

	value1 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("metadata-1"),
	}
	value2 := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[1]),
		MetadataBytes: []byte("metadata-2"),
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

	mhs := random.Multihashes(151)

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
}
