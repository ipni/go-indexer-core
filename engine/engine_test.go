package engine

import (
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/go-indexer-core/store/test"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

const protocolID = 0

func initEngine(t *testing.T, withCache bool) *Engine {
	var tmpDir string
	var err error
	if runtime.GOOS == "windows" {
		tmpDir, err = ioutil.TempDir("", "sth")
		if err != nil {
			t.Fatal(err)
		}
	} else {
		tmpDir = t.TempDir()
	}
	valueStore, err := storethehash.New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	var resultCache cache.Interface
	if withCache {
		resultCache = radixcache.New(100000)
	}
	return New(resultCache, valueStore)
}

func TestPassthrough(t *testing.T) {
	eng := initEngine(t, true)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs, err := test.RandomMultihashes(5)
	if err != nil {
		t.Fatal(err)
	}

	value1 := indexer.MakeValue(p, protocolID, []byte(mhs[0]))
	value2 := indexer.MakeValue(p, protocolID, []byte(mhs[1]))
	single := mhs[2]

	// First put should go to value store
	_, err = eng.Put(single, value1)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	_, found, _ := eng.valueStore.Get(single)
	if !found {
		t.Fatal("single put did not go to value store")
	}
	_, found, _ = eng.resultCache.Get(single)
	if found {
		t.Fatal("single put went to result cache")
	}

	// Getting the value should put it in cache
	v, found, _ := eng.Get(single)
	if !found || !v[0].Equal(value1) {
		t.Fatal("value not found in combined storage")
	}
	_, found, _ = eng.resultCache.Get(single)
	if !found {
		t.Fatal("multihash not moved to cache after miss get")
	}

	// Updating an existing multihash should also update cache
	_, err = eng.Put(single, value2)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	values, _, _ := eng.valueStore.Get(single)
	if len(values) != 2 {
		t.Fatal("values not updated in value store")
	}
	values, _, _ = eng.resultCache.Get(single)
	if len(values) != 2 {
		t.Fatal("values not updated in resutl cache")
	}

	// Remove should apply to both storages
	_, err = eng.Remove(single, value1)
	if err != nil {
		t.Fatal(err)
	}
	values, _, _ = eng.valueStore.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from value store")
	}
	values, _, _ = eng.resultCache.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from result cache")
	}

	// Putting many should only update in cache the ones
	// already stored, adding all to value store.
	err = eng.PutMany(mhs[2:], value1)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	values, _, _ = eng.valueStore.Get(single)
	if len(values) != 2 {
		t.Fatal("value not updated in value store after PutMany")
	}
	values, _, _ = eng.resultCache.Get(single)
	if len(values) != 2 {
		t.Fatal("value not updated in result cache after PutMany")
	}

	// This multihash should only be found in value store
	_, found, _ = eng.valueStore.Get(mhs[4])
	if !found {
		t.Fatal("single put did not go to value store")
	}
	_, found, _ = eng.resultCache.Get(mhs[4])
	if found {
		t.Fatal("single put went to result cache")
	}

	// RemoveMany should remove the corresponding from both storages
	err = eng.RemoveMany(mhs[2:], value1)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	values, _, _ = eng.valueStore.Get(single)
	if len(values) != 1 {
		t.Fatal("values not removed from value store")
	}
	values, _, _ = eng.resultCache.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from result cache after RemoveMany")
	}

	_, found, _ = eng.valueStore.Get(mhs[4])
	if found {
		t.Fatal("remove many did not remove values from value store")
	}
	_, found, _ = eng.resultCache.Get(mhs[4])
	if found {
		t.Fatal("remove many did not remove value from result cache")
	}
}

func TestOnlyValueStore(t *testing.T) {
	e2e(t, initEngine(t, false))
}

func TestBoth(t *testing.T) {
	e2e(t, initEngine(t, true))
}

func e2e(t *testing.T, eng *Engine) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs, err := test.RandomMultihashes(15)
	if err != nil {
		t.Fatal(err)
	}

	value1 := indexer.MakeValue(p, protocolID, []byte(mhs[0]))
	value2 := indexer.MakeValue(p, protocolID, []byte(mhs[1]))

	single := mhs[2]
	noadd := mhs[3]
	batch := mhs[4:]
	remove := mhs[4]

	// Put a single multihash
	t.Logf("Put/Get a single multihash in storage")
	_, err = eng.Put(single, value1)
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
	err = eng.PutMany(batch, value1)
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
	_, err = eng.Put(single, value2)
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
	_, err = eng.Remove(remove, value1)
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
	_, err = eng.Remove(single, value1)
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
	eng := initEngine(t, true)
	// Init storage
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs, err := test.RandomMultihashes(151)
	if err != nil {
		t.Fatal(err)
	}

	value := indexer.MakeValue(p, protocolID, []byte(mhs[0]))
	for _, c := range mhs[1:] {
		_, err = eng.Put(c, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	size, err := eng.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size == int64(0) {
		t.Error("failed to compute storage size")
	}
}
