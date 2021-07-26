package indexer

import (
	"io/ioutil"
	"testing"

	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/go-indexer-core/store/test"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

const protocolID = 0

func initStorage(t *testing.T, withCache bool) *Engine {
	tmpDir, err := ioutil.TempDir("", "sth")
	if err != nil {
		t.Fatal(err)
	}
	var resultCache cache.Interface
	var valueStore store.Interface

	valueStore, err = storethehash.New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if withCache {
		resultCache = radixcache.New(100000)
	}
	return NewEngine(resultCache, valueStore)
}

func TestPassthrough(t *testing.T) {
	s := initStorage(t, true)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := test.RandomCids(5)
	if err != nil {
		t.Fatal(err)
	}

	entry1 := entry.MakeValue(p, protocolID, cids[0].Bytes())
	entry2 := entry.MakeValue(p, protocolID, cids[1].Bytes())
	single := cids[2]

	// First put should go to value store
	_, err = s.Put(single, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	_, found, _ := s.valueStore.Get(single)
	if !found {
		t.Fatal("single put did not go to value store")
	}
	_, found, _ = s.resultCache.Get(single)
	if found {
		t.Fatal("single put went to result cache")
	}

	// Getting the value should put it in cache
	v, found, _ := s.Get(single)
	if !found || !v[0].Equal(entry1) {
		t.Fatal("value not found in combined storage")
	}
	_, found, _ = s.resultCache.Get(single)
	if !found {
		t.Fatal("cid not moved to cache after miss get")
	}

	// Updating an existing CID should also update cache
	_, err = s.Put(single, entry2)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	values, _, _ := s.valueStore.Get(single)
	if len(values) != 2 {
		t.Fatal("values not updated in value store")
	}
	values, _, _ = s.resultCache.Get(single)
	if len(values) != 2 {
		t.Fatal("values not updated in resutl cache")
	}

	// Remove should apply to both storages
	_, err = s.Remove(single, entry1)
	if err != nil {
		t.Fatal(err)
	}
	values, _, _ = s.valueStore.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from value store")
	}
	values, _, _ = s.resultCache.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from result cache")
	}

	// Putting many should only update in cache the ones
	// already stored, adding all to value store.
	err = s.PutMany(cids[2:], entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	values, _, _ = s.valueStore.Get(single)
	if len(values) != 2 {
		t.Fatal("value not updated in value store after PutMany")
	}
	values, _, _ = s.resultCache.Get(single)
	if len(values) != 2 {
		t.Fatal("value not updated in result cache after PutMany")
	}

	// This CID should only be found in value store
	_, found, _ = s.valueStore.Get(cids[4])
	if !found {
		t.Fatal("single put did not go to value store")
	}
	_, found, _ = s.resultCache.Get(cids[4])
	if found {
		t.Fatal("single put went to result cache")
	}

	// RemoveMany should remove the corresponding from both storages.
	err = s.RemoveMany(cids[2:], entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	values, _, _ = s.valueStore.Get(single)
	if len(values) != 1 {
		t.Fatal("values not removed from value store")
	}
	values, _, _ = s.resultCache.Get(single)
	if len(values) != 1 {
		t.Fatal("value not removed from result cache after RemoveMany")
	}

	_, found, _ = s.valueStore.Get(cids[4])
	if found {
		t.Fatal("remove many did not remove values from value store")
	}
	_, found, _ = s.resultCache.Get(cids[4])
	if found {
		t.Fatal("remove many did not remove value from result cache")
	}
}

func TestOnlyValueStore(t *testing.T) {
	s := initStorage(t, false)
	e2e(t, s)
}

func TestBoth(t *testing.T) {
	s := initStorage(t, true)
	e2e(t, s)
}

func e2e(t *testing.T, s *Engine) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := test.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	entry1 := entry.MakeValue(p, protocolID, cids[0].Bytes())
	entry2 := entry.MakeValue(p, protocolID, cids[1].Bytes())

	single := cids[2]
	noadd := cids[3]
	batch := cids[4:]
	remove := cids[4]

	// Put a single CID
	t.Logf("Put/Get a single CID in storage")
	_, err = s.Put(single, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}

	i, found, err := s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding single cid")
	}
	if !i[0].Equal(entry1) {
		t.Errorf("Got wrong value for single cid")
	}

	// Put a batch of CIDs
	t.Logf("Put/Get a batch of CIDs in storage")
	err = s.PutMany(batch, entry1)
	if err != nil {
		t.Fatal("Error putting batch of cids: ", err)
	}

	i, found, err = s.Get(cids[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding a cid from the batch")
	}
	if !i[0].Equal(entry1) {
		t.Errorf("Got wrong value for single cid")
	}

	// Put on an existing key
	t.Logf("Put/Get on existing key")
	_, err = s.Put(single, entry2)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	i, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding a cid from the batch")
	}
	if len(i) != 2 {
		t.Fatal("Update over existing key not correct")
	}
	if !i[1].Equal(entry2) {
		t.Errorf("Got wrong value for single cid")
	}

	// Get a key that is not set
	t.Logf("Get non-existing key")
	_, found, err = s.Get(noadd)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("Error, the key for the cid shouldn't be set")
	}

	// Remove a key
	t.Logf("Remove key")
	_, err = s.Remove(remove, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}

	_, found, err = s.Get(remove)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("cid should have been removed")
	}

	// Remove an entry from the key
	_, err = s.Remove(single, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	i, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("cid should still have one entry")
	}
	if len(i) != 1 {
		t.Errorf("wrong number of entries after remove")
	}

}

func SizeTest(t *testing.T) {
	s := initStorage(t, true)
	// Init storage
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := test.RandomCids(151)
	if err != nil {
		t.Fatal(err)
	}

	entry := entry.MakeValue(p, protocolID, cids[0].Bytes())
	for _, c := range cids[1:] {
		_, err = s.Put(c, entry)
		if err != nil {
			t.Fatal(err)
		}
	}

	size, err := s.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size == int64(0) {
		t.Error("failed to compute storage size")
	}
}
