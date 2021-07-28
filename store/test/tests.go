package test

import (
	"sync"
	"testing"

	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func E2ETest(t *testing.T, s store.Interface) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := RandomCids(15)
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
	t.Logf("Put/Get a single CID")
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
	t.Logf("Put/Get a batch of CIDs")
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

func SizeTest(t *testing.T, s store.Interface) {
	// Init storage
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := RandomCids(151)
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

func RemoveManyTest(t *testing.T, s store.Interface) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	entry := entry.MakeValue(p, protocolID, cids[0].Bytes())
	batch := cids[1:]

	// Put a batch of CIDs
	t.Logf("Put/Get a batch of CIDs")
	err = s.PutMany(batch, entry)
	if err != nil {
		t.Fatal("Error putting batch of cids: ", err)
	}

	// Put a single CID
	t.Logf("Remove key")
	err = s.RemoveMany(cids[2:], entry)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}

	i, found, err := s.Get(cids[1])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("cid should not have been removed")
	}
	if len(i) != 1 {
		t.Errorf("wrong number of cids removed")
	}

}

func ParallelUpdateTest(t *testing.T, s store.Interface) {
	cids, err := RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	single := cids[14]

	var wg sync.WaitGroup

	// Test parallel writes over same CID
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			t.Logf("Put/Get different cid")
			entry := entry.MakeValue(p, 0, cids[i].Bytes())
			_, err := s.Put(single, entry)
			if err != nil {
				t.Error("Error putting single cid: ", err)
			}
			wg.Done()
		}(&wg, i)
	}
	wg.Wait()
	x, found, err := s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding single cid")
	}
	if len(x) != 5 {
		t.Error("Entry has not been updated by routines correctly", len(x))
	}

	// Test remove for all except one
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			t.Logf("Remove cid")
			entry := entry.MakeValue(p, 0, cids[i].Bytes())
			_, err := s.Remove(single, entry)
			if err != nil {
				t.Error("Error removing single cid: ", err)
			}
			wg.Done()
		}(&wg, i)
	}
	wg.Wait()
	x, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding single cid")
	}
	if len(x) != 1 {
		t.Error("Entry has not been removed by routines correctly", len(x))
	}
}
