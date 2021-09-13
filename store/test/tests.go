package test

import (
	"sync"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func E2ETest(t *testing.T, s store.Interface) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs, err := RandomMultihashes(15)
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
	t.Logf("Put/Get a single multihash")
	_, err = s.Put(single, value1)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}

	i, found, err := s.Get(single)
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
	t.Logf("Put/Get a batch of multihashes")
	err = s.PutMany(batch, value1)
	if err != nil {
		t.Fatal("Error putting batch of multihashes:", err)
	}

	i, found, err = s.Get(mhs[5])
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
	_, err = s.Put(single, value2)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	i, found, err = s.Get(single)
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
	_, found, err = s.Get(noadd)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("Error, the key for the multihash should not be set")
	}

	// Remove a key
	t.Logf("Remove key")
	_, err = s.Remove(remove, value1)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}

	_, found, err = s.Get(remove)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("multihash should have been removed")
	}

	// Remove a value from the key
	_, err = s.Remove(single, value1)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}
	i, found, err = s.Get(single)
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

func SizeTest(t *testing.T, s store.Interface) {
	// Init storage
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs, err := RandomMultihashes(151)
	if err != nil {
		t.Fatal(err)
	}

	value := indexer.MakeValue(p, protocolID, []byte(mhs[0]))
	for _, c := range mhs[1:] {
		_, err = s.Put(c, value)
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

	mhs, err := RandomMultihashes(15)
	if err != nil {
		t.Fatal(err)
	}

	value := indexer.MakeValue(p, protocolID, []byte(mhs[0]))
	batch := mhs[1:]

	// Put a batch of multihashes
	t.Logf("Put/Get a batch of multihashes")
	err = s.PutMany(batch, value)
	if err != nil {
		t.Fatal("Error putting batch of multihashes:", err)
	}

	// Put a single multihash
	t.Logf("Remove key")
	err = s.RemoveMany(mhs[2:], value)
	if err != nil {
		t.Fatal("Error putting single multihash:", err)
	}

	i, found, err := s.Get(mhs[1])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("multihash should not have been removed")
	}
	if len(i) != 1 {
		t.Errorf("wrong number of multihashes removed")
	}

}

func ParallelUpdateTest(t *testing.T, s store.Interface) {
	mhs, err := RandomMultihashes(15)
	if err != nil {
		t.Fatal(err)
	}

	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	single := mhs[14]

	var wg sync.WaitGroup

	// Test parallel writes over same multihash
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			t.Logf("Put/Get different multihash")
			value := indexer.MakeValue(p, 0, []byte(mhs[i]))
			_, err := s.Put(single, value)
			if err != nil {
				t.Error("Error putting single multihash:", err)
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
		t.Errorf("Error finding single multihash")
	}
	if len(x) != 5 {
		t.Error("Value has not been updated by routines correctly", len(x))
	}

	// Test remove for all except one
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			t.Logf("Remove multihash")
			value := indexer.MakeValue(p, 0, []byte(mhs[i]))
			_, err := s.Remove(single, value)
			if err != nil {
				t.Error("Error removing single multihash:", err)
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
		t.Errorf("Error finding single multihash")
	}
	if len(x) != 1 {
		t.Error("Value has not been removed by routines correctly", len(x))
	}
}
