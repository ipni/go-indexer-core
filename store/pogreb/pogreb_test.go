package pogreb

import (
	"io/ioutil"
	"runtime"
	"sync"
	"testing"

	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"
	peer "github.com/libp2p/go-libp2p-core/peer"

	"github.com/filecoin-project/go-indexer-core/store/test"
)

func initPogreb() (store.Interface, error) {
	tmpDir, err := ioutil.TempDir("", "sth")
	if err != nil {
		return nil, err
	}
	return New(tmpDir)
}

func TestE2E(t *testing.T) {
	skipIf32bit(t)

	s, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	test.E2ETest(t, s)
}

func TestSize(t *testing.T) {
	skipIf32bit(t)

	s, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	test.SizeTest(t, s)
}

func TestRemoveMany(t *testing.T) {
	skipIf32bit(t)

	s, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	test.RemoveManyTest(t, s)
}

func TestRefC(t *testing.T) {
	skipIf32bit(t)

	sint, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	s := sint.(*pStorage)
	cids, err := test.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	entry1 := entry.MakeValue(p, 0, cids[0].Bytes())
	kEnt1, err := store.EntryKey(entry1)
	if err != nil {
		t.Fatal(err)
	}

	first := cids[2]
	second := cids[3]

	// Put a single CID
	t.Logf("Put/Get first ref")
	_, err = s.Put(first, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	checkRefC(t, s, kEnt1, 1)

	t.Logf("Put/Get second ref")
	_, err = s.Put(second, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	checkRefC(t, s, kEnt1, 2)

	t.Logf("Put/Get same value again")
	_, err = s.Put(second, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	checkRefC(t, s, kEnt1, 2)

	t.Logf("Remove second")
	_, err = s.Remove(second, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	checkRefC(t, s, kEnt1, 1)

	t.Logf("Remove first")
	_, err = s.Remove(first, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	ent, found, err := s.getEntry(kEnt1)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("Entry should have been removed with RefC==0", ent)
	}

}

func TestParallelRefC(t *testing.T) {
	skipIf32bit(t)
	sint, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	cids, err := test.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}
	s := sint.(*pStorage)

	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	entry1 := entry.MakeValue(p, 0, cids[0].Bytes())
	kEnt1, err := store.EntryKey(entry1)
	if err != nil {
		t.Fatal(err)
	}
	entry2 := entry.MakeValue(p, 0, cids[1].Bytes())
	kEnt2, err := store.EntryKey(entry2)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	// Test parallel writes over different CIDs
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			t.Logf("Put/Get different cid")
			_, err := s.Put(cids[i], entry1)
			if err != nil {
				t.Error("Error putting single cid: ", err)
			}
			wg.Done()
		}(&wg, i)
	}
	wg.Wait()
	checkRefC(t, s, kEnt1, 5)

	// Test parallel writes over different CIDs
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			t.Logf("Put/Get same cid")
			_, err := s.Put(cids[10], entry2)
			if err != nil {
				t.Error("Error putting single cid: ", err)
			}
			wg.Done()
		}(&wg)
	}
	wg.Wait()
	checkRefC(t, s, kEnt2, 1)

	// Test remove for all except one
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			t.Logf("Remove cid")
			_, err := s.Remove(cids[i], entry1)
			if err != nil {
				t.Error("Error removing single cid: ", err)
			}
			wg.Done()
		}(&wg, i)
	}
	wg.Wait()
	checkRefC(t, s, kEnt1, 1)
}

func checkRefC(t *testing.T, s *pStorage, k []byte, refC uint64) {
	ent, found, err := s.getEntry(k)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding single cid")
	}
	if ent.RefC != refC {
		t.Error("RefCount is not correct:", ent.RefC)
	}
}

func skipIf32bit(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Pogreb cannot use GOARCH=386")
	}
}
