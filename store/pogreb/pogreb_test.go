package pogreb

import (
	"io/ioutil"
	"runtime"
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

	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := test.RandomCids(15)
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
		t.Fatal("Entry should have been removed with RefC==0", ent)
	}
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
		t.Fatal("RefCount should have not changed:", ent.RefC)
	}
}

func skipIf32bit(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Pogreb cannot use GOARCH=386")
	}
}
