package storethehash

import (
	"io/ioutil"
	"runtime"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/filecoin-project/go-indexer-core/store/test"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func initSth() (store.Interface, error) {
	tmpDir, err := ioutil.TempDir("", "sth")
	if err != nil {
		return nil, err
	}
	return New(tmpDir)
}

func TestE2E(t *testing.T) {
	s, err := initSth()
	if err != nil {
		t.Fatal(err)
	}
	test.E2ETest(t, s)
}

func TestSize(t *testing.T) {
	s, err := initSth()
	if err != nil {
		t.Fatal(err)
	}
	test.SizeTest(t, s)
}

func TestRemoveMany(t *testing.T) {
	s, err := initSth()
	if err != nil {
		t.Fatal(err)
	}
	test.RemoveManyTest(t, s)
}

func TestPeriodicFlush(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}
	// Init storage
	tmpDir, err := ioutil.TempDir("", "sth")
	if err != nil {
		t.Fatal(err)
	}

	s, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	// Put some data in the first storage.
	cids, err := test.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	entry := entry.MakeValue(p, 0, cids[0].Bytes())
	for _, c := range cids[1:] {
		_, err = s.Put(c, entry)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Sleep for 2 sync Intervals to ensure that data is flushed
	time.Sleep(2 * DefaultSyncInterval)

	// Regenerate new storage
	s2, err := New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Get data. If re-generated correctly we should find the CID
	i, found, err := s2.Get(cids[3])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("Error finding single cid")
	}
	if !i[0].Equal(entry) {
		t.Errorf("Got wrong value for single cid")
	}

}

func TestRefC(t *testing.T) {
	// NOTE: This test is flaky in Windows. CI fails with
	// runtime: out of memory: cannot allocate 134217728-byte block (1417543680 in use)
	// fatal error: out of memory
	// Skipping it for now, but we should revisit this.
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}
	sint, err := initSth()
	if err != nil {
		t.Fatal(err)
	}
	s := sint.(*sthStorage)

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

func checkRefC(t *testing.T, s *sthStorage, k []byte, refC uint64) {
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
