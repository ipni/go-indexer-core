package storethehash_test

import (
	"io/ioutil"
	"runtime"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/go-indexer-core/store/test"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func initSth(t *testing.T) store.Interface {
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
	t.Log("---> tempdir:", tmpDir)
	s, err := storethehash.New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestE2E(t *testing.T) {
	s := initSth(t)
	test.E2ETest(t, s)
}

func TestSize(t *testing.T) {
	s := initSth(t)
	test.SizeTest(t, s)
}

func TestRemoveMany(t *testing.T) {
	s := initSth(t)
	test.RemoveManyTest(t, s)
}

func TestParallel(t *testing.T) {
	s := initSth(t)
	test.ParallelUpdateTest(t, s)
}

func TestPeriodicFlush(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}
	// Init storage
	tmpDir := t.TempDir()

	s, err := storethehash.New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	// Put some data in the first storage.
	cids, err := test.RandomCids(151)
	if err != nil {
		t.Fatal(err)
	}

	value := indexer.MakeValue(p, 0, cids[0].Bytes())
	for _, c := range cids[1:] {
		_, err = s.Put(c, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Sleep for 2 sync Intervals to ensure that data is flushed
	time.Sleep(2 * storethehash.DefaultSyncInterval)

	// Regenerate new storage
	s2, err := storethehash.New(tmpDir)
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
	if !i[0].Equal(value) {
		t.Errorf("Got wrong value for single cid")
	}

}
