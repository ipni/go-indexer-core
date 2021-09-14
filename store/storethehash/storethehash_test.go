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
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	//"github.com/multiformats/go-multihash"
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
	mhs, err := test.RandomMultihashes(151)
	if err != nil {
		t.Fatal(err)
	}

	value := indexer.MakeValue(p, 0, []byte(mhs[0]))

	// Check that a non-v0 CID hash can be stored.
	c, err := cid.Decode("baguqeeqqskyz3yh4jxnsdj57v5blazexyy")
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(c.Hash(), value)
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range mhs[1:] {
		_, err = s.Put(m, value)
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

	// Get data. If re-generated correctly we should find the multihash
	i, found, err := s2.Get(mhs[3])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("Error finding single multihash")
	}
	if !i[0].Equal(value) {
		t.Errorf("Got wrong value for single multihash")
	}

}
