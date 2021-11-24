package storethehash_test

import (
	"io/ioutil"
	"runtime"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/go-indexer-core/store/test"
	"github.com/libp2p/go-libp2p-core/peer"
)

func initSth(t *testing.T) indexer.Interface {
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

func TestMany(t *testing.T) {
	s := initSth(t)
	test.RemoveTest(t, s)
}

func TestRemoveProviderContext(t *testing.T) {
	s := initSth(t)
	test.RemoveProviderContextTest(t, s)
}

func TestRemoveProvider(t *testing.T) {
	s := initSth(t)
	test.RemoveProviderTest(t, s)
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

	syncInterval := 200 * time.Millisecond

	s, err := storethehash.New(tmpDir, storethehash.SyncInterval(syncInterval))
	if err != nil {
		t.Fatal(err)
	}
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	// Put some data in the first storage.
	mhs := test.RandomMultihashes(151)

	value := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("some-metadata"),
	}
	err = s.Put(value, mhs[1:]...)
	if err != nil {
		t.Fatal(err)
	}

	// Sleep for 2 sync Intervals to ensure that data is flushed
	time.Sleep(2 * syncInterval)

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

func TestClose(t *testing.T) {
	s := initSth(t)
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}
