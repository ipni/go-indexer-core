package storethehash_test

import (
	"context"
	"testing"
	"time"

	sth "github.com/ipld/go-storethehash/store"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/store/storethehash"
	"github.com/ipni/go-indexer-core/store/test"
	"github.com/libp2p/go-libp2p/core/peer"
)

func initSth(t *testing.T, vals ...int) *storethehash.SthStorage {
	var putConcurrency int
	if len(vals) > 0 {
		putConcurrency = vals[0]
	}
	s, err := storethehash.New(context.Background(), t.TempDir(), putConcurrency)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestE2E(t *testing.T) {
	s := initSth(t)
	test.E2ETest(t, s, false)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSize(t *testing.T) {
	s := initSth(t)
	test.SizeTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMany(t *testing.T) {
	s := initSth(t)
	test.RemoveTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProviderContext(t *testing.T) {
	s := initSth(t)
	test.RemoveProviderContextTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProvider(t *testing.T) {
	s := initSth(t)
	test.RemoveProviderTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParallel(t *testing.T) {
	s := initSth(t)
	test.ParallelUpdateTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestChangeConcurrency(t *testing.T) {
	s := initSth(t, 1)
	test.ParallelUpdateTest(t, s)

	s.SetPutConcurrency(1)

	s.SetPutConcurrency(4)
	s.SetPutConcurrency(4)
	test.ParallelUpdateTest(t, s)

	s.SetPutConcurrency(8)
	test.ParallelUpdateTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPeriodicFlush(t *testing.T) {
	// Init storage
	tmpDir := t.TempDir()

	syncInterval := 200 * time.Millisecond

	s, err := storethehash.New(context.Background(), tmpDir, 0, sth.SyncInterval(syncInterval))
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
	s2, err := storethehash.New(context.Background(), tmpDir, 16)
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

	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s2.Close(); err != nil {
		t.Fatal(err)
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
