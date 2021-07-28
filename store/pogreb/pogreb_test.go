package pogreb_test

import (
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/filecoin-project/go-indexer-core/store/pogreb"
	"github.com/filecoin-project/go-indexer-core/store/test"
)

func initPogreb() (store.Interface, error) {
	tmpDir, err := ioutil.TempDir("", "sth")
	if err != nil {
		return nil, err
	}
	return pogreb.New(tmpDir)
}

func TestE2E(t *testing.T) {
	skipIf32bit(t)

	s, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	test.E2ETest(t, s)
}

func TestParallel(t *testing.T) {
	skipIf32bit(t)

	s, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	test.ParallelUpdateTest(t, s)
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

func skipIf32bit(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Pogreb cannot use GOARCH=386")
	}
}
