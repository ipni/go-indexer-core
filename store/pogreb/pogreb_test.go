package pogreb_test

import (
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/pogreb"
	"github.com/filecoin-project/go-indexer-core/store/test"
)

func initPogreb(t *testing.T) indexer.Interface {
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
	s, err := pogreb.New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestE2E(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.E2ETest(t, s)
}

func TestParallel(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.ParallelUpdateTest(t, s)
}

func TestSize(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.SizeTest(t, s)
}

func TestRemove(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.RemoveTest(t, s)
}

func TestRemoveProviderContext(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.RemoveProviderContextTest(t, s)
}

func TestRemoveProvider(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.RemoveProviderTest(t, s)
}

func skipIf32bit(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Pogreb cannot use GOARCH=386")
	}
}
