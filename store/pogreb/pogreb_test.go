package pogreb_test

import (
	"runtime"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/pogreb"
	"github.com/filecoin-project/go-indexer-core/store/test"
)

func initPogreb(t *testing.T) indexer.Interface {
	s, err := pogreb.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestE2E(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.E2ETest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParallel(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.ParallelUpdateTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSize(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.SizeTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemove(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.RemoveTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProviderContext(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.RemoveProviderContextTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProvider(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.RemoveProviderTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGC(t *testing.T) {
	skipIf32bit(t)

	s := initPogreb(t)
	test.GCTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func skipIf32bit(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Pogreb cannot use GOARCH=386")
	}
}
