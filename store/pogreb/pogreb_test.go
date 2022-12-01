package pogreb_test

import (
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/pogreb"
	"github.com/filecoin-project/go-indexer-core/store/test"
)

func initPogreb(t *testing.T, encrypt bool) indexer.Interface {
	s, err := pogreb.New(t.TempDir(), encrypt, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestE2E(t *testing.T) {
	s := initPogreb(t, true)
	test.E2ETest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParallel(t *testing.T) {
	s := initPogreb(t, true)
	test.ParallelUpdateTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSize(t *testing.T) {
	s := initPogreb(t, true)
	test.SizeTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemove(t *testing.T) {
	s := initPogreb(t, true)
	test.RemoveTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProviderContext(t *testing.T) {
	s := initPogreb(t, true)
	test.RemoveProviderContextTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProvider(t *testing.T) {
	s := initPogreb(t, true)
	test.RemoveProviderTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	s := initPogreb(t, true)
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}
