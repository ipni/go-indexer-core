package pebble

import (
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/test"
)

func initPebble(t *testing.T) indexer.Interface {
	s, err := New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestE2E(t *testing.T) {
	s := initPebble(t)
	test.E2ETest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSize(t *testing.T) {
	s := initPebble(t)
	test.SizeTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMany(t *testing.T) {
	s := initPebble(t)
	test.RemoveTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProviderContext(t *testing.T) {
	s := initPebble(t)
	test.RemoveProviderContextTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProvider(t *testing.T) {
	s := initPebble(t)
	test.RemoveProviderTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParallel(t *testing.T) {
	s := initPebble(t)
	test.ParallelUpdateTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	s := initPebble(t)
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}
