package pebble

import (
	"math/rand"
	"testing"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/bench"
	"github.com/ipni/go-indexer-core/dhash"
	"github.com/ipni/go-indexer-core/store/test"
)

func initPebble(t *testing.T, doubleHashing bool) indexer.Interface {
	s, err := New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return dhash.New(s, doubleHashing)
}

func TestE2E(t *testing.T) {
	testE2E(t, true)
	testE2E(t, false)
}

func testE2E(t *testing.T, doubleHashing bool) {
	s := initPebble(t, doubleHashing)
	test.E2ETest(t, s, doubleHashing)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSize(t *testing.T) {
	testSize(t, true)
	testSize(t, false)
}

func testSize(t *testing.T, doubleHashing bool) {
	s := initPebble(t, doubleHashing)
	test.SizeTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMany(t *testing.T) {
	testMany(t, true)
	testMany(t, false)
}

func testMany(t *testing.T, doubleHashing bool) {
	s := initPebble(t, doubleHashing)
	test.RemoveTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProviderContext(t *testing.T) {
	testRemoveProviderContext(t, true)
	testRemoveProviderContext(t, false)
}

func testRemoveProviderContext(t *testing.T, doubleHashing bool) {
	s := initPebble(t, doubleHashing)
	test.RemoveProviderContextTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProvider(t *testing.T) {
	// Removing provider is not supported for double hashed datastore
	t.Skip()
	testRemoveProvider(t, true)
	testRemoveProvider(t, false)
}

func testRemoveProvider(t *testing.T, doubleHashing bool) {
	s := initPebble(t, doubleHashing)
	test.RemoveProviderTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParallel(t *testing.T) {
	testParallel(t, true)
	testParallel(t, false)
}

func testParallel(t *testing.T, doubleHashing bool) {
	s := initPebble(t, doubleHashing)
	test.ParallelUpdateTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	testClose(t, true)
	testClose(t, false)
}

func testClose(t *testing.T, doubleHashing bool) {
	s := initPebble(t, doubleHashing)
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStats(t *testing.T) {
	dir := t.TempDir()
	ds, err := New(dir, nil)
	if err != nil {
		t.Fatal()
	}
	defer ds.Close()
	subject := dhash.New(ds, true)
	rng := rand.New(rand.NewSource(1413))
	values, _ := bench.GenerateRandomValues(t, rng, bench.GeneratorConfig{
		NumProviders:         1,
		NumValuesPerProvider: func() uint64 { return 123 },
		NumEntriesPerValue:   func() uint64 { return 456 },
		ShuffleValues:        true,
	})
	mhs := make(map[string]struct{})
	for _, value := range values {
		err := subject.Put(value.Value, value.Entries...)
		if err != nil {
			t.Fatal()
		}
		for _, entry := range value.Entries {
			mhs[string(entry)] = struct{}{}
		}
	}
	if err := subject.Flush(); err != nil {
		t.Fatal(err)
	}
	gotStats, err := subject.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if gotStats == nil {
		t.Fatal("expected non-nil stats")
	}
	wantCount := uint64(len(mhs))
	// Assert that the returned count is at least as big as the expected count.
	// Note that the count is an estimation.
	if gotStats.MultihashCount < wantCount {
		t.Fatalf("expected count to be at least %d but got %d", wantCount, gotStats.MultihashCount)
	}
	t.Logf("estimated %d for exactl multihash count of %d", gotStats.MultihashCount, wantCount)
}
