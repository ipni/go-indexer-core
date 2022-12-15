package dhash_test

import (
	"math/rand"
	"testing"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/bench"
	"github.com/ipni/go-indexer-core/store/dhash"
	"github.com/ipni/go-indexer-core/store/dhash/pebble"
	"github.com/ipni/go-indexer-core/store/test"
)

func initDHash(t *testing.T) indexer.Interface {
	s, err := pebble.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return dhash.New(s)
}

func TestE2E(t *testing.T) {
	s := initDHash(t)
	test.E2ETest(t, s, true)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSize(t *testing.T) {
	s := initDHash(t)
	test.SizeTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMany(t *testing.T) {
	s := initDHash(t)
	test.RemoveTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProviderContext(t *testing.T) {
	s := initDHash(t)
	test.RemoveProviderContextTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParallel(t *testing.T) {
	s := initDHash(t)
	test.ParallelUpdateTest(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	s := initDHash(t)
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
	s, err := pebble.New(dir, nil)
	if err != nil {
		t.Fatal()
	}
	subject := dhash.New(s)
	defer subject.Close()
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
