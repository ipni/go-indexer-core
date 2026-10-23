package pebble

import (
	"testing"

	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/bench"
	"github.com/ipni/go-indexer-core/store/test"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func initPebble(t *testing.T) indexer.Interface {
	s, err := New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConformance(t *testing.T) {
	test.RunConformance(t, func(t *testing.T) indexer.Interface {
		return initPebble(t)
	})
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

func TestSize(t *testing.T) {
	s, err := New(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, s.Close())
	})

	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	require.NoError(t, err)

	mhs := random.Multihashes(151)
	value := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("test-metadata"),
	}
	require.NoError(t, s.Put(value, mhs[1:]...))
	require.NoError(t, s.Flush())

	size, err := s.Size()
	require.NoError(t, err)
	// Size estimates flushed sstable bytes. Each multihash key is one prefix
	// byte plus the multihash, and those digests do not compress, so the
	// estimate is at least one key per multihash. The value slot stored on
	// every multihash is identical and does compress, so the raw key-plus-slot
	// size is only an upper bound, with room for sstable overhead.
	n := len(mhs) - 1
	perKey := 1 + len(mhs[1])
	perSlot := marshalledValueKeyLength
	require.GreaterOrEqual(t, size, int64(n*perKey))
	require.LessOrEqual(t, size, int64(4*n*(perKey+perSlot)))
}

func TestStats(t *testing.T) {
	dir := t.TempDir()
	subject, err := New(dir, nil)
	if err != nil {
		t.Fatal()
	}
	defer subject.Close()
	rng := random.New()
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
