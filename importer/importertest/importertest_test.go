package importertest_test

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"iter"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importertest"
	"github.com/stretchr/testify/require"
)

const testKind importer.Kind = "test"

// memSource serves sorted entries of testKind. Its cursors are keys, and an
// open part holds as many entries as the size hint, or 10 without one.
type memSource struct {
	entries []importer.Entry
	// stopAfter, when positive, stops every stream that has more entries to
	// yield after stopAfter of them, with a *importer.PartialError
	stopAfter int
}

func (s *memSource) OpenStream(
	_ context.Context,
	kind importer.Kind,
	start, end []byte,
	sizeHint uint64,
) (
	[]byte,
	iter.Seq2[importer.Entry, error],
	error,
) {
	if kind != testKind {
		return nil, nil, importer.PermanentError(errors.New("unknown kind"))
	}
	from, _ := slices.BinarySearchFunc(s.entries, start, cmpKey)
	to := len(s.entries)
	if end != nil {
		to, _ = slices.BinarySearchFunc(s.entries, end, cmpKey)
	} else if n := int(cmp.Or(sizeHint, 10)); from+n < len(s.entries) {
		to = from + n
		end = s.entries[to].Key
	}
	return end, func(yield func(importer.Entry, error) bool) {
		for i, e := range s.entries[from:to] {
			if s.stopAfter > 0 && i == s.stopAfter {
				yield(importer.Entry{}, &importer.PartialError{Resume: append(bytes.Clone(s.entries[from+i-1].Key), 0)})
				return
			}
			if !yield(e, nil) {
				return
			}
		}
	}, nil
}

func cmpKey(e importer.Entry, key []byte) int { return bytes.Compare(e.Key, key) }

// memTarget keeps what it is written in a map. It counts the writes in
// flight, and each write waits for delay before it reads its entries.
type memTarget struct {
	delay time.Duration

	mu       sync.Mutex
	data     map[string]string
	inFlight atomic.Int64
	maxIn    int64
	synced   atomic.Bool
}

func (m *memTarget) Write(_ context.Context, kind importer.Kind, entries iter.Seq2[importer.Entry, error]) error {
	if kind != testKind {
		return importer.PermanentError(errors.New("unknown kind"))
	}

	in := m.inFlight.Add(1)
	m.mu.Lock()
	m.maxIn = max(m.maxIn, in)
	m.mu.Unlock()
	defer m.inFlight.Add(-1)

	time.Sleep(m.delay)
	for e, err := range entries {
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.data[string(e.Key)] = string(e.Value)
		m.synced.Store(false)
		m.mu.Unlock()
	}
	return nil
}

func (m *memTarget) Sync() error {
	m.synced.Store(true)
	return nil
}

func newMemSource(rng *rand.Rand, n int) *memSource {
	byKey := make(map[string]importer.Entry, n)
	for range n {
		key := make([]byte, 1+rng.IntN(8))
		for i := range key {
			key[i] = byte(rng.IntN(4))
		}
		byKey[string(key)] = importer.Entry{Key: key, Value: []byte{byte(rng.Uint32())}}
	}
	entries := slices.Collect(maps.Values(byKey))
	slices.SortFunc(entries, func(a, b importer.Entry) int { return bytes.Compare(a.Key, b.Key) })
	return &memSource{entries: entries}
}

func TestReadParts(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	src := newMemSource(rng, 500)
	parts := importertest.ReadParts(t, rng, src, testKind, 20)
	require.Greater(t, len(parts), 20)
	require.Nil(t, parts[0].Start)
	require.Nil(t, parts[len(parts)-1].End)
	var total, wantTotal uint64
	for i, p := range parts {
		if i > 0 {
			require.Equal(t, parts[i-1].End, p.Start)
		}
		if i < len(parts)-1 {
			// memSource cuts an open part after as many entries as the hint
			require.Len(t, p.Entries, int(cmp.Or(p.Hint, 10)))
		}
		total += p.Bytes()
	}
	require.Equal(t, src.entries, importertest.Entries(parts))
	for _, e := range src.entries {
		wantTotal += uint64(len(e.Key) + len(e.Value))
	}
	require.Equal(t, wantTotal, total)
}

func TestReadPartsPartial(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	src := newMemSource(rng, 500)
	src.stopAfter = 3
	parts := importertest.ReadParts(t, rng, src, testKind, 20)
	require.Greater(t, len(parts), 20)
	require.Equal(t, src.entries, importertest.Entries(parts))
}

func TestReadPartsEmpty(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	parts := importertest.ReadParts(t, rng, &memSource{}, testKind, 20)
	require.Len(t, parts, 1)
	require.Empty(t, importertest.Entries(parts))
}

func TestCopy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rng := rand.New(rand.NewPCG(3, 4))
		src := newMemSource(rng, 2000)
		dst := &memTarget{data: map[string]string{}, delay: time.Second}
		const workers = 4
		parts := importertest.Copy(t, rng, src, dst, testKind, 20, workers)
		require.Greater(t, parts, 100)

		want := make(map[string]string, len(src.entries))
		for _, e := range src.entries {
			want[string(e.Key)] = string(e.Value)
		}
		require.Equal(t, want, dst.data)
		require.True(t, dst.synced.Load())
		// Each write waits a second before it reads its entries, while
		// Copy opens the next parts, so every worker writes at once. Copy
		// writes the last part, whose end is nil, itself, beside the workers.
		require.EqualValues(t, workers+1, dst.maxIn)
	})
}

func TestCopyPartial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rng := rand.New(rand.NewPCG(7, 8))
		src := newMemSource(rng, 1000)
		src.stopAfter = 3
		dst := &memTarget{data: map[string]string{}}
		importertest.Copy(t, rng, src, dst, testKind, 20, 3)

		want := make(map[string]string, len(src.entries))
		for _, e := range src.entries {
			want[string(e.Key)] = string(e.Value)
		}
		require.Equal(t, want, dst.data)
	})
}
