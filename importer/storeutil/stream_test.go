package storeutil

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"testing"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/stretchr/testify/require"
)

// testStreamItem is an entry or an error that a part yields, for comparing
// sequences
type testStreamItem struct {
	key    string
	resume string // the Resume of a PartialError
	err    error  // any other error
}

// testCollect yields every item of seq, continuing past every error
func testCollect(seq iter.Seq2[importer.Entry, error]) []testStreamItem {
	var out []testStreamItem
	for e, err := range seq {
		var pe *importer.PartialError
		switch {
		case errors.As(err, &pe):
			out = append(out, testStreamItem{resume: string(pe.Resume)})
		case err != nil:
			out = append(out, testStreamItem{err: err})
		default:
			out = append(out, testStreamItem{key: string(e.Key)})
		}
	}
	return out
}

// testSeq yields an entry of key k and a 1-byte value for each k, then err
// when it is not nil
func testSeq(err error, keys ...string) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		for _, k := range keys {
			if !yield(importer.Entry{Key: []byte(k), Value: []byte{1}}, nil) {
				return
			}
		}
		if err != nil {
			yield(importer.Entry{}, err)
		}
	}
}

func testEntries(keys ...string) []importer.Entry {
	var out []importer.Entry
	for e := range testSeq(nil, keys...) {
		out = append(out, e)
	}
	return out
}

func TestStreamPart(t *testing.T) {
	boom := errors.New("boom")
	keys := func(ks ...string) []testStreamItem {
		var out []testStreamItem
		for _, k := range ks {
			out = append(out, testStreamItem{key: k})
		}
		return out
	}
	partial := func(resume string) testStreamItem { return testStreamItem{resume: resume + "\x00"} }

	for _, tc := range []struct {
		name     string
		head     []importer.Entry
		rest     iter.Seq2[importer.Entry, error]
		maxBytes uint64
		want     []testStreamItem
	}{
		{"no limit", testEntries("a", "b"), testSeq(nil, "c"), 0, keys("a", "b", "c")},
		{"nil rest", testEntries("a"), nil, 0, keys("a")},
		{"under the limit", testEntries("a"), testSeq(nil, "b"), 100, keys("a", "b")},
		// Each entry is 2 bytes: the limit is reached after "b"
		{
			"partial before the next entry, once",
			testEntries("a"), testSeq(nil, "b", "c", "d"), 4,
			append(append(keys("a", "b"), partial("b")), keys("c", "d")...),
		},
		{"limit at the last entry", testEntries("a"), testSeq(nil, "b"), 4, keys("a", "b")},
		{
			"error after the limit",
			nil, testSeq(boom, "a", "b"), 4,
			append(keys("a", "b"), partial("b"), testStreamItem{err: boom}),
		},
		{"error under the limit", nil, testSeq(boom, "a"), 100, append(keys("a"), testStreamItem{err: boom})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := testCollect(StreamPart(t.Context(), tc.head, tc.rest, tc.maxBytes))
			require.Equal(t, tc.want, got)
		})
	}

	t.Run("stop at the partial error", func(t *testing.T) {
		var got []string
		for e, err := range StreamPart(t.Context(), nil, testSeq(nil, "a", "b", "c"), 2) {
			if err != nil {
				break
			}
			got = append(got, string(e.Key))
		}
		require.Equal(t, []string{"a"}, got)
	})

	t.Run("head checks ctx", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		got := testCollect(StreamPart(ctx, testEntries("a"), nil, 0))
		require.Len(t, got, 1)
		require.ErrorIs(t, got[0].err, context.Canceled)
	})

	t.Run("resume does not alias the entry key", func(t *testing.T) {
		key := []byte("a")
		rest := func(yield func(importer.Entry, error) bool) {
			if yield(importer.Entry{Key: key, Value: []byte{1}}, nil) {
				key[0] = 'z'
				yield(importer.Entry{Key: key, Value: []byte{1}}, nil)
			}
		}
		got := testCollect(StreamPart(t.Context(), nil, rest, 2))
		require.Equal(t, []testStreamItem{{key: "a"}, partial("a"), {key: "z"}}, got)
	})
}

// TestStreamPartReadsRestOnce checks that the rest is read once, from its
// start, also when the consumer continues past the PartialError
func TestStreamPartReadsRestOnce(t *testing.T) {
	var opened int
	rest := func(yield func(importer.Entry, error) bool) {
		opened++
		for i := range 5 {
			if !yield(importer.Entry{Key: fmt.Appendf(nil, "k%d", i), Value: []byte{1}}, nil) {
				return
			}
		}
	}
	var got []string
	for e, err := range StreamPart(t.Context(), nil, rest, 3) {
		if err == nil {
			got = append(got, string(e.Key))
		}
	}
	require.Equal(t, 1, opened)
	require.True(t, slices.Equal([]string{"k0", "k1", "k2", "k3", "k4"}, got), "%v", got)
}
