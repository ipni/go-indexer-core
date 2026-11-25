// Package importertest tests implementations of importer.Source and
// importer.Target. ReadParts reads a kind from a source and checks the
// source's contract on every part. Copy copies a kind from a source to a
// target part by part, the way an import does, with retries, cut-off parts
// and concurrent writes.
package importertest

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// maxParts is the most parts one kind may have before ReadParts and Copy
// fail the test, so that a source whose part ends never reach nil fails
// instead of running forever
const maxParts = 100_000

// Part is one part of a kind as a source served it
type Part struct {
	// Start and End are the part's cursors; a nil End ends the kind
	Start, End []byte
	// Hint is the size hint the part was opened with, 0 for none
	Hint uint64
	// Entries are copies of the part's entries
	Entries []importer.Entry
}

// Bytes returns the bytes of the part's keys and values
func (p Part) Bytes() uint64 {
	var n uint64
	for _, e := range p.Entries {
		n += uint64(len(e.Key) + len(e.Value))
	}
	return n
}

// Entries returns the entries of parts, in order
func Entries(parts []Part) []importer.Entry {
	var all []importer.Entry
	for _, p := range parts {
		all = append(all, p.Entries...)
	}
	return all
}

// ReadParts reads all of kind from src, one open part after another, each
// with a random size hint from 0 to maxHint. It follows partial parts: when
// a part's entries end with an *importer.PartialError, it opens the rest of
// the part from the error's Resume to the part's end, closed, and the part
// is complete when its end is reached. A part whose end is nil continues
// with an open request, and ends at the end that request returns. It checks
// the source's contract on every part:
//   - an open part's end is non-empty and is not its start, and the last
//     part's end is nil;
//   - every key is non-empty, and the keys ascend within the part and from
//     one part to the next;
//   - a partial part yields at least one entry before it stops, and resumes
//     after its last key;
//   - opening the part again closed, after the first read broke out of the
//     loop part way through for some parts, returns the same end and the
//     same entries.
//
// It returns the parts in order.
func ReadParts(
	t testing.TB,
	rng *rand.Rand,
	src importer.Source,
	kind importer.Kind,
	maxHint uint64,
) []Part {
	t.Helper()
	ctx := t.Context()
	var parts []Part
	var last []byte
	var start []byte
	for {
		require.Less(t, len(parts), maxParts, "part ends do not reach the end of the data")
		hint := rng.Uint64N(maxHint + 1)
		end, entries, err := src.OpenStream(ctx, kind, start, nil, hint)
		require.NoError(t, err)
		checkOpenEnd(t, start, end)

		var got []importer.Entry
		broke := rng.IntN(3) == 0
		if broke {
			var partial *importer.PartialError
			got = collect(t, untilPartial(entries, &partial), rng.IntN(4))
		} else {
			got, end = readPart(t, src, kind, start, end, hint, entries)
		}
		// Read the part again whole: closed, or the same open part when it is
		// the last one
		whole, wholeEnd := readPart(t, src, kind, start, end, hint, mustReopen(t, src, kind, start, end, hint))
		if broke {
			require.LessOrEqual(t, len(got), len(whole))
			require.Equal(t, whole[:len(got)], got, "a part read again differs")
			end = wholeEnd
		} else {
			require.Equal(t, end, wholeEnd, "a part read again ends elsewhere")
			require.Equal(t, whole, got, "a part read again differs")
		}
		got = whole
		for _, e := range got {
			require.NotEmpty(t, e.Key, "empty key")
			if last != nil {
				require.Positive(t, bytes.Compare(e.Key, last), "keys do not ascend: %x after %x", e.Key, last)
			}
			last = e.Key
		}
		parts = append(parts, Part{Start: start, End: end, Hint: hint, Entries: got})
		if end == nil {
			return parts
		}
		start = end
	}
}

// readPart returns copies of all entries of the part from start to end,
// whose first open returned entries, and the part's end. It follows partial
// stops: when the entries end with an *importer.PartialError, it opens the
// part again from the error's Resume to end and reads on. A part whose end
// is nil continues with an open request, and when that request returns an
// end, the part ends there and readPart returns that end.
func readPart(t testing.TB, src importer.Source, kind importer.Kind, start, end []byte, hint uint64, entries iter.Seq2[importer.Entry, error]) ([]importer.Entry, []byte) {
	t.Helper()
	out := []importer.Entry{}
	for range maxParts {
		var partial *importer.PartialError
		got := collect(t, untilPartial(entries, &partial), -1)
		out = append(out, got...)
		if partial == nil {
			return out, end
		}
		checkResume(t, end, got, partial.Resume)
		next, more, err := src.OpenStream(t.Context(), kind, partial.Resume, end, hint)
		require.NoError(t, err)
		if end != nil {
			require.Equal(t, end, next, "a resumed part's end differs")
		} else {
			checkOpenEnd(t, partial.Resume, next)
			end = next
		}
		entries = more
	}
	require.FailNow(t, "partial stops do not reach the part's end")
	return nil, nil
}

// checkResume checks the Resume cursor of a partial stop of a part that
// ends at end, after the entries got: the stop yielded at least one entry,
// and Resume is after its last key and before end
func checkResume(t testing.TB, end []byte, got []importer.Entry, resume []byte) {
	t.Helper()
	require.NotEmpty(t, got, "a part stopped partial before its first entry")
	require.Positive(t, bytes.Compare(resume, got[len(got)-1].Key), "a part resumes at %x, not after its last key", resume)
	if end != nil {
		require.Negative(t, bytes.Compare(resume, end), "a part resumes at %x, not before its end %x", resume, end)
	}
}

// untilPartial returns entries that end cleanly where entries yield an
// *importer.PartialError, as the import's engine hands a part to
// importer.Target.Write, and sets partial to that error
func untilPartial(entries iter.Seq2[importer.Entry, error], partial **importer.PartialError) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		for e, err := range entries {
			if errors.As(err, partial) {
				return
			}
			if !yield(e, err) {
				return
			}
		}
	}
}

// checkOpenEnd checks the end that an open part starting at start returned
func checkOpenEnd(t testing.TB, start, end []byte) {
	t.Helper()
	if end == nil {
		return
	}
	require.NotEmpty(t, end, "an open part's end is empty but not nil")
	require.NotEqual(t, start, end, "an open part's end equals its start")
}

// mustReopen opens again a part from start to end with the size hint it was
// first opened with, and checks that the source returned end: the requested
// end of a closed part, or nil for the last part opened again
func mustReopen(t testing.TB, src importer.Source, kind importer.Kind, start, end []byte, sizeHint uint64) iter.Seq2[importer.Entry, error] {
	t.Helper()
	got, entries, err := src.OpenStream(t.Context(), kind, start, end, sizeHint)
	require.NoError(t, err)
	require.Equal(t, end, got, "a reopened part's end differs")
	return entries
}

// collect returns copies of the first n entries, or of all of them when n
// is negative. It breaks out of the loop as soon as it holds n entries, and
// with n 0 it breaks out at the first entry, before it takes any. The
// result is never nil, so that results compare equal by their entries.
func collect(t testing.TB, entries iter.Seq2[importer.Entry, error], n int) []importer.Entry {
	t.Helper()
	out := []importer.Entry{}
	for e, err := range entries {
		if n == 0 {
			break
		}
		require.NoError(t, err)
		out = append(out, importer.Entry{Key: bytes.Clone(e.Key), Value: bytes.Clone(e.Value)})
		if len(out) == n {
			break
		}
	}
	return out
}

// errCutOff is the error that Copy's cut-off parts end with, as when a
// part's body is cut off in transfer
var errCutOff = errors.New("importertest: part cut off")

// Copy copies all of kind from src to dst the way an import does, and syncs
// dst. It opens one open part after another, each with a random size hint
// from 0 to maxHint, and writes up to workers parts at a time while it opens
// the next ones. It follows partial parts: it hands Write the entries up to
// an *importer.PartialError, and then opens the rest of the part from the
// error's Resume and writes it the same way, until the part's end. The last
// part, whose end is nil, is written before Copy opens another part, since
// its continuation can return an end, after which Copy opens the next part.
// Some parts are cut off part way through: their entries end with a
// transient error, so the write must fail with an error that is not
// permanent, and the part is opened again closed and written whole. Some
// parts are written a second time after they succeed, as when a reply is
// lost. It returns the number of parts.
func Copy(t testing.TB, rng *rand.Rand, src importer.Source, dst importer.Target, kind importer.Kind, maxHint uint64, workers int) int {
	t.Helper()
	require.Positive(t, workers)
	ctx := t.Context()
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	var start []byte
	parts := 0
	for {
		require.Less(t, parts, maxParts, "part ends do not reach the end of the data")
		hint := rng.Uint64N(maxHint + 1)
		end, entries, err := src.OpenStream(ctx, kind, start, nil, hint)
		require.NoError(t, err)
		checkOpenEnd(t, start, end)
		parts++

		// cutAfter is the number of entries after which the part is cut
		// off, or -1 to write it whole
		cutAfter := -1
		if rng.IntN(4) == 0 {
			cutAfter = rng.IntN(4)
		}
		rewrite := rng.IntN(4) == 0
		if end == nil {
			end = writePart(ctx, t, src, dst, kind, start, end, hint, entries, cutAfter, rewrite)
			require.False(t, t.Failed())
			if end == nil {
				break
			}
			start = end
			continue
		}
		sem <- struct{}{}
		partStart := start
		wg.Go(func() {
			defer func() { <-sem }()
			writePart(ctx, t, src, dst, kind, partStart, end, hint, entries, cutAfter, rewrite)
		})
		start = end
	}
	wg.Wait()
	require.False(t, t.Failed())
	require.NoError(t, dst.Sync())
	return parts
}

// writePart writes one part that Copy opened, and opens and writes it again
// as Copy describes. It returns the part's end: end, or for a part whose
// end is nil, the end that a continuation after a partial stop returned. It
// runs in its own goroutine for a part with an end, so it reports failures
// with assert.
func writePart(ctx context.Context, t testing.TB, src importer.Source, dst importer.Target, kind importer.Kind, start, end []byte, hint uint64, entries iter.Seq2[importer.Entry, error], cutAfter int, rewrite bool) []byte {
	cut := false
	if cutAfter >= 0 {
		entries = cutOff(entries, cutAfter, &cut)
	}
	written, err := writeWhole(ctx, t, src, dst, kind, end, hint, entries)
	if cut {
		if !assert.Error(t, err, "a write of a cut-off part succeeded") ||
			!assert.False(t, importer.IsPermanent(err), "a cut-off part is a permanent error: %v", err) {
			return end
		}
	} else if !assert.NoError(t, err) {
		return end
	} else {
		end = written
	}
	if !cut && !rewrite {
		return end
	}
	got, again, err := src.OpenStream(ctx, kind, start, end, hint)
	if !assert.NoError(t, err) || !assert.Equal(t, end, got, "a reopened part's end differs") {
		return end
	}
	written, err = writeWhole(ctx, t, src, dst, kind, end, hint, again)
	assert.NoError(t, err)
	return written
}

// writeWhole writes a part that ends at end, whose first open returned
// entries, as the import's engine does. It hands Write the entries up to an
// *importer.PartialError, and then opens the rest of the part from the
// error's Resume to end and writes it the same way, until the part is
// complete. A part whose end is nil continues with an open request, and
// when that request returns an end, the part ends there. It returns the
// part's end, and the first error that Write returned.
func writeWhole(ctx context.Context, t testing.TB, src importer.Source, dst importer.Target, kind importer.Kind, end []byte, hint uint64, entries iter.Seq2[importer.Entry, error]) ([]byte, error) {
	for range maxParts {
		var partial *importer.PartialError
		var last []byte
		n := 0
		err := dst.Write(ctx, kind, func(yield func(importer.Entry, error) bool) {
			for e, err := range untilPartial(entries, &partial) {
				if err == nil {
					last = append(last[:0], e.Key...)
					n++
				}
				if !yield(e, err) {
					return
				}
			}
		})
		if err != nil || partial == nil {
			return end, err
		}
		if !assert.Positive(t, n, "a part stopped partial before its first entry") ||
			!assert.Positive(t, bytes.Compare(partial.Resume, last), "a part resumes at %x, not after its last key", partial.Resume) {
			return end, nil
		}
		next, more, err := src.OpenStream(ctx, kind, partial.Resume, end, hint)
		if !assert.NoError(t, err) {
			return end, nil
		}
		if end != nil {
			if !assert.Equal(t, end, next, "a resumed part's end differs") {
				return end, nil
			}
		} else {
			end = next
		}
		entries = more
	}
	assert.Fail(t, "partial stops do not reach the part's end")
	return end, nil
}

// cutOff returns entries that end with errCutOff after n entries, and sets
// cut when they do. Entries with n or fewer entries end without it.
func cutOff(entries iter.Seq2[importer.Entry, error], n int, cut *bool) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		i := 0
		for e, err := range entries {
			if err == nil && i == n {
				*cut = true
				yield(importer.Entry{}, errCutOff)
				return
			}
			i++
			if !yield(e, err) {
				return
			}
		}
	}
}
