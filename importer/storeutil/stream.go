package storeutil

import (
	"context"
	"iter"

	"github.com/ipni/go-indexer-core/importer"
)

// DefaultPartBytes is about how many bytes of wire keys and values an open
// part holds when the caller gives no size hint
const DefaultPartBytes = 128 << 20

// CommitBytes is about how many bytes of writes a target commits in one
// batch
const CommitBytes = 1 << 20

// StreamPart yields the entries of a part: head, then the entries of rest.
// head is what PlanPart returned, nil for a closed part, and rest reads the
// part from the store after head; a nil rest yields none. It checks ctx
// before each entry of head; rest checks ctx itself, on every key it reads,
// including keys it does not send.
//
// Once the entries it yielded hold maxBytes bytes of keys and values, it
// yields an importer.PartialError before the next item, entry or error, and
// never after the last item. Its Resume is the last key yielded
// followed by 0x00. A maxBytes of 0 sets no limit.
//
// A caller must stop at an error, except that it may continue past a
// PartialError: the loop goes on with the entries after it.
func StreamPart(
	ctx context.Context,
	head []importer.Entry,
	rest iter.Seq2[importer.Entry, error],
	maxBytes uint64,
) iter.Seq2[importer.Entry, error] {
	return yieldWithMaxBytes(
		concat(
			sliceWithCtx(ctx, head),
			rest,
		),
		maxBytes,
	)
}

// concat yields the pairs of each of seqs in turn; a nil seq yields none. It
// goes on after an error, as its consumer decides when to stop.
func concat[K, V any](seqs ...iter.Seq2[K, V]) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for _, seq := range seqs {
			if seq == nil {
				continue
			}
			for k, v := range seq {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}

// sliceWithCtx yields the elements of s, checking ctx before each. A done
// ctx ends it with ctx's cause.
func sliceWithCtx[T any](ctx context.Context, s []T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, v := range s {
			if err := context.Cause(ctx); err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if !yield(v, nil) {
				return
			}
		}
	}
}

// yieldWithMaxBytes yields the items of entries. Once the entries yielded
// hold maxBytes bytes of keys and values, it yields an importer.PartialError
// before the next item, entry or error, and never after the last item. The
// PartialError is yielded once, and the loop goes on if the consumer does.
// A maxBytes of 0 yields entries as is.
func yieldWithMaxBytes(
	entries iter.Seq2[importer.Entry, error],
	maxBytes uint64,
) iter.Seq2[importer.Entry, error] {
	if maxBytes == 0 {
		// No limit, return the entries iterator as is
		return entries
	}

	return func(yield func(importer.Entry, error) bool) {
		var sent uint64
		// resume is the cursor after the entry that reached maxBytes, its key
		// followed by 0x00, from then until the PartialError is yielded. It is
		// a copy, as an entry's key is valid only until the next iteration.
		var resume []byte
		for e, err := range entries {
			if resume != nil {
				// The item after the one that reached maxBytes
				if !yield(importer.Entry{}, &importer.PartialError{Resume: resume}) {
					return
				}
				resume = nil
			}
			if !yield(e, err) {
				return
			}

			// Count the bytes only until they reach maxBytes, so that the
			// PartialError is yielded once
			if sent >= maxBytes {
				continue
			}
			if sent += uint64(len(e.Key) + len(e.Value)); sent >= maxBytes {
				resume = make([]byte, len(e.Key)+1)
				copy(resume, e.Key)
			}
		}
	}
}
