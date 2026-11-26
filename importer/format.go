// Package importer defines how an indexer import copies a store's data: a
// store streams its data out through a Source and takes it in through a
// Target, one part of one kind at a time. The kinds and their wire encoding
// are the import's protocol, the same for every store (see kind.go).
package importer

import (
	"context"
	"iter"
)

// Kind names one kind of data the import copies, such as KindRecords. The
// kind implies the keyspace, so its wire keys carry no store prefix.
type Kind string

// Entry is one wire key and value of a kind
type Entry struct{ Key, Value []byte }

// Source streams a store's data out, one part of one kind at a time. It is
// called concurrently for many parts.
//
// Cursors (start, end and the returned part end) are opaque to callers,
// which never compare or interpret them. A nil start is the beginning of
// the kind's data.
//
// Errors marked with PermanentError stop the import, such as the error of
// a source that is frozen. Any other error is transient, and the caller
// retries the part.
type Source interface {
	// OpenStream opens the part of kind's data from start to end.
	//
	// With a non-nil end the part is closed: it runs to end, which the
	// source returned as a part end before, and OpenStream returns end.
	// Opening [start, partEnd) again yields the same keyspace, so a retry
	// of a part rereads it whole.
	//
	// With a nil end the part is open: the source picks the end after about
	// sizeHint bytes of wire keys and values, or after its own default size
	// when sizeHint is 0. A nil partEnd means the part runs to the end of
	// the data. An open part's end never equals its start. To pick the end,
	// the source may read the part's first batch when it opens the part,
	// and the entries then yield that batch from memory.
	//
	// The entries come in ascending key order, and every key is non-empty.
	// They are read from the store as the caller ranges over them. The
	// caller ranges over them once, and may break out early: the source
	// may hold resources from OpenStream on, such as a sender's open
	// response, and releases them when the loop ends, early or not. An
	// entry's slices are valid until the next iteration. An error from the
	// entries ends the part, and the caller retries it.
	//
	// The entries may end with a *PartialError instead, open part or closed:
	// the source stops the part at an entry boundary once it has yielded
	// about twice the size hint, or twice its default size when sizeHint is
	// 0, so that one request stays bounded when the data spreads unevenly
	// over the keys. The part then continues from the error's Resume cursor
	// with a closed request to the same end, OpenStream(ctx, kind, Resume,
	// end, sizeHint), which may stop early again. A part whose end is nil
	// continues with an open request from Resume. When that request returns
	// an end, the part ends there, and the data after that end is a new
	// open part.
	OpenStream(
		ctx context.Context,
		kind Kind,
		start, end []byte,
		sizeHint uint64,
	) (
		partEnd []byte,
		entries iter.Seq2[Entry, error],
		err error,
	)
}

// Target takes a store's data in, one part of one kind at a time
type Target interface {
	// Write writes one part of kind: every entry that entries yields. Parts
	// of one kind are written concurrently and never overlap. Writing a part
	// again is safe: it leaves the store as one write of it does. An entry's
	// slices are valid until the next iteration.
	//
	// The entries never yield a *PartialError. The caller ends the
	// iteration it hands to Write at a source's PartialError, so Write
	// commits what it got and returns nil, and the caller then opens the
	// rest of the part from the error's Resume cursor.
	//
	// When entries yields an error, Write stops and returns an error. An
	// entry that breaks the kind's wire encoding is a permanent error. A
	// permanent error stops the import; any other error retries the part.
	Write(
		ctx context.Context,
		kind Kind,
		entries iter.Seq2[Entry, error],
	) error

	// Sync makes every Write that has returned durable
	Sync() error
}
