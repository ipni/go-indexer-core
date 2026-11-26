package pebble

import (
	"context"
	"fmt"
	"iter"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/storeutil"
)

// The store is an import target: Write and Sync write the kinds that
// OpenStream serves.
var _ importer.Target = (*store)(nil)

// Write implements importer.Target. It writes the kinds that OpenStream
// serves: it adds the store's prefix to each wire key,
// writes records and context map entries as they are, and sets a
// multihash's value to a slot for each record ID, replacing any value the
// multihash had. Its batches are committed without a log sync, and Sync
// syncs the log.
//
// An entry that breaks its kind's wire encoding is a permanent error, and so
// is a failed write:
//   - a record's key is 20 bytes, and its value decodes as a value record;
//   - a context map entry's key is 20 bytes, and its value 10;
//   - a multihash's key is not empty, and its value is not empty and is a
//     whole number of 20-byte record IDs.
func (s *store) Write(ctx context.Context, kind importer.Kind, entries iter.Seq2[importer.Entry, error]) error {
	p, err := kindPrefix(kind)
	if err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()

	// keyBuf and slotsBuf hold the store key and a multihash's slots of the
	// current entry, shared between iterations to reduce allocations. keyBuf
	// starts with room for a prefix byte and a multihash with a 64-byte
	// digest, and slotsBuf with room for the slots of initialKeyBufContexts
	// contexts.
	keyBuf := make([]byte, 0, 1+2+64)
	slotsBuf := make([]byte, 0, initialKeyBufContexts*marshalledValueKeyLength)
	for e, err := range entries {
		if err != nil {
			return err
		}
		if err := s.checkImportEntry(kind, e); err != nil {
			return importer.PermanentError(err)
		}

		key, value, nextKeyBuf, nextSlotsBuf := fromWireEntry(p, e, keyBuf, slotsBuf)
		if err := b.Set(key, value, nil); err != nil {
			return importer.PermanentError(err)
		}
		if b.Len() >= storeutil.CommitBytes {
			if err := commitBatch(ctx, b); err != nil {
				return err
			}
			b.Reset()
		}

		// Update the buffers for the next iteration to avoid allocations
		keyBuf, slotsBuf = nextKeyBuf, nextSlotsBuf
	}
	if b.Empty() {
		return nil
	}
	return commitBatch(ctx, b)
}

// commitBatch commits b without a log sync, unless ctx is done
func commitBatch(ctx context.Context, b *pebble.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return importer.PermanentError(err)
	}
	return nil
}

// checkImportEntry returns an error when e breaks the wire encoding of kind
func (s *store) checkImportEntry(kind importer.Kind, e importer.Entry) error {
	switch kind {
	case importer.KindRecords:
		if len(e.Key) != recordIDLength {
			return fmt.Errorf("%w: record ID %x is not %d bytes", importer.ErrWireEncoding, e.Key, recordIDLength)
		}
		if _, err := s.vcodec.unmarshalValue(e.Value); err != nil {
			return fmt.Errorf("%w: record %x: %w", importer.ErrWireEncoding, e.Key, err)
		}
	case importer.KindContextMap:
		if len(e.Key) != recordIDLength {
			return fmt.Errorf("%w: context map key %x is not %d bytes", importer.ErrWireEncoding, e.Key, recordIDLength)
		}
		if len(e.Value) != contextTokenLength {
			return fmt.Errorf("%w: context map entry %x has a token of %d bytes", importer.ErrWireEncoding, e.Key, len(e.Value))
		}
	case importer.KindMultihashes:
		if len(e.Key) == 0 || len(e.Value) == 0 || len(e.Value)%recordIDLength != 0 {
			return fmt.Errorf("%w: multihash %x with %d bytes of record IDs", importer.ErrWireEncoding, e.Key, len(e.Value))
		}
	default:
		return fmt.Errorf("%w: unknown kind %q", importer.ErrWireEncoding, kind)
	}
	return nil
}

// fromWireEntry converts an entry of the kind with prefix p from the
// import's wire layout to Pebble's on-disk layout, the reverse of
// toWireEntry. key is the wire key with the prefix byte in front. A record's
// or context map entry's value is stored as sent. A multihash's value, its
// context keys, becomes the store's slot list: a length byte, then the value
// key (0x02 and the context key) for each context key, as marshalValueKeys
// writes it. key is built in keyBuf and the slots in slotsBuf, and both are
// returned for reuse. The entry must have passed checkImportEntry.
func fromWireEntry(
	p keyPrefix,
	e importer.Entry,
	keyBuf, slotsBuf []byte,
) (
	key, value, reusedKeyBuf, reusedSlotsBuf []byte,
) {
	key = append(append(keyBuf[:0], byte(p)), e.Key...)
	if p != multihashKeyPrefix {
		return key, e.Value, key, slotsBuf
	}
	slots := slotsBuf[:0]
	for off := 0; off < len(e.Value); off += recordIDLength {
		slots = append(slots, marshalledValueKeyLength-1, byte(valueKeyPrefix))
		slots = append(slots, e.Value[off:off+recordIDLength]...)
	}
	return key, slots, key, slots
}

// Sync implements importer.Target with a synced write of an empty record
// to Pebble's write-ahead log. The log is written in order, so the fsync
// makes every batch committed before it durable. Unlike Flush, it writes no
// sstable: the data stays in the memtable and the log. It relies on the
// write-ahead log, which a store opened with DisableWAL does not have.
func (s *store) Sync() error {
	return s.db.LogData(nil, pebble.Sync)
}
