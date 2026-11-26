package pebble

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"iter"
	"math"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/storeutil"
	"github.com/multiformats/go-varint"
)

// This file streams the value store's kinds of an indexer import out of a
// store and into another: importer.KindRecords (0x02 keys),
// importer.KindContextMap (0x06) and importer.KindMultihashes (0x01). A
// wire key, and a cursor, is a store key without its prefix byte.
//
// The target writes what it is given. It checks only that each entry has
// its kind's wire layout.

// recordIDLength is the length of a record ID: a value key without its
// prefix byte
const recordIDLength = 2 * providerHashLen

// Kinds are the kinds of the store, which it serves as an import source
// and writes as a target
var Kinds = []importer.Kind{
	importer.KindRecords,
	importer.KindContextMap,
	importer.KindMultihashes,
}

// kindPrefix returns the key prefix of a kind's keys in the store
func kindPrefix(kind importer.Kind) (keyPrefix, error) {
	switch kind {
	case importer.KindRecords:
		return valueKeyPrefix, nil
	case importer.KindContextMap:
		return contextMapKeyPrefix, nil
	case importer.KindMultihashes:
		return multihashKeyPrefix, nil
	default:
		return 0, importer.PermanentError(fmt.Errorf("unknown kind %q", kind))
	}
}

// The store is an import source: OpenStream serves importer.KindRecords,
// importer.KindContextMap and importer.KindMultihashes in their wire
// encoding. A caller checks for import support with a type assertion on the
// indexer.Interface that New returns.
var _ importer.Source = (*store)(nil)

// OpenStream implements importer.Source. It picks the end of an open part
// with storeutil.PlanPart, which reads a sample of the part, after about
// 128 MiB of wire keys and values without a size hint. A record's and a
// context map entry's cluster is its provider's hash, and a multihash's
// cluster is its code and digest length (see clusterLen). It stops a part,
// open or closed, with an importer.PartialError once the part has yielded
// twice the size hint, or twice 128 MiB without one.
//
// It skips keys with an empty value. A multihash's value becomes the record
// IDs of its live slots; merge-delete slots (0x05) are dropped, and a
// multihash with no live slot is skipped. A closed part whose end is not
// after its start is a permanent error.
func (s *store) OpenStream(
	ctx context.Context,
	kind importer.Kind,
	start, end []byte,
	sizeHint uint64,
) (
	[]byte,
	iter.Seq2[importer.Entry, error],
	error,
) {
	p, err := kindPrefix(kind)
	if err != nil {
		return nil, nil, err
	}
	sizeHint = cmp.Or(sizeHint, storeutil.DefaultPartBytes)
	maxBytes := 2 * min(sizeHint, math.MaxUint64/2)

	if end != nil {
		// End is specified - do not do the estimation, read whole part
		if bytes.Compare(end, start) <= 0 {
			return nil, nil, importer.PermanentError(fmt.Errorf("part end %x is not after its start %x", end, start))
		}
		return end, s.streamOut(ctx, nil, &pebble.IterOptions{
			LowerBound: storeKey(p, start),
			UpperBound: storeKey(p, end),
		}, maxBytes), nil
	}

	kindEnd := []byte{byte(p) + 1}
	partEnd, head, next, err := storeutil.PlanPart(
		func(from []byte) iter.Seq2[importer.Entry, error] {
			return s.streamOut(ctx, nil, &pebble.IterOptions{
				LowerBound: storeKey(p, from),
				UpperBound: kindEnd,
			}, 0)
		},
		start, sizeHint, clusterLen(kind),
	)
	if err != nil {
		return nil, nil, err
	}

	// The rest of the part after the sample, none when the sample is the
	// whole part
	var rest *pebble.IterOptions
	if next != nil {
		rest = &pebble.IterOptions{LowerBound: storeKey(p, next), UpperBound: kindEnd}
		if partEnd != nil {
			rest.UpperBound = storeKey(p, partEnd)
		}
	}
	return partEnd, s.streamOut(ctx, head, rest, maxBytes), nil
}

// clusterLen returns the storeutil.ClusterLen of kind's wire keys.
//
// A record's ID and a context map key start with H(provider), 10 bytes.
// One provider can hold many more records and contexts than another, and
// the hash of a provider's context spreads them evenly after it, so the
// provider's hash is the cluster.
//
// A multihash starts with its code and digest length, two varints. A hash
// function's digests spread evenly over the digest bytes, but the codes and
// lengths hold very different numbers of multihashes, so the two varints
// are the cluster: 2 bytes for sha2-256. A key whose varints do not decode
// is its own cluster.
func clusterLen(kind importer.Kind) storeutil.ClusterLenFunc {
	switch kind {
	case importer.KindMultihashes:
		return func(key []byte) int {
			// The first varint is the multihash code and n its length in bytes
			_, n, err := varint.FromUvarint(key)
			if err != nil {
				// A key whose varints do not decode is its own cluster
				return len(key)
			}
			// The second is the digest length and m its length in bytes
			_, m, err := varint.FromUvarint(key[n:])
			if err != nil {
				// A key whose varints do not decode is its own cluster
				return len(key)
			}
			// The cluster is the two varints
			return n + m
		}

	default:
		// Both context values and context map keys start with the provider's hash
		// that clusters them.
		return func([]byte) int { return providerHashLen }
	}
}

// storeKey returns the store key of a wire key or cursor of prefix p
func storeKey(p keyPrefix, wire []byte) []byte {
	return append([]byte{byte(p)}, wire...)
}

// streamOut yields head, then the wire entries of the keys that a Pebble
// iterator with opts reads, with storeutil.StreamPart; nil opts read none.
// The keys must all have one prefix. It reads the keys with one Pebble
// iterator, which it closes when the loop ends, and yields the iterator's
// keys and values without copying them, so an entry is valid until the next
// iteration. It checks ctx on every key it reads, including the keys it does
// not send (empty values, multihashes with no live slot).
func (s *store) streamOut(
	ctx context.Context,
	head []importer.Entry,
	opts *pebble.IterOptions,
	maxBytes uint64,
) iter.Seq2[importer.Entry, error] {
	return storeutil.StreamPart(
		ctx,
		head,
		wireEntries(pebbleKVs(ctx, s.db, opts)),
		maxBytes,
	)
}

// initialKeyBufContexts is how many context keys the buffer for a
// multihash's context keys holds before it grows. Most multihashes are
// provided in a few contexts.
const initialKeyBufContexts = 3

// wireEntries converts the keys of kvs to wire entries with toWireEntry and
// drops the keys it does not send. An error from kvs is yielded and ends the
// loop.
func wireEntries(kvs iter.Seq2[pebbleKV, error]) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		// keyBuf holds the context keys of the current multihash, shared
		// between iterations to reduce allocations
		keyBuf := make([]byte, 0, initialKeyBufContexts*marshalledValueKeyLength)

		for kv, err := range kvs {
			if err != nil {
				yield(importer.Entry{}, err)
				return
			}
			e, nextKeyBuf, send := toWireEntry(kv.key, kv.value, keyBuf)
			// toWireEntry might have extended keyBuf, reuse it in subsequent
			// iterations to avoid more allocations
			keyBuf = nextKeyBuf
			if send && !yield(e, nil) {
				return
			}
		}
	}
}

// toWireEntry converts a store key with prefix p and its value from Pebble's
// on-disk layout to the import's wire layout. entry's key is the store key
// without its prefix byte. A record's or context map entry's value is sent
// as stored. A multihash's value, a list of slots, becomes the context keys
// of its live slots, written into keyBuf and returned as reusedKeyBuf. send
// is false for a key that is not sent: the prefix alone, an empty value, or
// a multihash with no live slot or whose value does not decode. entry shares
// its key and value with the arguments.
func toWireEntry(
	key, value []byte,
	keyBuf []byte,
) (
	entry importer.Entry,
	reusedKeyBuf []byte,
	send bool,
) {
	if len(key) <= 1 || len(value) == 0 {
		// Skipping malformed keys and multihash keys where merging yields no results
		return importer.Entry{}, keyBuf, false
	}

	switch keyPrefix(key[0]) {
	case multihashKeyPrefix:
		// Multihash value is a list of context keys, we need to strip the value prefixes
		// to get the context keys
		if len(value)%marshalledValueKeyLength != 0 {
			log.Warnw("Import source skips a multihash whose value does not decode", "key", key, "err", indexer.ErrCodecOverflow)
			return importer.Entry{}, keyBuf, false
		}

		keyBuf = keyBuf[:0]
		for off := 0; off < len(value); off += marshalledValueKeyLength {
			vk := value[off+1 : off+marshalledValueKeyLength]
			if keyPrefix(vk[0]) == valueKeyPrefix {
				keyBuf = append(keyBuf, vk[1:]...)
			}
		}

		if len(keyBuf) == 0 {
			// No slot names a value record, for example when every slot is
			// a merge-delete operand with no value left to apply it to. Get
			// finds no value for such a multihash either.
			return importer.Entry{}, keyBuf, false
		}

		return importer.Entry{Key: key[1:], Value: keyBuf}, keyBuf, true

	default:
		return importer.Entry{Key: key[1:], Value: value}, keyBuf, true
	}

}
