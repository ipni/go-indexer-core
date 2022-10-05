package pebble

import (
	"bytes"
	"errors"

	"github.com/libp2p/go-libp2p/core/peer"
	"lukechampine.com/blake3"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/multiformats/go-multihash"
)

type (
	keyPrefix byte
	key       []byte
	keyer     interface {
		multihashKey(mh multihash.Multihash) (key, error)
		multihashesKeyRange() (start, end key, err error)
		keyToMultihash(key) (multihash.Multihash, error)
		valuesByProviderKeyRange(pid peer.ID) (start, end key, err error)
		valueKey(value indexer.Value, md bool) (key, error)
	}
	blake3Keyer struct {
		hasher *blake3.Hasher
	}
)

const (
	// unknownKeyPrefix signals an unknown key prefix.
	unknownKeyPrefix keyPrefix = iota
	// multihashKeyPrefix represents the prefix of a key that represent a multihash.
	multihashKeyPrefix
	// valueKeyPrefix represents the prefix of a key that is associated to indexer.Value
	// records.
	valueKeyPrefix
	// mergeDeleteKeyPrefix represents the in-memory prefix added to a key in order to signal that
	// it should be removed during merge. See: valueKeysValueMerger.
	mergeDeleteKeyPrefix
)

var _ keyer = (*blake3Keyer)(nil)

// prefix returns the keyPrefix of this key by checking its first byte.
// If no known prefix is found unknownKeyPrefix is returned.
func (k key) prefix() keyPrefix {
	if len(k) == 0 {
		return unknownKeyPrefix
	}
	switch k[0] {
	case byte(multihashKeyPrefix):
		return multihashKeyPrefix
	case byte(valueKeyPrefix):
		return valueKeyPrefix
	case byte(mergeDeleteKeyPrefix):
		return mergeDeleteKeyPrefix
	default:
		return unknownKeyPrefix
	}
}

// next returns the next key after the current kye in lexicographical order.
// See: bytes.Compare
func (k key) next() key {
	var next key
	for i := len(k) - 1; i >= 0; i-- {
		b := k[i]
		if b == 0xff {
			continue
		}
		next = make([]byte, i+1)
		copy(next, k)
		next[i] = b + 1
		break
	}
	return next
}

// stripMergeDelete removes the mergeDeleteKeyPrefix prefix from this key only if the key
// has the prefix mergeDeleteKeyPrefix.
// It returns the key prefix and the resulting key; mergeDeleteKeyPrefix as the returned prefix
// signals that the strip has occurred.
func (k key) stripMergeDelete() (keyPrefix, key) {
	prefix := k.prefix()
	switch prefix {
	case mergeDeleteKeyPrefix:
		return prefix, k[1:]
	default:
		return prefix, k
	}
}

// newBlake3Keyer instantiates a new keyer that uses blake3 hash function, where the
// generated key lengths are:
// - l + 1 for indexer.Value keys
// - l + 2 for merge-delete indexer.Value keys
// - multihash length + 1 for multihash keys
func newBlake3Keyer(l int) *blake3Keyer {
	return &blake3Keyer{
		// Instantiate the hasher with half the given length. Because,
		// hasher is only used for generating indexer.Value keys, and
		// such keys are made up of: some prefix + hash of provider ID
		// + hash of context ID.
		// Using half the given length means we will avoid doubling the
		// key length while maintaining the ability to lookup values
		// key-range by provider ID since all such keys will have the
		// same prefix.
		hasher: blake3.New(l/2, nil),
	}
}

// valuesByProviderKeyRange returns the key range that contains all the indexer.Value records
// that belong to the given provider ID.
func (b *blake3Keyer) valuesByProviderKeyRange(pid peer.ID) (start, end key, err error) {
	b.hasher.Reset()
	if _, err := b.hasher.Write([]byte(pid)); err != nil {
		return nil, nil, err
	}
	start = b.hasher.Sum([]byte{byte(valueKeyPrefix)})
	end = start.next()
	return
}

// multihashesKeyRange returns the key range that contains all the records identified by
// multihashKeyPrefix, i.e. all the stored multihashes to which one or more indexer.Value records
// are associated.
func (b *blake3Keyer) multihashesKeyRange() (start, end key, err error) {
	start = []byte{byte(multihashKeyPrefix)}
	end = start.next()
	return
}

// valueKey returns the key by which an indexer.Value is identified
func (b *blake3Keyer) valueKey(v indexer.Value, md bool) (key, error) {
	b.hasher.Reset()
	if _, err := b.hasher.Write([]byte(v.ProviderID)); err != nil {
		return nil, err
	}
	pidk := b.hasher.Sum(nil)

	b.hasher.Reset()
	if _, err := b.hasher.Write(v.ContextID); err != nil {
		return nil, err
	}
	ctxk := b.hasher.Sum(nil)

	var buf bytes.Buffer
	klen := 1 + len(pidk) + len(ctxk)
	if md {
		buf.Grow(1 + klen)
		buf.WriteByte(byte(mergeDeleteKeyPrefix))
	} else {
		buf.Grow(klen)
	}
	buf.WriteByte(byte(valueKeyPrefix))
	buf.Write(pidk)
	buf.Write(ctxk)
	return buf.Bytes(), nil
}

// multihashKey returns the key by which a multihash is identified
func (b *blake3Keyer) multihashKey(mh multihash.Multihash) (key, error) {
	var buf bytes.Buffer
	buf.Grow(1 + len(mh))
	buf.WriteByte(byte(multihashKeyPrefix))
	buf.Write(mh)
	return buf.Bytes(), nil
}

// keyToMultihash extracts the multihash to which the given key is associated.
// An error is returned if the given key does not have multihashKeyPrefix.
func (b *blake3Keyer) keyToMultihash(k key) (multihash.Multihash, error) {
	switch k.prefix() {
	case multihashKeyPrefix:
		keyData := k[1:]
		mhData := make([]byte, len(keyData))
		copy(mhData, keyData)
		return multihash.Multihash(mhData), nil
	default:
		return nil, errors.New("key prefix mismatch")
	}
}
