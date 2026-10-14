package pebble

import (
	"bytes"
	"errors"
	"io"

	"github.com/ipni/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"lukechampine.com/blake3"
)

var (
	_ keyer     = (*blake3Keyer)(nil)
	_ io.Closer = (*blake3Keyer)(nil)
	_ io.Closer = (*key)(nil)
	_ io.Closer = (*keyList)(nil)
)

type (
	keyPrefix byte
	key       struct {
		buf []byte
		p   *pool
	}
	keyList struct {
		keys []*key
		p    *pool
	}
	keyer interface {
		multihashKey(mh multihash.Multihash) (*key, error)
		multihashesKeyRange() (start, end *key, err error)
		keyToMultihash(*key) (multihash.Multihash, error)
		valuesByProviderKeyRange(pid peer.ID) (start, end *key, err error)
		contextMapsByProviderKeyRange(pid peer.ID) (start, end *key, err error)
		contextMapKey(pid peer.ID, contextID []byte) (*key, error)
		valueKey(value *indexer.Value, md bool) (*key, error)
		valueKeyWithToken(pid peer.ID, token []byte, md bool) (*key, error)
	}
	blake3Keyer struct {
		hasher *blake3.Hasher
		p      *pool
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
	// contextMapKeyPrefix represents the prefix of a key that maps a provider ID and
	// context ID to the opaque token used as the context half of a value key.
	contextMapKeyPrefix
)

// contextTokenLength is the length of the opaque token stored in a context map entry
// and used as the context half of a value key. It matches the blake3 digest size used
// for the provider and context halves of legacy value keys.
const contextTokenLength = defaultKeyerLength / 2

// prefix returns the keyPrefix of this key by checking its first byte.
// If no known prefix is found unknownKeyPrefix is returned.
func (k *key) prefix() keyPrefix {
	if len(k.buf) == 0 {
		return unknownKeyPrefix
	}
	switch k.buf[0] {
	case byte(multihashKeyPrefix):
		return multihashKeyPrefix
	case byte(valueKeyPrefix):
		return valueKeyPrefix
	case byte(mergeDeleteKeyPrefix):
		return mergeDeleteKeyPrefix
	case byte(contextMapKeyPrefix):
		return contextMapKeyPrefix
	default:
		return unknownKeyPrefix
	}
}

// next returns the next key after the current key in lexicographical order.
// See: bytes.Compare
func (k *key) next() *key {
	var next *key
	for i := len(k.buf) - 1; i >= 0; i-- {
		b := k.buf[i]
		if b == 0xff {
			continue
		}
		next = k.p.leaseKey()
		next.maybeGrow(i + 1)
		next.buf = next.buf[:i+1]
		copy(next.buf, k.buf)
		next.buf[i] = b + 1
		break
	}
	return next
}

// replacePrefix returns a copy of k with its prefix byte replaced by prefix.
func (k *key) replacePrefix(prefix keyPrefix) *key {
	c := k.p.leaseKey()
	c.maybeGrow(len(k.buf))
	c.append(k.buf...)
	c.buf[0] = byte(prefix)
	return c
}

func (k *key) append(b ...byte) {
	k.buf = append(k.buf, b...)
}

func (k *key) maybeGrow(n int) {
	l := len(k.buf)
	switch {
	case n <= cap(k.buf)-l:
	case l == 0:
		k.buf = make([]byte, 0, n*pooledSliceCapGrowthFactor)
	default:
		k.buf = append(make([]byte, 0, (l+n)*pooledSliceCapGrowthFactor), k.buf...)
	}
}

func (k *key) Close() error {
	if cap(k.buf) <= pooledKeyMaxCap {
		k.buf = k.buf[:0]
		k.p.keyPool.Put(k)
	}
	return nil
}

func (kl *keyList) append(b ...*key) {
	kl.keys = append(kl.keys, b...)
}

func (kl *keyList) maybeGrow(n int) {
	l := len(kl.keys)
	switch {
	case n <= cap(kl.keys)-l:
	case l == 0:
		kl.keys = make([]*key, 0, n*pooledSliceCapGrowthFactor)
	default:
		kl.keys = append(make([]*key, 0, (l+n)*pooledSliceCapGrowthFactor), kl.keys...)
	}
}

func (kl *keyList) Close() error {
	if cap(kl.keys) <= pooledKeyListMaxCap {
		for _, k := range kl.keys {
			_ = k.Close()
		}
		kl.keys = kl.keys[:0]
		kl.p.keyListPool.Put(kl)
	}
	return nil
}

// newBlake3Keyer instantiates a new keyer that uses blake3 hash function, where the
// generated key lengths are:
// - l + 1 for indexer.Value keys
// - l + 2 for merge-delete indexer.Value keys
// - multihash length + 1 for multihash keys
func newBlake3Keyer(l int, p *pool) *blake3Keyer {
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
		p:      p,
	}
}

// valuesByProviderKeyRange returns the key range that contains all the indexer.Value records
// that belong to the given provider ID.
func (b *blake3Keyer) valuesByProviderKeyRange(pid peer.ID) (start, end *key, err error) {
	pidk, err := b.hashPeerID(pid)
	if err != nil {
		return nil, nil, err
	}
	start = b.p.leaseKey()
	start.maybeGrow(1 + len(pidk))
	start.append(byte(valueKeyPrefix))
	start.append(pidk...)
	end = start.next()
	return
}

// contextMapsByProviderKeyRange returns the key range that contains all context-map entries
// for the given provider ID.
func (b *blake3Keyer) contextMapsByProviderKeyRange(pid peer.ID) (start, end *key, err error) {
	pidk, err := b.hashPeerID(pid)
	if err != nil {
		return nil, nil, err
	}
	start = b.p.leaseKey()
	start.maybeGrow(1 + len(pidk))
	start.append(byte(contextMapKeyPrefix))
	start.append(pidk...)
	end = start.next()
	return
}

// multihashesKeyRange returns the key range that contains all the records identified by
// multihashKeyPrefix, i.e. all the stored multihashes to which one or more indexer.Value records
// are associated.
func (b *blake3Keyer) multihashesKeyRange() (start, end *key, err error) {
	start = b.p.leaseKey()
	start.maybeGrow(1)
	start.append(byte(multihashKeyPrefix))
	end = start.next()
	return
}

// contextMapKey returns the key that stores the opaque token for a provider ID and context ID.
// The key holds the provider and context hashes in the same layout as a legacy value key,
// so the two differ only in the prefix byte.
func (b *blake3Keyer) contextMapKey(pid peer.ID, contextID []byte) (*key, error) {
	pidk, err := b.hashPeerID(pid)
	if err != nil {
		return nil, err
	}
	ctxk, err := b.hashContextID(contextID)
	if err != nil {
		return nil, err
	}
	mk := b.p.leaseKey()
	mk.maybeGrow(1 + len(pidk) + len(ctxk))
	mk.append(byte(contextMapKeyPrefix))
	mk.append(pidk...)
	mk.append(ctxk...)
	return mk, nil
}

// valueKey returns the legacy key by which an indexer.Value is identified: the provider ID
// and context ID are each blake3-hashed. Callers that use a mapped token should use
// valueKeyWithToken instead.
func (b *blake3Keyer) valueKey(v *indexer.Value, md bool) (*key, error) {
	ctxk, err := b.hashContextID(v.ContextID)
	if err != nil {
		return nil, err
	}
	return b.valueKeyWithToken(v.ProviderID, ctxk, md)
}

// valueKeyWithToken returns the key by which an indexer.Value is identified using the given
// opaque token as the context half. The token is stored as-is and is not hashed again.
func (b *blake3Keyer) valueKeyWithToken(pid peer.ID, token []byte, md bool) (*key, error) {
	pidk, err := b.hashPeerID(pid)
	if err != nil {
		return nil, err
	}

	vk := b.p.leaseKey()
	klen := 1 + len(pidk) + len(token)
	if md {
		vk.maybeGrow(1 + klen)
		vk.append(byte(mergeDeleteKeyPrefix))
	} else {
		vk.maybeGrow(klen)
	}
	vk.append(byte(valueKeyPrefix))
	vk.append(pidk...)
	vk.append(token...)
	return vk, nil
}

func (b *blake3Keyer) hashPeerID(pid peer.ID) ([]byte, error) {
	b.hasher.Reset()
	if _, err := b.hasher.Write([]byte(pid)); err != nil {
		return nil, err
	}
	return b.hasher.Sum(nil), nil
}

func (b *blake3Keyer) hashContextID(contextID []byte) ([]byte, error) {
	b.hasher.Reset()
	if _, err := b.hasher.Write(contextID); err != nil {
		return nil, err
	}
	return b.hasher.Sum(nil), nil
}

// multihashKey returns the key by which a multihash is identified
func (b *blake3Keyer) multihashKey(mh multihash.Multihash) (*key, error) {
	mhk := b.p.leaseKey()
	mhk.maybeGrow(1 + len(mh))
	mhk.append(byte(multihashKeyPrefix))
	mhk.append(mh...)
	return mhk, nil
}

// keyToMultihash extracts the multihash to which the given key is associated.
// An error is returned if the given key does not have multihashKeyPrefix.
func (b *blake3Keyer) keyToMultihash(k *key) (multihash.Multihash, error) {
	if k.prefix() != multihashKeyPrefix {
		return nil, errors.New("key prefix mismatch")
	}
	return bytes.Clone(k.buf[1:]), nil
}

func (b *blake3Keyer) Close() error {
	b.hasher.Reset()
	b.p.blake3KeyerPool.Put(b)
	return nil
}
