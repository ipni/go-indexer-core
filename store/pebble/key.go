package pebble

import (
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
		valueKey(value *indexer.Value, md bool) (*key, error)
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
)

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
	default:
		return unknownKeyPrefix
	}
}

// next returns the next key after the current kye in lexicographical order.
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
	b.hasher.Reset()
	if _, err := b.hasher.Write([]byte(pid)); err != nil {
		return nil, nil, err
	}

	start = b.p.leaseKey()
	start.append(b.hasher.Sum([]byte{byte(valueKeyPrefix)})...)
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

// valueKey returns the key by which an indexer.Value is identified
func (b *blake3Keyer) valueKey(v *indexer.Value, md bool) (*key, error) {
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

	vk := b.p.leaseKey()
	klen := 1 + len(pidk) + len(ctxk)
	if md {
		vk.maybeGrow(1 + klen)
		vk.append(byte(mergeDeleteKeyPrefix))
	} else {
		vk.maybeGrow(klen)
	}
	vk.append(byte(valueKeyPrefix))
	vk.append(pidk...)
	vk.append(ctxk...)
	return vk, nil
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
	switch k.prefix() {
	case multihashKeyPrefix:
		keyData := k.buf[1:]
		mh := make([]byte, len(keyData))
		copy(mh, keyData)
		return mh, nil
	default:
		return nil, errors.New("key prefix mismatch")
	}
}

func (b *blake3Keyer) Close() error {
	b.hasher.Reset()
	b.p.blake3KeyerPool.Put(b)
	return nil
}
