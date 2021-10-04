// Package memory defines an in-memory value store
//
// The index data stored by the memory value store is not persisted. This value
// store is primarily useful for testing or for short-lived indexer instances
// that do not need to store huge amounts of data.
//
// When creating an indexer that uses this value store, the indexer should
// generally not be given a cache.
package memory

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/gammazero/radixtree"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

var _ indexer.Interface = &memoryStore{}

type memoryStore struct {
	// multihash -> indexer.Value
	rtree *radixtree.Bytes
	// IndexEntery interning
	interns *radixtree.Bytes
	mutex   sync.Mutex
}

type memoryIter struct {
	iter   radixtree.Iterator
	values []indexer.Value
}

func New() *memoryStore {
	return &memoryStore{
		rtree:   radixtree.New(),
		interns: radixtree.New(),
	}
}

// Get retrieves a slice of values for a multihash
func (s *memoryStore) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	k := string(m)

	s.mutex.Lock()
	defer s.mutex.Unlock()
	vals, found := s.get(k)
	if !found || len(vals) == 0 {
		return nil, found, nil
	}

	ret := make([]indexer.Value, len(vals))
	for i, v := range vals {
		ret[i] = *v
	}

	return ret, true, nil
}

func (s *memoryStore) Iter() (indexer.Iterator, error) {
	return &memoryIter{
		iter: s.rtree.Iter(),
	}, nil
}

func (it *memoryIter) Next() (multihash.Multihash, []indexer.Value, error) {
	key, val, done := it.iter.Next()
	if done {
		it.values = nil
		return nil, nil, io.EOF
	}

	m := multihash.Multihash([]byte(key))
	vals, ok := val.([]*indexer.Value)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected type stored by %q", m.B58String())
	}

	it.values = it.values[:0]
	for _, v := range vals {
		it.values = append(it.values, *v)
	}
	return m, it.values, nil
}

// Put stores a value for multiple multihashes
func (s *memoryStore) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	if len(value.Metadata) == 0 {
		return errors.New("value missing metadata")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Store the new value or update the matching value and return a pointer to
	// the internally stored value.
	if len(mhs) == 0 {
		s.internValue(&value, false)
		return nil
	}
	interned := s.internValue(&value, true)

keysLoop:
	for i := range mhs {
		k := string(mhs[i])
		existing, found := s.get(k)
		if found {
			for j, v := range existing {
				if v == interned {
					// Key is already mapped to value
					continue keysLoop
				}
				// TODO: Should be able to remove this code block: It should
				// not be possible for the provided value to match an existing
				// value, but not be the interned value.
				if value.Match(*v) {
					panic("should not be possible")
					// Replace existing matching value
					existing[j] = interned
					s.rtree.Put(k, existing)
					continue keysLoop
				}
			}
		}

		s.rtree.Put(k, append(existing, interned))
	}

	return nil
}

// Remove removes a value from multiple multihashes
func (s *memoryStore) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, val, found := s.findInternValue(&value)
	if !found {
		return nil
	}

	for i := range mhs {
		removeIndex(s.rtree, string(mhs[i]), val)
	}
	return nil
}

// RemoveProvider removes all values for specified provider.  This is used
// when a provider is no longer indexed by the indexer.
func (s *memoryStore) RemoveProvider(providerID peer.ID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.removeProviderValues(providerID)
	s.removeProviderInterns(providerID)
	return nil
}

// RemoveProviderContext removes all values for specified providerID that have
// the specified contextID.  The mappings of multihashes to these values are
// removed when they are retrieved, by detecting that the metadata of these
// values no longer exists.
func (s *memoryStore) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.deleteInternValue(providerID, contextID) {
		return nil
	}

	delVal := indexer.Value{
		ProviderID: providerID,
		ContextID:  contextID,
	}

	var deletes []string
	updates := make(map[string][]*indexer.Value)
	s.rtree.Walk("", func(k string, v interface{}) bool {
		values := v.([]*indexer.Value)
		var needUpdate bool
		for i := 0; i < len(values); {
			if values[i].Match(delVal) {
				if len(values) == 1 {
					// Last value, so just delete index
					deletes = append(deletes, k)
					needUpdate = false
					break
				}
				values[i] = values[len(values)-1]
				values[len(values)-1] = nil
				values = values[:len(values)-1]
				needUpdate = true
				continue
			}
			i++
		}
		if needUpdate {
			updates[k] = values
		}
		return false
	})

	for _, k := range deletes {
		s.rtree.Delete(k)
	}

	for k, values := range updates {
		s.rtree.Put(k, values)
	}

	return nil
}

// Size returns the total storage capacity being used
func (s *memoryStore) Size() (int64, error) {
	return 0, nil
}

// Flush commits changes to storage
func (s *memoryStore) Flush() error { return nil }

// Close gracefully closes the store flushing all pending data from memory
func (s *memoryStore) Close() error { return nil }

func (s *memoryStore) get(k string) ([]*indexer.Value, bool) {
	// Search current cache
	v, found := s.rtree.Get(k)
	if !found {
		return nil, false
	}
	return v.([]*indexer.Value), true
}

// valueInSlice checks if the value already exists in slice of values
func valueInSlice(value *indexer.Value, values []*indexer.Value) bool {
	for _, v := range values {
		if value == v || value.Equal(*v) {
			return true
		}
	}
	return false
}

// internValue stores a single copy of a Value under a key composed of
// ProviderID and ContextID, and then returns a pointer to the internally
// stored value.
//
// Metadata is not included in the key because all Values with the same
// ProviderID and ContextID must have the same metadata.
//
// When storing a Value that has a ProviderID and ContextID that matches an
// existing value, the existing value's metadata is updated.
//
// A set of multihahses and a metadata, for a particular (providerID,
// contextID) can only be set once.  This means that for any (providerID,
// contextID) there will only ever be one instance of a particular metadata
// value.  So, two different Values cannot have the same provider ID and
// context ID, but different metadata.  Therefore, metadata is not needed as
// part of the unique key for a value.
func (s *memoryStore) internValue(value *indexer.Value, saveNew bool) *indexer.Value {
	k, v, found := s.findInternValue(value)
	if found {
		// The provided value has matching ProviderID and ContextID but
		// different Metadata.  Treat this as an update.
		if !bytes.Equal(v.Metadata, value.Metadata) {
			metadata := make([]byte, len(value.Metadata))
			copy(metadata, value.Metadata)
			v.Metadata = metadata
		}
		return v
	}

	if !saveNew {
		return nil
	}

	// Intern new value
	s.interns.Put(k, value)
	return value
}

func (s *memoryStore) findInternValue(value *indexer.Value) (string, *indexer.Value, bool) {
	var b strings.Builder
	b.Grow(len(value.ProviderID) + len(value.ContextID))
	b.WriteString(string(value.ProviderID))
	b.Write(value.ContextID)
	k := b.String()
	v, found := s.interns.Get(k)
	if found {
		// Found existing interned value
		return k, v.(*indexer.Value), true
	}

	return k, nil, false
}

func (s *memoryStore) deleteInternValue(providerID peer.ID, contextID []byte) bool {
	var b strings.Builder
	b.Grow(len(providerID) + len(contextID))
	b.WriteString(string(providerID))
	b.Write(contextID)
	return s.interns.Delete(b.String())
}

func removeIndex(tree *radixtree.Bytes, k string, value *indexer.Value) bool {
	v, found := tree.Get(k)
	if !found {
		return false
	}

	values := v.([]*indexer.Value)
	for i, v := range values {
		if v == value || v.Match(*value) {
			if len(values) == 1 {
				tree.Delete(k)
			} else {
				values[i] = values[len(values)-1]
				values[len(values)-1] = nil
				tree.Put(k, values[:len(values)-1])
			}
			return true
		}
	}
	return false
}

func (s *memoryStore) removeProviderValues(providerID peer.ID) {
	var deletes []string

	s.rtree.Walk("", func(k string, v interface{}) bool {
		values := v.([]*indexer.Value)
		for i := range values {
			if providerID == values[i].ProviderID {
				if len(values) == 1 {
					deletes = append(deletes, k)
				} else {
					values[i] = values[len(values)-1]
					values[len(values)-1] = nil
					s.rtree.Put(k, values[:len(values)-1])
				}
			}
		}
		return false
	})

	for _, k := range deletes {
		s.rtree.Delete(k)
	}
}

func (s *memoryStore) removeProviderInterns(providerID peer.ID) {
	var deletes []string
	s.interns.Walk(string(providerID), func(k string, v interface{}) bool {
		deletes = append(deletes, k)
		return false
	})
	for _, k := range deletes {
		s.interns.Delete(k)
	}
}
