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
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/gammazero/radixtree"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

var _ store.Interface = &memoryStore{}

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
	vals, found := s.getNoLock(k)
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

// Put stores an additional value for a multihash if the value is not already stored
func (s *memoryStore) Put(m multihash.Multihash, value indexer.Value) (bool, error) {
	k := string(m)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Get from cache
	existing, found := s.getNoLock(k)

	// If found values(s) then check the value to put is already there.
	if found && valueInSlice(&value, existing) {
		return false, nil
	}

	s.rtree.Put(k, append(existing, s.internValue(&value)))
	return true, nil
}

// PutMany stores a value for multiple multihashes
func (s *memoryStore) PutMany(mhs []multihash.Multihash, value indexer.Value) error {
	var interned *indexer.Value

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i := range mhs {
		k := string(mhs[i])
		existing, found := s.getNoLock(k)
		if found && valueInSlice(&value, existing) {
			continue
		}

		if interned == nil {
			interned = s.internValue(&value)
		}
		s.rtree.Put(k, append(existing, interned))
	}

	return nil
}

// Remove removes a value for a multihash
func (s *memoryStore) Remove(m multihash.Multihash, value indexer.Value) (bool, error) {
	k := string(m)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	return removeValue(s.rtree, k, &value), nil
}

// RemoveMany removes a value from multiple multihashes
func (s *memoryStore) RemoveMany(mhs []multihash.Multihash, value indexer.Value) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i := range mhs {
		removeValue(s.rtree, string(mhs[i]), &value)
	}
	return nil
}

// RemoveProvider removes all values for specified provider.  This is used
// when a provider is no longer indexed by the indexer.
func (s *memoryStore) RemoveProvider(providerID peer.ID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	removeProviderValues(s.rtree, providerID)
	removeProviderInterns(s.interns, providerID)
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

func (s *memoryStore) getNoLock(k string) ([]*indexer.Value, bool) {
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

func (s *memoryStore) internValue(value *indexer.Value) *indexer.Value {
	var b strings.Builder
	b.Grow(len(value.ProviderID) + len(value.Metadata))
	b.Write([]byte(value.ProviderID))
	b.Write(value.Metadata)
	k := b.String()
	v, found := s.interns.Get(k)
	if !found {
		// Intern new value
		s.interns.Put(k, value)
		return value
	}
	// Found existing interned value
	return v.(*indexer.Value)
}

func removeValue(tree *radixtree.Bytes, k string, value *indexer.Value) bool {
	// Get from current cache
	v, found := tree.Get(k)
	if !found {
		return false
	}

	values := v.([]*indexer.Value)
	for i, v := range values {
		if v == value || v.Equal(*value) {
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

func removeProviderValues(tree *radixtree.Bytes, providerID peer.ID) int {
	var count int
	var deletes []string

	tree.Walk("", func(k string, v interface{}) bool {
		values := v.([]*indexer.Value)
		for i := range values {
			if providerID == values[i].ProviderID {
				count++
				if len(values) == 1 {
					deletes = append(deletes, k)
				} else {
					values[i] = values[len(values)-1]
					values[len(values)-1] = nil
					tree.Put(k, values[:len(values)-1])
				}
			}
		}
		return false
	})

	for _, k := range deletes {
		tree.Delete(k)
	}

	return count
}

func removeProviderInterns(tree *radixtree.Bytes, providerID peer.ID) {
	var deletes []string
	tree.Walk(string(providerID), func(k string, v interface{}) bool {
		deletes = append(deletes, k)
		return false
	})
	for _, k := range deletes {
		tree.Delete(k)
	}
}
