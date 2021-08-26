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
	"sync"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/gammazero/radixtree"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

var _ store.Interface = &memoryStore{}

type memoryStore struct {
	// CID -> indexer.Value
	rtree *radixtree.Bytes
	// IndexEntery interning
	interns *radixtree.Bytes
	mutex   sync.Mutex
}

func New() *memoryStore {
	return &memoryStore{
		rtree:   radixtree.New(),
		interns: radixtree.New(),
	}
}

// cidToKey gets the multihash from a CID to be used as a cache key
func cidToKey(c cid.Cid) string { return string(c.Hash()) }

// Get retrieves a slice of values for a CID
func (s *memoryStore) Get(c cid.Cid) ([]indexer.Value, bool, error) {
	k := cidToKey(c)

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

// Put stores an additional value for a CID if the value is not already stored
func (s *memoryStore) Put(c cid.Cid, value indexer.Value) (bool, error) {
	k := cidToKey(c)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Get from current or previous cache
	existing, found := s.getNoLock(k)

	// If found values(s) then check the value to put is already there.
	if found && valueInSlice(&value, existing) {
		return false, nil
	}

	s.rtree.Put(k, append(existing, s.internValue(&value)))
	return true, nil
}

// PutMany stores a value for multiple CIDs
func (s *memoryStore) PutMany(cids []cid.Cid, value indexer.Value) error {
	var interned *indexer.Value

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i := range cids {
		k := cidToKey(cids[i])
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

// Remove removes a value for a CID
func (s *memoryStore) Remove(c cid.Cid, value indexer.Value) (bool, error) {
	k := cidToKey(c)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	return removeValue(s.rtree, k, &value), nil
}

// RemoveMany removes a value from multiple CIDs
func (s *memoryStore) RemoveMany(cids []cid.Cid, value indexer.Value) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i := range cids {
		removeValue(s.rtree, cidToKey(cids[i]), &value)
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
	k := string(value.ProviderID) + string(value.Metadata)
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
