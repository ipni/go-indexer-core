package radixcache

import (
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// concurrrency is the lock granularity for radixtree. Must be power of two.
// This can be adjusted, but testing seems to indicate 16 is a good balance.
const concurrency = 16

// multiCache is a set of multiple radixCache instances
type multiCache struct {
	cacheSet []*radixCache
}

var _ cache.Interface = &multiCache{}

// cidToKey gets the multihash from a CID to be used as a cache key
func cidToKey(c cid.Cid) string { return string(c.Hash()) }

// New creates a new multiCache
func New(size int) *multiCache {
	cacheSetSize := concurrency
	if size < 256 {
		cacheSetSize = 1
	}
	cacheSet := make([]*radixCache, cacheSetSize)
	rotateSize := size / (cacheSetSize * 2)
	for i := range cacheSet {
		cacheSet[i] = newRadixCache(rotateSize)
	}
	return &multiCache{
		cacheSet: cacheSet,
	}
}

func (s *multiCache) Get(c cid.Cid) ([]entry.Value, bool, error) {
	// Keys indexed as multihash
	k := cidToKey(c)
	cache := s.getCache(k)

	ents, found := cache.get(k)
	if !found {
		return nil, false, nil
	}

	ret := make([]entry.Value, len(ents))
	for i, v := range ents {
		ret[i] = *v
	}
	return ret, true, nil
}

func (s *multiCache) Put(c cid.Cid, value entry.Value) (bool, error) {
	return s.PutCheck(c, value), nil
}

// PutCheck stores an entry.Value for a CID if the value is not already
// stored.  New entries are added to the entries that are already there.
// Returns true if a new value was added to the cache.
//
// Only rotate one cache at a time. This may leave older entries in other
// caches, but if CIDs are dirstributed evenly over the cache set then over
// time all members should be rotated the same amount on average.  This is done
// so that it is not necessary to lock all caches in order to perform a
// rotation.  This also means that items age out more incrementally.
func (s *multiCache) PutCheck(c cid.Cid, value entry.Value) bool {
	k := cidToKey(c)

	cache := s.getCache(k)
	return cache.put(k, value)
}

func (s *multiCache) PutMany(cids []cid.Cid, value entry.Value) error {
	s.PutManyCount(cids, value)
	return nil
}

// PutManyCount stores an entry.Value for multiple CIDs.  Returns the
// number of new entries stored.  A new entry is counted whenever a value
// is added to the list of values for a CID, whether or not that CID was
// already in the cache.
//
// This is more efficient than using Put to store individual values, becase
// PutMany allows the same entry.Value to be reused across all sub-caches.
func (s *multiCache) PutManyCount(cids []cid.Cid, value entry.Value) uint64 {
	if len(s.cacheSet) == 1 {
		keys := make([]string, len(cids))
		for i := range cids {
			keys[i] = cidToKey(cids[i])
		}
		return uint64(s.cacheSet[0].putMany(keys, value))
	}
	var stored uint64
	var reuseEnt *entry.Value
	interns := make(map[*radixCache]*entry.Value, len(s.cacheSet))

	for i := range cids {
		k := cidToKey(cids[i])
		cache := s.getCache(k)
		ent, ok := interns[cache]
		if !ok {
			// Intern the value once for this cache to avoid repeared lookups
			// on every call to cache.put().  If the value is not already
			// interned for the cache, then reuse an value that is already
			// interned elsewhere.
			cache.mutex.Lock()
			if reuseEnt == nil {
				ent = cache.internEntry(&value)
				reuseEnt = ent
			} else {
				ent = cache.internEntry(reuseEnt)
			}
			cache.mutex.Unlock()
			interns[cache] = ent
		}
		if cache.putInterned(k, ent) {
			stored++
		}
	}

	return stored
}

func (s *multiCache) Remove(c cid.Cid, value entry.Value) (bool, error) {
	return s.RemoveCheck(c, value), nil
}

// RemoveCheck removes an entry.Value for a CID.  Returns true if an
// entry was removed from cache.
func (s *multiCache) RemoveCheck(c cid.Cid, value entry.Value) bool {
	k := cidToKey(c)

	cache := s.getCache(k)
	return cache.remove(k, &value)
}

func (s *multiCache) RemoveMany(cids []cid.Cid, value entry.Value) error {
	s.RemoveManyCount(cids, value)
	return nil
}

// RemoveManyCount removes an entry.Value from multiple CIDs.  Returns
// the number of values removed.
func (s *multiCache) RemoveManyCount(cids []cid.Cid, value entry.Value) uint64 {
	var removed uint64

	for i := range cids {
		k := cidToKey(cids[i])
		cache := s.getCache(k)
		if cache.remove(k, &value) {
			removed++
		}
	}

	return removed
}

func (s *multiCache) RemoveProvider(providerID peer.ID) error {
	s.RemoveProviderCount(providerID)
	return nil
}

// RemoveProvider removes all enrties for specified provider.  Returns the
// total number of entries removed from the cache.
func (s *multiCache) RemoveProviderCount(providerID peer.ID) uint64 {
	countChan := make(chan uint64)
	for _, cache := range s.cacheSet {
		go func(c *radixCache) {
			countChan <- uint64(c.removeProvider(providerID))
		}(cache)
	}
	var total uint64
	for i := 0; i < len(s.cacheSet); i++ {
		total += <-countChan
	}
	return total
}

func (s *multiCache) Stats() CacheStats {
	statsChan := make(chan CacheStats)
	for _, cache := range s.cacheSet {
		go func(cache *radixCache) {
			statsChan <- cache.stats()
		}(cache)
	}

	var totalStats CacheStats
	for i := 0; i < len(s.cacheSet); i++ {
		stats := <-statsChan
		totalStats.Cids += stats.Cids
		totalStats.Values += stats.Values
		totalStats.UniqueValues += stats.UniqueValues
		totalStats.InternedValues += stats.InternedValues
		totalStats.Rotations += stats.Rotations
	}

	return totalStats
}

// getCache returns the cache that stores the given key.  This function must
// evenly distribute keys over the set of caches.
func (s *multiCache) getCache(k string) *radixCache {
	var idx int
	if k != "" {
		// Use last bits of key for good distribution
		//
		// bitwise modulus requires that size of cache set is power of 2
		idx = int(k[len(k)-1]) & (len(s.cacheSet) - 1)
	}
	return s.cacheSet[idx]
}

func (c *multiCache) Size() (int64, error) {
	panic("not implemented")
}
