package radixcache

import (
	"sync"

	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/gammazero/radixtree"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// radixCache is a rotatable cache with value deduplication.
type radixCache struct {
	// CID -> IndexEntry
	current  *radixtree.Bytes
	previous *radixtree.Bytes

	// IndexEntery interning
	curEnts  *radixtree.Bytes
	prevEnts *radixtree.Bytes

	mutex      sync.Mutex
	rotations  uint64
	rotateSize int
}

type CacheStats struct {
	Cids           uint64
	Values         uint64
	UniqueValues   uint64
	InternedValues uint64
	Rotations      uint64
}

// newRadixCache created a new radixCache instance
func newRadixCache(rotateSize int) *radixCache {
	return &radixCache{
		current:    radixtree.New(),
		curEnts:    radixtree.New(),
		rotateSize: rotateSize,
	}
}

// stats returns the following:
//   - Number of CIDs stored in cache
//   - Number of IntexEentry values for all CIDs
//   - Number of unique IndexEntry values
//   - Number of interned IndexEntry values
//   - Number of cache rotations
//
// The number of unique and interred IndexEntry objects will be the same unless
// items have been removed from cache.
func (c *radixCache) stats() CacheStats {
	unique := make(map[*entry.Value]struct{})
	var cidCount, valCount, interned uint64

	c.mutex.Lock()
	defer c.mutex.Unlock()

	walkFunc := func(k string, v interface{}) bool {
		values := v.([]*entry.Value)
		valCount += uint64(len(values))
		for _, val := range values {
			unique[val] = struct{}{}
		}
		return false
	}

	cidCount = uint64(c.current.Len())
	c.current.Walk("", walkFunc)
	if c.previous != nil {
		cidCount += uint64(c.previous.Len())
		c.previous.Walk("", walkFunc)
	}

	interned = uint64(c.curEnts.Len())
	if c.prevEnts != nil {
		interned += uint64(c.prevEnts.Len())
	}

	return CacheStats{
		Cids:           cidCount,
		Values:         valCount,
		UniqueValues:   uint64(len(unique)),
		InternedValues: interned,
		Rotations:      c.rotations,
	}
}

func (c *radixCache) get(k string) ([]*entry.Value, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.getNoLock(k)
}

func (c *radixCache) getNoLock(k string) ([]*entry.Value, bool) {
	// Search current cache
	v, found := c.current.Get(k)
	if !found {
		if c.previous == nil {
			return nil, false
		}

		// Search previous if not found in current
		v, found = c.previous.Get(k)
		if !found {
			return nil, false
		}

		// Pull the interned entries for these values forward from the previous
		// cache to keep using the same pointers as the va used in the cache
		// values that get pulled forward.
		values := v.([]*entry.Value)
		for i, val := range values {
			values[i] = c.internEntry(val)
		}

		// Move the value found in the previous tree into the current one.
		c.current.Put(k, values)
		c.previous.Delete(k)
	}
	return v.([]*entry.Value), true
}

// put stores an entry in the cache if the entry is not already stored.
// Returns true if a new entry was added to the cache.
func (c *radixCache) put(k string, value entry.Value) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Get from current or previous cache
	existing, found := c.getNoLock(k)

	// If found values(s) then check the value to put is already there.
	if found && valueInSlice(&value, existing) {
		return false
	}

	if c.current.Len() >= c.rotateSize {
		c.rotate()
	}

	c.current.Put(k, append(existing, c.internEntry(&value)))
	return true
}

func (c *radixCache) putInterned(k string, value *entry.Value) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	existing, found := c.getNoLock(k)
	if found && valueInSlice(value, existing) {
		return false
	}

	if c.current.Len() >= c.rotateSize {
		c.rotate()
	}

	c.current.Put(k, append(existing, value))
	return true
}

func (c *radixCache) putMany(keys []string, value entry.Value) int {
	var count int
	var interned *entry.Value

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, k := range keys {
		existing, found := c.getNoLock(k)
		if found && valueInSlice(&value, existing) {
			continue
		}

		if c.current.Len() > c.rotateSize {
			c.rotate()
		}

		if interned == nil {
			interned = c.internEntry(&value)
		}
		c.current.Put(k, append(existing, interned))
		count++
	}

	return count
}

func (c *radixCache) remove(k string, value *entry.Value) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	removed := removeEntry(c.current, k, value)
	if c.previous != nil && removeEntry(c.previous, k, value) {
		removed = true
	}
	return removed
}

func (c *radixCache) removeProvider(providerID peer.ID) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	count := removeProviderEntries(c.current, providerID)
	removeProviderInterns(c.curEnts, providerID)
	if c.previous != nil {
		count += removeProviderEntries(c.previous, providerID)
		if c.prevEnts != nil {
			removeProviderInterns(c.prevEnts, providerID)
		}
	}
	return count
}

func (c *radixCache) rotate() {
	c.previous, c.current = c.current, radixtree.New()
	c.prevEnts, c.curEnts = c.curEnts, radixtree.New()
	c.rotations++
}

func (c *radixCache) internEntry(value *entry.Value) *entry.Value {
	k := string(value.ProviderID) + string(value.Metadata)
	v, found := c.curEnts.Get(k)
	if !found {
		if c.prevEnts != nil {
			v, found = c.prevEnts.Get(k)
			if found {
				// Pull interned entry forward from previous cache
				c.curEnts.Put(k, v)
				c.prevEnts.Delete(k)
				return v.(*entry.Value)
			}
		}
		// Intern new value
		c.curEnts.Put(k, value)
		return value
	}
	// Found existing interned value
	return v.(*entry.Value)
}

func removeEntry(tree *radixtree.Bytes, k string, value *entry.Value) bool {
	// Get from current cache
	v, found := tree.Get(k)
	if !found {
		return false
	}

	values := v.([]*entry.Value)
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

func removeProviderEntries(tree *radixtree.Bytes, providerID peer.ID) int {
	var count int
	var deletes []string

	tree.Walk("", func(k string, v interface{}) bool {
		values := v.([]*entry.Value)
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

// valueInSlice checks if the value already exists in slice of values
func valueInSlice(value *entry.Value, values []*entry.Value) bool {
	for _, v := range values {
		if value == v || value.Equal(*v) {
			return true
		}
	}
	return false
}
