package radixcache

import (
	"sync"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/gammazero/radixtree"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// radixCache is a rotatable cache with value deduplication.
type radixCache struct {
	// multihash -> indexer.Value
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
	Indexes        uint64
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
//   - Number of indexes stored in cache
//   - Number of values for all indexess
//   - Number of unique values
//   - Number of interned values
//   - Number of cache rotations
//
// The number of unique and interred indexer.Value objects will be the same unless
// items have been removed from cache.
func (c *radixCache) stats() CacheStats {
	unique := make(map[*indexer.Value]struct{})
	var indexCount, valCount, interned uint64

	c.mutex.Lock()
	defer c.mutex.Unlock()

	walkFunc := func(k string, v interface{}) bool {
		values := v.([]*indexer.Value)
		valCount += uint64(len(values))
		for _, val := range values {
			unique[val] = struct{}{}
		}
		return false
	}

	indexCount = uint64(c.current.Len())
	c.current.Walk("", walkFunc)
	if c.previous != nil {
		indexCount += uint64(c.previous.Len())
		c.previous.Walk("", walkFunc)
	}

	interned = uint64(c.curEnts.Len())
	if c.prevEnts != nil {
		interned += uint64(c.prevEnts.Len())
	}

	return CacheStats{
		Indexes:        indexCount,
		Values:         valCount,
		UniqueValues:   uint64(len(unique)),
		InternedValues: interned,
		Rotations:      c.rotations,
	}
}

func (c *radixCache) get(k string) ([]*indexer.Value, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.getNoLock(k)
}

func (c *radixCache) getNoLock(k string) ([]*indexer.Value, bool) {
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

		// Pull the interned values for these values forward from the previous
		// cache to keep using the same pointers as the va used in the cache
		// values that get pulled forward.
		values := v.([]*indexer.Value)
		for i, val := range values {
			values[i] = c.internValue(val)
		}

		// Move the value found in the previous tree into the current one.
		c.current.Put(k, values)
		c.previous.Delete(k)
	}
	return v.([]*indexer.Value), true
}

// put stores a value in the cache if the value is not already stored.
// Returns true if a new value was added to the cache.
func (c *radixCache) put(k string, value indexer.Value) bool {
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

	c.current.Put(k, append(existing, c.internValue(&value)))
	return true
}

func (c *radixCache) putInterned(k string, value *indexer.Value) bool {
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

func (c *radixCache) putMany(keys []string, value indexer.Value) int {
	var count int
	var interned *indexer.Value

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
			interned = c.internValue(&value)
		}
		c.current.Put(k, append(existing, interned))
		count++
	}

	return count
}

func (c *radixCache) remove(k string, value *indexer.Value) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	removed := removeValue(c.current, k, value)
	if c.previous != nil && removeValue(c.previous, k, value) {
		removed = true
	}
	return removed
}

func (c *radixCache) removeProvider(providerID peer.ID) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	count := removeProviderValues(c.current, providerID)
	removeProviderInterns(c.curEnts, providerID)
	if c.previous != nil {
		count += removeProviderValues(c.previous, providerID)
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

func (c *radixCache) internValue(value *indexer.Value) *indexer.Value {
	k := string(value.ProviderID) + string(value.Metadata)
	v, found := c.curEnts.Get(k)
	if !found {
		if c.prevEnts != nil {
			v, found = c.prevEnts.Get(k)
			if found {
				// Pull interned value forward from previous cache
				c.curEnts.Put(k, v)
				c.prevEnts.Delete(k)
				return v.(*indexer.Value)
			}
		}
		// Intern new value
		c.curEnts.Put(k, value)
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

// valueInSlice checks if the value already exists in slice of values
func valueInSlice(value *indexer.Value, values []*indexer.Value) bool {
	for _, v := range values {
		if value == v || value.Equal(*v) {
			return true
		}
	}
	return false
}
