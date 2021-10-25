package radixcache

import (
	"bytes"
	"strings"
	"sync"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/gammazero/radixtree"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
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
	evictions  int
	rotateSize int
}

// New creates a new radixCache instance.
func New(maxSize int) *radixCache {
	return &radixCache{
		current:    radixtree.New(),
		curEnts:    radixtree.New(),
		rotateSize: maxSize >> 1,
	}
}

func (c *radixCache) Get(m multihash.Multihash) ([]indexer.Value, bool) {
	k := string(m)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	vals, found := c.get(k)
	if !found {
		return nil, false
	}

	ret := make([]indexer.Value, len(vals))
	for i, v := range vals {
		ret[i] = *v
	}
	return ret, true
}

func (c *radixCache) Put(value indexer.Value, mhs ...multihash.Multihash) int {
	var count int

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Store the new value or update the matching value and return a pointer to
	// the internally stored value.
	if len(mhs) == 0 {
		c.internValue(&value, true, false)
		return 0
	}
	interned := c.internValue(&value, true, true)

keysLoop:
	for i := range mhs {
		k := string(mhs[i])
		existing, found := c.get(k)
		if found {
			for _, v := range existing {
				if v == interned {
					// Key is already mapped to value
					continue keysLoop
				}
				// There is no need to match and replace any existing value,
				// because the existing values with the same ProviderID and
				// ContextID would already have had their metadata updaed by
				// internValue().  This is true for items that only existed in
				// old cache, since the interning process would have brought
				// their value pointers forward into the new cache.
			}
		}

		if c.current.Len() > c.rotateSize {
			c.rotate()
		}

		c.current.Put(k, append(existing, interned))
		count++
	}

	// Prevent possible unbounded memory growth, that results from repeatedly
	// adding and deleting entries with different values, without causing
	// rotation.
	if c.curEnts.Len() > (c.rotateSize << 1) {
		c.rotate()
		c.previous.Walk("", func(k string, v interface{}) bool {
			values := v.([]*indexer.Value)
			for _, val := range values {
				_, _, found := c.findInternValue(val)
				if !found {
					panic("cannot find interned value for cached index")
				}
			}
			return false
		})
		c.current = c.previous
		c.previous = nil
		c.prevEnts = nil

		// TODO: if there are still too many values, then need to refuse to
		// cache some.  This means that there are more values than multihashes,
		// which probably indicates a misuse of the indexer.
	}

	return count
}

func (c *radixCache) Remove(value indexer.Value, mhs ...multihash.Multihash) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, val, found := c.findInternValue(&value)
	if !found {
		return 0
	}

	var count int
	for i := range mhs {
		k := string(mhs[i])
		removed := removeIndex(c.current, k, val)
		if c.previous != nil && removeIndex(c.previous, k, val) {
			removed = true
		}
		if removed {
			count++
		}
	}

	// If nothing left in previous cache, then it is safe to discard previous
	// interned values.
	if c.prevEnts != nil && c.previous != nil && c.previous.Len() == 0 {
		c.prevEnts = nil
	}
	//if c.current.Len()

	return count
}

func (c *radixCache) RemoveProvider(providerID peer.ID) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var count int
	var deletes []string
	var tree *radixtree.Bytes

	walkFunc := func(k string, v interface{}) bool {
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
	}

	hasCurValues := removeProviderInterns(c.curEnts, providerID)
	if hasCurValues {
		// Remove provider values only if there were any provider interns.
		tree = c.current
		c.current.Walk("", walkFunc)
		for _, k := range deletes {
			c.current.Delete(k)
		}
	}
	if c.previous != nil {
		var hasPrevValues bool
		if c.prevEnts != nil {
			hasPrevValues = removeProviderInterns(c.prevEnts, providerID)
		}
		// There may not be any previous interns if they were all pulled
		// forward, but there could still be previous indexes.  So, walk the
		// previous cache if there are any current or previous values.
		if hasPrevValues || hasCurValues {
			deletes = deletes[:0]
			tree = c.previous
			c.previous.Walk("", walkFunc)
			for _, k := range deletes {
				c.previous.Delete(k)
			}
		}
	}

	return count
}

func (c *radixCache) RemoveProviderContext(providerID peer.ID, contextID []byte) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	valKey, val, found := c.findInternValue(&indexer.Value{
		ProviderID: providerID,
		ContextID:  contextID,
	})
	if !found {
		return 0
	}

	var deletes []string
	var count int
	var tree *radixtree.Bytes

	walkFunc := func(k string, v interface{}) bool {
		values := v.([]*indexer.Value)
		for i := range values {
			if values[i] == val {
				if len(values) == 1 {
					deletes = append(deletes, k)
				} else {
					values[i] = values[len(values)-1]
					values[len(values)-1] = nil
					tree.Put(k, values[:len(values)-1])
				}
				count++
				break
			}
		}
		return false
	}

	tree = c.current
	c.current.Walk("", walkFunc)
	for _, k := range deletes {
		c.current.Delete(k)
	}
	if c.previous != nil {
		deletes = deletes[:0]
		tree = c.previous
		c.previous.Walk("", walkFunc)
		for _, k := range deletes {
			c.previous.Delete(k)
		}
	}

	// Only need to delete the value from the current interns, because
	// findInternValue would have pulled forward any from the previous cache
	// interns.
	c.curEnts.Delete(valKey)

	return count
}

func (c *radixCache) IndexCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	indexCount := c.current.Len()
	if c.previous != nil {
		indexCount += c.previous.Len()
	}
	return indexCount
}

func (c *radixCache) Stats() cache.CacheStats {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	indexCount := c.current.Len()
	if c.previous != nil {
		indexCount += c.previous.Len()
	}

	valueCount := c.curEnts.Len()
	if c.prevEnts != nil {
		valueCount += c.prevEnts.Len()
	}

	return cache.CacheStats{
		Indexes:   indexCount,
		Values:    valueCount,
		Evictions: c.evictions,
	}
}

func (c *radixCache) get(k string) ([]*indexer.Value, bool) {
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
		// cache to reuse the interned values instead of allocating them again
		// in the current cache.
		values := v.([]*indexer.Value)
		for i, val := range values {
			values[i] = c.internValue(val, false, true)
		}

		// Move the value found in the previous tree into the current one.
		c.current.Put(k, values)
		c.previous.Delete(k)
	}
	return v.([]*indexer.Value), true
}

func (c *radixCache) rotate() {
	if c.previous != nil {
		c.evictions += c.previous.Len()
	}
	c.previous, c.current = c.current, radixtree.New()
	c.prevEnts, c.curEnts = c.curEnts, radixtree.New()
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
func (c *radixCache) internValue(value *indexer.Value, updateMeta, saveNew bool) *indexer.Value {
	k, v, found := c.findInternValue(value)
	if found {
		// If the provided value has matching ProviderID and ContextID but
		// different Metadata, then update the interned value's metadata.
		if updateMeta && !bytes.Equal(v.MetadataBytes, value.MetadataBytes) {
			v.MetadataBytes = make([]byte, len(value.MetadataBytes))
			copy(v.MetadataBytes, value.MetadataBytes)
		}
		return v
	}

	if !saveNew {
		return nil
	}

	// Intern new value
	c.curEnts.Put(k, value)
	return value
}

func (c *radixCache) findInternValue(value *indexer.Value) (string, *indexer.Value, bool) {
	var b strings.Builder
	b.Grow(len(value.ProviderID) + len(value.ContextID))
	b.WriteString(string(value.ProviderID))
	b.Write(value.ContextID)
	k := b.String()
	v, found := c.curEnts.Get(k)
	if found {
		// Found existing interned value
		return k, v.(*indexer.Value), true
	}

	if c.prevEnts != nil {
		v, found = c.prevEnts.Get(k)
		if found {
			// Pull interned value forward from previous cache
			c.curEnts.Put(k, v)
			c.prevEnts.Delete(k)
			return k, v.(*indexer.Value), true
		}
	}

	return k, nil, false
}

func removeIndex(tree *radixtree.Bytes, k string, value *indexer.Value) bool {
	// Get from current cache
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

func removeProviderInterns(tree *radixtree.Bytes, providerID peer.ID) bool {
	var deletes []string
	tree.Walk(string(providerID), func(k string, v interface{}) bool {
		deletes = append(deletes, k)
		return false
	})
	for _, k := range deletes {
		tree.Delete(k)
	}
	return len(deletes) != 0
}
