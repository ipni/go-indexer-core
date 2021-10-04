package engine

import (
	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

// Engine is an implementation of indexer.Interface that combines a result
// cache and a value store
type Engine struct {
	resultCache cache.Interface
	valueStore  indexer.Interface
}

var _ indexer.Interface = &Engine{}

// New implements the indexer.Interface.  It creates a new Engine with the
// given result cache and value store
func New(resultCache cache.Interface, valueStore indexer.Interface) *Engine {
	if valueStore == nil {
		panic("valueStore is required")
	}
	return &Engine{
		resultCache: resultCache,
		valueStore:  valueStore,
	}
}

// Get retrieves a slice of index.Value for a multihash
func (e *Engine) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	if e.resultCache == nil {
		// If no result cache, get from value store
		return e.valueStore.Get(m)
	}

	// Check if multihash in resultCache
	v, found := e.resultCache.Get(m)
	if !found && e.valueStore != nil {
		var err error
		v, found, err = e.valueStore.Get(m)
		if err != nil {
			return nil, false, err
		}
		if found {
			// Store result in result cache.
			for i := range v {
				e.resultCache.Put(v[i], m)
			}
		}
	}
	return v, found, nil
}

// Iter creates a new value store iterator
func (e *Engine) Iter() (indexer.Iterator, error) {
	return e.valueStore.Iter()
}

// Put implements the indexer.Interface
func (e *Engine) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	if e.resultCache != nil {
		var mhsCopy []multihash.Multihash
		var putVal bool
		for i := 0; i < len(mhs); {
			v, found := e.resultCache.Get(mhs[i])

			// If multihash found, check if value already exists in
			// cache. Values in cache must already be in the value store, in
			// which case there is no need to store anything new.
			if found {
				found = false
				for j := range v {
					if v[j].Equal(value) {
						found = true
						break
					}
				}
				if found {
					// The multihash was found and is already mapped to this
					// value, so do not try to put it in the value store.  The
					// value store will handle this, but at a higher cost
					// requiring reading from disk.
					if mhsCopy == nil {
						// Copy-on-write
						mhsCopy = make([]multihash.Multihash, len(mhs))
						copy(mhsCopy, mhs)
						mhs = mhsCopy
					}
					mhs[i] = mhs[len(mhs)-1]
					mhs[len(mhs)-1] = nil
					mhs = mhs[:len(mhs)-1]
					continue
				}
				// Add this value to those already in the result cache, since
				// the multihash was already cached.
				e.resultCache.Put(value, mhs[i])
				putVal = true
			}
			i++
		}
		if !putVal {
			// If there was no put to update existing metadata in the cache,
			// then do it here.
			e.resultCache.Put(value)
		}
	}
	return e.valueStore.Put(value, mhs...)
}

// Remove removes the specified value from multiple multihashes
func (e *Engine) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	// Remove first from valueStore
	err := e.valueStore.Remove(value, mhs...)
	if err != nil {
		return err
	}

	if e.resultCache == nil {
		return nil
	}

	e.resultCache.Remove(value, mhs...)
	return nil
}

// RemoveProvider removes all values for specified provider.  This is used
// when a provider is no longer indexed by the indexer.
func (e *Engine) RemoveProvider(providerID peer.ID) error {
	// Remove first from valueStore
	err := e.valueStore.RemoveProvider(providerID)
	if err != nil {
		return err
	}

	if e.resultCache == nil {
		return nil
	}

	e.resultCache.RemoveProvider(providerID)
	return nil
}

// RemoveProviderContext removes all values for specified provider that
// have the specified contextID.
func (e *Engine) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	// Remove first from valueStore
	err := e.valueStore.RemoveProviderContext(providerID, contextID)
	if err != nil {
		return err
	}

	if e.resultCache == nil {
		return nil
	}

	e.resultCache.RemoveProviderContext(providerID, contextID)
	return nil
}

// Size returns the total bytes of storage used by the value store to store the
// indexed content.  This does not include memory used by the result cache.
func (e *Engine) Size() (int64, error) {
	return e.valueStore.Size()
}

// Close gracefully closes the store flushing all pending data from memory
func (e *Engine) Close() error {
	return e.valueStore.Close()
}

// Flush commits changes to storage
func (e *Engine) Flush() error {
	return e.valueStore.Flush()
}
