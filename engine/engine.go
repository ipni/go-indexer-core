package engine

import (
	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/store"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("indexer-core")

// Engine is an implementation of indexer.Interface that combines a result
// cache and a value store
type Engine struct {
	resultCache cache.Interface
	valueStore  store.Interface
}

var _ indexer.Interface = &Engine{}

// New implements the indexer.Interface.  It creates a new Engine with the
// given result cache and value store
func New(resultCache cache.Interface, valueStore store.Interface) *Engine {
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
	if e.resultCache != nil {
		// Check if multihash in resultCache
		v, found, err := e.resultCache.Get(m)
		if err != nil {
			return nil, false, err
		}

		if !found && e.valueStore != nil {
			v, found, err = e.valueStore.Get(m)
			if err != nil {
				return nil, false, err
			}
			// TODO: What about adding a resultCache interface that includes
			// putValues(multihash, []indexer.Value) function
			// so we don't need to loop through indexer.Value slice to move from
			// one storage to another?
			if found {
				// Move from value store to result cache
				for i := range v {
					_, err := e.resultCache.Put(m, v[i])
					if err != nil {
						// Only log error since request has been satisified
						log.Errorw("failed to put value into result cache", "err", err)
						break
					}
				}
			}
		}
		return v, found, err
	}

	// If no result cache, get from value store
	return e.valueStore.Get(m)
}

// Put stores a value for a multihash if the value is not already stored.  New values
// are added to those that are already stored for the multihash.
func (e *Engine) Put(m multihash.Multihash, value indexer.Value) (bool, error) {
	// If there is a result cache, check first if multihash already in cache
	if e.resultCache != nil {
		v, found, err := e.resultCache.Get(m)
		if err != nil {
			return false, err
		}
		// If multihash found, check if value already exists in cache. Values
		// in cache must already be in the value store, in which case there is
		// no need to store anything new.
		if found {
			for i := range v {
				// If exists, then already in value store
				if v[i].Equal(value) {
					return false, nil
				}
			}
			// Add this value to those already in the result cache
			_, err := e.resultCache.Put(m, value)
			if err != nil {
				log.Errorw("failed to put value into result cache", "err", err)
			}
		}
	}

	// If value was not in result cache, then store in value store
	return e.valueStore.Put(m, value)
}

// PutMany stores one indexer.Value for multiple multihashes
func (e *Engine) PutMany(mhs []multihash.Multihash, value indexer.Value) error {
	if e.resultCache == nil {
		return e.valueStore.PutMany(mhs, value)
	}

	for i := range mhs {
		_, err := e.Put(mhs[i], value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Remove removes a value for the specified multihash
func (e *Engine) Remove(m multihash.Multihash, value indexer.Value) (bool, error) {
	ok, err := e.valueStore.Remove(m, value)
	if err != nil {
		return false, err
	}

	// If not located in valueStore or no result cache, nothing more to do
	if !ok || e.resultCache == nil {
		return false, nil
	}

	_, err = e.resultCache.Remove(m, value)
	if err != nil {
		return false, err
	}
	return true, nil
}

// RemoveMany removes the specified value from multiple multihashes
func (e *Engine) RemoveMany(mhs []multihash.Multihash, value indexer.Value) error {
	// Remove first from valueStore
	err := e.valueStore.RemoveMany(mhs, value)
	if err != nil {
		return err
	}

	if e.resultCache == nil {
		return nil
	}

	return e.resultCache.RemoveMany(mhs, value)
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

	return e.resultCache.RemoveProvider(providerID)
}

// Size returns the total bytes of storage used by the value store to store the
// indexed content.  This does not include memory used by the result cache.
func (e *Engine) Size() (int64, error) {
	return e.valueStore.Size()
}
