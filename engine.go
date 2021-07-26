package indexer

import (
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("indexer-core")

// Engine combines a result cache and a value store
type Engine struct {
	resultCache cache.Interface
	valueStore  store.Interface
}

// NewEngine creates a new Engine from with the given cache and store
func NewEngine(resultCache cache.Interface, valueStore store.Interface) *Engine {
	if valueStore == nil {
		panic("valueStore is required")
	}
	return &Engine{
		resultCache: resultCache,
		valueStore:  valueStore,
	}
}

// Get retrieves a slice of entry.Value for a CID
func (e *Engine) Get(c cid.Cid) ([]entry.Value, bool, error) {
	if e.resultCache != nil {
		// Check if CID in resultCache
		v, found, err := e.resultCache.Get(c)
		if err != nil {
			return nil, false, err
		}

		if !found && e.valueStore != nil {
			v, found, err = e.valueStore.Get(c)
			if err != nil {
				return nil, false, err
			}
			// TODO: What about adding a resultCache interface that includes
			// putEntries(cid, []entry.Value) function
			// so we don't need to loop through entry.Value slice to move from
			// one storage to another?
			if found {
				// Move from value store to result cache
				for i := range v {
					_, err := e.resultCache.Put(c, v[i])
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
	return e.valueStore.Get(c)
}

// Put stores a value for a CID if the value is not already stored.  New values
// are added to those that are already stored.
func (e *Engine) Put(c cid.Cid, value entry.Value) (bool, error) {
	// If there is a result cache, check first if cid already in cache
	if e.resultCache != nil {
		v, found, err := e.resultCache.Get(c)
		if err != nil {
			return false, err
		}
		// If CID found, check if value already exists in cache. Values in
		// cache must already be in the value store, in which case there is no
		// need to store anything new.
		if found {
			for i := range v {
				// If exists, then already in value store
				if v[i].Equal(value) {
					return false, nil
				}
			}
			// Add this value to those already in the result cache
			_, err := e.resultCache.Put(c, value)
			if err != nil {
				log.Errorw("failed to put value into result cache", "err", err)
			}
		}
	}

	// If value was not in result cache, then store in value store
	return e.valueStore.Put(c, value)
}

// PutMany stores one entry.Value for multiple CIDs
func (e *Engine) PutMany(cids []cid.Cid, value entry.Value) error {
	if e.resultCache == nil {
		return e.valueStore.PutMany(cids, value)
	}

	for i := range cids {
		_, err := e.Put(cids[i], value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Remove removes the entry.Value for the specified CID
func (e *Engine) Remove(c cid.Cid, value entry.Value) (bool, error) {
	ok, err := e.valueStore.Remove(c, value)
	if err != nil {
		return false, err
	}

	// If not located in valueStore or no result cache, nothing more to do
	if !ok || e.resultCache == nil {
		return false, nil
	}

	_, err = e.resultCache.Remove(c, value)
	if err != nil {
		return false, err
	}
	return true, nil
}

// RemoveMany removes the entry.Value from multiple CIDs
func (e *Engine) RemoveMany(cids []cid.Cid, value entry.Value) error {
	// Remove first from valueStore
	err := e.valueStore.RemoveMany(cids, value)
	if err != nil {
		return err
	}

	if e.resultCache == nil {
		return nil
	}

	return e.resultCache.RemoveMany(cids, value)
}

// RemoveProvider removes all entries for specified provider.  This is used
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

// Size returns the total storage capacity being used
func (e *Engine) Size() (int64, error) {
	// NOTE: Do not return a size for the resultCache since this is only a
	// limited amount of memory and is not representative of the total amount
	// of data stored by the indexer.
	return e.valueStore.Size()
}
