package engine

import (
	"context"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
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

func (e *Engine) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	startTime := time.Now()
	ctx := context.Background()
	defer func() {
		stats.Record(ctx, metrics.GetIndexLatency.M(metrics.MsecSince(startTime)))
	}()

	if e.resultCache == nil {
		// If no result cache, get from value store
		return e.valueStore.Get(m)
	}

	// Check if multihash in resultCache
	v, found := e.resultCache.Get(m)
	if !found {
		stats.Record(ctx, metrics.CacheMisses.M(1))
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
	} else {
		stats.Record(ctx, metrics.CacheHits.M(1))
	}
	return v, found, nil
}

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
		e.updateCacheStats()
	}
	err := e.valueStore.Put(value, mhs...)
	if err != nil {
		return err
	}
	stats.Record(context.Background(), metrics.IngestMultihashes.M(int64(len(mhs))))
	return nil
}

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
	e.updateCacheStats()
	return nil
}

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
	e.updateCacheStats()
	return nil
}

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
	e.updateCacheStats()
	return nil
}

func (e *Engine) Size() (int64, error) {
	return e.valueStore.Size()
}

func (e *Engine) Flush() error {
	return e.valueStore.Flush()
}

func (e *Engine) Close() error {
	return e.valueStore.Close()
}

func (e *Engine) Iter() (indexer.Iterator, error) {
	return e.valueStore.Iter()
}

func (e *Engine) updateCacheStats() {
	st := e.resultCache.Stats()
	stats.Record(context.Background(),
		metrics.CacheItems.M(int64(st.Indexes)),
		metrics.CacheValues.M(int64(st.Values)),
		metrics.CacheEvictions.M(int64(st.Evictions)),
	)
}
