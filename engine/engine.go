package engine

import (
	"context"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/cache"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// Engine is an implementation of indexer.Interface that combines a value store
// and an optional result cache.
type Engine struct {
	valueStore indexer.Interface

	resultCache    cache.Interface
	cacheOnPut     bool
	prevCacheStats atomic.Value
}

var _ indexer.Interface = &Engine{}

func init() {
	metrics.SetupMetrics()
}

// New implements the indexer.Interface. It creates a new Engine with the given
// result cache and value store.
func New(valueStore indexer.Interface, options ...Option) *Engine {
	if valueStore == nil {
		panic("valueStore is required")
	}

	opts, err := getOpts(options)
	if err != nil {
		panic(err.Error())
	}

	return &Engine{
		valueStore:  valueStore,
		resultCache: opts.cache,
		cacheOnPut:  opts.cacheOnPut,
	}
}

func (e *Engine) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	startTime := time.Now()
	defer func() {
		metrics.GetIndexLatency.Set(metrics.MsecSince(startTime))
	}()

	// If there is a result cache, look there first. If the value is in the
	// cache then it was already written to any value store.
	if e.resultCache != nil {
		v, found := e.resultCache.Get(m)
		if found {
			metrics.CacheHits.Inc()
			return v, true, nil
		}
		metrics.CacheMisses.Inc()
	}

	v, found, err := e.valueStore.Get(m)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	if e.resultCache == nil {
		return v, found, nil
	}

	for i := range v {
		e.resultCache.Put(v[i], m)
	}
	e.updateCacheStats()

	return v, found, nil
}

func (e *Engine) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	mhsCount := len(mhs)

	if e.resultCache != nil {
		if e.cacheOnPut {
			mhs = slices.DeleteFunc(mhs, func(mh multihash.Multihash) bool {
				v, found := e.resultCache.Get(mh)
				if !found {
					return false
				}
				// If multihash cached, check if value already exists in cache.
				// Values in cache must already be in the value store, in which
				// case there is nothing new to store.
				for j := range v {
					if v[j].Equal(value) {
						return true
					}
				}
				// Add this new value to those already in the result cache.
				return false
			})
			e.resultCache.Put(value, mhs...)
		} else {
			var addToCache []multihash.Multihash
			mhs = slices.DeleteFunc(mhs, func(mh multihash.Multihash) bool {
				v, found := e.resultCache.Get(mh)
				if !found {
					return false
				}
				// If multihash cached, check if value already exists in cache.
				// Values in cache must already be in the value store, in which
				// case there is nothing new to store.
				for j := range v {
					if v[j].Equal(value) {
						return true
					}
				}
				// Add this new value to those already in the result cache.
				addToCache = append(addToCache, mh)
				return false
			})
			e.resultCache.Put(value, addToCache...)
		}
		e.updateCacheStats()
	}

	err := e.valueStore.Put(value, mhs...)
	if err != nil {
		return err
	}

	metrics.IngestMultihashes.Add(float64(mhsCount))

	return nil
}

func (e *Engine) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	// Remove first from valueStore.
	err := e.valueStore.Remove(value, mhs...)
	if err != nil {
		return err
	}

	if e.resultCache != nil {
		e.resultCache.Remove(value, mhs...)
		e.updateCacheStats()
	}

	return nil
}

func (e *Engine) RemoveProvider(ctx context.Context, providerID peer.ID) error {
	// Remove first from valueStore.
	err := e.valueStore.RemoveProvider(ctx, providerID)
	if err != nil {
		return err
	}
	metrics.RemovedProviders.Inc()

	if e.resultCache != nil {
		e.resultCache.RemoveProvider(providerID)
		e.updateCacheStats()
	}

	return nil
}

func (e *Engine) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	// Remove first from valueStore.
	err := e.valueStore.RemoveProviderContext(providerID, contextID)
	if err != nil {
		return err
	}

	if e.resultCache != nil {
		e.resultCache.RemoveProviderContext(providerID, contextID)
		e.updateCacheStats()
	}

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

func (e *Engine) updateCacheStats() {
	st := e.resultCache.Stats()
	var prevStats *cache.Stats

	prevStatsI := e.prevCacheStats.Load()
	if prevStatsI == nil {
		prevStats = &cache.Stats{}
	} else {
		prevStats = prevStatsI.(*cache.Stats)
	}

	// Only record stats that have changed.
	var updated bool
	if st.Indexes != prevStats.Indexes {
		metrics.CacheMultihashes.Set(float64(st.Indexes))
		updated = true
	}
	if st.Values != prevStats.Values {
		metrics.CacheValues.Set(float64(st.Values))
		updated = true
	}
	if st.Evictions != prevStats.Evictions {
		metrics.CacheEvictions.Set(float64(st.Evictions))
		updated = true
	}

	if updated {
		e.prevCacheStats.Store(&st)
	}
}

func (e *Engine) Stats() (*indexer.Stats, error) {
	return e.valueStore.Stats()
}
