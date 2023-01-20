package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/dhstore"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/cache"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/go-indexer-core/store/dhash"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
)

var log = logging.Logger("indexer-core")

// Engine is an implementation of indexer.Interface that combines a result
// cache and a value store.
type Engine struct {
	resultCache cache.Interface
	valueStore  indexer.Interface
	cacheOnPut  bool

	prevCacheStats atomic.Value

	dhBatchSize int
	dhMergeURL  string
	dhMetaURL   string
	httpClient  http.Client
	vsNoNewMH   bool
}

var _ indexer.Interface = &Engine{}

// New implements the indexer.Interface. It creates a new Engine with the given
// result cache and value store.
func New(resultCache cache.Interface, valueStore indexer.Interface, options ...Option) *Engine {
	opts, err := getOpts(options)
	if err != nil {
		panic(err.Error())
	}

	if valueStore == nil {
		panic("valueStore is required")
	}

	var dhMergeURL, dhMetaURL string
	if opts.dhstoreURL != "" {
		dhMergeURL = opts.dhstoreURL + "/multihash"
		dhMetaURL = opts.dhstoreURL + "/metadata"
	}

	return &Engine{
		resultCache: resultCache,
		valueStore:  valueStore,
		cacheOnPut:  opts.cacheOnPut,

		dhBatchSize: opts.dhBatchSize,
		dhMergeURL:  dhMergeURL,
		dhMetaURL:   dhMetaURL,
		vsNoNewMH:   opts.vsNoNewMH,
	}
}

func (e *Engine) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	startTime := time.Now()
	ctx := context.Background()
	defer func() {
		stats.Record(ctx, metrics.GetIndexLatency.M(metrics.MsecSince(startTime)))
	}()

	if e.resultCache == nil {
		// If no result cache, get from value store.
		return e.valueStore.Get(m)
	}

	// Return not found for any double-hashed multihash.
	dm, err := multihash.Decode(m)
	if err != nil {
		return nil, false, err
	}
	if dm.Code == multihash.DBL_SHA2_256 {
		return nil, false, nil
	}

	// Check if multihash in resultCache.
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
			e.updateCacheStats()

			if e.dhMergeURL != "" {
				for i := range v {
					metaReq, valKey, err := makeDHMetadataRequest(v[i])
					if err != nil {
						log.Errorw("Cannot make dhstore metadata request", "err", err)
						continue
					}
					merge, err := makeDHMerge(m, valKey)
					if err != nil {
						log.Errorw("Cannot make dhstore merge request", "err", err)
						continue
					}
					if err = e.sendDHMetadata(ctx, metaReq); err != nil {
						log.Errorw("Cannot send dhstore metadata request", "err", err)
						continue
					}
					if err = e.sendDHMerges(ctx, []dhstore.Merge{merge}); err != nil {
						log.Errorw("Cannot send dhstore merge request", "err", err)
						continue
					}
				}
			}
		}
	} else {
		stats.Record(ctx, metrics.CacheHits.M(1))
	}
	return v, found, nil
}

func (e *Engine) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	if e.resultCache != nil {
		var addToCache, mhsCopy []multihash.Multihash
		for i := 0; i < len(mhs); {
			v, found := e.resultCache.Get(mhs[i])

			// If multihash found, check if value already exists in cache.
			// Values in cache must already be in the value store, in which
			// case there is no need to store anything new.
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
					// value, so do not try to put it in the value store. The
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
				addToCache = append(addToCache, mhs[i])
			} else if e.cacheOnPut {
				addToCache = append(addToCache, mhs[i])
			}
			i++
		}

		// Update value for existing multihashes, or add new index entries to
		// cache if cacheOnPut is set.
		e.resultCache.Put(value, addToCache...)
		e.updateCacheStats()
	}

	var err error
	if e.vsNoNewMH {
		err = e.valueStore.Put(value)
	} else {
		err = e.valueStore.Put(value, mhs...)
	}
	if err != nil {
		return err
	}

	if e.dhMergeURL != "" {
		err = e.storeDH(context.Background(), value, mhs)
		if err != nil {
			return err
		}
	}
	stats.Record(context.Background(), metrics.IngestMultihashes.M(int64(len(mhs))))
	return nil
}

func makeDHMetadataRequest(value indexer.Value) (dhstore.PutMetadataRequest, []byte, error) {
	valueKey := dhash.CreateValueKey(value.ProviderID, value.ContextID)

	encMetadata, err := dhash.EncryptMetadata(value.MetadataBytes, valueKey)
	if err != nil {
		return dhstore.PutMetadataRequest{}, nil, err
	}

	return dhstore.PutMetadataRequest{
		Key:   dhash.SHA256(valueKey, nil),
		Value: encMetadata,
	}, valueKey, nil
}

func makeDHMerge(mh multihash.Multihash, valueKey []byte) (dhstore.Merge, error) {
	mh2, err := dhash.SecondMultihash(mh)
	if err != nil {
		return dhstore.Merge{}, err
	}

	// Encrypt value key with original multihash.
	encValueKey, err := dhash.EncryptValueKey(valueKey, mh)
	if err != nil {
		return dhstore.Merge{}, err
	}

	return dhstore.Merge{
		Key:   mh2,
		Value: encValueKey,
	}, nil
}

func (e *Engine) storeDH(ctx context.Context, value indexer.Value, mhs []multihash.Multihash) error {
	metaReq, valueKey, err := makeDHMetadataRequest(value)
	if err != nil {
		return err
	}

	err = e.sendDHMetadata(ctx, metaReq)
	if err != nil {
		return err
	}

	merges := make([]dhstore.Merge, 0, e.dhBatchSize)
	for _, mh := range mhs {
		dm, err := multihash.Decode(mh)
		if err != nil {
			return err
		}
		if dm.Code == multihash.DBL_SHA2_256 {
			return errors.New("put double-hashed index not supported")
		}

		merge, err := makeDHMerge(mh, valueKey)
		if err != nil {
			return err
		}

		merges = append(merges, merge)
		if len(merges) == cap(merges) {
			err = e.sendDHMerges(ctx, merges)
			if err != nil {
				return err
			}
			merges = merges[:0]
		}
	}

	// Send remaining merge requests.
	if len(merges) != 0 {
		err = e.sendDHMerges(ctx, merges)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) sendDHMetadata(ctx context.Context, putMetaReq dhstore.PutMetadataRequest) error {
	data, err := json.Marshal(&putMetaReq)
	if err != nil {
		return err
	}

	log.Infow("Sending metadata to dhstore", "method", http.MethodPut, "url", e.dhMetaURL, "data", string(data))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, e.dhMetaURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	rsp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send metadata: %v", http.StatusText(rsp.StatusCode))
	}

	return nil
}

func (e *Engine) sendDHMerges(ctx context.Context, merges []dhstore.Merge) error {
	mergeReq := dhstore.MergeIndexRequest{
		Merges: merges,
	}

	data, err := json.Marshal(mergeReq)
	if err != nil {
		return err
	}

	log.Infow("Sending merges to dhstore", "method", http.MethodPut, "url", e.dhMergeURL, "count", len(merges))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, e.dhMergeURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	rsp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send metadata: %v", http.StatusText(rsp.StatusCode))
	}

	return nil
}

func (e *Engine) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	// Remove first from valueStore.
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

func (e *Engine) RemoveProvider(ctx context.Context, providerID peer.ID) error {
	// Remove first from valueStore.
	err := e.valueStore.RemoveProvider(ctx, providerID)
	if err != nil {
		return err
	}

	if e.resultCache == nil {
		return nil
	}

	e.resultCache.RemoveProvider(providerID)
	e.updateCacheStats()
	stats.Record(context.Background(), metrics.RemovedProviders.M(1))
	return nil
}

func (e *Engine) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	// Remove first from valueStore.
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
	var prevStats *cache.Stats

	prevStatsI := e.prevCacheStats.Load()
	if prevStatsI == nil {
		prevStats = &cache.Stats{}
	} else {
		prevStats = prevStatsI.(*cache.Stats)
	}

	// Only record stats that have changed.
	var ms []stats.Measurement
	if st.Indexes != prevStats.Indexes {
		ms = append(ms, metrics.CacheMultihashes.M(int64(st.Indexes)))
	}
	if st.Values != prevStats.Values {
		ms = append(ms, metrics.CacheValues.M(int64(st.Values)))
	}
	if st.Evictions != prevStats.Evictions {
		ms = append(ms, metrics.CacheEvictions.M(int64(st.Evictions)))
	}

	if len(ms) != 0 {
		e.prevCacheStats.Store(&st)
		stats.Record(context.Background(), ms...)
	}
}

func (e *Engine) Stats() (*indexer.Stats, error) {
	return e.valueStore.Stats()
}
