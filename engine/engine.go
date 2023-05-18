package engine

import (
	"bytes"
	"context"
	"encoding/base64"
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
	"github.com/ipni/go-libipni/dhash"
	"github.com/libp2p/go-libp2p/core/peer"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var log = logging.Logger("indexer-core")

const shardKeyHeader = "x-ipni-dhstore-shard-key"

// Engine is an implementation of indexer.Interface that combines a result
// cache and a value store.
type Engine struct {
	resultCache cache.Interface
	valueStore  indexer.Interface
	cacheOnPut  bool

	prevCacheStats atomic.Value

	dhBatchSize      int
	dhKeyShard       bool
	dhMergeURL       string
	dhMetaURL        string
	dhMetaDeleteURLs []string
	httpClient       http.Client
	vsNoNewMH        bool
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
	var dhMetaDeleteURLs []string
	if opts.dhstoreURL != "" {
		mdSuffix := "/metadata"
		dhMergeURL = opts.dhstoreURL + "/multihash"
		dhMetaURL = opts.dhstoreURL + mdSuffix
		dhMetaDeleteURLs = make([]string, len(opts.dhstoreClusterURLs)+1)
		dhMetaDeleteURLs[0] = dhMetaURL
		for i, u := range opts.dhstoreClusterURLs {
			dhMetaDeleteURLs[i+1] = u + mdSuffix
		}
	}

	return &Engine{
		resultCache: resultCache,
		valueStore:  valueStore,
		cacheOnPut:  opts.cacheOnPut,

		dhBatchSize:      opts.dhBatchSize,
		dhKeyShard:       opts.dhKeyShard,
		dhMergeURL:       dhMergeURL,
		dhMetaURL:        dhMetaURL,
		dhMetaDeleteURLs: dhMetaDeleteURLs,

		vsNoNewMH: opts.vsNoNewMH,
		httpClient: http.Client{
			Timeout: opts.httpClientTimeout,
		},
	}
}

func (e *Engine) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	startTime := time.Now()
	ctx := context.Background()
	defer func() {
		stats.Record(ctx, metrics.GetIndexLatency.M(metrics.MsecSince(startTime)))
	}()

	// Return not found for any double-hashed multihash.
	dm, err := multihash.Decode(m)
	if err != nil {
		return nil, false, err
	}
	if dm.Code == multihash.DBL_SHA2_256 {
		return nil, false, nil
	}

	// If there is a result cache, look there first. If the value is in the
	// cache then it was already written to any dhstore.
	if e.resultCache != nil {
		v, found := e.resultCache.Get(m)
		if found {
			stats.Record(ctx, metrics.CacheHits.M(1))
			return v, true, nil
		}
		stats.Record(ctx, metrics.CacheMisses.M(1))
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

	// Double hash this non double hashed result and store it in dhstore. This
	// is only done when using a result cache to avoid repeatedly writing the
	// same data to dhstore.
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

	start := time.Now()
	err = e.sendDHMetadata(ctx, metaReq)
	if err != nil {
		return err
	}

	stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "put")),
		stats.WithMeasurements(metrics.DHMetadataLatency.M(metrics.MsecSince(start))))

	var mergeCount int
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
			mergeCount += len(merges)
			merges = merges[:0]
		}
	}

	// Send remaining merge requests.
	if len(merges) != 0 {
		err = e.sendDHMerges(ctx, merges)
		if err != nil {
			return err
		}
		mergeCount += len(merges)
	}

	log.Infow("Sent metadata and merges to dhstore", "mergeCount", mergeCount)

	return nil
}

func (e *Engine) sendDHMetadata(ctx context.Context, putMetaReq dhstore.PutMetadataRequest) error {
	data, err := json.Marshal(&putMetaReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, e.dhMetaURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	shardKey := base64.RawStdEncoding.EncodeToString(putMetaReq.Key)
	req.Header.Set(shardKeyHeader, shardKey)

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

func (e *Engine) sendDHMergesCluster(ctx context.Context, merges []dhstore.Merge) error {
	mergeReq := dhstore.MergeIndexRequest{
		Merges: make([]dhstore.Merge, 1),
	}
	b64Enc := base64.RawStdEncoding
	var keyBuf []byte

	start := time.Now()

	// Send a separate dhstore.MergeIndexRequest for each merge.
	for i := range merges {
		mergeReq.Merges[0] = merges[i]

		// TODO: The Value of all merges is the same. Encode it once and then
		// Use json.RawMessage to avoid endocing it again.
		data, err := json.Marshal(mergeReq)
		if err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, e.dhMergeURL, bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		// Reuse buffer for base64 encoding.
		key := merges[i].Key
		encLen := b64Enc.EncodedLen(len(key))
		if encLen > len(keyBuf) {
			keyBuf = make([]byte, encLen)
		}
		b64Enc.Encode(keyBuf, key)
		req.Header.Set(shardKeyHeader, string(keyBuf[:encLen]))

		rsp, err := e.httpClient.Do(req)
		if err != nil {
			return err
		}
		rsp.Body.Close()

		if rsp.StatusCode != http.StatusAccepted {
			return fmt.Errorf("failed to send merge: %s", http.StatusText(rsp.StatusCode))
		}
	}

	stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "put")),
		stats.WithMeasurements(metrics.DHMultihashLatency.M(metrics.MsecSince(start))))

	return nil
}

func (e *Engine) sendDHMerges(ctx context.Context, merges []dhstore.Merge) error {
	if e.dhKeyShard {
		return e.sendDHMergesCluster(ctx, merges)
	}

	mergeReq := dhstore.MergeIndexRequest{
		Merges: merges,
	}

	data, err := json.Marshal(mergeReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, e.dhMergeURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	rsp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}

	stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "put")),
		stats.WithMeasurements(metrics.DHMultihashLatency.M(metrics.MsecSince(start))))

	defer rsp.Body.Close()

	if rsp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send metadata: %v", http.StatusText(rsp.StatusCode))
	}

	return nil
}

func (e *Engine) sendDHMetadataDelete(ctx context.Context, providerID peer.ID, contextID []byte) error {
	vk := dhash.CreateValueKey(providerID, contextID)
	hvk := dhash.SHA256(vk, nil)
	b58hvk := b58.Encode(hvk)

	var shardKey string
	if e.dhKeyShard {
		shardKey = base64.RawStdEncoding.EncodeToString(hvk)
	}

	for _, u := range e.dhMetaDeleteURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u+"/"+b58hvk, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if e.dhKeyShard {
			req.Header.Set(shardKeyHeader, shardKey)
		}

		start := time.Now()
		rsp, err := e.httpClient.Do(req)
		if err != nil {
			return err
		}
		rsp.Body.Close()

		if rsp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to delete metadata: %v", http.StatusText(rsp.StatusCode))
		}

		stats.RecordWithOptions(context.Background(),
			stats.WithTags(tag.Insert(metrics.Method, "delete")),
			stats.WithMeasurements(metrics.DHMetadataLatency.M(metrics.MsecSince(start))))

		log.Infow("Sent metadata delete to dhstore", "url", u, "valueKey", hvk)
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

	if e.resultCache != nil {
		e.resultCache.RemoveProvider(providerID)
		e.updateCacheStats()
	}

	stats.Record(context.Background(), metrics.RemovedProviders.M(1))
	return nil
}

func (e *Engine) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	// Remove first from valueStore.
	err := e.valueStore.RemoveProviderContext(providerID, contextID)
	if err != nil {
		return err
	}

	if len(e.dhMetaDeleteURLs) != 0 {
		err = e.sendDHMetadataDelete(context.Background(), providerID, contextID)
		if err != nil {
			return err
		}
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
