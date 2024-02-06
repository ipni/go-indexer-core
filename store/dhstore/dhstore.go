// Package dhstore defines an dhstore client.
//
// When creating an indexer that uses this value store, the indexer should
// generally not be given a cache, unless the same multihashes and values are
// frequently written.
package dhstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/go-indexer-core/store/dhstore/client"
	"github.com/ipni/go-libipni/dhash"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

const defaultMaxIdleConns = 128

var log = logging.Logger("store/pebble")

type dhStore struct {
	batchSize       int
	mergeURL        string
	metaURL         string
	indexDeleteURLs []string
	metaDeleteURLs  []string
	httpClient      http.Client
}

var (
	_ indexer.Interface = (*dhStore)(nil)

	ErrNotSupported = errors.New("not supported")
)

func New(dhstoreURL string, options ...Option) (*dhStore, error) {
	if dhstoreURL == "" {
		return nil, errors.New("dhstore url required")
	}
	dhsURL, err := url.Parse(dhstoreURL)
	if err != nil {
		return nil, err
	}

	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	const (
		mdPath = "metadata"
		mhPath = "multihash"
	)
	mergeURL := dhsURL.JoinPath(mhPath)
	indexDeleteURLs := make([]string, len(opts.dhstoreClusterURLs)+1)
	indexDeleteURLs[0] = mergeURL.String()
	metaURL := dhsURL.JoinPath(mdPath)
	metaDeleteURLs := make([]string, len(opts.dhstoreClusterURLs)+1)
	metaDeleteURLs[0] = metaURL.String()
	for i, u := range opts.dhstoreClusterURLs {
		metaDeleteURLs[i+1] = u.JoinPath(mdPath).String()
		indexDeleteURLs[i+1] = u.JoinPath(mhPath).String()
	}

	maxIdleConns := defaultMaxIdleConns
	ht := http.DefaultTransport.(*http.Transport).Clone()
	ht.MaxIdleConns = maxIdleConns
	ht.MaxIdleConnsPerHost = maxIdleConns

	httpClient := http.Client{
		Timeout:   opts.httpClientTimeout,
		Transport: ht,
	}

	return &dhStore{
		batchSize:       opts.batchSize,
		mergeURL:        mergeURL.String(),
		metaURL:         metaURL.String(),
		metaDeleteURLs:  metaDeleteURLs,
		indexDeleteURLs: indexDeleteURLs,
		httpClient:      httpClient,
	}, nil
}

func (s *dhStore) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	// Log warning and return not found.
	log.Warnw("Get ignored, request must be sent to dstore ")
	return nil, false, nil
}

func (s *dhStore) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	if len(value.MetadataBytes) == 0 {
		return errors.New("value missing metadata")
	}

	metaReq, valueKey, err := makeDHMetadataRequest(value)
	if err != nil {
		return err
	}

	start := time.Now()

	ctx := context.Background()
	err = s.sendDHMetadata(ctx, metaReq)
	if err != nil {
		return err
	}

	stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "put")),
		stats.WithMeasurements(metrics.DHMetadataLatency.M(metrics.MsecSince(start))))

	size := s.batchSize
	if len(mhs) < size {
		size = len(mhs)
	}
	merges := make([]client.Index, 0, size)
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
			err = s.sendDHMergeIndexRequest(ctx, merges)
			if err != nil {
				return err
			}
			merges = merges[:0]
		}
	}

	// Send remaining merge requests.
	if len(merges) != 0 {
		err = s.sendDHMergeIndexRequest(ctx, merges)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *dhStore) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	ctx := context.Background()
	valueKey := dhash.CreateValueKey(value.ProviderID, value.ContextID)
	size := s.batchSize
	if len(mhs) < size {
		size = len(mhs)
	}
	dels := make([]client.Index, 0, size)

	for _, mh := range mhs {
		dm, err := multihash.Decode(mh)
		if err != nil {
			return err
		}
		if dm.Code == multihash.DBL_SHA2_256 {
			return errors.New("put double-hashed index not supported")
		}

		del, err := makeDHMerge(mh, valueKey)
		if err != nil {
			return err
		}

		dels = append(dels, del)
		if len(dels) == cap(dels) {
			err = s.sendDHDeleteIndexRequest(ctx, dels)
			if err != nil {
				return err
			}
			dels = dels[:0]
		}
	}

	// Send remaining delete requests.
	if len(dels) != 0 {
		err := s.sendDHDeleteIndexRequest(ctx, dels)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *dhStore) RemoveProvider(ctx context.Context, providerID peer.ID) error {
	return ErrNotSupported
}

func (s *dhStore) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	if len(s.metaDeleteURLs) == 0 {
		return nil
	}
	ctx := context.Background()

	vk := dhash.CreateValueKey(providerID, contextID)
	hvk := dhash.SHA256(vk, nil)
	b58hvk := "/" + base58.Encode(hvk)

	errs := make(chan error, len(s.metaDeleteURLs))
	for _, u := range s.metaDeleteURLs {
		go func(dhURL string) {
			err := s.sendDelContextRequest(ctx, dhURL)
			if err != nil && ctx.Err() == nil {
				log.Errorw("Failed metadata delete, retrying", "err", err, "provider", providerID)
				// If ctx is still ok, then try once more with the specific
				// dhstore that returned the error. This avoids having to retry
				// sending everything to all dhstores again.
				time.Sleep(50 * time.Millisecond)
				err = s.sendDelContextRequest(ctx, dhURL)
				if err != nil {
					log.Errorw("Failed metadata delete retry", "err", err, "provider", providerID)
				}
			}
			errs <- err
		}(u + b58hvk)
	}

	for i := 0; i < len(s.metaDeleteURLs); i++ {
		err := <-errs
		if err != nil {
			// Return first error. Goroutines will complete because errs
			// channel is buffered to allow them all to write.
			return err
		}
		// No need to check context, because goroutines will exit if canceled.
	}
	log.Infow("Sent metadata delete to all dhstores", "dhstores", len(s.metaDeleteURLs), "provider", providerID)
	return nil
}

func (s *dhStore) sendDelContextRequest(ctx context.Context, dhURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, dhURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	rsp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, rsp.Body)
	rsp.Body.Close()

	if rsp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete metadata at %s: %s", dhURL, http.StatusText(rsp.StatusCode))
	}

	stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "delete")),
		stats.WithMeasurements(metrics.DHMetadataLatency.M(metrics.MsecSince(start))))

	return nil
}

func (s *dhStore) Size() (int64, error) {
	return 0, nil
}

func (s *dhStore) Flush() error { return nil }

func (s *dhStore) Close() error { return nil }

func (s *dhStore) Iter() (indexer.Iterator, error) {
	return nil, ErrNotSupported
}

func (s *dhStore) Stats() (*indexer.Stats, error) {
	return nil, nil
}

func makeDHMerge(mh multihash.Multihash, valueKey []byte) (client.Index, error) {
	mh2, err := dhash.SecondMultihash(mh)
	if err != nil {
		return client.Index{}, err
	}

	// Encrypt value key with original multihash.
	encValueKey, err := dhash.EncryptValueKey(valueKey, mh)
	if err != nil {
		return client.Index{}, err
	}

	return client.Index{
		Key:   mh2,
		Value: encValueKey,
	}, nil
}

func makeDHMetadataRequest(value indexer.Value) (client.PutMetadataRequest, []byte, error) {
	valueKey := dhash.CreateValueKey(value.ProviderID, value.ContextID)

	encMetadata, err := dhash.EncryptMetadata(value.MetadataBytes, valueKey)
	if err != nil {
		return client.PutMetadataRequest{}, nil, err
	}

	return client.PutMetadataRequest{
		Key:   dhash.SHA256(valueKey, nil),
		Value: encMetadata,
	}, valueKey, nil
}

func (s *dhStore) sendDHMetadata(ctx context.Context, putMetaReq client.PutMetadataRequest) error {
	data, err := json.Marshal(&putMetaReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.metaURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	rsp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, rsp.Body)
	rsp.Body.Close()

	if rsp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send metadata: %v", http.StatusText(rsp.StatusCode))
	}

	return nil
}

func (s *dhStore) sendDHMergeIndexRequest(ctx context.Context, merges []client.Index) error {
	mergeReq := client.MergeIndexRequest{
		Merges: merges,
	}

	data, err := json.Marshal(mergeReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.mergeURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	rsp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, rsp.Body)
	rsp.Body.Close()

	if rsp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send merges: %v", http.StatusText(rsp.StatusCode))
	}

	stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "put")),
		stats.WithMeasurements(metrics.DHMultihashLatency.M(metrics.MsecSince(start))))

	return nil
}

func (s *dhStore) sendDHDeleteIndexRequest(ctx context.Context, merges []client.Index) error {
	mergeReq := client.MergeIndexRequest{
		Merges: merges,
	}

	data, err := json.Marshal(mergeReq)
	if err != nil {
		return err
	}

	// Buffer channel to all goroutines can write without a channel reader.
	errs := make(chan error, len(s.indexDeleteURLs))
	for _, u := range s.indexDeleteURLs {
		go func(dhURL string) {
			err := s.sendDelRequest(ctx, dhURL, data)
			if err != nil && ctx.Err() == nil {
				// If ctx is still ok, then try once more with the specific
				// dhstore that returned the error. This avoids having to retry
				// sending everything to all dhstores again.
				time.Sleep(50 * time.Millisecond)
				err = s.sendDelRequest(ctx, dhURL, data)
			}
			errs <- err
		}(u)
	}

	for i := 0; i < len(s.indexDeleteURLs); i++ {
		err = <-errs
		if err != nil {
			// Return first error. Goroutines will complete because errs
			// channel is buffered to allow them all to write.
			return err
		}
		// No need to check context, because goroutines will exit if canceled.
	}
	return nil
}

func (s *dhStore) sendDelRequest(ctx context.Context, dhURL string, reqData []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, dhURL, bytes.NewBuffer(reqData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	rsp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, rsp.Body)
	rsp.Body.Close()
	if rsp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send delete requests: %v", http.StatusText(rsp.StatusCode))
	}
	return nil
}
