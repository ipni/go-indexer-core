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
	dhBatchSize      int
	dhMergeURL       string
	dhMetaURL        string
	dhMetaDeleteURLs []string
	httpClient       http.Client
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
	dhMergeURL := dhsURL.JoinPath(mhPath)
	dhMetaURL := dhsURL.JoinPath(mdPath)
	dhMetaDeleteURLs := make([]string, len(opts.dhstoreClusterURLs)+1)
	dhMetaDeleteURLs[0] = dhMetaURL.String()
	for i, ustr := range opts.dhstoreClusterURLs {
		u, err := url.Parse(ustr)
		if err != nil {
			return nil, err
		}
		dhMetaDeleteURLs[i+1] = u.JoinPath(mdPath).String()
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
		dhBatchSize:      opts.dhBatchSize,
		dhMergeURL:       dhMergeURL.String(),
		dhMetaURL:        dhMetaURL.String(),
		dhMetaDeleteURLs: dhMetaDeleteURLs,

		httpClient: httpClient,
	}, nil
}

func (s *dhStore) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	// Return not found for any double-hashed or invalid  multihash.
	dm, err := multihash.Decode(m)
	if err != nil {
		log.Warnw("Get ignored bad multihash", "err", err)
		return nil, false, nil
	}
	if dm.Code == multihash.DBL_SHA2_256 {
		return nil, false, nil
	}

	return nil, false, nil
}

func (s *dhStore) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	if len(value.MetadataBytes) == 0 {
		return errors.New("value missing metadata")
	}

	ctx := context.Background()

	metaReq, valueKey, err := makeDHMetadataRequest(value)
	if err != nil {
		return err
	}

	start := time.Now()
	err = s.sendDHMetadata(ctx, metaReq)
	if err != nil {
		return err
	}

	stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "put")),
		stats.WithMeasurements(metrics.DHMetadataLatency.M(metrics.MsecSince(start))))

	var mergeCount int
	merges := make([]client.Index, 0, s.dhBatchSize)
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
			mergeCount += len(merges)
			merges = merges[:0]
		}
	}

	// Send remaining merge requests.
	if len(merges) != 0 {
		err = s.sendDHMergeIndexRequest(ctx, merges)
		if err != nil {
			return err
		}
		mergeCount += len(merges)
	}

	log.Infow("Sent metadata and merges to dhstore", "mergeCount", mergeCount, "elapsed", time.Since(start).String())

	return nil
}

func (s *dhStore) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	return ErrNotSupported
}

func (s *dhStore) RemoveProvider(ctx context.Context, providerID peer.ID) error {
	return ErrNotSupported
}

func (s *dhStore) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	if len(s.dhMetaDeleteURLs) == 0 {
		return nil
	}
	ctx := context.Background()

	vk := dhash.CreateValueKey(providerID, contextID)
	hvk := dhash.SHA256(vk, nil)
	b58hvk := base58.Encode(hvk)

	for _, u := range s.dhMetaDeleteURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u+"/"+b58hvk, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		rsp, err := s.httpClient.Do(req)
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.dhMetaURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	rsp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.dhMergeURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	rsp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	rsp.Body.Close()

	stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "put")),
		stats.WithMeasurements(metrics.DHMultihashLatency.M(metrics.MsecSince(start))))

	if rsp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send metadata: %v", http.StatusText(rsp.StatusCode))
	}
	return nil
}
