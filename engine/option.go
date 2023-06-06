package engine

import (
	"fmt"
	"net/url"
	"time"
)

const (
	defaultDHBatchSize = 4096
	defaultHttpTimeout = 5 * time.Second
)

// config contains all options for configuring Engine.
type config struct {
	cacheOnPut         bool
	dhBatchSize        int
	dhstoreURL         string
	dhstoreClusterURLs []string
	httpClientTimeout  time.Duration
}

type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		dhBatchSize:       defaultDHBatchSize,
		httpClientTimeout: defaultHttpTimeout,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithCacheOnPut sets whether or not entries are cached on Put.
func WithCacheOnPut(on bool) Option {
	return func(c *config) error {
		c.cacheOnPut = on
		return nil
	}
}

// WithDHBatchSize configures the batch size when sending batches of merge
// requests to DHStore. A value < 1 results in the default size.
func WithDHBatchSize(size int) Option {
	return func(c *config) error {
		if size > 0 {
			c.dhBatchSize = size
		}
		return nil
	}
}

// WithDHStore sets the base URL for the dhstore service, and tells the core to
// send its values to the dhstore.
func WithDHStore(dhsURL string) Option {
	return func(c *config) error {
		if dhsURL != "" {
			u, err := url.Parse(dhsURL)
			if err != nil {
				return err
			}
			c.dhstoreURL = u.String()
		}
		return nil
	}
}

// WithDHStoreCluster provide addional URLs that the core will send delete
// requests to. Deletes are send to the dhstoreURL as well as to all
// dhstoreClusterURLs. This is required as deletes need to be applied to all
// nodes until consistent hashing is implemented. dhstoreURL must not be
// included in this list.
func WithDHStoreCluster(clusterUrls []string) Option {
	return func(c *config) error {
		if len(clusterUrls) == 0 {
			return nil
		}
		urls := make([]string, 0, len(clusterUrls))
		for _, clusterURL := range clusterUrls {
			if clusterURL != "" {
				u, err := url.Parse(clusterURL)
				if err != nil {
					return err
				}
				urls = append(urls, u.String())
			}
		}
		c.dhstoreClusterURLs = urls
		return nil
	}
}

// WithHttpClientTimeout sets http timeout for queries to DHStore
func WithHttpClientTimeout(timeout time.Duration) Option {
	return func(cfg *config) error {
		cfg.httpClientTimeout = timeout
		return nil
	}
}
