package engine

import (
	"fmt"

	"github.com/ipni/go-indexer-core/cache"
)

// config contains all options for configuring Engine.
type config struct {
	cache      cache.Interface
	cacheOnPut bool
}

type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	var cfg config
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithCache provides a the engine with an index result cache.
func WithCache(cache cache.Interface) Option {
	return func(c *config) error {
		c.cache = cache
		return nil
	}
}

// WithCacheOnPut sets whether or not entries are cached on Put.
func WithCacheOnPut(on bool) Option {
	return func(c *config) error {
		c.cacheOnPut = on
		return nil
	}
}
