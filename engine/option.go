package engine

import (
	"fmt"
	"net/url"
)

// config contains all options for configuring Engine.
type config struct {
	cacheOnPut bool
	dhstoreURL string
	vsNoNewMH  bool
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

// WithCacheOnPut sets whether or not entries are cached on Put.
func WithCacheOnPut(on bool) Option {
	return func(c *config) error {
		c.cacheOnPut = on
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

// WithVSNoNewMH blocks putting new multihashes into the value store when set
// to true. New indexes will still be send to the DHStore service if one is
// configured.
func WithVSNoNewMH(ok bool) Option {
	return func(c *config) error {
		c.vsNoNewMH = ok
		return nil
	}
}
