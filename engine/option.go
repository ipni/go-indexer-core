package engine

import "fmt"

// config contains all options for configuring Engine.
type config struct {
	cacheOnPut bool
}

type Option func(*config) error

// apply applies the given options to this config.
func (c *config) apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("httpserver option %d failed: %s", i, err)
		}
	}
	return nil
}

// CacheOnPut sets whether or not entries are cached on Put.
func CacheOnPut(on bool) Option {
	return func(c *config) error {
		c.cacheOnPut = on
		return nil
	}
}
