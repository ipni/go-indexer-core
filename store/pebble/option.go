package pebble

import (
	"time"
)

// Option configures a Pebble store.
type Option func(*storeOptions) error

type storeOptions struct {
	metering *MeteringConfig
}

// MeteringConfig configures the background per-provider metering scanner.
type MeteringConfig struct {
	// BatchSize is the maximum number of keys read per batch. Defaults to 1000000.
	BatchSize int
	// Interval is the wait before the next automatic scan. A manual trigger
	// restarts this wait. Zero disables automatic scans (manual MeteringTriggerScan
	// still works).
	Interval time.Duration
	// TimeFill is the fraction of time a scan spends reading, in (0, 1].
	// After a batch that took T, the scan sleeps T*(1-TimeFill)/TimeFill.
	// 1 runs the next batch immediately. 0 selects 0.1.
	TimeFill float64
	// ExportProviderMetrics records per-provider scan gauges. Each gauge is one
	// Prometheus series per provider. Leave false unless the provider set is
	// known to be small; the HTTP stats API still returns every provider.
	ExportProviderMetrics bool
}

func (c *MeteringConfig) withDefaults() MeteringConfig {
	out := *c
	if out.BatchSize <= 0 {
		out.BatchSize = 1000000
	}
	if out.TimeFill <= 0 || out.TimeFill > 1 {
		out.TimeFill = 0.1
	}
	return out
}

// WithMetering enables the background metering scanner with the given config.
func WithMetering(cfg MeteringConfig) Option {
	return func(o *storeOptions) error {
		c := cfg.withDefaults()
		o.metering = &c
		return nil
	}
}

func newStoreOptions(opts ...Option) (*storeOptions, error) {
	o := &storeOptions{}
	for _, apply := range opts {
		if err := apply(o); err != nil {
			return nil, err
		}
	}
	return o, nil
}
