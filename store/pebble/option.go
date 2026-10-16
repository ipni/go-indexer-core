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
	// Cleanup removes multihash slots whose value record is already gone. Each
	// removal is a merge-delete committed with the batch that read the slot.
	// Defaults to false.
	//
	// A slot the scan removes counts in the cleanup metrics, not in the scan
	// totals or the provider's DeletedContexts. A multihash whose key the scan
	// removes also counts in the cleanup metrics. A multihash that stays in the
	// store without live slots counts as deleted, including one whose slots
	// were all removed while emptied keys are kept in the store. Without
	// Cleanup, every slot stays and is counted in the totals.
	Cleanup bool
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
