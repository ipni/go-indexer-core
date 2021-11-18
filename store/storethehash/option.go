package storethehash

import (
	"time"

	sthtypes "github.com/ipld/go-storethehash/store/types"
)

// TODO: Benchmark and fine-tune for better performance.
const (
	defaultBurstRate     = 4 * 1024 * 1024
	defaultIndexSizeBits = uint8(24)
	defaultSyncInterval  = time.Second
)

// config contains all options for configuring storethehash valuestore.
type config struct {
	burstRate     sthtypes.Work
	indexSizeBits uint8
	syncInterval  time.Duration
}

type Option func(*config)

// apply applies the given options to this config.
func (c *config) apply(opts []Option) {
	for _, opt := range opts {
		opt(c)
	}
}

func IndexBitSize(indexBitSize uint8) Option {
	return func(cfg *config) {
		cfg.indexSizeBits = indexBitSize
	}
}

func SyncInterval(syncInterval time.Duration) Option {
	return func(cfg *config) {
		cfg.syncInterval = syncInterval
	}
}

func BurstRate(burstRate uint64) Option {
	return func(cfg *config) {
		cfg.burstRate = sthtypes.Work(burstRate)

	}
}
