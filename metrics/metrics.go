package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	PromRegistry = prometheus.NewRegistry()

	CacheHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "core_cache_hits",
			Help: "Number of retrieval cache hits",
		},
	)

	CacheMisses = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "core_cache_misses",
			Help: "Number of retrieval cache misses",
		},
	)

	CacheMultihashes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "core_cache_multihashes",
			Help: "Number of cached multihashes",
		},
	)

	CacheValues = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "core_cache_values",
			Help: "Number of cached values",
		},
	)

	CacheEvictions = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "core_cache_evictions",
			Help: "Number of indexes evicted from cache",
		},
	)

	GetIndexLatency = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "core_get_index_latency",
			Help: "Internal lookup time for a single index in milliseconds",
		},
	)

	IngestMultihashes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "core_ingest_multihashes",
			Help: "Number of multihashes put into the indexer",
		},
	)

	RemovedProviders = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "removed_providers",
			Help: "Number of providers removed from indexer",
		},
	)
)

func MsecSince(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}

func SetupMetrics() {
	PromRegistry.MustRegister(
		CacheHits, CacheMisses, CacheMultihashes, CacheValues, CacheEvictions,
		GetIndexLatency, IngestMultihashes, RemovedProviders,
	)
}
