package metrics

import (
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// Keys
var (
	Method, _   = tag.NewKey("method")
	Provider, _ = tag.NewKey("provider")
)

// Measures
var (
	CacheHits        = stats.Int64("core/cache/hits", "Number of retrieval cache hits", stats.UnitDimensionless)
	CacheMisses      = stats.Int64("core/cache/misses", "Number of retrieval cache misses", stats.UnitDimensionless)
	CacheMultihashes = stats.Int64("core/cache/multihashes", "Number of cached multihashes", stats.UnitDimensionless)
	CacheValues      = stats.Int64("core/cache/values", "Number of cached values", stats.UnitDimensionless)
	CacheEvictions   = stats.Int64("core/cache/evictions", "Number of indexes evicted from cache", stats.UnitDimensionless)
	CacheMisuse      = stats.Int64("core/cache/misuse", "Cache clears due to high value to multihash ratio (indexer misuse)", stats.UnitDimensionless)

	GetIndexLatency   = stats.Float64("core/get_index_latency", "Internal lookup time for a single index", stats.UnitMilliseconds)
	IngestMultihashes = stats.Int64("core/ingest_multihashes", "Number of multihashes put into the indexer", stats.UnitDimensionless)
	RemovedProviders  = stats.Int64("core/removed_providers", "Number of providers removed from indexer", stats.UnitDimensionless)
	StoreSize         = stats.Int64("core/storage_size", "Bytes of storage used to store the indexed content", stats.UnitBytes)

	DHMultihashLatency = stats.Float64("core/dh_multihash_latency", "Time that the indexer spends on sending encrypted multihashes to dhstore", stats.UnitMilliseconds)
	DHMetadataLatency  = stats.Float64("core/dh_metadata_latency", "Time that the indexer spends on sending encrypted metadata to dhstore", stats.UnitMilliseconds)

	MeteringTotalMultihashes = stats.Int64("core/metering/total_multihashes", "Total multihashes from the latest metering scan", stats.UnitDimensionless)
	MeteringTotalSlots       = stats.Int64("core/metering/total_slots", "Total multihash value-key slots from the latest metering scan", stats.UnitDimensionless)
	MeteringScanCompletedAt  = stats.Float64("core/metering/scan_completed_at", "Unix timestamp of the latest completed metering scan", stats.UnitDimensionless)
	MeteringScanDurationMs   = stats.Float64("core/metering/scan_duration_ms", "Duration of the latest completed metering scan in milliseconds", stats.UnitMilliseconds)
	MeteringScanKeysRead     = stats.Int64("core/metering/scan_keys_read", "Keys read so far in the in-progress metering scan", stats.UnitDimensionless)
	MeteringScanBytesRead    = stats.Int64("core/metering/scan_bytes_read", "Bytes read so far in the in-progress metering scan", stats.UnitBytes)

	MeteringProviderMultihashes = stats.Int64("core/metering/provider_multihashes", "Distinct multihashes per provider from the latest metering scan", stats.UnitDimensionless)
)

// Views
var (
	cacheHitsView = &view.View{
		Measure:     CacheHits,
		Aggregation: view.Count(),
	}
	cacheMissesView = &view.View{
		Measure:     CacheMisses,
		Aggregation: view.Count(),
	}
	cacheMultihashesView = &view.View{
		Measure:     CacheMultihashes,
		Aggregation: view.LastValue(),
	}
	cacheValuesView = &view.View{
		Measure:     CacheValues,
		Aggregation: view.LastValue(),
	}
	cacheEvictionsView = &view.View{
		Measure:     CacheEvictions,
		Aggregation: view.LastValue(),
	}
	cacheMisuseView = &view.View{
		Measure:     CacheMisuse,
		Aggregation: view.Count(),
	}

	getIndexLatencyView = &view.View{
		Measure:     GetIndexLatency,
		Aggregation: view.Distribution(0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000),
	}
	ingestMultihashesView = &view.View{
		Measure:     IngestMultihashes,
		Aggregation: view.Sum(),
	}
	removedProvidersView = &view.View{
		Measure:     RemovedProviders,
		Aggregation: view.Sum(),
	}

	storeSizeView = &view.View{
		Measure:     StoreSize,
		Aggregation: view.LastValue(),
	}

	dhMultihashLatency = &view.View{
		Measure:     DHMultihashLatency,
		Aggregation: view.Distribution(0, 10, 20, 50, 70, 100, 200, 300, 400, 500, 1000, 2000, 3000, 5000, 7000, 10_000, 30_000, 60_000),
		TagKeys:     []tag.Key{Method},
	}

	dhMetadataLatency = &view.View{
		Measure:     DHMetadataLatency,
		Aggregation: view.Distribution(0, 10, 20, 50, 70, 100, 200, 300, 400, 500, 1000, 2000, 3000, 5000, 7000, 10_000, 30_000, 60_000),
		TagKeys:     []tag.Key{Method},
	}

	meteringTotalMultihashesView = &view.View{
		Measure:     MeteringTotalMultihashes,
		Aggregation: view.LastValue(),
	}
	meteringTotalSlotsView = &view.View{
		Measure:     MeteringTotalSlots,
		Aggregation: view.LastValue(),
	}
	meteringScanCompletedAtView = &view.View{
		Measure:     MeteringScanCompletedAt,
		Aggregation: view.LastValue(),
	}
	meteringScanDurationMsView = &view.View{
		Measure:     MeteringScanDurationMs,
		Aggregation: view.LastValue(),
	}
	meteringScanKeysReadView = &view.View{
		Measure:     MeteringScanKeysRead,
		Aggregation: view.LastValue(),
	}
	meteringScanBytesReadView = &view.View{
		Measure:     MeteringScanBytesRead,
		Aggregation: view.LastValue(),
	}

	// Per-provider series. One time series per provider for each view. Do not
	// register these unless the operator has a bounded provider set.
	meteringProviderMultihashesView = &view.View{
		Measure:     MeteringProviderMultihashes,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{Provider},
	}
)

// DefaultViews with all views in it.
var DefaultViews = []*view.View{
	cacheHitsView,
	cacheMissesView,
	cacheMultihashesView,
	cacheValuesView,
	cacheEvictionsView,
	cacheMisuseView,
	getIndexLatencyView,
	ingestMultihashesView,
	removedProvidersView,
	storeSizeView,
	dhMultihashLatency,
	dhMetadataLatency,
}

// MeteringViews are OpenCensus views for metering scan totals and progress.
// These series are not labeled by provider.
var MeteringViews = []*view.View{
	meteringTotalMultihashesView,
	meteringTotalSlotsView,
	meteringScanCompletedAtView,
	meteringScanDurationMsView,
	meteringScanKeysReadView,
	meteringScanBytesReadView,
}

// MeteringProviderViews are OpenCensus views labeled by provider. Registering
// them exports one series per provider for each view. Leave them unregistered
// when the provider set is unbounded.
var MeteringProviderViews = []*view.View{
	meteringProviderMultihashesView,
}

func MsecSince(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}
