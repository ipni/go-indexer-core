package metrics

import (
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// Keys
var (
	Method, _ = tag.NewKey("method")
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

func MsecSince(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}
