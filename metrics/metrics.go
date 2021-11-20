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
	CacheHits      = stats.Int64("core/cache/hits", "Number of retireval cache hits", stats.UnitDimensionless)
	CacheMisses    = stats.Int64("core/cache/misses", "Number of retireval cache misses", stats.UnitDimensionless)
	CacheItems     = stats.Int64("core/cache/items", "Number of indexes in cache", stats.UnitDimensionless)
	CacheValues    = stats.Int64("core/cache/values", "Number of values in cache", stats.UnitDimensionless)
	CacheEvictions = stats.Int64("core/cache/evictions", "Number of indexes evicted from cache", stats.UnitDimensionless)
	CacheMisuse    = stats.Int64("core/cache/misuse", "Cache clears due to high value to multihash ratio (indexer misuse)", stats.UnitDimensionless)

	GetIndexLatency   = stats.Float64("core/get_index_latency", "Time to retrieve an index", stats.UnitMilliseconds)
	IngestMultihashes = stats.Int64("core/ingest_multihashes", "Number of multihashes put into the indexer", stats.UnitDimensionless)
	IngestValues      = stats.Int64("core/ingest_values", "Number of values put into the indexer", stats.UnitDimensionless)
	StoreSize         = stats.Int64("core/storage_size", "Bytes of storage used to store the indexed content", stats.UnitBytes)
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
	cacheItemsView = &view.View{
		Measure:     CacheItems,
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
	ingestValuesView = &view.View{
		Measure:     IngestValues,
		Aggregation: view.Count(),
	}

	storeSizeView = &view.View{
		Measure:     StoreSize,
		Aggregation: view.LastValue(),
	}
)

// DefaultViews with all views in it.
var DefaultViews = []*view.View{
	cacheHitsView,
	cacheMissesView,
	cacheItemsView,
	cacheValuesView,
	cacheEvictionsView,
	cacheMisuseView,
	getIndexLatencyView,
	ingestMultihashesView,
	ingestValuesView,
	storeSizeView,
}

func MsecSince(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}
