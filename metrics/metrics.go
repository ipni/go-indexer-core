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

	MeteringTotalMultihashes      = stats.Int64("core/metering/total_multihashes", "Active multihashes from the latest metering scan", stats.UnitDimensionless)
	MeteringTotalSlots            = stats.Int64("core/metering/total_slots", "Value-key slots in active multihashes from the latest metering scan", stats.UnitDimensionless)
	MeteringTotalDeleted          = stats.Int64("core/metering/total_deleted", "Multihashes whose value records are all gone, from the latest metering scan", stats.UnitDimensionless)
	MeteringDeletedSlots          = stats.Int64("core/metering/deleted_slots", "Value-key slots in multihashes whose value records are all gone, from the latest metering scan", stats.UnitDimensionless)
	MeteringDeletedKeyBytes       = stats.Int64("core/metering/deleted_key_bytes", "Key bytes of multihashes whose value records are all gone, from the latest metering scan", stats.UnitBytes)
	MeteringDeletedValueBytes     = stats.Int64("core/metering/deleted_value_bytes", "Value bytes of multihashes whose value records are all gone, from the latest metering scan", stats.UnitBytes)
	MeteringScanCompletedAt       = stats.Float64("core/metering/scan_completed_at", "Unix timestamp of the latest completed metering scan", stats.UnitDimensionless)
	MeteringScanDurationMs        = stats.Float64("core/metering/scan_duration_ms", "Duration of the latest completed metering scan in milliseconds", stats.UnitMilliseconds)
	MeteringScanStartedAt         = stats.Float64("core/metering/scan_started_at", "Unix timestamp when the current metering scan started. Zero when no scan is running", stats.UnitDimensionless)
	MeteringScanKeysRead          = stats.Int64("core/metering/scan_keys_read", "Keys read so far in the in-progress metering scan", stats.UnitDimensionless)
	MeteringScanBytesRead         = stats.Int64("core/metering/scan_bytes_read", "Bytes read so far in the in-progress metering scan", stats.UnitBytes)
	MeteringScanDeleted           = stats.Int64("core/metering/scan_deleted", "Multihashes whose value records are all gone, counted so far in the in-progress metering scan. Zero when no scan is running", stats.UnitDimensionless)
	MeteringScanDeletedSlots      = stats.Int64("core/metering/scan_deleted_slots", "Value-key slots in multihashes whose value records are all gone, counted so far in the in-progress metering scan. Zero when no scan is running", stats.UnitDimensionless)
	MeteringScanDeletedKeyBytes   = stats.Int64("core/metering/scan_deleted_key_bytes", "Key bytes of multihashes whose value records are all gone, counted so far in the in-progress metering scan. Zero when no scan is running", stats.UnitBytes)
	MeteringScanDeletedValueBytes = stats.Int64("core/metering/scan_deleted_value_bytes", "Value bytes of multihashes whose value records are all gone, counted so far in the in-progress metering scan. Zero when no scan is running", stats.UnitBytes)
	MeteringScanPercentDone       = stats.Float64("core/metering/scan_percent_done", "Estimated percent done for the in-progress metering scan, from cursor position in the sha2-256 multihash keyspace. Zero when no scan is running", stats.UnitDimensionless)
	MeteringScanElapsedSeconds    = stats.Float64("core/metering/scan_elapsed_seconds", "Seconds since the in-progress metering scan started. Zero when no scan is running", stats.UnitSeconds)
	MeteringScanRemainingSeconds  = stats.Float64("core/metering/scan_remaining_seconds", "Estimated seconds remaining for the in-progress metering scan. Zero when no scan is running or the estimate is not yet available", stats.UnitSeconds)
	MeteringScansStarted          = stats.Int64("core/metering/scans_started", "Metering scans started or resumed by this process", stats.UnitDimensionless)
	MeteringScansFinished         = stats.Int64("core/metering/scans_finished", "Metering scans that finished counting", stats.UnitDimensionless)
	MeteringBatchesStarted        = stats.Int64("core/metering/batches_started", "Metering scan batches started", stats.UnitDimensionless)
	MeteringBatchesFinished       = stats.Int64("core/metering/batches_finished", "Metering scan batches committed", stats.UnitDimensionless)
	MeteringBatchStartedAt        = stats.Float64("core/metering/batch_started_at", "Unix timestamp when the current metering batch started. Zero when no batch is running", stats.UnitDimensionless)
	MeteringBatchDurationMs       = stats.Float64("core/metering/batch_duration_ms", "Duration of a committed metering batch in milliseconds, excluding the pause before the next batch", stats.UnitMilliseconds)

	// Cleanup counters count what this process has merge-deleted. They start at
	// zero when the process starts and are not restored from a previous scan.
	// Byte counters are logical sizes of committed merge-deletes, not disk space
	// reclaimed: Pebble frees space when compaction applies the deletes.
	MeteringCleanupSlots       = stats.Int64("core/metering/cleanup_slots", "Value-key slots merge-deleted by metering cleanup in this process", stats.UnitDimensionless)
	MeteringCleanupMultihashes = stats.Int64("core/metering/cleanup_multihashes", "Multihash keys removed by metering cleanup in this process. Zero while emptied keys are kept in the store", stats.UnitDimensionless)
	MeteringCleanupKeyBytes    = stats.Int64("core/metering/cleanup_key_bytes", "Key bytes of multihash keys removed by metering cleanup in this process. Zero while emptied keys are kept in the store", stats.UnitBytes)
	MeteringCleanupValueBytes  = stats.Int64("core/metering/cleanup_value_bytes", "Packed value bytes of slots merge-deleted by metering cleanup in this process. A logical size, not disk space reclaimed; compaction frees space later", stats.UnitBytes)

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
	meteringTotalDeletedView = &view.View{
		Measure:     MeteringTotalDeleted,
		Aggregation: view.LastValue(),
	}
	meteringDeletedSlotsView = &view.View{
		Measure:     MeteringDeletedSlots,
		Aggregation: view.LastValue(),
	}
	meteringDeletedKeyBytesView = &view.View{
		Measure:     MeteringDeletedKeyBytes,
		Aggregation: view.LastValue(),
	}
	meteringDeletedValueBytesView = &view.View{
		Measure:     MeteringDeletedValueBytes,
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
	meteringScanStartedAtView = &view.View{
		Measure:     MeteringScanStartedAt,
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
	meteringScanDeletedView = &view.View{
		Measure:     MeteringScanDeleted,
		Aggregation: view.LastValue(),
	}
	meteringScanDeletedSlotsView = &view.View{
		Measure:     MeteringScanDeletedSlots,
		Aggregation: view.LastValue(),
	}
	meteringScanDeletedKeyBytesView = &view.View{
		Measure:     MeteringScanDeletedKeyBytes,
		Aggregation: view.LastValue(),
	}
	meteringScanDeletedValueBytesView = &view.View{
		Measure:     MeteringScanDeletedValueBytes,
		Aggregation: view.LastValue(),
	}
	meteringScanPercentDoneView = &view.View{
		Measure:     MeteringScanPercentDone,
		Aggregation: view.LastValue(),
	}
	meteringScanElapsedSecondsView = &view.View{
		Measure:     MeteringScanElapsedSeconds,
		Aggregation: view.LastValue(),
	}
	meteringScanRemainingSecondsView = &view.View{
		Measure:     MeteringScanRemainingSeconds,
		Aggregation: view.LastValue(),
	}
	meteringScansStartedView = &view.View{
		Measure:     MeteringScansStarted,
		Aggregation: view.Sum(),
	}
	meteringScansFinishedView = &view.View{
		Measure:     MeteringScansFinished,
		Aggregation: view.Sum(),
	}
	meteringBatchesStartedView = &view.View{
		Measure:     MeteringBatchesStarted,
		Aggregation: view.Sum(),
	}
	meteringBatchesFinishedView = &view.View{
		Measure:     MeteringBatchesFinished,
		Aggregation: view.Sum(),
	}
	meteringBatchStartedAtView = &view.View{
		Measure:     MeteringBatchStartedAt,
		Aggregation: view.LastValue(),
	}
	meteringBatchDurationMsView = &view.View{
		Measure: MeteringBatchDurationMs,
		// A million-key batch is a few seconds of work. Wider buckets catch
		// stalls and cleanup writes.
		Aggregation: view.Distribution(
			50, 100, 250, 500,
			1_000, 2_500, 5_000, 10_000,
			25_000, 50_000, 100_000, 250_000, 600_000,
		),
	}
	meteringCleanupSlotsView = &view.View{
		Measure:     MeteringCleanupSlots,
		Aggregation: view.Sum(),
	}
	meteringCleanupMultihashesView = &view.View{
		Measure:     MeteringCleanupMultihashes,
		Aggregation: view.Sum(),
	}
	meteringCleanupKeyBytesView = &view.View{
		Measure:     MeteringCleanupKeyBytes,
		Aggregation: view.Sum(),
	}
	meteringCleanupValueBytesView = &view.View{
		Measure:     MeteringCleanupValueBytes,
		Aggregation: view.Sum(),
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
	meteringTotalDeletedView,
	meteringDeletedSlotsView,
	meteringDeletedKeyBytesView,
	meteringDeletedValueBytesView,
	meteringScanCompletedAtView,
	meteringScanDurationMsView,
	meteringScanStartedAtView,
	meteringScanKeysReadView,
	meteringScanBytesReadView,
	meteringScanDeletedView,
	meteringScanDeletedSlotsView,
	meteringScanDeletedKeyBytesView,
	meteringScanDeletedValueBytesView,
	meteringScanPercentDoneView,
	meteringScanElapsedSecondsView,
	meteringScanRemainingSecondsView,
	meteringScansStartedView,
	meteringScansFinishedView,
	meteringBatchesStartedView,
	meteringBatchesFinishedView,
	meteringBatchStartedAtView,
	meteringBatchDurationMsView,
	meteringCleanupSlotsView,
	meteringCleanupMultihashesView,
	meteringCleanupKeyBytesView,
	meteringCleanupValueBytesView,
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
