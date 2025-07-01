package metrics

import (
	"context"
	"time"

	"github.com/cockroachdb/pebble/v2"
	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var log = logging.Logger("metrics/pebble")

// Tags
var (
	cacheTag, _ = tag.NewKey("cache")
)

// Measures
var (
	// flushCount reports the total number of flushes
	flushCount = stats.Int64("ipni/pebble/flush_count", "The total number of flushes.", stats.UnitDimensionless)
	// readAdmp reports current read amplification of the database.
	// It's computed as the number of sublevels in L0 + the number of non-empty
	// levels below L0.
	// Read amplification factor should be in the single digits. A value exceeding 50 for 1 hour
	// strongly suggests that the LSM tree has an unhealthy shape.
	readAmp = stats.Int64("ipni/pebble/read_amp", "current read amplification of the database. "+
		"It's computed as the number of sublevels in L0 + the number of non-empty"+
		" levels below L0.", stats.UnitDimensionless)

	// NOTE: cache metrics report tagged values for both block and table caches
	// cacheSize reports the number of bytes inuse by the cache
	cacheSize = stats.Int64("ipni/pebble/cache_size", "The number of bytes inuse by the cache.", stats.UnitDimensionless)
	// cacheCount reports the count of objects (blocks or tables) in the cache
	cacheCount = stats.Int64("ipni/pebble/cache_count", "The count of objects (blocks or tables) in the cache.", stats.UnitDimensionless)
	// cacheHits reports number of cache hits
	cacheHits = stats.Int64("ipni/pebble/cache_hits", "The number of cache hits.", stats.UnitDimensionless)
	// cacheMisses reports number of cache misses.
	cacheMisses = stats.Int64("ipni/pebble/cache_misses", "The number of cache misses.", stats.UnitDimensionless)

	// compactCount is the total number of compactions, and per-compaction type counts.
	compactCount = stats.Int64("ipni/pebble/compact_count", "The total number of compactions, and per-compaction type counts.", stats.UnitDimensionless)
	// compactEstimatedDebt is an estimate of the number of bytes that need to be compacted for the LSM
	// to reach a stable state.
	compactEstimatedDebt = stats.Int64("ipni/pebble/compact_estimated_debt", "An estimate of the number of bytes that need to be compacted for the LSM"+
		" to reach a stable state.", stats.UnitDimensionless)
	// compactInProgressBytes is a number of bytes present in sstables being written by in-progress
	// compactions. This value will be zero if there are no in-progress
	// compactions.
	compactInProgressBytes = stats.Int64("ipni/pebble/compact_in_progress_bytes", "A number of bytes present in sstables being written by in-progress"+
		" compactions. This value will be zero if there are no in-progress"+
		" compactions.", stats.UnitDimensionless)
	// compactNumInProgress is a number of compactions that are in-progress.
	compactNumInProgress = stats.Int64("ipni/pebble/compact_num_in_progress", "A number of compactions that are in-progress.", stats.UnitDimensionless)
	// compactMarkedFiles is a count of files that are marked for
	// compaction. Such files are compacted in a rewrite compaction
	// when no other compactions are picked.
	compactMarkedFiles = stats.Int64("ipni/pebble/compact_marked_files", "A count of files that are marked for"+
		" compaction. Such files are compacted in a rewrite compaction"+
		" when no other compactions are picked.", stats.UnitDimensionless)

	// l0NumFiles is the total number of files in L0. The number of L0 files should not be in the high thousands.
	// High values indicate heavy write load that is causing accumulation of files in level 0. These files are not
	// being compacted quickly enough to lower levels, resulting in a misshapen LSM.
	l0NumFiles = stats.Int64("ipni/pebble/compact_l0_num_files", "The total number of files in L0. The number of L0 files should not be in the high thousands."+
		" High values indicate heavy write load that is causing accumulation of files in level 0. These files are not"+
		" being compacted quickly enough to lower levels, resulting in a misshapen LSM.", stats.UnitDimensionless)
)

// Views
var (
	flushCountView = &view.View{
		Measure:     flushCount,
		Aggregation: view.LastValue(),
	}
	readAmpView = &view.View{
		Measure:     readAmp,
		Aggregation: view.LastValue(),
	}
	cacheSizeView = &view.View{
		Measure:     cacheSize,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{cacheTag},
	}
	cacheCountView = &view.View{
		Measure:     cacheCount,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{cacheTag},
	}
	pebbleCacheHitsView = &view.View{
		Measure:     cacheHits,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{cacheTag},
	}
	pebbleCacheMissesView = &view.View{
		Measure:     cacheMisses,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{cacheTag},
	}
	compactCountView = &view.View{
		Measure:     compactCount,
		Aggregation: view.LastValue(),
	}
	compactEstimatedDebtView = &view.View{
		Measure:     compactEstimatedDebt,
		Aggregation: view.LastValue(),
	}
	compactInProgressBytesView = &view.View{
		Measure:     compactInProgressBytes,
		Aggregation: view.LastValue(),
	}
	compactNumInProgressView = &view.View{
		Measure:     compactNumInProgress,
		Aggregation: view.LastValue(),
	}
	compactMarkedFilesView = &view.View{
		Measure:     compactMarkedFiles,
		Aggregation: view.LastValue(),
	}
	l0NumFilesView = &view.View{
		Measure:     l0NumFiles,
		Aggregation: view.LastValue(),
	}
)

// PebbleViews contains pebble specific views
var PebbleViews = []*view.View{
	flushCountView,
	readAmpView,
	cacheSizeView,
	cacheCountView,
	pebbleCacheHitsView,
	pebbleCacheMissesView,
	compactCountView,
	compactEstimatedDebtView,
	compactInProgressBytesView,
	compactNumInProgressView,
	compactMarkedFilesView,
	l0NumFilesView,
}

// ObservePebbleMetrics is used to periodically report metrics from the pebble database
func ObservePebbleMetrics(ctx context.Context, interval time.Duration, db *pebble.DB) {
	var t *time.Timer
	log.Infow("Started observing pebble metrics")
	for {
		select {
		case <-ctx.Done():
			log.Infow("Finished observing pebble metrics")
			return
		default:
			reportMetrics(db.Metrics())
		}

		t = time.NewTimer(interval)
		select {
		case <-ctx.Done():
			log.Infow("Finished observing pebble metrics")
			return
		case <-t.C:
		}
	}
}

func reportMetrics(m *pebble.Metrics) {
	ctx := context.Background()
	stats.Record(ctx,
		flushCount.M(m.Flush.Count),
		readAmp.M(int64(m.ReadAmp())),

		compactCount.M(int64(m.Compact.Count)),
		compactEstimatedDebt.M(int64(m.Compact.EstimatedDebt)),
		compactInProgressBytes.M(int64(m.Compact.InProgressBytes)),
		compactNumInProgress.M(int64(m.Compact.NumInProgress)),
		compactMarkedFiles.M(int64(m.Compact.MarkedFiles)),
		l0NumFiles.M(int64(m.Levels[0].NumFiles)))

	// Block cache metrics
	stats.RecordWithOptions(ctx,
		stats.WithMeasurements(cacheCount.M(m.BlockCache.Count)),
		stats.WithTags(tag.Insert(cacheTag, "block")))

	stats.RecordWithOptions(ctx,
		stats.WithMeasurements(cacheSize.M(m.BlockCache.Size)),
		stats.WithTags(tag.Insert(cacheTag, "block")))

	stats.RecordWithOptions(ctx,
		stats.WithMeasurements(cacheHits.M(m.BlockCache.Hits)),
		stats.WithTags(tag.Insert(cacheTag, "block")))

	stats.RecordWithOptions(ctx,
		stats.WithMeasurements(cacheMisses.M(m.BlockCache.Misses)),
		stats.WithTags(tag.Insert(cacheTag, "block")))

	// Table cache metrics
	stats.RecordWithOptions(ctx,
		stats.WithMeasurements(cacheCount.M(m.TableCache.Count)),
		stats.WithTags(tag.Insert(cacheTag, "table")))

	stats.RecordWithOptions(ctx,
		stats.WithMeasurements(cacheSize.M(m.TableCache.Size)),
		stats.WithTags(tag.Insert(cacheTag, "table")))

	stats.RecordWithOptions(ctx,
		stats.WithMeasurements(cacheHits.M(m.TableCache.Hits)),
		stats.WithTags(tag.Insert(cacheTag, "table")))

	stats.RecordWithOptions(ctx,
		stats.WithMeasurements(cacheMisses.M(m.TableCache.Misses)),
		stats.WithTags(tag.Insert(cacheTag, "table")))
}
