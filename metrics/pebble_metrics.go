package metrics

import (
	"context"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	cmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/unit"
)

// pebbleMetrics asynchronously reports metrics of pebble DB
type pebbleMetrics struct {
	metricsProvider func() *pebble.Metrics

	// flushCount reports the total number of flushes
	flushCount instrument.Int64ObservableGauge
	// readAdmp reports current read amplification of the database.
	// It's computed as the number of sublevels in L0 + the number of non-empty
	// levels below L0.
	// Read amplification factor should be in the single digits. A value exceeding 50 for 1 hour
	// strongly suggests that the LSM tree has an unhealthy shape.
	readAmp instrument.Int64ObservableGauge

	// NOTE: cache metrics report tagged values for both block and table caches
	// cacheSize reports the number of bytes inuse by the cache
	cacheSize instrument.Int64ObservableGauge
	// cacheCount reports the count of objects (blocks or tables) in the cache
	cacheCount instrument.Int64ObservableGauge
	// cacheHits reports number of cache hits
	cacheHits instrument.Int64ObservableGauge
	// cacheMisses reports number of cache misses.
	cacheMisses instrument.Int64ObservableGauge

	// compactCount is the total number of compactions, and per-compaction type counts.
	compactCount instrument.Int64ObservableCounter
	// compactEstimatedDebt is an estimate of the number of bytes that need to be compacted for the LSM
	// to reach a stable state.
	compactEstimatedDebt instrument.Int64ObservableGauge
	// compactInProgressBytes is a number of bytes present in sstables being written by in-progress
	// compactions. This value will be zero if there are no in-progress
	// compactions.
	compactInProgressBytes instrument.Int64ObservableGauge
	// compactNumInProgress is a number of compactions that are in-progress.
	compactNumInProgress instrument.Int64ObservableGauge
	// compactMarkedFiles is a count of files that are marked for
	// compaction. Such files are compacted in a rewrite compaction
	// when no other compactions are picked.
	compactMarkedFiles instrument.Int64ObservableGauge

	// l0NumFiles is the total number of files in L0. The number of L0 files should not be in the high thousands.
	// High values indicate heavy write load that is causing accumulation of files in level 0. These files are not
	// being compacted quickly enough to lower levels, resulting in a misshapen LSM.
	l0NumFiles instrument.Int64ObservableGauge
}

func newPebbleMetrics(meter cmetric.Meter, metricsProvider func() *pebble.Metrics) (*pebbleMetrics, error) {
	var err error

	var pm pebbleMetrics

	pm.metricsProvider = metricsProvider

	if pm.flushCount, err = meter.Int64ObservableGauge(
		"core/pebble/flush_count",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The total number of flushes."),
	); err != nil {
		return nil, err
	}

	if pm.readAmp, err = meter.Int64ObservableGauge(
		"core/pebble/read_amp",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("current read amplification of the database. "+
			"It's computed as the number of sublevels in L0 + the number of non-empty"+
			" levels below L0."),
	); err != nil {
		return nil, err
	}

	if pm.cacheSize, err = meter.Int64ObservableGauge(
		"core/pebble/cache_size",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The number of bytes inuse by the cache."),
	); err != nil {
		return nil, err
	}

	if pm.cacheCount, err = meter.Int64ObservableGauge(
		"core/pebble/cache_count",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The count of objects (blocks or tables) in the cache."),
	); err != nil {
		return nil, err
	}

	if pm.cacheHits, err = meter.Int64ObservableGauge(
		"core/pebble/cache_hits",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The number of cache hits."),
	); err != nil {
		return nil, err
	}

	if pm.cacheMisses, err = meter.Int64ObservableGauge(
		"core/pebble/cache_misses",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The number of cache misses."),
	); err != nil {
		return nil, err
	}

	if pm.compactCount, err = meter.Int64ObservableCounter(
		"core/pebble/compact_count",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The total number of compactions, and per-compaction type counts."),
	); err != nil {
		return nil, err
	}

	if pm.compactEstimatedDebt, err = meter.Int64ObservableGauge(
		"core/pebble/compact_estimated_debt",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("An estimate of the number of bytes that need to be compacted for the LSM"+
			" to reach a stable state."),
	); err != nil {
		return nil, err
	}

	if pm.compactInProgressBytes, err = meter.Int64ObservableGauge(
		"core/pebble/compact_in_progress_bytes",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("A number of bytes present in sstables being written by in-progress"+
			" compactions. This value will be zero if there are no in-progress"+
			" compactions."),
	); err != nil {
		return nil, err
	}

	if pm.compactNumInProgress, err = meter.Int64ObservableGauge(
		"core/pebble/compact_num_in_progress",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("A number of compactions that are in-progress."),
	); err != nil {
		return nil, err
	}

	if pm.compactMarkedFiles, err = meter.Int64ObservableGauge(
		"core/pebble/compact_marked_files",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("A count of files that are marked for"+
			" compaction. Such files are compacted in a rewrite compaction"+
			" when no other compactions are picked."),
	); err != nil {
		return nil, err
	}

	if pm.l0NumFiles, err = meter.Int64ObservableGauge(
		"core/pebble/compact_l0_num_files",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("The total number of files in L0. The number of L0 files should not be in the high thousands."+
			" High values indicate heavy write load that is causing accumulation of files in level 0. These files are not"+
			" being compacted quickly enough to lower levels, resulting in a misshapen LSM."),
	); err != nil {
		return nil, err
	}

	return &pm, nil
}

func (pm *pebbleMetrics) observableMetrcics() []instrument.Asynchronous {
	return []instrument.Asynchronous{
		pm.flushCount,
		pm.readAmp,
		pm.cacheSize,
		pm.cacheCount,
		pm.cacheHits,
		pm.cacheMisses,
		pm.compactCount,
		pm.compactEstimatedDebt,
		pm.compactInProgressBytes,
		pm.compactNumInProgress,
		pm.compactMarkedFiles,
		pm.l0NumFiles,
	}
}

func (pm *pebbleMetrics) observe(ctx context.Context, o cmetric.Observer) {
	m := pm.metricsProvider()
	o.ObserveInt64(pm.flushCount, m.Flush.Count)
	o.ObserveInt64(pm.readAmp, int64(m.ReadAmp()))

	o.ObserveInt64(pm.cacheCount, m.BlockCache.Count, attribute.String("cache", "block"))
	o.ObserveInt64(pm.cacheSize, m.BlockCache.Size, attribute.String("cache", "block"))
	o.ObserveInt64(pm.cacheHits, m.BlockCache.Hits, attribute.String("cache", "block"))
	o.ObserveInt64(pm.cacheMisses, m.BlockCache.Misses, attribute.String("cache", "block"))

	o.ObserveInt64(pm.cacheCount, m.TableCache.Count, attribute.String("cache", "table"))
	o.ObserveInt64(pm.cacheSize, m.TableCache.Size, attribute.String("cache", "table"))
	o.ObserveInt64(pm.cacheHits, m.TableCache.Hits, attribute.String("cache", "table"))
	o.ObserveInt64(pm.cacheMisses, m.TableCache.Misses, attribute.String("cache", "table"))

	o.ObserveInt64(pm.compactCount, int64(m.Compact.Count))
	o.ObserveInt64(pm.compactEstimatedDebt, int64(m.Compact.EstimatedDebt))
	o.ObserveInt64(pm.compactInProgressBytes, int64(m.Compact.InProgressBytes))
	o.ObserveInt64(pm.compactNumInProgress, int64(m.Compact.NumInProgress))
	o.ObserveInt64(pm.compactMarkedFiles, int64(m.Compact.MarkedFiles))

	o.ObserveInt64(pm.l0NumFiles, int64(m.Levels[0].NumFiles))
}
