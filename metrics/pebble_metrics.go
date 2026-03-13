package metrics

import (
	"context"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const cacheTag = "cache"

var (
	flushCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pebble_flush_count",
			Help: "Total number of flushes",
		},
	)

	readAmp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pebble_read_amp",
			Help: "Current read amplification of the database. Computed as the number of sublevels in L0 + the number of non-empty levels below L0.",
		},
	)

	compactCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pebble_compact_count",
			Help: "Total number of compactions of various types.",
		},
	)

	compactEstimateDebt = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pebble_compact_estimate_debt",
			Help: "Estimate of the number of bytes that need to be compacted for the LSM to reach a stable state.",
		},
	)

	compactInProgressBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pebble_compact_in_progress_bytes",
			Help: "Number of bytes present in sstables being written by in-progress compactions. This value will be zero if there are no in-progress compactions.",
		},
	)

	compactNumInProgress = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pebble_compact_num_in_progress",
			Help: "Number of compactions that are in-progress.",
		},
	)

	compactMarkedFiles = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pebble_compact_marked_files",
			Help: "Count of files that are marked for compaction. Such files are compacted in a rewrite compaction when no other compactions are picked.",
		},
	)

	l0TablesCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pebble_l0_tables_count",
			Help: "Total count of sstables in L0. The number of L0 sstables should not be in the high thousands. High values indicate heavy write load that is causing accumulation of sstables in level 0. These sstables are not being compacted quickly enough to lower levels, resulting in a misshapen LSM.",
		},
	)

	cacheCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pebble_cache_count",
			Help: "Count of objects (blocks or tables) in the cache.",
		},
		[]string{cacheTag},
	)

	cacheSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pebble_cache_size",
			Help: "Number of bytes inuse by the cache.",
		},
		[]string{cacheTag},
	)

	cacheHits = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pebble_cache_hits",
			Help: "Number of cache hits.",
		},
		[]string{cacheTag},
	)

	cacheMisses = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pebble_cache_misses",
			Help: "Number of cache misses.",
		},
		[]string{cacheTag},
	)
)

func reportMetrics(db *pebble.DB) {
	const (
		cacheTagBlock = "block"
		cacheTagFile  = "file"
	)

	m := db.Metrics()
	flushCount.Set(float64(m.Flush.Count))
	readAmp.Set(float64(m.ReadAmp()))
	compactCount.Set(float64(m.Compact.Count))
	compactEstimateDebt.Set(float64(m.Compact.EstimatedDebt))
	compactInProgressBytes.Set(float64(m.Compact.InProgressBytes))
	compactNumInProgress.Set(float64(m.Compact.NumInProgress))
	compactMarkedFiles.Set(float64(m.Compact.MarkedFiles))
	l0TablesCount.Set(float64(m.Levels[0].TablesCount))

	// Block cache metrics
	cacheCount.WithLabelValues(cacheTagBlock).Set(float64(m.BlockCache.Count))
	cacheSize.WithLabelValues(cacheTagBlock).Set(float64(m.BlockCache.Size))
	cacheHits.WithLabelValues(cacheTagBlock).Set(float64(m.BlockCache.Hits))
	cacheMisses.WithLabelValues(cacheTagBlock).Set(float64(m.BlockCache.Misses))

	// File cache metrics
	cacheCount.WithLabelValues(cacheTagFile).Set(float64(m.FileCache.TableCount + m.FileCache.BlobFileCount))
	cacheSize.WithLabelValues(cacheTagFile).Set(float64(m.FileCache.Size))
	cacheHits.WithLabelValues(cacheTagFile).Set(float64(m.FileCache.Hits))
	cacheMisses.WithLabelValues(cacheTagFile).Set(float64(m.FileCache.Misses))
}

// ObservePebbleMetrics is used to periodically report metrics from the pebble database
func ObservePebbleMetrics(ctx context.Context, interval time.Duration, db *pebble.DB) {
	reportMetrics(db)

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			reportMetrics(db)
			timer.Reset(interval)
		case <-ctx.Done():
			return
		}
	}
}

func SetupPebbleMetrics() {
	PromRegistry.MustRegister(
		flushCount, readAmp, compactCount, compactEstimateDebt,
		compactInProgressBytes, compactNumInProgress, compactMarkedFiles,
		l0TablesCount, cacheCount, cacheSize, cacheHits, cacheMisses,
	)
}
