package metrics

import (
	"context"
	"sync/atomic"

	cmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/unit"
)

type coreMetrics struct {
	CacheHits        instrument.Int64Counter
	CacheMisses      instrument.Int64Counter
	cacheMultihashes instrument.Int64ObservableGauge
	cacheValues      instrument.Int64ObservableGauge
	cacheEvictions   instrument.Int64ObservableGauge
	CacheMisuse      instrument.Int64Counter

	GetIndexLatency   instrument.Int64Histogram
	IngestMultihashes instrument.Int64Counter
	RemovedProviders  instrument.Int64Counter
	storeSize         instrument.Int64ObservableGauge

	StoreSizeValue        atomic.Value
	CacheMultihashesValue atomic.Value
	CacheValuesValue      atomic.Value
	CacheEvictionsValue   atomic.Value

	DHHttpLatency instrument.Int64Histogram
}

func newCoreMetrics(meter cmetric.Meter) (*coreMetrics, error) {
	var m coreMetrics
	var err error

	m.StoreSizeValue.Store(int64(0))
	m.CacheMultihashesValue.Store(int64(0))
	m.CacheValuesValue.Store(int64(0))
	m.CacheEvictionsValue.Store(int64(0))

	if m.CacheHits, err = meter.Int64Counter("core/cache/hits",
		instrument.WithDescription("Number of retrieval cache hits"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}
	if m.CacheMisses, err = meter.Int64Counter("core/cache/misses",
		instrument.WithDescription("Number of retrieval cache misses"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}
	if m.cacheMultihashes, err = meter.Int64ObservableGauge("core/cache/multihashes",
		instrument.WithDescription("Number of cached multihashes"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}
	if m.cacheValues, err = meter.Int64ObservableGauge("core/cache/values",
		instrument.WithDescription("Number of cached values"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}
	if m.cacheEvictions, err = meter.Int64ObservableGauge("core/cache/evictions",
		instrument.WithDescription("Number of indexes evicted from cache"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}
	if m.CacheMisuse, err = meter.Int64Counter("core/cache/misuse",
		instrument.WithDescription("Cache clears due to high value to multihash ratio (indexer misuse)"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.GetIndexLatency, err = meter.Int64Histogram("core/get_index_latency",
		instrument.WithUnit(unit.Milliseconds),
		instrument.WithDescription("Internal lookup time for a single index")); err != nil {
		return nil, err
	}

	if m.IngestMultihashes, err = meter.Int64Counter("core/ingest_multihashes",
		instrument.WithDescription("Number of multihashes put into the indexer"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.RemovedProviders, err = meter.Int64Counter("core/removed_providers",
		instrument.WithDescription("Number of providers removed from indexer"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.storeSize, err = meter.Int64ObservableGauge("core/storage_size",
		instrument.WithDescription("Bytes of storage used to store the indexed content"),
		instrument.WithUnit(unit.Bytes)); err != nil {
		return nil, err
	}

	if m.DHHttpLatency, err = meter.Int64Histogram("core/dh_latency",
		instrument.WithUnit(unit.Milliseconds),
		instrument.WithDescription("Time that the indexer spends on sending encrypted multihashes or metadata to dhstore")); err != nil {
		return nil, err
	}

	return &m, nil
}

func (m *coreMetrics) observableMetrics() []instrument.Asynchronous {
	return []instrument.Asynchronous{
		m.cacheMultihashes,
		m.cacheValues,
		m.cacheEvictions,
		m.storeSize,
	}
}

func (m *coreMetrics) observe(ctx context.Context, o cmetric.Observer) {
	o.ObserveInt64(m.cacheMultihashes, m.CacheMultihashesValue.Load().(int64))
	o.ObserveInt64(m.cacheValues, m.CacheValuesValue.Load().(int64))
	o.ObserveInt64(m.cacheEvictions, m.CacheEvictionsValue.Load().(int64))
	o.ObserveInt64(m.storeSize, m.StoreSizeValue.Load().(int64))
}
