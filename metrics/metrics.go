package metrics

import (
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"
	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel/exporters/prometheus"
	cmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregation"
)

var log = logging.Logger("indexer-core/metrics")

type Metrics struct {
	Pebble *pebbleMetrics
	Core   *coreMetrics
	meter  cmetric.Meter

	exporter *prometheus.Exporter
	Provider *metric.MeterProvider
	reg      cmetric.Registration
}

func aggregationSelector(ik metric.InstrumentKind) aggregation.Aggregation {
	if ik == metric.InstrumentKindHistogram {
		return aggregation.ExplicitBucketHistogram{
			Boundaries: []float64{0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000},
			NoMinMax:   false,
		}
	}
	return metric.DefaultAggregationSelector(ik)
}

func New(pebbleMetricsProvider func() *pebble.Metrics) (*Metrics, error) {
	var m Metrics
	var err error
	if m.exporter, err = prometheus.New(prometheus.WithoutUnits(),
		prometheus.WithAggregationSelector(aggregationSelector)); err != nil {
		return nil, err
	}

	m.Provider = metric.NewMeterProvider(metric.WithReader(m.exporter))
	m.meter = m.Provider.Meter("ipni/core")

	if pebbleMetricsProvider != nil {
		m.Pebble, err = newPebbleMetrics(m.meter, pebbleMetricsProvider)
		if err != nil {
			return nil, err
		}
	}

	m.Core, err = newCoreMetrics(m.meter)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (m *Metrics) SetPebbleMetricsProvider(pebbleMetricsProvider func() *pebble.Metrics) error {
	var err error
	if m.Pebble != nil {
		return errors.New("pebble metrics provider has already been set")
	}
	m.Pebble, err = newPebbleMetrics(m.meter, pebbleMetricsProvider)
	if err != nil {
		return err
	}
	return nil
}

func (m *Metrics) observe(ctx context.Context, o cmetric.Observer) error {
	m.Core.observe(ctx, o)
	if m.Pebble != nil {
		m.Pebble.observe(ctx, o)
	}
	return nil
}

func MsecSince(startTime time.Time) int64 {
	return time.Since(startTime).Nanoseconds() / 1e6
}

func (m *Metrics) Start(_ context.Context) error {
	var err error

	observableMetrics := m.Core.observableMetrics()
	if m.Pebble != nil {
		observableMetrics = append(observableMetrics, m.Pebble.observableMetrcics()...)
	}

	m.reg, err = m.meter.RegisterCallback(
		m.observe,
		observableMetrics...,
	)

	log.Infow("Core metrics registered")

	return err
}

func (m *Metrics) Shutdown(ctx context.Context) error {
	return m.reg.Unregister()
}
