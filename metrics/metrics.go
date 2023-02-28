package metrics

import (
	"context"
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

	reg cmetric.Registration
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
	var exporter *prometheus.Exporter
	if exporter, err = prometheus.New(prometheus.WithoutUnits(),
		prometheus.WithAggregationSelector(aggregationSelector)); err != nil {
		return nil, err
	}

	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	m.meter = provider.Meter("ipni/core")

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
	if m.reg != nil {
		if err := m.reg.Unregister(); err != nil {
			return err
		}
	}

	var err error
	m.Pebble, err = newPebbleMetrics(m.meter, pebbleMetricsProvider)
	if err != nil {
		return err
	}

	return m.registerMetricsCallbacks()
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
	err := m.registerMetricsCallbacks()
	if err == nil {
		log.Infow("Core metrics registered")
	}
	return err
}

func (m *Metrics) registerMetricsCallbacks() error {
	var err error

	observableMetrics := m.Core.observableMetrics()
	if m.Pebble != nil {
		observableMetrics = append(observableMetrics, m.Pebble.observableMetrcics()...)
	}

	m.reg, err = m.meter.RegisterCallback(
		m.observe,
		observableMetrics...,
	)

	return err
}

func (m *Metrics) Shutdown(ctx context.Context) error {
	log.Infow("Core metrics unregistered")
	return m.reg.Unregister()
}
