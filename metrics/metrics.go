package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/pebble"
	logging "github.com/ipfs/go-log/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	s        *http.Server
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

func New(metricsAddr string, pebbleMetricsProvider func() *pebble.Metrics) (*Metrics, error) {
	var m Metrics
	var err error
	if m.exporter, err = prometheus.New(prometheus.WithoutUnits(),
		prometheus.WithAggregationSelector(aggregationSelector)); err != nil {
		return nil, err
	}

	provider := metric.NewMeterProvider(metric.WithReader(m.exporter))
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

	m.s = &http.Server{
		Addr:    metricsAddr,
		Handler: metricsMux(),
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

func metricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func (m *Metrics) Start(_ context.Context) error {
	observableMetrics := m.Core.observableMetrics()
	if m.Pebble != nil {
		observableMetrics = append(observableMetrics, m.Pebble.observableMetrcics()...)
	}

	// registration object isn't needed as we aren't going to unregister metrics
	_, err := m.meter.RegisterCallback(
		m.observe,
		observableMetrics...,
	)

	if err != nil {
		return err
	}

	mln, err := net.Listen("tcp", m.s.Addr)
	if err != nil {
		return err
	}

	go func() { _ = m.s.Serve(mln) }()

	log.Infow("Metrics server started", "addr", mln.Addr())
	return nil
}

func (m *Metrics) Shutdown(ctx context.Context) error {
	return m.s.Shutdown(ctx)
}
