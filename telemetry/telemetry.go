package telemetry

import (
	"net/http"
	"strconv"

	"github.com/maxpert/marmot/cfg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var registry *prometheus.Registry

type Histogram interface {
	Observe(float64)
}

type Counter interface {
	Inc()
	Add(float64)
}

type Gauge interface {
	Set(float64)
	Inc()
	Dec()
	Add(float64)
	Sub(float64)
	SetToCurrentTime()
}

// Vec types for labeled metrics
type CounterVec interface {
	With(labels ...string) Counter
}

type GaugeVec interface {
	With(labels ...string) Gauge
}

type HistogramVec interface {
	With(labels ...string) Histogram
}

type NoopStat struct{}

// noopCounterVec, noopGaugeVec, noopHistogramVec for type safety
type noopCounterVec struct{}
type noopGaugeVec struct{}
type noopHistogramVec struct{}

func (n noopCounterVec) With(labels ...string) Counter     { return NoopStat{} }
func (n noopGaugeVec) With(labels ...string) Gauge         { return NoopStat{} }
func (n noopHistogramVec) With(labels ...string) Histogram { return NoopStat{} }

// Prometheus Vec wrappers
type prometheusCounterVec struct {
	vec    *prometheus.CounterVec
	labels []string
}

func (p *prometheusCounterVec) With(labelValues ...string) Counter {
	return p.vec.WithLabelValues(labelValues...)
}

type prometheusGaugeVec struct {
	vec    *prometheus.GaugeVec
	labels []string
}

func (p *prometheusGaugeVec) With(labelValues ...string) Gauge {
	return p.vec.WithLabelValues(labelValues...)
}

type prometheusHistogramVec struct {
	vec    *prometheus.HistogramVec
	labels []string
}

func (p *prometheusHistogramVec) With(labelValues ...string) Histogram {
	return p.vec.WithLabelValues(labelValues...)
}

func (n NoopStat) Observe(float64) {
}

func (n NoopStat) Set(float64) {
}

func (n NoopStat) Dec() {
}

func (n NoopStat) Sub(float64) {
}

func (n NoopStat) SetToCurrentTime() {
}

func (n NoopStat) Inc() {
}

func (n NoopStat) Add(float64) {
}

func NewCounter(name string, help string) Counter {
	if registry == nil {
		return NoopStat{}
	}

	ret := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "marmot",
		Subsystem: "v2",
		Name:      name,
		Help:      help,
		ConstLabels: map[string]string{
			"node_id": strconv.FormatUint(cfg.Config.NodeID, 10),
		},
	})

	registry.MustRegister(ret)
	return ret
}

func NewGauge(name string, help string) Gauge {
	if registry == nil {
		return NoopStat{}
	}

	ret := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "marmot",
		Subsystem: "v2",
		Name:      name,
		Help:      help,
		ConstLabels: map[string]string{
			"node_id": strconv.FormatUint(cfg.Config.NodeID, 10),
		},
	})

	registry.MustRegister(ret)
	return ret
}

func NewHistogram(name string, help string) Histogram {
	if registry == nil {
		return NoopStat{}
	}

	ret := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "marmot",
		Subsystem: "v2",
		Name:      name,
		Help:      help,
		ConstLabels: map[string]string{
			"node_id": strconv.FormatUint(cfg.Config.NodeID, 10),
		},
	})

	registry.MustRegister(ret)
	return ret
}

func NewHistogramWithBuckets(name, help string, buckets []float64) Histogram {
	if registry == nil {
		return NoopStat{}
	}

	ret := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "marmot",
		Subsystem: "v2",
		Name:      name,
		Help:      help,
		Buckets:   buckets,
		ConstLabels: map[string]string{
			"node_id": strconv.FormatUint(cfg.Config.NodeID, 10),
		},
	})

	registry.MustRegister(ret)
	return ret
}

func NewCounterVec(name, help string, labels []string) CounterVec {
	if registry == nil {
		return noopCounterVec{}
	}

	ret := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "marmot",
		Subsystem: "v2",
		Name:      name,
		Help:      help,
		ConstLabels: map[string]string{
			"node_id": strconv.FormatUint(cfg.Config.NodeID, 10),
		},
	}, labels)

	registry.MustRegister(ret)
	return &prometheusCounterVec{vec: ret, labels: labels}
}

func NewGaugeVec(name, help string, labels []string) GaugeVec {
	if registry == nil {
		return noopGaugeVec{}
	}

	ret := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "marmot",
		Subsystem: "v2",
		Name:      name,
		Help:      help,
		ConstLabels: map[string]string{
			"node_id": strconv.FormatUint(cfg.Config.NodeID, 10),
		},
	}, labels)

	registry.MustRegister(ret)
	return &prometheusGaugeVec{vec: ret, labels: labels}
}

func NewHistogramVec(name, help string, labels []string, buckets []float64) HistogramVec {
	if registry == nil {
		return noopHistogramVec{}
	}

	ret := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "marmot",
		Subsystem: "v2",
		Name:      name,
		Help:      help,
		Buckets:   buckets,
		ConstLabels: map[string]string{
			"node_id": strconv.FormatUint(cfg.Config.NodeID, 10),
		},
	}, labels)

	registry.MustRegister(ret)
	return &prometheusHistogramVec{vec: ret, labels: labels}
}

func InitializeTelemetry() {
	if !cfg.Config.Prometheus.Enabled {
		return
	}

	registry = prometheus.NewRegistry()

	// Register process and Go runtime collectors for CPU/memory metrics
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(collectors.NewGoCollector())

	log.Info().Msg("Prometheus metrics enabled - will be served on gRPC port at /metrics")
}

// GetMetricsHandler returns the HTTP handler for Prometheus metrics
// Returns nil if Prometheus is not enabled
func GetMetricsHandler() http.Handler {
	if registry == nil {
		return nil
	}
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry})
}
