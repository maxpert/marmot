package telemetry

import (
	"net/http"
	"strconv"

	"github.com/maxpert/marmot/cfg"
	"github.com/prometheus/client_golang/prometheus"
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

type NoopStat struct{}

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

func InitializeTelemetry() {
	if !cfg.Config.Prometheus.Enabled {
		return
	}

	registry = prometheus.NewRegistry()
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
