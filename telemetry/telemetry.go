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
		Namespace: cfg.Config.Prometheus.Namespace,
		Subsystem: cfg.Config.Prometheus.Subsystem,
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
		Namespace: cfg.Config.Prometheus.Namespace,
		Subsystem: cfg.Config.Prometheus.Subsystem,
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
		Namespace: cfg.Config.Prometheus.Namespace,
		Subsystem: cfg.Config.Prometheus.Subsystem,
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
	if !cfg.Config.Prometheus.Enable {
		return
	}

	registry = prometheus.NewRegistry()
	server := http.Server{
		Addr:    cfg.Config.Prometheus.Bind,
		Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}),
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Error().Err(err).Msg("Unable to start controller listener")
		}
	}()
}
