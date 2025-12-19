package publisher

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/common"
	"github.com/rs/zerolog/log"
)

// DatabaseManager interface to avoid import cycle
type DatabaseManager interface {
	GetTableSchema(database, table string) (TableSchema, error)
}

// RegistryConfig configures the CDC publisher registry
type RegistryConfig struct {
	DataDir     string                  // For PublishLog path
	DBManager   DatabaseManager         // For table schemas
	SinkConfigs []cfg.SinkConfiguration // From config
}

// Registry manages the lifecycle of all CDC workers
type Registry struct {
	log       *PublishLog
	workers   []*Worker
	dbManager DatabaseManager
	running   atomic.Bool
	mu        sync.Mutex
}

// NewRegistry creates a new CDC publisher registry
func NewRegistry(config RegistryConfig) (*Registry, error) {
	// Validate config
	if config.DataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if config.DBManager == nil {
		return nil, fmt.Errorf("database manager is required")
	}

	// Create PublishLog at {dataDir}/publish_log/
	pubLogPath := filepath.Join(config.DataDir, "publish_log")
	pubLog, err := NewPublishLog(pubLogPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create publish log: %w", err)
	}

	registry := &Registry{
		log:       pubLog,
		workers:   make([]*Worker, 0, len(config.SinkConfigs)),
		dbManager: config.DBManager,
	}

	// Create workers for each sink configuration
	for _, sinkCfg := range config.SinkConfigs {
		if err := registry.AddSink(sinkCfg); err != nil {
			// Cleanup on error: close all worker sinks and publish log
			for _, worker := range registry.workers {
				if worker.config.Sink != nil {
					worker.config.Sink.Close()
				}
			}
			if pubLog != nil {
				pubLog.Close()
			}
			return nil, fmt.Errorf("failed to add sink %q: %w", sinkCfg.Name, err)
		}
	}

	log.Info().
		Int("workers", len(registry.workers)).
		Msg("CDC publisher registry initialized")

	return registry, nil
}

// AddSink creates and adds a new worker for the given sink configuration
func (r *Registry) AddSink(config cfg.SinkConfiguration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Create sink based on config.Type
	snk, err := createSink(config)
	if err != nil {
		return fmt.Errorf("failed to create sink: %w", err)
	}

	// Create transformer based on config.Format (stateless, no cleanup needed)
	trans, err := createTransformer(config.Format)
	if err != nil {
		snk.Close()
		return fmt.Errorf("failed to create transformer: %w", err)
	}

	// Create filter from config.FilterTables, config.FilterDatabases
	filter, err := NewGlobFilter(config.FilterTables, config.FilterDatabases)
	if err != nil {
		snk.Close()
		return fmt.Errorf("failed to create filter: %w", err)
	}

	// Create SchemaProvider wrapper for dbManager
	schemaProvider := &schemaProviderAdapter{
		dbMgr: r.dbManager,
	}

	// Create WorkerConfig with all parameters
	workerConfig := WorkerConfig{
		Name:            config.Name,
		Log:             r.log,
		Sink:            snk,
		Transformer:     trans,
		Filter:          filter,
		SchemaProvider:  schemaProvider,
		TopicPrefix:     config.TopicPrefix,
		BatchSize:       config.BatchSize,
		PollInterval:    time.Duration(config.PollIntervalMS) * time.Millisecond,
		RetryInitial:    time.Duration(config.RetryInitialMS) * time.Millisecond,
		RetryMax:        time.Duration(config.RetryMaxMS) * time.Millisecond,
		RetryMultiplier: config.RetryMultiplier,
	}

	// Create Worker
	worker, err := NewWorker(workerConfig)
	if err != nil {
		snk.Close()
		return fmt.Errorf("failed to create worker: %w", err)
	}

	// Add to workers slice
	r.workers = append(r.workers, worker)

	log.Info().
		Str("sink", config.Name).
		Str("type", config.Type).
		Str("format", config.Format).
		Msg("Added CDC sink")

	return nil
}

// Start starts all workers
func (r *Registry) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running.Load() {
		return fmt.Errorf("registry already running")
	}

	log.Info().Int("workers", len(r.workers)).Msg("Starting CDC publisher registry")

	// Start all workers
	for _, worker := range r.workers {
		worker.Start()
	}

	r.running.Store(true)

	return nil
}

// Stop stops all workers and closes the publish log
func (r *Registry) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running.Swap(false) {
		return // Already stopped
	}

	log.Info().Msg("Stopping CDC publisher registry")

	// Stop all workers
	for _, worker := range r.workers {
		worker.Stop()
	}

	// Close PublishLog
	if err := r.log.Close(); err != nil {
		log.Warn().Err(err).Msg("Failed to close publish log")
	}

	log.Info().Msg("CDC publisher registry stopped")
}

// Append adds CDC events to the publish log
// Called from transaction commit path
// Note: PublishLog.Append is thread-safe (uses Pebble DB which handles concurrency)
func (r *Registry) Append(events []CDCEvent) error {
	if !r.running.Load() {
		return fmt.Errorf("registry not running")
	}
	return r.log.Append(events)
}

// AppendCDC converts CDC entries and appends them to the publish log
// Note: PublishLog.Append is thread-safe (uses Pebble DB which handles concurrency)
func (r *Registry) AppendCDC(txnID uint64, database string, entries []common.CDCEntry, commitTSNanos int64, nodeID uint64) error {
	if !r.running.Load() {
		return fmt.Errorf("registry not running")
	}

	events := ConvertToCDCEvents(txnID, database, entries, commitTSNanos, nodeID)
	if len(events) == 0 {
		return nil
	}

	return r.log.Append(events)
}

// schemaProviderAdapter adapts DatabaseManager to SchemaProvider interface
type schemaProviderAdapter struct {
	dbMgr DatabaseManager
}

func (s *schemaProviderAdapter) GetSchema(database, table string) (TableSchema, error) {
	return s.dbMgr.GetTableSchema(database, table)
}

// createSink creates a sink based on the configuration
func createSink(config cfg.SinkConfiguration) (Sink, error) {
	factoryMu.RLock()
	factory, exists := sinkFactories[config.Type]
	factoryMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown sink type: %s", config.Type)
	}

	return factory(config)
}

// SinkFactory is a function that creates a Sink from a configuration
type SinkFactory func(cfg.SinkConfiguration) (Sink, error)

// TransformerFactory is a function that creates a Transformer
type TransformerFactory func() Transformer

var (
	sinkFactories        = make(map[string]SinkFactory)
	transformerFactories = make(map[string]TransformerFactory)
	factoryMu            sync.RWMutex
)

// RegisterSink registers a sink factory for a type
func RegisterSink(sinkType string, factory SinkFactory) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	sinkFactories[sinkType] = factory
}

// RegisterTransformer registers a transformer factory for a format
func RegisterTransformer(format string, factory TransformerFactory) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	transformerFactories[format] = factory
}

// createTransformer creates a transformer based on the format
func createTransformer(format string) (Transformer, error) {
	factoryMu.RLock()
	factory, exists := transformerFactories[format]
	factoryMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown format: %s", format)
	}

	return factory(), nil
}
