package publisher

import (
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// SchemaProvider provides table schemas for transformation
type SchemaProvider interface {
	GetSchema(database, table string) (TableSchema, error)
}

const (
	// Default batch size for reading events per poll cycle
	DefaultBatchSize = 100
	// Default interval between poll cycles
	DefaultPollInterval = 100 * time.Millisecond
	// Default initial retry delay for failed publish operations
	DefaultRetryInitial = 100 * time.Millisecond
	// Default maximum retry delay (exponential backoff cap)
	DefaultRetryMax = 30 * time.Second
	// Default exponential backoff multiplier
	DefaultRetryMultiplier = 2.0
	// Maximum number of retry attempts before giving up on a publish operation
	DefaultMaxRetries = 100
)

// WorkerConfig configures the CDC publisher worker
type WorkerConfig struct {
	Name            string         // Sink name (for cursor tracking)
	Log             *PublishLog    // Publish log to read from
	Sink            Sink           // Destination sink
	Transformer     Transformer    // Event transformer
	Filter          Filter         // Event filter
	SchemaProvider  SchemaProvider // Provides table schemas
	TopicPrefix     string         // Topic prefix (e.g., "marmot.cdc")
	BatchSize       int            // Events per poll cycle
	PollInterval    time.Duration  // Poll interval
	RetryInitial    time.Duration  // Initial retry delay
	RetryMax        time.Duration  // Max retry delay
	RetryMultiplier float64        // Backoff multiplier
	MaxRetries      int            // Maximum retry attempts (0 = unlimited)
}

// Worker polls the PublishLog and publishes events to a sink
type Worker struct {
	config      WorkerConfig
	cursor      uint64        // Current position
	stopCh      chan struct{} // Stop signal
	doneCh      chan struct{} // Done signal
	running     atomic.Bool
	lifecycleMu sync.Mutex // Protects Start/Stop lifecycle operations
}

// NewWorker creates a new CDC publisher worker
func NewWorker(config WorkerConfig) (*Worker, error) {
	// Validate config
	if config.Name == "" {
		return nil, fmt.Errorf("worker name is required")
	}
	if config.Log == nil {
		return nil, fmt.Errorf("publish log is required")
	}
	if config.Sink == nil {
		return nil, fmt.Errorf("sink is required")
	}
	if config.Transformer == nil {
		return nil, fmt.Errorf("transformer is required")
	}
	if config.Filter == nil {
		return nil, fmt.Errorf("filter is required")
	}
	if config.SchemaProvider == nil {
		return nil, fmt.Errorf("schema provider is required")
	}

	// Set defaults
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultBatchSize
	}
	if config.PollInterval <= 0 {
		config.PollInterval = DefaultPollInterval
	}
	if config.RetryInitial <= 0 {
		config.RetryInitial = DefaultRetryInitial
	}
	if config.RetryMax <= 0 {
		config.RetryMax = DefaultRetryMax
	}
	if config.RetryMultiplier <= 0 {
		config.RetryMultiplier = DefaultRetryMultiplier
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = DefaultMaxRetries
	}

	// Load cursor from publish log
	cursor, err := config.Log.GetCursor(config.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get cursor: %w", err)
	}

	// If cursor is 0 (new sink), find earliest available entry
	if cursor == 0 {
		earliest, err := findEarliestEntry(config.Log)
		if err != nil {
			return nil, fmt.Errorf("failed to find earliest entry: %w", err)
		}
		cursor = earliest
	}

	w := &Worker{
		config: config,
		cursor: cursor,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	return w, nil
}

// findEarliestEntry finds the earliest available entry in the log
func findEarliestEntry(pubLog *PublishLog) (uint64, error) {
	// Try to read from position 0
	events, err := pubLog.ReadFrom(0, 1)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		// Log is empty - start from 0
		return 0, nil
	}
	// Start from the first available entry minus 1
	// (ReadFrom reads from cursor+1, so we want cursor to be SeqNum-1)
	return events[0].SeqNum - 1, nil
}

// intentKeyForSink converts a binary intent key to a string for sink compatibility
// Uses base64url encoding (URL-safe, no padding) for compact representation
func intentKeyForSink(key []byte) string {
	return base64.RawURLEncoding.EncodeToString(key)
}

// Start starts the worker goroutine
func (w *Worker) Start() {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()

	if w.running.Load() {
		return // Already running
	}

	w.running.Store(true)
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})

	log.Info().
		Str("worker", w.config.Name).
		Uint64("cursor", w.cursor).
		Msg("Starting CDC publisher worker")

	go w.pollLoop()
}

// Stop stops the worker gracefully
func (w *Worker) Stop() {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()

	if !w.running.Load() {
		return // Not running
	}

	log.Info().Str("worker", w.config.Name).Msg("Stopping CDC publisher worker")

	close(w.stopCh)
	<-w.doneCh // Wait for goroutine to finish
	w.running.Store(false)

	log.Info().Str("worker", w.config.Name).Msg("CDC publisher worker stopped")
}

// pollLoop is the main worker loop
func (w *Worker) pollLoop() {
	defer close(w.doneCh)

	for {
		select {
		case <-w.stopCh:
			return
		default:
			// Read batch of events
			events, err := w.config.Log.ReadFrom(w.cursor, w.config.BatchSize)
			if err != nil {
				log.Error().
					Err(err).
					Str("worker", w.config.Name).
					Uint64("cursor", w.cursor).
					Msg("Failed to read from publish log")
				w.sleep(w.config.PollInterval)
				continue
			}

			if len(events) == 0 {
				// No events available - sleep and retry
				w.sleep(w.config.PollInterval)
				continue
			}

			// Process each event
			for _, event := range events {
				if err := w.processEvent(event); err != nil {
					log.Error().
						Err(err).
						Str("worker", w.config.Name).
						Uint64("seq", event.SeqNum).
						Msg("Failed to process event")
					// processEvent already handles retries - this shouldn't happen
					return
				}
				// Update cursor after successful processing
				w.cursor = event.SeqNum
			}
		}
	}
}

// processEvent processes a single CDC event
// Delivery semantics: At-least-once delivery with cursor tracking.
// - Events are published first, then cursor is advanced.
// - If cursor advance fails, event may be redelivered on restart.
// - Filtered events advance cursor without publishing (exactly-once skip).
func (w *Worker) processEvent(event CDCEvent) error {
	// Check if event should be filtered
	if !w.config.Filter.Match(event.Database, event.Table) {
		// Event filtered out - advance cursor and skip
		// Note: Cursor advance failure for filtered events is non-critical since
		// the event would be filtered again on redelivery, but we log for visibility
		if err := w.config.Log.AdvanceCursor(w.config.Name, event.SeqNum); err != nil {
			log.Warn().
				Err(err).
				Str("worker", w.config.Name).
				Uint64("seq", event.SeqNum).
				Msg("Failed to advance cursor for filtered event")
		}
		return nil
	}

	// Get table schema
	schema, err := w.config.SchemaProvider.GetSchema(event.Database, event.Table)
	if err != nil {
		return fmt.Errorf("failed to get schema for %s.%s: %w", event.Database, event.Table, err)
	}

	// Transform event
	data, err := w.config.Transformer.Transform(event, schema)
	if err != nil {
		return fmt.Errorf("failed to transform event: %w", err)
	}

	// Build topic name
	topic := w.buildTopic(event.Database, event.Table)

	// Convert binary intent key to string for sink compatibility
	intentKey := intentKeyForSink(event.IntentKey)

	// Publish with retry
	if err := w.publishWithRetry(topic, intentKey, data); err != nil {
		return err
	}

	// For DELETE operations, also send tombstone
	if event.Operation == OpDelete {
		tombstone := w.config.Transformer.Tombstone(intentKey)
		if err := w.publishWithRetry(topic, intentKey, tombstone); err != nil {
			return err
		}
	}

	// Advance cursor after successful publish
	// Note: If cursor advance fails after successful publish, the event may be
	// redelivered on restart (at-least-once delivery guarantee)
	if err := w.config.Log.AdvanceCursor(w.config.Name, event.SeqNum); err != nil {
		log.Warn().
			Err(err).
			Str("worker", w.config.Name).
			Uint64("seq", event.SeqNum).
			Msg("Failed to advance cursor after successful publish - event may be redelivered")
	}

	return nil
}

// buildTopic builds the topic name for an event
func (w *Worker) buildTopic(database, table string) string {
	if w.config.TopicPrefix == "" {
		return fmt.Sprintf("%s.%s", database, table)
	}
	return fmt.Sprintf("%s.%s.%s", w.config.TopicPrefix, database, table)
}

// publishWithRetry publishes data with exponential backoff retry
// Returns error if max retries exhausted or worker stopped
func (w *Worker) publishWithRetry(topic, key string, data []byte) error {
	delay := w.config.RetryInitial
	attempts := 0

	for {
		err := w.config.Sink.Publish(topic, key, data)
		if err == nil {
			return nil
		}

		attempts++

		// Check if we've exhausted max retries (0 = unlimited)
		if w.config.MaxRetries > 0 && attempts >= w.config.MaxRetries {
			return fmt.Errorf("exhausted max retries (%d) for topic %s: %w", w.config.MaxRetries, topic, err)
		}

		// Log error and retry
		log.Warn().
			Err(err).
			Str("worker", w.config.Name).
			Str("topic", topic).
			Int("attempt", attempts).
			Dur("retry_delay", delay).
			Msg("Failed to publish event, retrying")

		// Sleep with stop check
		if !w.sleep(delay) {
			return fmt.Errorf("worker stopped during retry")
		}

		// Exponential backoff
		delay = time.Duration(float64(delay) * w.config.RetryMultiplier)
		if delay > w.config.RetryMax {
			delay = w.config.RetryMax
		}
	}
}

// sleep sleeps for the given duration, checking stopCh
// Returns true if sleep completed, false if stopped
func (w *Worker) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-w.stopCh:
		return false
	case <-timer.C:
		return true
	}
}
