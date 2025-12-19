package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jizhuozhi/go-future"
	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

const (
	batchCommitTimeout = 30 * time.Second
)

// Package-level metrics (registered once)
var (
	batchCommitterMetricsOnce   sync.Once
	batchCommitterFlushCounter  telemetry.CounterVec
	batchCommitterBatchSizeHist telemetry.Histogram
	batchCommitterFlushDurHist  telemetry.Histogram

	// Checkpoint metrics
	batchCommitterCheckpointCounter     telemetry.CounterVec
	batchCommitterCheckpointDurHist     telemetry.Histogram
	batchCommitterWALSizeBeforeHist     telemetry.Histogram
	batchCommitterWALFramesLogHist      telemetry.Histogram
	batchCommitterWALFramesCheckpointed telemetry.Histogram
	batchCommitterCheckpointBusyCounter telemetry.CounterVec
	batchCommitterCheckpointEfficiency  telemetry.Histogram
)

func initBatchCommitterMetrics() {
	batchCommitterMetricsOnce.Do(func() {
		batchCommitterFlushCounter = telemetry.NewCounterVec(
			"batch_committer_flushes_total",
			"Total number of batch flushes by trigger reason",
			[]string{"trigger"},
		)
		batchCommitterBatchSizeHist = telemetry.NewHistogramWithBuckets(
			"batch_committer_batch_size",
			"Number of transactions per batch flush",
			[]float64{1, 5, 10, 25, 50, 100, 200, 500},
		)
		batchCommitterFlushDurHist = telemetry.NewHistogramWithBuckets(
			"batch_committer_flush_duration_ms",
			"Time taken to flush a batch in milliseconds",
			[]float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100},
		)
		batchCommitterCheckpointCounter = telemetry.NewCounterVec(
			"batch_committer_checkpoint_total",
			"Total checkpoints by mode (passive/restart/skipped)",
			[]string{"mode"},
		)
		batchCommitterCheckpointDurHist = telemetry.NewHistogramWithBuckets(
			"batch_committer_checkpoint_duration_ms",
			"Checkpoint duration in milliseconds",
			[]float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		)
		batchCommitterWALSizeBeforeHist = telemetry.NewHistogramWithBuckets(
			"batch_committer_wal_size_mb",
			"WAL file size in MB before checkpoint",
			[]float64{1, 2, 4, 8, 16, 32, 64},
		)
		batchCommitterWALFramesLogHist = telemetry.NewHistogramWithBuckets(
			"batch_committer_wal_frames_log",
			"Total WAL frames from PRAGMA",
			[]float64{100, 500, 1000, 2000, 4000, 8000, 16000},
		)
		batchCommitterWALFramesCheckpointed = telemetry.NewHistogramWithBuckets(
			"batch_committer_wal_frames_checkpointed",
			"Frames checkpointed from PRAGMA",
			[]float64{100, 500, 1000, 2000, 4000, 8000, 16000},
		)
		batchCommitterCheckpointBusyCounter = telemetry.NewCounterVec(
			"batch_committer_checkpoint_busy_total",
			"Checkpoint busy status (0=success, 1=busy)",
			[]string{"busy"},
		)
		batchCommitterCheckpointEfficiency = telemetry.NewHistogram(
			"batch_committer_checkpoint_efficiency",
			"Checkpoint efficiency (checkpointed/log frames)",
		)
	})
}

type pendingCommit struct {
	cdcEntries []*IntentEntry
	stmts      []protocol.Statement
	promise    *future.Promise[error]
	err        error
}

type SQLiteBatchCommitter struct {
	dbPath string
	db     *sql.DB

	mu      sync.Mutex
	pending map[uint64]*pendingCommit

	maxBatchSize int
	maxWaitTime  time.Duration

	flushCh chan struct{} // Signal immediate flush when batch full
	stopCh  chan struct{}
	stopped atomic.Bool
	wg      sync.WaitGroup

	// Checkpoint configuration
	checkpointEnabled         bool
	checkpointPassiveThreshMB float64
	checkpointRestartThreshMB float64
	allowDynamicBatchSize     bool

	// Checkpoint state
	checkpointRunning atomic.Bool
}

func NewSQLiteBatchCommitter(
	dbPath string,
	maxBatchSize int,
	maxWaitTime time.Duration,
	checkpointEnabled bool,
	passiveThreshMB float64,
	restartThreshMB float64,
	allowDynamicBatchSize bool,
) *SQLiteBatchCommitter {
	initBatchCommitterMetrics()
	return &SQLiteBatchCommitter{
		dbPath:                    dbPath,
		pending:                   make(map[uint64]*pendingCommit),
		maxBatchSize:              maxBatchSize,
		maxWaitTime:               maxWaitTime,
		flushCh:                   make(chan struct{}, 1),
		stopCh:                    make(chan struct{}),
		checkpointEnabled:         checkpointEnabled,
		checkpointPassiveThreshMB: passiveThreshMB,
		checkpointRestartThreshMB: restartThreshMB,
		allowDynamicBatchSize:     allowDynamicBatchSize,
	}
}

func (bc *SQLiteBatchCommitter) Start() error {
	db, err := bc.openOptimizedConnection()
	if err != nil {
		return fmt.Errorf("failed to open batch committer connection: %w", err)
	}
	bc.db = db

	bc.wg.Add(1)
	go bc.flushLoop()
	return nil
}

func (bc *SQLiteBatchCommitter) openOptimizedConnection() (*sql.DB, error) {
	// Build DSN with batch-optimized settings
	// WAL mode for compatibility with other connections
	// _txlock=immediate to acquire write lock at BEGIN
	dsn := bc.dbPath
	if !strings.Contains(dsn, ":memory:") {
		if strings.Contains(dsn, "?") {
			dsn += "&_journal_mode=WAL&_txlock=immediate"
		} else {
			dsn += "?_journal_mode=WAL&_txlock=immediate"
		}
	}

	db, err := sql.Open(SQLiteDriverName, dsn)
	if err != nil {
		return nil, err
	}

	// Single connection for batch writes
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Apply batch-optimized PRAGMAs
	// See: https://www.powersync.com/blog/sqlite-optimizations-for-ultra-high-performance
	// See: https://sqlite.org/wal.html
	pragmas := []string{
		"PRAGMA synchronous = OFF",             // Skip fsync per-commit (batch amortizes crash risk)
		"PRAGMA cache_size = -64000",           // 64MB page cache
		"PRAGMA temp_store = MEMORY",           // Temp tables in RAM
		"PRAGMA mmap_size = 268435456",         // 256MB memory-mapped I/O
		"PRAGMA journal_mode = WAL",            // WAL mode for concurrent reads
		"PRAGMA wal_autocheckpoint = 1000",     // ~4MB WAL before checkpoint (smaller = faster checkpoints)
		"PRAGMA journal_size_limit = 67108864", // 64MB max WAL size after checkpoint
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to set %s: %w", pragma, err)
		}
	}

	return db, nil
}

func (bc *SQLiteBatchCommitter) Stop() {
	if !bc.stopped.CompareAndSwap(false, true) {
		return
	}
	close(bc.stopCh)
	bc.wg.Wait()

	if bc.db != nil {
		bc.db.Close()
	}
}

func (bc *SQLiteBatchCommitter) Enqueue(txnID uint64, commitTS hlc.Timestamp, cdcEntries []*IntentEntry, stmts []protocol.Statement) *future.Future[error] {
	p := future.NewPromise[error]()

	bc.mu.Lock()
	bc.pending[txnID] = &pendingCommit{
		cdcEntries: cdcEntries,
		stmts:      stmts,
		promise:    p,
	}
	effectiveMaxBatchSize := bc.maxBatchSize
	if bc.allowDynamicBatchSize && bc.checkpointRunning.Load() {
		effectiveMaxBatchSize = bc.maxBatchSize * 2
	}
	shouldFlush := len(bc.pending) >= effectiveMaxBatchSize
	bc.mu.Unlock()

	// Signal immediate flush if batch is full
	if shouldFlush {
		select {
		case bc.flushCh <- struct{}{}:
		default: // Don't block if flush already pending
		}
	}

	return p.Future()
}

func (bc *SQLiteBatchCommitter) flushLoop() {
	defer bc.wg.Done()

	ticker := time.NewTicker(bc.maxWaitTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bc.tryFlush("timer")
		case <-bc.flushCh:
			bc.tryFlush("size")
		case <-bc.stopCh:
			bc.tryFlush("stop")
			return
		}
	}
}

func (bc *SQLiteBatchCommitter) tryFlush(trigger string) {
	// Skip timer-based flush during checkpoint
	if trigger == "timer" && bc.checkpointRunning.Load() {
		return
	}

	bc.mu.Lock()
	if len(bc.pending) == 0 {
		bc.mu.Unlock()
		return
	}
	batch := bc.pending
	bc.pending = make(map[uint64]*pendingCommit)
	bc.mu.Unlock()

	bc.flush(batch, trigger)
}

func (bc *SQLiteBatchCommitter) getSchemaCache(conn *sql.Conn) (*SchemaCache, error) {
	cache := NewSchemaCache()
	err := conn.Raw(func(driverConn interface{}) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}
		return cache.Reload(sqliteConn)
	})
	if err != nil {
		return nil, err
	}
	return cache, nil
}

func (bc *SQLiteBatchCommitter) flush(batch map[uint64]*pendingCommit, trigger string) {
	start := time.Now()
	batchSize := len(batch)

	// Record metrics at end
	defer func() {
		batchCommitterFlushCounter.With(trigger).Inc()
		batchCommitterBatchSizeHist.Observe(float64(batchSize))
		batchCommitterFlushDurHist.Observe(float64(time.Since(start).Milliseconds()))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), batchCommitTimeout)
	defer cancel()

	conn, err := bc.db.Conn(ctx)
	if err != nil {
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}

	// Load schema inside transaction (atomic with writes)
	schemaCache, err := bc.getSchemaCache(conn)
	if err != nil {
		tx.Rollback()
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}

	// Create schema adapter for the unified applier
	schemaAdapter := &schemaCacheAdapter{cache: schemaCache}

	// Apply all CDC entries
	for _, pc := range batch {
		for _, entry := range pc.cdcEntries {
			opType := OpType(entry.Operation)
			var err error
			switch opType {
			case OpTypeInsert, OpTypeReplace:
				err = ApplyCDCInsert(tx, entry.Table, entry.NewValues)
			case OpTypeUpdate:
				err = ApplyCDCUpdate(tx, schemaAdapter, entry.Table, entry.OldValues, entry.NewValues)
			case OpTypeDelete:
				err = ApplyCDCDelete(tx, schemaAdapter, entry.Table, entry.OldValues)
			default:
				err = fmt.Errorf("unsupported operation type for CDC: %v", opType)
			}
			if err != nil {
				pc.err = err
				break
			}
		}
	}

	// Commit (single fsync for entire batch)
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}

	// Adaptive checkpoint
	if bc.checkpointEnabled {
		walSizeMB := bc.checkWALSize()
		if walSizeMB >= bc.checkpointPassiveThreshMB {
			go bc.backgroundCheckpoint(walSizeMB)
		}
	}

	// Resolve all promises
	for _, pc := range batch {
		pc.promise.Set(nil, pc.err)
	}
}

// checkWALSize returns WAL file size in MB, or 0.0 if not exists/error.
// Fast syscall (~10μs), does not block on I/O.
func (bc *SQLiteBatchCommitter) checkWALSize() float64 {
	walPath := bc.dbPath

	// Handle DSN query strings
	if idx := strings.Index(walPath, "?"); idx != -1 {
		walPath = walPath[:idx]
	}

	// In-memory databases don't have WAL
	if strings.Contains(walPath, ":memory:") {
		return 0.0
	}

	walPath += "-wal"

	info, err := os.Stat(walPath)
	if err != nil {
		return 0.0
	}

	return float64(info.Size()) / (1024 * 1024)
}

// backgroundCheckpoint runs checkpoint in goroutine without blocking flush.
func (bc *SQLiteBatchCommitter) backgroundCheckpoint(walSizeMB float64) {
	bc.checkpointRunning.Store(true)
	defer bc.checkpointRunning.Store(false)

	start := time.Now()

	// Determine mode based on WAL size
	var mode string
	switch {
	case walSizeMB < bc.checkpointPassiveThreshMB:
		batchCommitterCheckpointCounter.With("skipped").Inc()
		return
	case walSizeMB < bc.checkpointRestartThreshMB:
		mode = "PASSIVE"
	default:
		mode = "RESTART"
	}

	// Execute PRAGMA checkpoint
	var busy, logFrames, checkpointedFrames int
	query := fmt.Sprintf("PRAGMA wal_checkpoint(%s)", mode)
	err := bc.db.QueryRow(query).Scan(&busy, &logFrames, &checkpointedFrames)

	duration := time.Since(start)

	// Record metrics
	batchCommitterCheckpointCounter.With(mode).Inc()
	batchCommitterCheckpointDurHist.Observe(float64(duration.Milliseconds()))
	batchCommitterWALSizeBeforeHist.Observe(walSizeMB)

	if err == nil {
		batchCommitterWALFramesLogHist.Observe(float64(logFrames))
		batchCommitterWALFramesCheckpointed.Observe(float64(checkpointedFrames))
		batchCommitterCheckpointBusyCounter.With(fmt.Sprintf("%d", busy)).Inc()

		if logFrames > 0 {
			efficiency := float64(checkpointedFrames) / float64(logFrames)
			batchCommitterCheckpointEfficiency.Observe(efficiency)
		}

		log.Debug().
			Str("mode", mode).
			Int("log_frames", logFrames).
			Int("checkpointed", checkpointedFrames).
			Int("busy", busy).
			Float64("wal_size_mb", walSizeMB).
			Int64("duration_ms", duration.Milliseconds()).
			Msg("Adaptive checkpoint completed")
	} else {
		log.Warn().
			Err(err).
			Str("mode", mode).
			Float64("wal_size_mb", walSizeMB).
			Msg("Background checkpoint failed")
	}
}
