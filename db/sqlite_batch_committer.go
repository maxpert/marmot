package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
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
	commitTS   hlc.Timestamp
	promise    *future.Promise[error]
	err        error
}

type sealCapturedRowsFunc func(txnID uint64) error

type SQLiteBatchCommitter struct {
	dbPath string
	db     *sql.DB

	mu      sync.Mutex
	pending map[uint64]*pendingCommit

	sealCapturedRows sealCapturedRowsFunc

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

	// Incremental vacuum configuration
	incrementalVacuumEnabled     bool
	incrementalVacuumPages       int
	incrementalVacuumTimeLimitMS int

	// Background task state
	checkpointRunning atomic.Bool
	vacuumRunning     atomic.Bool
	vacuumScheduled   atomic.Bool
	lastFlushNanos    atomic.Int64
	bgWg              sync.WaitGroup
}

func (bc *SQLiteBatchCommitter) SetSealCapturedRows(fn sealCapturedRowsFunc) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.sealCapturedRows = fn
}

func NewSQLiteBatchCommitter(
	dbPath string,
	maxBatchSize int,
	maxWaitTime time.Duration,
	checkpointEnabled bool,
	passiveThreshMB float64,
	restartThreshMB float64,
	allowDynamicBatchSize bool,
	incrementalVacuumEnabled bool,
	incrementalVacuumPages int,
	incrementalVacuumTimeLimitMS int,
) *SQLiteBatchCommitter {
	initBatchCommitterMetrics()
	return &SQLiteBatchCommitter{
		dbPath:                       dbPath,
		pending:                      make(map[uint64]*pendingCommit),
		maxBatchSize:                 maxBatchSize,
		maxWaitTime:                  maxWaitTime,
		flushCh:                      make(chan struct{}, 1),
		stopCh:                       make(chan struct{}),
		checkpointEnabled:            checkpointEnabled,
		checkpointPassiveThreshMB:    passiveThreshMB,
		checkpointRestartThreshMB:    restartThreshMB,
		allowDynamicBatchSize:        allowDynamicBatchSize,
		incrementalVacuumEnabled:     incrementalVacuumEnabled,
		incrementalVacuumPages:       incrementalVacuumPages,
		incrementalVacuumTimeLimitMS: incrementalVacuumTimeLimitMS,
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
	// cache=shared ensures writes are immediately visible to other connections
	dsn := bc.dbPath
	if !strings.Contains(dsn, ":memory:") {
		if strings.Contains(dsn, "?") {
			dsn += "&_journal_mode=WAL&_txlock=immediate&cache=shared"
		} else {
			dsn += "?_journal_mode=WAL&_txlock=immediate&cache=shared"
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
		"PRAGMA synchronous = NORMAL",          // Marmot CDC WAL is redo source; SQLite must remain crash-consistent.
		"PRAGMA cache_size = -64000",           // 64MB page cache
		"PRAGMA temp_store = MEMORY",           // Temp tables in RAM
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
	if err := ensureAppliedTxnTable(db); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (bc *SQLiteBatchCommitter) Stop() {
	if !bc.stopped.CompareAndSwap(false, true) {
		return
	}
	close(bc.stopCh)
	bc.wg.Wait()
	bc.bgWg.Wait()

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
		commitTS:   commitTS,
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
	sealCapturedRows := bc.sealCapturedRows
	bc.mu.Unlock()

	bc.flush(batch, trigger, sealCapturedRows)
}

// Flush synchronously flushes all pending DML transactions.
// Used as a barrier before DDL to ensure all DML commits first.
func (bc *SQLiteBatchCommitter) Flush() {
	bc.mu.Lock()
	if len(bc.pending) == 0 {
		bc.mu.Unlock()
		return
	}
	batch := bc.pending
	bc.pending = make(map[uint64]*pendingCommit)
	sealCapturedRows := bc.sealCapturedRows
	bc.mu.Unlock()

	bc.flush(batch, "ddl_barrier", sealCapturedRows)
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

func (bc *SQLiteBatchCommitter) flush(batch map[uint64]*pendingCommit, trigger string, sealCapturedRows sealCapturedRowsFunc) {
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

	txnIDs := make([]uint64, 0, len(batch))
	for txnID := range batch {
		txnIDs = append(txnIDs, txnID)
	}
	sort.Slice(txnIDs, func(i, j int) bool { return txnIDs[i] < txnIDs[j] })

	if sealCapturedRows != nil {
		var wg sync.WaitGroup
		for _, txnID := range txnIDs {
			wg.Add(1)
			go func(txnID uint64) {
				defer wg.Done()
				if err := sealCapturedRows(txnID); err != nil {
					batch[txnID].err = err
				}
			}(txnID)
		}
		wg.Wait()
	}

	hasReadyTxn := false
	for _, txnID := range txnIDs {
		if batch[txnID].err == nil {
			hasReadyTxn = true
			break
		}
	}
	if !hasReadyTxn {
		for _, pc := range batch {
			pc.promise.Set(nil, pc.err)
		}
		return
	}

	conn, err := bc.db.Conn(ctx)
	if err != nil {
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}
	defer conn.Close()

	// Load schema before taking the transaction lock.
	schemaCache, err := bc.getSchemaCache(conn)
	if err != nil {
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}

	// Create schema adapter for the unified applier
	schemaAdapter := &schemaCacheAdapter{cache: schemaCache}

	// Apply each logical transaction under a savepoint. A failed transaction
	// rolls back its own row changes without poisoning successful peers in the
	// same fsync batch.
	for _, txnID := range txnIDs {
		pc := batch[txnID]
		if pc.err != nil {
			continue
		}
		savepoint := fmt.Sprintf("marmot_batch_%d", txnID)
		if _, err := tx.Exec("SAVEPOINT " + savepoint); err != nil {
			pc.err = err
			continue
		}
		for _, entry := range pc.cdcEntries {
			if err := ApplyCDCEntry(tx, schemaAdapter, entry); err != nil {
				pc.err = err
				break
			}
		}
		if pc.err == nil {
			pc.err = MarkSQLiteTxnApplied(tx, txnID, pc.commitTS)
		}
		if pc.err != nil {
			_, _ = tx.Exec("ROLLBACK TO " + savepoint)
			_, _ = tx.Exec("RELEASE " + savepoint)
			continue
		}
		if _, err := tx.Exec("RELEASE " + savepoint); err != nil {
			pc.err = err
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
	bc.lastFlushNanos.Store(time.Now().UnixNano())

	// Adaptive checkpoint for larger WAL sizes
	if bc.checkpointEnabled {
		walSizeMB := bc.checkWALSize()
		if walSizeMB >= bc.checkpointPassiveThreshMB && bc.checkpointRunning.CompareAndSwap(false, true) {
			bc.bgWg.Add(1)
			go func() {
				defer bc.bgWg.Done()
				bc.backgroundCheckpoint(walSizeMB)
			}()
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
	if bc.stopped.Load() {
		bc.checkpointRunning.Store(false)
		return
	}

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

		bc.scheduleIncrementalVacuumIfIdle()
	} else {
		log.Warn().
			Err(err).
			Str("mode", mode).
			Float64("wal_size_mb", walSizeMB).
			Msg("Background checkpoint failed")
	}
}

func (bc *SQLiteBatchCommitter) scheduleIncrementalVacuumIfIdle() {
	if !bc.incrementalVacuumEnabled || bc.stopped.Load() || bc.vacuumRunning.Load() {
		return
	}
	if !bc.vacuumScheduled.CompareAndSwap(false, true) {
		return
	}
	bc.bgWg.Add(1)
	go func() {
		defer bc.bgWg.Done()
		defer bc.vacuumScheduled.Store(false)
		const idleDelay = 500 * time.Millisecond
		time.Sleep(idleDelay)
		if bc.stopped.Load() || !bc.hasBeenWriteIdleFor(idleDelay) {
			return
		}
		bc.backgroundIncrementalVacuum()
	}()
}

func (bc *SQLiteBatchCommitter) hasBeenWriteIdleFor(d time.Duration) bool {
	last := bc.lastFlushNanos.Load()
	if last == 0 {
		return true
	}
	return time.Since(time.Unix(0, last)) >= d
}

// backgroundIncrementalVacuum reclaims freelist pages with a time budget.
// Uses PRAGMA incremental_vacuum(N) to free N pages per iteration until
// time limit is reached or no more pages to free.
func (bc *SQLiteBatchCommitter) backgroundIncrementalVacuum() {
	if bc.stopped.Load() {
		return
	}

	if !bc.vacuumRunning.CompareAndSwap(false, true) {
		return // Already running
	}
	defer bc.vacuumRunning.Store(false)

	start := time.Now()
	timeLimit := time.Duration(bc.incrementalVacuumTimeLimitMS) * time.Millisecond
	pagesPerIteration := bc.incrementalVacuumPages
	totalPagesFreed := 0
	iterations := 0

	for time.Since(start) < timeLimit {
		// PRAGMA incremental_vacuum(N) returns nothing, but frees up to N pages
		// We check freelist_count before and after to see if work was done
		var freelistBefore int
		if err := bc.db.QueryRow("PRAGMA freelist_count").Scan(&freelistBefore); err != nil {
			log.Debug().Err(err).Msg("Failed to get freelist count")
			break
		}

		if freelistBefore == 0 {
			break // No pages to free
		}

		// Free up to N pages
		if _, err := bc.db.Exec(fmt.Sprintf("PRAGMA incremental_vacuum(%d)", pagesPerIteration)); err != nil {
			log.Debug().Err(err).Msg("Incremental vacuum failed")
			break
		}

		var freelistAfter int
		if err := bc.db.QueryRow("PRAGMA freelist_count").Scan(&freelistAfter); err != nil {
			break
		}

		pagesFreed := freelistBefore - freelistAfter
		if pagesFreed <= 0 {
			break // No progress, stop
		}

		totalPagesFreed += pagesFreed
		iterations++
	}

	if totalPagesFreed > 0 {
		log.Debug().
			Int("pages_freed", totalPagesFreed).
			Int("iterations", iterations).
			Int64("duration_ms", time.Since(start).Milliseconds()).
			Msg("Incremental vacuum completed")
	}
}
