package db

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// Transaction represents a distributed transaction
// Implements Percolator-style distributed transactions with write intents
// Note: Write intents are stored ONLY in MetaStore (not in memory) for durability
// and to ensure cleanup happens correctly even after crashes or partial failures.
type Transaction struct {
	ID                    uint64
	NodeID                uint64
	StartTS               hlc.Timestamp
	CommitTS              hlc.Timestamp
	Status                TxnStatus
	Statements            []protocol.Statement
	StatementFallback     bool   // Execute SQL statements directly when CDC hooks are unavailable
	RequiredSchemaVersion uint64 // Minimum schema version required for this transaction
	mu                    sync.RWMutex
}

// MinAppliedTxnIDFunc returns the minimum last_applied_txn_id across all peers for a database
// Used for GC coordination to prevent deleting logs needed by lagging peers
type MinAppliedTxnIDFunc func(database string) (uint64, error)

// ClusterMinWatermarkFunc returns the minimum applied seq_num across all alive nodes
// Used for GC coordination via gossip protocol (no extra RPC calls needed)
type ClusterMinWatermarkFunc func() uint64

// RefreshReplicationStatesFunc refreshes peer replication states before GC decisions
// This queries all alive peers and updates local state, ensuring fresh watermarks
type RefreshReplicationStatesFunc func(ctx context.Context) error

// TransactionManager manages distributed transactions
// All transaction state is stored in MetaStore (PebbleDB) - no in-memory caching
type TransactionManager struct {
	db                       *sql.DB   // User database for data operations
	metaStore                MetaStore // MetaStore for transaction metadata
	clock                    *hlc.Clock
	schemaCache              *SchemaCache // Schema cache for table metadata
	mu                       sync.RWMutex
	gcInterval               time.Duration
	gcThreshold              time.Duration
	gcMinRetention           time.Duration // Minimum retention for replication
	gcMaxRetention           time.Duration // Force GC after this duration
	heartbeatTimeout         time.Duration
	stopGC                   chan struct{}
	gcRunning                bool
	databaseName             string                       // Name of database this manager manages
	getMinAppliedTxnID       MinAppliedTxnIDFunc          // Callback for GC coordination
	getClusterMinWatermark   ClusterMinWatermarkFunc      // Callback for GC via gossip watermark
	refreshReplicationStates RefreshReplicationStatesFunc // Callback to refresh watermarks before GC
	batchCommitter           *SQLiteBatchCommitter        // SQLite write batcher (nil if disabled)
	notifier                 CDCNotifier                  // Injected, can be nil
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(db *sql.DB, metaStore MetaStore, clock *hlc.Clock, schemaCache *SchemaCache) *TransactionManager {
	// Import config values (with fallback to defaults if config not loaded)
	gcInterval := 60 * time.Second // Default: 60s (MUST be >= anti-entropy interval for fresh watermarks)
	gcThreshold := 1 * time.Hour
	gcMinRetention := 1 * time.Hour
	gcMaxRetention := 4 * time.Hour
	heartbeatTimeout := 10 * time.Second

	// Try to use config values if available
	if cfg.Config != nil {
		heartbeatTimeout = time.Duration(cfg.Config.Transaction.HeartbeatTimeoutSeconds) * time.Second
		gcMinRetention = time.Duration(cfg.Config.Replication.GCMinRetentionHours) * time.Hour
		gcMaxRetention = time.Duration(cfg.Config.Replication.GCMaxRetentionHours) * time.Hour
		gcInterval = time.Duration(cfg.Config.Replication.GCIntervalS) * time.Second
	}

	tm := &TransactionManager{
		db:               db,
		metaStore:        metaStore,
		clock:            clock,
		schemaCache:      schemaCache,
		gcInterval:       gcInterval,
		gcThreshold:      gcThreshold,
		gcMinRetention:   gcMinRetention,
		gcMaxRetention:   gcMaxRetention,
		heartbeatTimeout: heartbeatTimeout,
		stopGC:           make(chan struct{}),
		gcRunning:        false,
		databaseName:     "", // Set later via SetDatabaseName()
	}

	// Start background garbage collection
	tm.StartGarbageCollection()

	return tm
}

// SetDatabaseName sets the database name for this transaction manager
// Used for GC coordination across peers
func (tm *TransactionManager) SetDatabaseName(name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.databaseName = name
}

// SetMinAppliedTxnIDFunc sets the callback for querying minimum applied txn_id across peers
// Used for GC safe point calculation
func (tm *TransactionManager) SetMinAppliedTxnIDFunc(fn MinAppliedTxnIDFunc) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.getMinAppliedTxnID = fn
}

// SetClusterMinWatermarkFunc sets the callback for getting cluster minimum watermark
// This is obtained from the gossip protocol without extra RPC calls
func (tm *TransactionManager) SetClusterMinWatermarkFunc(fn ClusterMinWatermarkFunc) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.getClusterMinWatermark = fn
}

// SetRefreshReplicationStatesFunc sets the callback for refreshing peer replication states
// This is called before GC Phase 2 to ensure watermarks are fresh before deletion decisions
func (tm *TransactionManager) SetRefreshReplicationStatesFunc(fn RefreshReplicationStatesFunc) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.refreshReplicationStates = fn
}

// SetNotifier sets the CDC notifier for signaling after commits
func (tm *TransactionManager) SetNotifier(n CDCNotifier) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.notifier = n
}

// batchCommitEnabled returns true if batch committing is enabled and configured
func (tm *TransactionManager) batchCommitEnabled() bool {
	return tm.batchCommitter != nil
}

// SetBatchCommitter sets the batch committer (called by ReplicatedDatabase)
func (tm *TransactionManager) SetBatchCommitter(bc *SQLiteBatchCommitter) {
	tm.batchCommitter = bc
}

// BeginTransaction starts a new distributed transaction with auto-generated ID
func (tm *TransactionManager) BeginTransaction(nodeID uint64) (*Transaction, error) {
	startTS := tm.clock.Now()

	// Generate unique transaction ID using Percolator/TiDB pattern: (physical_ms << 18) | logical
	// This guarantees uniqueness by keeping physical and logical in separate bit ranges
	txnID := startTS.ToTxnID()

	return tm.BeginTransactionWithID(txnID, nodeID, startTS)
}

// BeginTransactionWithID starts a distributed transaction with a specific ID
// Used by coordinator replication to ensure consistent txn_id across cluster
// Transaction state is persisted to MetaStore only - no in-memory caching
func (tm *TransactionManager) BeginTransactionWithID(txnID, nodeID uint64, startTS hlc.Timestamp) (*Transaction, error) {
	// Persist transaction record to MetaStore
	if err := tm.metaStore.BeginTransaction(txnID, nodeID, startTS); err != nil {
		return nil, fmt.Errorf("failed to create transaction record: %w", err)
	}

	// Return a transient object for use during this request
	// Actual state is in MetaStore
	txn := &Transaction{
		ID:         txnID,
		NodeID:     nodeID,
		StartTS:    startTS,
		Status:     TxnStatusPending,
		Statements: make([]protocol.Statement, 0),
	}

	return txn, nil
}

// AddStatement adds a statement to the transaction buffer
func (tm *TransactionManager) AddStatement(txn *Transaction, stmt protocol.Statement) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending (status: %s)", txn.ID, txn.Status)
	}

	txn.Statements = append(txn.Statements, stmt)
	return nil
}

// WriteIntent creates a write intent for a row
// This is the CRITICAL part: write intents act as distributed locks
// Intents are stored ONLY in MetaStore (not in memory) for durability
func (tm *TransactionManager) WriteIntent(txn *Transaction, intentType IntentType, tableName, intentKey string,
	stmt protocol.Statement, dataSnapshot []byte) error {

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	// Only DML intents need OpType conversion - DDL uses SQL statement directly
	var op OpType
	if intentType == IntentTypeDML {
		op = StatementTypeToOpType(stmt.Type)
	} else {
		op = OpTypeInsert // Placeholder for non-DML intents (DDL uses SQLStatement field)
	}

	// Persist the intent directly to MetaStore (durable storage)
	err := tm.metaStore.WriteIntent(txn.ID, intentType, tableName, intentKey,
		op, stmt.SQL, dataSnapshot, txn.StartTS, txn.NodeID)

	if err != nil {
		return fmt.Errorf("failed to persist write intent: %w", err)
	}

	return nil
}

// CommitTransaction commits the transaction.
// DML: Get CDC entries → apply via batch committer → cleanup
// DDL: Flush pending DML → get intents → apply DDL → cleanup
func (tm *TransactionManager) CommitTransaction(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	// Get CDC entries - determines DML vs DDL path
	cdcEntries, err := tm.metaStore.GetIntentEntries(txn.ID)
	if err != nil {
		return fmt.Errorf("failed to load CDC entries: %w", err)
	}

	// Calculate commit timestamp
	txn.CommitTS = tm.calculateCommitTS(txn.StartTS)

	if len(cdcEntries) > 0 {
		// DML path: apply CDC entries
		if tm.batchCommitEnabled() {
			txn.Statements = tm.rebuildStatementsFromCDC(cdcEntries, nil)
			fut := tm.batchCommitter.Enqueue(txn.ID, txn.CommitTS, cdcEntries, txn.Statements)
			if _, err := fut.Get(); err != nil {
				return fmt.Errorf("batch commit failed: %w", err)
			}
		} else {
			if err := tm.applyCDCEntries(txn.ID, cdcEntries); err != nil {
				return err
			}
			txn.Statements = tm.rebuildStatementsFromCDC(cdcEntries, nil)
		}
	} else {
		// Statement/DDL path: flush pending DML first to ensure isolation
		if tm.batchCommitEnabled() {
			tm.batchCommitter.Flush()
		}

		intents, err := tm.metaStore.GetIntentsByTxn(txn.ID)
		if err != nil {
			return fmt.Errorf("failed to fetch write intents: %w", err)
		}

		// Non-hook fallback: when explicitly marked by replication/coordination
		// path, apply concrete DML SQL directly in commit phase.
		if txn.StatementFallback {
			if err := tm.applyStatementFallbackDML(txn.ID, txn.Statements); err != nil {
				return err
			}
		}

		if err := tm.applyNonDMLIntents(txn.ID, intents); err != nil {
			return err
		}
		// Write non-DML statements to CDC storage for streaming replication
		if err := tm.writeNonDMLToCDC(txn.ID, intents); err != nil {
			return err
		}
		txn.Statements = tm.rebuildStatementsFromCDC(nil, intents)
	}

	// Finalize commit in MetaStore
	if err := tm.finalizeCommit(txn); err != nil {
		return err
	}

	// Signal CDC subscribers that new data is available
	if tm.notifier != nil {
		tm.notifier.Signal(tm.databaseName, txn.ID)
	}

	// Cleanup
	tm.cleanupAfterCommit(txn)

	return nil
}

func isStatementFallbackDML(stmt protocol.Statement) bool {
	if len(stmt.OldValues) > 0 || len(stmt.NewValues) > 0 {
		return false
	}
	if stmt.SQL == "" {
		return false
	}
	switch stmt.Type {
	case protocol.StatementInsert, protocol.StatementUpdate, protocol.StatementDelete, protocol.StatementReplace:
		return true
	default:
		return false
	}
}

// applyStatementFallbackDML executes DML SQL directly when CDC hooks were not available.
func (tm *TransactionManager) applyStatementFallbackDML(txnID uint64, statements []protocol.Statement) error {
	if len(statements) == 0 {
		return nil
	}

	tx, err := tm.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin fallback DML transaction: %w", err)
	}
	defer tx.Rollback()

	applied := 0
	for _, stmt := range statements {
		if !isStatementFallbackDML(stmt) {
			continue
		}
		if len(stmt.ExtractedParams) > 0 {
			if _, err := tx.Exec(stmt.SQL, stmt.ExtractedParams...); err != nil {
				return fmt.Errorf("failed to execute fallback DML: %w", err)
			}
		} else {
			if _, err := tx.Exec(stmt.SQL); err != nil {
				return fmt.Errorf("failed to execute fallback DML: %w", err)
			}
		}
		applied++
	}

	if applied == 0 {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit fallback DML transaction: %w", err)
	}

	log.Debug().Uint64("txn_id", txnID).Int("statement_count", applied).Msg("Applied statement-based DML fallback")
	return nil
}

// calculateCommitTS determines the commit timestamp (must be > start_ts).
func (tm *TransactionManager) calculateCommitTS(startTS hlc.Timestamp) hlc.Timestamp {
	commitTS := tm.clock.Now()
	if hlc.Compare(commitTS, startTS) <= 0 {
		commitTS = tm.clock.Update(startTS)
		commitTS.Logical++
	}
	return commitTS
}

// rebuildStatementsFromCDC reconstructs protocol.Statement slice from CDC entries
// and non-DML intents for in-memory transaction tracking.
func (tm *TransactionManager) rebuildStatementsFromCDC(cdcEntries []*IntentEntry, intents []*WriteIntentRecord) []protocol.Statement {
	statements := make([]protocol.Statement, 0, len(cdcEntries)+len(intents))

	// Add DML statements from CDC entries
	for _, entry := range cdcEntries {
		stmt := protocol.Statement{
			TableName: entry.Table,
			IntentKey: entry.IntentKey,
			OldValues: entry.OldValues,
			NewValues: entry.NewValues,
			Type:      OpTypeToStatementType(OpType(entry.Operation)),
		}
		statements = append(statements, stmt)
	}

	// Add non-DML statements from intents (only if no CDC entries)
	if len(cdcEntries) == 0 {
		// Filter and sort non-DML intents by CreatedAt to preserve execution order.
		nonDMLIntents := make([]*WriteIntentRecord, 0, len(intents))
		for _, intent := range intents {
			if intent.IntentType == IntentTypeDDL && intent.SQLStatement != "" {
				nonDMLIntents = append(nonDMLIntents, intent)
			}
		}

		// Sort by CreatedAt to ensure deterministic execution order.
		sort.Slice(nonDMLIntents, func(i, j int) bool {
			return nonDMLIntents[i].CreatedAt < nonDMLIntents[j].CreatedAt
		})

		for _, intent := range nonDMLIntents {
			stmt := protocol.Statement{
				TableName: intent.TableName,
				SQL:       intent.SQLStatement,
				Type:      protocol.StatementDDL,
			}
			var loadSnap LoadDataSnapshot
			if err := DeserializeData(intent.DataSnapshot, &loadSnap); err == nil && loadSnap.Type == int(protocol.StatementLoadData) {
				stmt.Type = protocol.StatementLoadData
				stmt.LoadDataPayload = loadSnap.Data
			}
			statements = append(statements, stmt)
		}
	}

	return statements
}

// applyCDCEntries applies CDC data entries to SQLite within a single transaction.
func (tm *TransactionManager) applyCDCEntries(txnID uint64, entries []*IntentEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Begin SQLite transaction to batch all DML writes
	tx, err := tm.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin SQLite transaction: %w", err)
	}
	defer tx.Rollback()

	schemaAdapter := &schemaCacheAdapter{cache: tm.schemaCache}

	for _, entry := range entries {
		var err error
		switch OpType(entry.Operation) {
		case OpTypeInsert, OpTypeReplace:
			err = ApplyCDCInsert(tx, entry.Table, entry.NewValues)
		case OpTypeUpdate:
			err = ApplyCDCUpdate(tx, schemaAdapter, entry.Table, entry.OldValues, entry.NewValues)
		case OpTypeDelete:
			err = ApplyCDCDelete(tx, schemaAdapter, entry.Table, entry.OldValues)
		default:
			err = fmt.Errorf("unsupported operation type: %v", entry.Operation)
		}
		if err != nil {
			return fmt.Errorf("failed to write CDC data for %s: %w", entry.Table, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit CDC transaction: %w", err)
	}

	return nil
}

// applyNonDMLIntents executes DDL and LOAD DATA statements from write intents.
func (tm *TransactionManager) applyNonDMLIntents(txnID uint64, intents []*WriteIntentRecord) error {
	nonDMLIntents := make([]*WriteIntentRecord, 0, len(intents))
	for _, intent := range intents {
		if intent.IntentType == IntentTypeDDL && intent.SQLStatement != "" {
			nonDMLIntents = append(nonDMLIntents, intent)
		}
	}

	sort.Slice(nonDMLIntents, func(i, j int) bool {
		return nonDMLIntents[i].CreatedAt < nonDMLIntents[j].CreatedAt
	})

	hasDDL := false
	for _, intent := range nonDMLIntents {
		var loadSnap LoadDataSnapshot
		if err := DeserializeData(intent.DataSnapshot, &loadSnap); err == nil && loadSnap.Type == int(protocol.StatementLoadData) {
			if _, err := ApplyLoadData(tm.db, loadSnap.SQL, loadSnap.Data); err != nil {
				return fmt.Errorf("failed to execute LOAD DATA statement: %w", err)
			}
			log.Debug().Uint64("txn_id", txnID).Msg("LOAD DATA statement executed")
			continue
		}
		if _, err := tm.db.Exec(intent.SQLStatement); err != nil {
			return fmt.Errorf("failed to execute DDL statement: %w", err)
		}
		hasDDL = true

		log.Debug().Uint64("txn_id", txnID).Str("sql", intent.SQLStatement).Msg("DDL statement executed")
	}

	// Reload schema cache after DDL operations.
	if hasDDL && tm.schemaCache != nil {
		if err := tm.reloadSchemaCache(); err != nil {
			log.Warn().Err(err).Uint64("txn_id", txnID).Msg("Failed to reload schema cache after DDL")
		}
	}

	return nil
}

// writeNonDMLToCDC writes DDL/LOAD DATA statements to CDC storage for streaming replication.
func (tm *TransactionManager) writeNonDMLToCDC(txnID uint64, intents []*WriteIntentRecord) error {
	var seq uint64

	for _, intent := range intents {
		if intent.IntentType != IntentTypeDDL || intent.SQLStatement == "" {
			continue
		}

		var loadSnap LoadDataSnapshot
		isLoadData := DeserializeData(intent.DataSnapshot, &loadSnap) == nil && loadSnap.Type == int(protocol.StatementLoadData)

		row := &EncodedCapturedRow{
			Table: intent.TableName,
		}
		if isLoadData {
			row.Op = uint8(OpTypeLoadData)
			row.LoadSQL = loadSnap.SQL
			row.LoadData = loadSnap.Data
		} else {
			row.Op = uint8(OpTypeDDL)
			row.DDLSQL = intent.SQLStatement
		}

		data, err := EncodeRow(row)
		if err != nil {
			return fmt.Errorf("failed to encode non-DML row: %w", err)
		}

		if err := tm.metaStore.WriteCapturedRow(txnID, seq, data); err != nil {
			return fmt.Errorf("failed to write DDL to CDC: %w", err)
		}

		seq++
		if isLoadData {
			log.Debug().Uint64("txn_id", txnID).Msg("LOAD DATA written to CDC for streaming")
		} else {
			log.Debug().Uint64("txn_id", txnID).Str("ddl", intent.SQLStatement).Msg("DDL written to CDC for streaming")
		}
	}

	return nil
}

// finalizeCommit marks the transaction as committed in MetaStore.
func (tm *TransactionManager) finalizeCommit(txn *Transaction) error {
	// Count rows instead of serializing statements
	rowCount := uint32(len(txn.Statements))

	// Get database name from statement or fall back to transaction manager's database
	dbName := ""
	if len(txn.Statements) > 0 && txn.Statements[0].Database != "" {
		dbName = txn.Statements[0].Database
	}
	if dbName == "" {
		tm.mu.RLock()
		dbName = tm.databaseName
		tm.mu.RUnlock()
	}

	// Collect unique table names from statements
	tableSet := make(map[string]struct{})
	for _, stmt := range txn.Statements {
		if stmt.TableName != "" {
			tableSet[stmt.TableName] = struct{}{}
		}
	}
	tables := make([]string, 0, len(tableSet))
	for t := range tableSet {
		tables = append(tables, t)
	}
	tablesInvolved := strings.Join(tables, ",")

	// Pass empty statements and rowCount to CommitTransaction
	if err := tm.metaStore.CommitTransaction(txn.ID, txn.CommitTS, nil, dbName, tablesInvolved, txn.RequiredSchemaVersion, rowCount); err != nil {
		return fmt.Errorf("failed to mark transaction as committed: %w", err)
	}

	txn.Status = TxnStatusCommitted
	return nil
}

// cleanupAfterCommit performs synchronous cleanup to prevent goroutine explosion.
func (tm *TransactionManager) cleanupAfterCommit(txn *Transaction) {
	if err := tm.metaStore.CleanupAfterCommit(txn.ID); err != nil {
		log.Warn().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to cleanup after commit")
	}
}

// AbortTransaction aborts the transaction and cleans up write intents
func (tm *TransactionManager) AbortTransaction(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	// Abort the transaction in MetaStore (deletes record)
	if err := tm.metaStore.AbortTransaction(txn.ID); err != nil {
		return fmt.Errorf("failed to abort transaction: %w", err)
	}

	txn.Status = TxnStatusAborted

	// Clean up all write intents via MetaStore
	_ = tm.metaStore.DeleteIntentsByTxn(txn.ID)

	// Clean up CDC intent entries
	_ = tm.metaStore.DeleteIntentEntries(txn.ID)

	// Clean up raw captured rows (from hook callback)
	_ = tm.metaStore.DeleteCapturedRows(txn.ID)

	return nil
}

// GetTransaction retrieves a transaction by ID from MetaStore
// Returns nil if transaction doesn't exist or is not PENDING
func (tm *TransactionManager) GetTransaction(txnID uint64) *Transaction {
	rec, err := tm.metaStore.GetTransaction(txnID)
	if err != nil || rec == nil {
		return nil
	}

	// Only return PENDING transactions (COMMITTED/ABORTED are done)
	if rec.Status != TxnStatusPending {
		return nil
	}

	// Reconstruct Transaction from MetaStore record
	txn := &Transaction{
		ID:     rec.TxnID,
		NodeID: rec.NodeID,
		StartTS: hlc.Timestamp{
			WallTime: rec.StartTSWall,
			Logical:  rec.StartTSLogical,
			NodeID:   rec.NodeID,
		},
		Status:     rec.Status,
		Statements: make([]protocol.Statement, 0),
	}

	return txn
}

// Heartbeat updates the last_heartbeat timestamp for a transaction
// This keeps long-running transactions alive and prevents them from being garbage collected
func (tm *TransactionManager) Heartbeat(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("cannot heartbeat non-pending transaction %d (status: %s)", txn.ID, txn.Status)
	}

	return tm.metaStore.Heartbeat(txn.ID)
}

// StartGarbageCollection starts the background garbage collection goroutine
func (tm *TransactionManager) StartGarbageCollection() {
	tm.mu.Lock()
	if tm.gcRunning {
		tm.mu.Unlock()
		return
	}
	tm.gcRunning = true
	tm.stopGC = make(chan struct{}) // Fresh channel for this GC cycle
	tm.mu.Unlock()

	go tm.gcLoop()
}

// StopGarbageCollection stops the background garbage collection.
// Safe to call multiple times.
func (tm *TransactionManager) StopGarbageCollection() {
	tm.mu.Lock()
	if !tm.gcRunning {
		tm.mu.Unlock()
		return
	}
	tm.gcRunning = false
	tm.mu.Unlock()

	// Safe close - only close if not already closed
	select {
	case <-tm.stopGC:
		// Already closed
	default:
		close(tm.stopGC)
	}
}

// gcLoop runs the garbage collection loop
func (tm *TransactionManager) gcLoop() {
	ticker := time.NewTicker(tm.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.runGarbageCollection()
		case <-tm.stopGC:
			return
		}
	}
}

// runGarbageCollection performs garbage collection in two phases:
// Phase 1 (Critical): Cleanup stale transactions and orphaned intents - always runs
// Phase 2 (Background): Cleanup old transaction records - skipped under load
func (tm *TransactionManager) runGarbageCollection() {
	// ====================
	// PHASE 1: CRITICAL - Always runs
	// ====================
	// Cleanup stale transactions and orphaned intents.
	// This is critical for preventing intent leaks that block new transactions.
	staleCount, err := tm.cleanupStaleTransactions()
	if err != nil {
		log.Error().Err(err).Msg("GC Phase 1: Failed to cleanup stale transactions")
	}

	if staleCount > 0 {
		log.Info().Int("stale_txns", staleCount).Msg("GC Phase 1: Cleaned up stale transactions")
	}

	// ====================
	// PHASE 2: BACKGROUND
	// ====================
	// Clean up old committed/aborted transaction records
	oldTxnCount, err := tm.cleanupOldTransactionRecords()
	if err != nil {
		log.Error().Err(err).Msg("GC Phase 2: Failed to cleanup old transaction records")
	}

	if oldTxnCount > 0 {
		log.Info().
			Int("old_txn_records", oldTxnCount).
			Msg("GC Phase 2: Cleaned up old data")
	}
}

// cleanupStaleTransactions aborts transactions that haven't had a heartbeat within the timeout
func (tm *TransactionManager) cleanupStaleTransactions() (int, error) {
	return tm.metaStore.CleanupStaleTransactions(tm.heartbeatTimeout)
}

// cleanupOldTransactionRecords removes old COMMITTED/ABORTED transaction records
// with GC safe point coordination to prevent deleting logs needed by lagging peers
// Uses belt-and-suspenders: both txn_id from anti-entropy and seq_num from gossip watermark
func (tm *TransactionManager) cleanupOldTransactionRecords() (int, error) {
	// Get callbacks under lock
	tm.mu.RLock()
	refreshFn := tm.refreshReplicationStates
	getMinAppliedFn := tm.getMinAppliedTxnID
	getWatermarkFn := tm.getClusterMinWatermark
	dbName := tm.databaseName
	tm.mu.RUnlock()

	var minAppliedTxnID uint64 = 0
	var minAppliedSeqNum uint64 = 0

	// Skip replication tracking for system database (it's not replicated)
	if dbName != "" && dbName != SystemDatabaseName {
		// CRITICAL: Refresh peer replication states BEFORE getting watermarks
		// This ensures we have fresh data before making GC deletion decisions
		if refreshFn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := refreshFn(ctx); err != nil {
				// Log warning but continue with potentially stale data
				// The retention windows provide safety margin
				log.Warn().Err(err).Str("database", dbName).Msg("GC: Failed to refresh replication states, using cached watermarks")
			}
			cancel()
		}

		// Get min applied txn_id from anti-entropy (now refreshed)
		if getMinAppliedFn != nil {
			minTxnID, err := getMinAppliedFn(dbName)
			if err == nil {
				minAppliedTxnID = minTxnID
			} else {
				log.Warn().Err(err).Str("database", dbName).Msg("GC: Failed to get min applied txn_id")
			}
		}

		// Get cluster min watermark from gossip (if available)
		if getWatermarkFn != nil {
			minAppliedSeqNum = getWatermarkFn()
		}
	}

	count, err := tm.metaStore.CleanupOldTransactionRecords(tm.gcMinRetention, tm.gcMaxRetention, minAppliedTxnID, minAppliedSeqNum)
	if err != nil {
		return 0, err
	}

	if count > 0 {
		log.Info().
			Str("database", dbName).
			Int("deleted_records", count).
			Uint64("min_txn_id", minAppliedTxnID).
			Uint64("min_seq_num", minAppliedSeqNum).
			Msg("GC: Cleaned up old transaction records")
	}

	return count, nil
}

// SerializeData helper for data snapshots
func SerializeData(data interface{}) ([]byte, error) {
	return encoding.Marshal(data)
}

// DeserializeData helper for data snapshots
func DeserializeData(data []byte, v interface{}) error {
	return encoding.Unmarshal(data, v)
}

// schemaCacheAdapter adapts SchemaCache to CDCSchemaProvider interface
type schemaCacheAdapter struct {
	cache *SchemaCache
}

func (s *schemaCacheAdapter) GetPrimaryKeys(tableName string) ([]string, error) {
	schema, err := s.cache.GetSchemaFor(tableName)
	if err != nil {
		return nil, err
	}
	return schema.PrimaryKeys, nil
}

// unmarshalCDCValue deserializes a msgpack-encoded value and converts []byte to string.
// SQLite returns TEXT values as []byte from preupdate hooks, so we convert back for proper type affinity.
func unmarshalCDCValue(data []byte) (interface{}, error) {
	var value interface{}
	if err := encoding.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	// Convert []byte to string - SQLite returns TEXT as []byte from preupdate hooks
	if b, ok := value.([]byte); ok {
		return string(b), nil
	}
	return value, nil
}
