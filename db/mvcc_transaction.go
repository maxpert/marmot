package db

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

// MVCCTransaction represents a transaction with MVCC support
// Implements Percolator-style distributed transactions
// Note: Write intents are stored ONLY in MetaStore (not in memory) for durability
// and to ensure cleanup happens correctly even after crashes or partial failures.
type MVCCTransaction struct {
	ID         uint64
	NodeID     uint64
	StartTS    hlc.Timestamp
	CommitTS   hlc.Timestamp
	Status     string
	Statements []protocol.Statement
	mu         sync.RWMutex
}

// WriteIntent represents a provisional write (intent)
// Acts as both a lock and a provisional value
type WriteIntent struct {
	TableName    string
	RowKey       string
	TxnID        uint64
	Timestamp    hlc.Timestamp
	Operation    string
	SQLStatement string
	DataSnapshot []byte
}

// MinAppliedTxnIDFunc returns the minimum last_applied_txn_id across all peers for a database
// Used for GC coordination to prevent deleting logs needed by lagging peers
type MinAppliedTxnIDFunc func(database string) (uint64, error)

// ClusterMinWatermarkFunc returns the minimum applied seq_num across all alive nodes
// Used for GC coordination via gossip protocol (no extra RPC calls needed)
type ClusterMinWatermarkFunc func() uint64

// MVCCTransactionManager manages MVCC transactions
// All transaction state is stored in MetaStore (BadgerDB) - no in-memory caching
type MVCCTransactionManager struct {
	db                     *sql.DB   // User database for data operations
	metaStore              MetaStore // MetaStore for transaction metadata
	clock                  *hlc.Clock
	schemaProvider         *protocol.SchemaProvider // Schema provider for table metadata
	mu                     sync.RWMutex
	gcInterval             time.Duration
	gcThreshold            time.Duration
	gcMinRetention         time.Duration // Minimum retention for replication
	gcMaxRetention         time.Duration // Force GC after this duration
	heartbeatTimeout       time.Duration
	stopGC                 chan struct{}
	gcRunning              bool
	databaseName           string                  // Name of database this manager manages
	getMinAppliedTxnID     MinAppliedTxnIDFunc     // Callback for GC coordination
	getClusterMinWatermark ClusterMinWatermarkFunc // Callback for GC via gossip watermark
}

// NewMVCCTransactionManager creates a new transaction manager
func NewMVCCTransactionManager(db *sql.DB, metaStore MetaStore, clock *hlc.Clock) *MVCCTransactionManager {
	// Import config values (with fallback to defaults if config not loaded)
	gcInterval := 30 * time.Second
	gcThreshold := 1 * time.Hour
	gcMinRetention := 1 * time.Hour
	gcMaxRetention := 4 * time.Hour
	heartbeatTimeout := 10 * time.Second

	// Try to use config values if available
	if cfg.Config != nil {
		gcInterval = time.Duration(cfg.Config.MVCC.GCIntervalSeconds) * time.Second
		gcThreshold = time.Duration(cfg.Config.MVCC.GCRetentionHours) * time.Hour
		heartbeatTimeout = time.Duration(cfg.Config.MVCC.HeartbeatTimeoutSeconds) * time.Second
		gcMinRetention = time.Duration(cfg.Config.Replication.GCMinRetentionHours) * time.Hour
		gcMaxRetention = time.Duration(cfg.Config.Replication.GCMaxRetentionHours) * time.Hour
	}

	tm := &MVCCTransactionManager{
		db:               db,
		metaStore:        metaStore,
		clock:            clock,
		schemaProvider:   protocol.NewSchemaProvider(db),
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
func (tm *MVCCTransactionManager) SetDatabaseName(name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.databaseName = name
}

// SetMinAppliedTxnIDFunc sets the callback for querying minimum applied txn_id across peers
// Used for GC safe point calculation
func (tm *MVCCTransactionManager) SetMinAppliedTxnIDFunc(fn MinAppliedTxnIDFunc) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.getMinAppliedTxnID = fn
}

// SetClusterMinWatermarkFunc sets the callback for getting cluster minimum watermark
// This is obtained from the gossip protocol without extra RPC calls
func (tm *MVCCTransactionManager) SetClusterMinWatermarkFunc(fn ClusterMinWatermarkFunc) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.getClusterMinWatermark = fn
}

// BeginTransaction starts a new MVCC transaction with auto-generated ID
func (tm *MVCCTransactionManager) BeginTransaction(nodeID uint64) (*MVCCTransaction, error) {
	startTS := tm.clock.Now()

	// Generate unique transaction ID using Percolator/TiDB pattern: (physical_ms << 18) | logical
	// This guarantees uniqueness by keeping physical and logical in separate bit ranges
	txnID := startTS.ToTxnID()

	return tm.BeginTransactionWithID(txnID, nodeID, startTS)
}

// BeginTransactionWithID starts an MVCC transaction with a specific ID
// Used by coordinator replication to ensure consistent txn_id across cluster
// Transaction state is persisted to MetaStore only - no in-memory caching
func (tm *MVCCTransactionManager) BeginTransactionWithID(txnID, nodeID uint64, startTS hlc.Timestamp) (*MVCCTransaction, error) {
	// Persist transaction record to MetaStore
	if err := tm.metaStore.BeginTransaction(txnID, nodeID, startTS); err != nil {
		return nil, fmt.Errorf("failed to create transaction record: %w", err)
	}

	// Return a transient object for use during this request
	// Actual state is in MetaStore
	txn := &MVCCTransaction{
		ID:         txnID,
		NodeID:     nodeID,
		StartTS:    startTS,
		Status:     TxnStatusPending,
		Statements: make([]protocol.Statement, 0),
	}

	return txn, nil
}

// AddStatement adds a statement to the transaction buffer
func (tm *MVCCTransactionManager) AddStatement(txn *MVCCTransaction, stmt protocol.Statement) error {
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
func (tm *MVCCTransactionManager) WriteIntent(txn *MVCCTransaction, tableName, rowKey string,
	stmt protocol.Statement, dataSnapshot []byte) error {

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	// Persist the intent directly to MetaStore (durable storage)
	err := tm.metaStore.WriteIntent(txn.ID, tableName, rowKey,
		statementTypeToOperation(stmt.Type), stmt.SQL, dataSnapshot, txn.StartTS, txn.NodeID)

	if err != nil {
		// Check if this is a write-write conflict
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "PRIMARY KEY constraint failed") {
			// Query to get the conflicting transaction
			existingIntent, queryErr := tm.metaStore.GetIntent(tableName, rowKey)
			if queryErr == nil && existingIntent != nil {
				// If same transaction already has intent on this row, update it
				// This happens when DELETE then INSERT on same row in same txn
				if existingIntent.TxnID == txn.ID {
					// Delete and re-insert to update
					tm.metaStore.DeleteIntent(tableName, rowKey, txn.ID)
					updateErr := tm.metaStore.WriteIntent(txn.ID, tableName, rowKey,
						statementTypeToOperation(stmt.Type), stmt.SQL, dataSnapshot, txn.StartTS, txn.NodeID)
					if updateErr != nil {
						return fmt.Errorf("failed to update write intent: %w", updateErr)
					}
					return nil
				}

				// Write-write conflict detected - return error for client to retry
				return fmt.Errorf("write-write conflict: row %s:%s locked by transaction %d (current txn: %d)",
					tableName, rowKey, existingIntent.TxnID, txn.ID)
			}
		}
		return fmt.Errorf("failed to persist write intent: %w", err)
	}

	return nil
}

// statementTypeToOperation converts protocol.StatementType to operation string
func statementTypeToOperation(st protocol.StatementType) string {
	switch st {
	case protocol.StatementInsert, protocol.StatementReplace:
		return OpInsert
	case protocol.StatementUpdate:
		return OpUpdate
	case protocol.StatementDelete:
		return OpDelete
	default:
		return "UNKNOWN"
	}
}

// CommitTransaction commits the transaction using 2PC
// Phase 1: Validate all write intents still held (fetched from MetaStore)
// Phase 2: Get commit timestamp, apply CDC data, mark as COMMITTED, cleanup intents
// All data is loaded from MetaStore - no in-memory caching
func (tm *MVCCTransactionManager) CommitTransaction(txn *MVCCTransaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	// Phase 1: Fetch and validate intents
	intents, err := tm.validateIntents(txn)
	if err != nil {
		return err
	}

	// Phase 2: Calculate commit timestamp
	txn.CommitTS = tm.calculateCommitTS(txn.StartTS)

	// Phase 3: Apply data changes (CDC entries or DDL)
	if err := tm.applyDataChanges(txn, intents); err != nil {
		return err
	}

	// Phase 4: Finalize commit in MetaStore
	if err := tm.finalizeCommit(txn); err != nil {
		return err
	}

	// Phase 5: Cleanup (non-critical, synchronous to prevent goroutine explosion)
	tm.cleanupAfterCommit(txn, intents)

	return nil
}

// validateIntents fetches and validates all write intents for the transaction.
func (tm *MVCCTransactionManager) validateIntents(txn *MVCCTransaction) ([]*WriteIntentRecord, error) {
	intents, err := tm.metaStore.GetIntentsByTxn(txn.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch write intents: %w", err)
	}

	for _, intent := range intents {
		valid, err := tm.metaStore.ValidateIntent(intent.TableName, intent.RowKey, txn.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to validate intent: %w", err)
		}
		if !valid {
			return nil, fmt.Errorf("write intent lost for %s:%s - transaction aborted",
				intent.TableName, intent.RowKey)
		}
	}

	return intents, nil
}

// calculateCommitTS determines the commit timestamp (must be > start_ts).
func (tm *MVCCTransactionManager) calculateCommitTS(startTS hlc.Timestamp) hlc.Timestamp {
	commitTS := tm.clock.Now()
	if hlc.Compare(commitTS, startTS) <= 0 {
		commitTS = tm.clock.Update(startTS)
		commitTS.Logical++
	}
	return commitTS
}

// applyDataChanges applies CDC entries or DDL statements from intents.
func (tm *MVCCTransactionManager) applyDataChanges(txn *MVCCTransaction, intents []*WriteIntentRecord) error {
	cdcEntries, err := tm.metaStore.GetIntentEntries(txn.ID)
	if err != nil {
		return fmt.Errorf("failed to load CDC entries: %w", err)
	}

	log.Debug().
		Uint64("txn_id", txn.ID).
		Int("cdc_entries", len(cdcEntries)).
		Int("intents", len(intents)).
		Msg("CommitTransaction: applying data changes")

	// Apply CDC entries (DML operations)
	if err := tm.applyCDCEntries(txn.ID, cdcEntries); err != nil {
		return err
	}

	// Apply DDL statements if no CDC entries
	if len(cdcEntries) == 0 && len(intents) > 0 {
		if err := tm.applyDDLIntents(txn.ID, intents); err != nil {
			return err
		}
	}

	return nil
}

// applyCDCEntries applies CDC data entries to SQLite.
func (tm *MVCCTransactionManager) applyCDCEntries(txnID uint64, entries []*IntentEntry) error {
	for _, entry := range entries {
		stmt := protocol.Statement{
			TableName: entry.Table,
			RowKey:    entry.RowKey,
			OldValues: entry.OldValues,
			NewValues: entry.NewValues,
			Type:      opCodeToStatementType(entry.Operation),
		}

		if err := tm.writeCDCData(stmt); err != nil {
			return fmt.Errorf("failed to write CDC data for %s: %w", entry.Table, err)
		}
	}
	return nil
}

// applyDDLIntents executes DDL statements from write intents.
func (tm *MVCCTransactionManager) applyDDLIntents(txnID uint64, intents []*WriteIntentRecord) error {
	for _, intent := range intents {
		// Skip database operations and intents without SQL
		if intent.TableName == TableDatabaseOperations || intent.SQLStatement == "" {
			continue
		}

		if _, err := tm.db.Exec(intent.SQLStatement); err != nil {
			return fmt.Errorf("failed to execute DDL statement: %w", err)
		}

		sqlPreview := intent.SQLStatement
		if len(sqlPreview) > 50 {
			sqlPreview = sqlPreview[:50]
		}
		log.Debug().Uint64("txn_id", txnID).Str("sql", sqlPreview).Msg("CommitTransaction: DDL SQL exec complete")
	}
	return nil
}

// finalizeCommit marks the transaction as committed in MetaStore.
func (tm *MVCCTransactionManager) finalizeCommit(txn *MVCCTransaction) error {
	statementsJSON, err := msgpack.Marshal(txn.Statements)
	if err != nil {
		return fmt.Errorf("failed to serialize statements: %w", err)
	}

	dbName := ""
	if len(txn.Statements) > 0 {
		dbName = txn.Statements[0].Database
	}

	if err := tm.metaStore.CommitTransaction(txn.ID, txn.CommitTS, statementsJSON, dbName); err != nil {
		return fmt.Errorf("failed to mark transaction as committed: %w", err)
	}

	txn.Status = TxnStatusCommitted
	return nil
}

// cleanupAfterCommit performs synchronous cleanup to prevent goroutine explosion.
func (tm *MVCCTransactionManager) cleanupAfterCommit(txn *MVCCTransaction, intents []*WriteIntentRecord) {
	// Mark intents for cleanup first (fast path - allows immediate overwrite)
	if err := tm.metaStore.MarkIntentsForCleanup(txn.ID); err != nil {
		log.Warn().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to mark intents for cleanup")
	}

	// Create MVCC versions (non-critical but keeps history consistent)
	tm.createMVCCVersions(txn, intents)

	// Delete intents and CDC entries
	if err := tm.metaStore.DeleteIntentsByTxn(txn.ID); err != nil {
		log.Warn().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to cleanup intents after commit")
	}
	if err := tm.metaStore.DeleteIntentEntries(txn.ID); err != nil {
		log.Warn().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to cleanup CDC entries after commit")
	}
}

// opCodeToStatementType converts operation code back to protocol.StatementType
func opCodeToStatementType(op uint8) protocol.StatementType {
	switch op {
	case OpInsertInt:
		return protocol.StatementInsert
	case OpUpdateInt:
		return protocol.StatementUpdate
	case OpDeleteInt:
		return protocol.StatementDelete
	default:
		return protocol.StatementInsert
	}
}

// createMVCCVersions creates MVCC version records for committed data (for read history)
// Non-critical but keeps history consistent for LWW conflict resolution
func (tm *MVCCTransactionManager) createMVCCVersions(txn *MVCCTransaction, intents []*WriteIntentRecord) {
	// Convert each write intent to MVCC version for read history
	for _, intent := range intents {
		err := tm.metaStore.CreateMVCCVersion(intent.TableName, intent.RowKey,
			txn.CommitTS, txn.NodeID, txn.ID, intent.Operation, intent.DataSnapshot)
		if err != nil {
			// Log error but continue - this is async and non-critical
			log.Error().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to create MVCC version")
		}
	}
}

// AbortTransaction aborts the transaction and cleans up write intents
func (tm *MVCCTransactionManager) AbortTransaction(txn *MVCCTransaction) error {
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
	tm.metaStore.DeleteIntentsByTxn(txn.ID)

	// Clean up CDC intent entries
	tm.metaStore.DeleteIntentEntries(txn.ID)

	return nil
}

// GetTransaction retrieves a transaction by ID from MetaStore
// Returns nil if transaction doesn't exist or is not PENDING
func (tm *MVCCTransactionManager) GetTransaction(txnID uint64) *MVCCTransaction {
	rec, err := tm.metaStore.GetTransaction(txnID)
	if err != nil || rec == nil {
		return nil
	}

	// Only return PENDING transactions (COMMITTED/ABORTED are done)
	if rec.Status != TxnStatusPending {
		return nil
	}

	// Reconstruct MVCCTransaction from MetaStore record
	txn := &MVCCTransaction{
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
func (tm *MVCCTransactionManager) Heartbeat(txn *MVCCTransaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("cannot heartbeat non-pending transaction %d (status: %s)", txn.ID, txn.Status)
	}

	return tm.metaStore.Heartbeat(txn.ID)
}

// StartGarbageCollection starts the background garbage collection goroutine
func (tm *MVCCTransactionManager) StartGarbageCollection() {
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
func (tm *MVCCTransactionManager) StopGarbageCollection() {
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
func (tm *MVCCTransactionManager) gcLoop() {
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
// Phase 2 (Background): Cleanup old records and MVCC versions - skipped under load
func (tm *MVCCTransactionManager) runGarbageCollection() {
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
	// PHASE 2: BACKGROUND - Skipped under load
	// ====================
	// Cleanup old transaction records and MVCC versions.
	// This is non-critical and can be deferred when system is busy.
	if tm.isMetaStoreBusy() {
		log.Debug().Msg("GC Phase 2: Skipping - MetaStore under load")
		return
	}

	// Clean up old committed/aborted transaction records
	oldTxnCount, err := tm.cleanupOldTransactionRecords()
	if err != nil {
		log.Error().Err(err).Msg("GC Phase 2: Failed to cleanup old transaction records")
	}

	// Clean up old MVCC versions (keep last N versions per row)
	keepVersions := 10 // Default
	if cfg.Config != nil {
		keepVersions = cfg.Config.MVCC.VersionRetentionCount
	}
	oldVersionCount, err := tm.cleanupOldMVCCVersions(keepVersions)
	if err != nil {
		log.Error().Err(err).Msg("GC Phase 2: Failed to cleanup old MVCC versions")
	}

	if oldTxnCount > 0 || oldVersionCount > 0 {
		log.Info().
			Int("old_txn_records", oldTxnCount).
			Int("old_mvcc_versions", oldVersionCount).
			Msg("GC Phase 2: Cleaned up old data")
	}
}

// isMetaStoreBusy checks if MetaStore batch channel is under pressure
// Returns true if we should skip non-critical GC work
func (tm *MVCCTransactionManager) isMetaStoreBusy() bool {
	// Check if MetaStore supports load detection
	if loadChecker, ok := tm.metaStore.(interface{ IsBusy() bool }); ok {
		return loadChecker.IsBusy()
	}
	return false
}

// cleanupStaleTransactions aborts transactions that haven't had a heartbeat within the timeout
func (tm *MVCCTransactionManager) cleanupStaleTransactions() (int, error) {
	return tm.metaStore.CleanupStaleTransactions(tm.heartbeatTimeout)
}

// cleanupOldTransactionRecords removes old COMMITTED/ABORTED transaction records
// with GC safe point coordination to prevent deleting logs needed by lagging peers
// Uses belt-and-suspenders: both txn_id from anti-entropy and seq_num from gossip watermark
func (tm *MVCCTransactionManager) cleanupOldTransactionRecords() (int, error) {
	// Get GC safe points from both mechanisms
	tm.mu.RLock()
	getMinAppliedFn := tm.getMinAppliedTxnID
	getWatermarkFn := tm.getClusterMinWatermark
	dbName := tm.databaseName
	tm.mu.RUnlock()

	var minAppliedTxnID uint64 = 0
	var minAppliedSeqNum uint64 = 0

	// Skip replication tracking for system database (it's not replicated)
	if dbName != "" && dbName != SystemDatabaseName {
		// Get min applied txn_id from anti-entropy (if available)
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

// cleanupOldMVCCVersions removes old MVCC versions, keeping the latest N versions per row
func (tm *MVCCTransactionManager) cleanupOldMVCCVersions(keepVersions int) (int, error) {
	return tm.metaStore.CleanupOldMVCCVersions(keepVersions)
}

// SerializeData helper for data snapshots
func SerializeData(data interface{}) ([]byte, error) {
	return msgpack.Marshal(data)
}

// DeserializeData helper for data snapshots
func DeserializeData(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// =======================
// LWW CONFLICT RESOLUTION
// =======================

// ApplyDeltaWithLWW applies a delta change using Last-Writer-Wins based on HLC timestamps
// This is used during partition healing to merge divergent writes
// Returns: applied (bool), error
func (tm *MVCCTransactionManager) ApplyDeltaWithLWW(tableName, rowKey string, sqlStmt string,
	incomingTS hlc.Timestamp, txnID uint64) (bool, error) {

	// Check the latest MVCC version for this row via MetaStore
	existing, err := tm.metaStore.GetLatestVersion(tableName, rowKey)
	if err != nil {
		return false, fmt.Errorf("failed to check existing version: %w", err)
	}

	if existing == nil {
		// No existing version - apply the change
		return tm.applyDeltaChange(tableName, rowKey, sqlStmt, incomingTS, txnID)
	}

	existingTS := hlc.Timestamp{
		WallTime: existing.TSWall,
		Logical:  existing.TSLogical,
		NodeID:   existing.NodeID,
	}

	// LWW: Compare timestamps
	cmp := hlc.Compare(incomingTS, existingTS)
	if cmp > 0 {
		// Incoming is newer - apply the change
		return tm.applyDeltaChange(tableName, rowKey, sqlStmt, incomingTS, txnID)
	}
	if cmp == 0 && incomingTS.NodeID > existingTS.NodeID {
		// Tie-breaker: higher node ID wins
		return tm.applyDeltaChange(tableName, rowKey, sqlStmt, incomingTS, txnID)
	}

	// Existing version is newer or same - skip
	log.Debug().
		Str("table", tableName).
		Str("row", rowKey).
		Int64("incoming_wall", incomingTS.WallTime).
		Int64("existing_wall", existing.TSWall).
		Msg("LWW: Skipping older delta change")
	return false, nil
}

// applyDeltaChange executes the SQL and records the MVCC version
func (tm *MVCCTransactionManager) applyDeltaChange(tableName, rowKey, sqlStmt string,
	ts hlc.Timestamp, txnID uint64) (bool, error) {

	// Execute the SQL statement on the user database
	_, err := tm.db.Exec(sqlStmt)
	if err != nil {
		// Log but don't fail - row might not exist for UPDATE/DELETE
		log.Debug().Err(err).Str("sql", sqlStmt).Msg("Delta change execution failed")
	}

	// Record MVCC version in MetaStore
	err = tm.metaStore.CreateMVCCVersion(tableName, rowKey, ts, ts.NodeID, txnID, "DELTA", nil)
	if err != nil {
		return false, fmt.Errorf("failed to record MVCC version: %w", err)
	}

	log.Debug().
		Str("table", tableName).
		Str("row", rowKey).
		Int64("ts_wall", ts.WallTime).
		Msg("LWW: Applied delta change")
	return true, nil
}

// GetLatestVersion returns the latest MVCC version timestamp for a row
func (tm *MVCCTransactionManager) GetLatestVersion(tableName, rowKey string) (*hlc.Timestamp, error) {
	rec, err := tm.metaStore.GetLatestVersion(tableName, rowKey)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}

	return &hlc.Timestamp{
		WallTime: rec.TSWall,
		Logical:  rec.TSLogical,
		NodeID:   rec.NodeID,
	}, nil
}

// writeCDCData writes CDC row data directly to the base table
// This is the core CDC implementation - replicas receive row data, not SQL
func (tm *MVCCTransactionManager) writeCDCData(stmt protocol.Statement) error {
	switch stmt.Type {
	case protocol.StatementInsert, protocol.StatementReplace:
		return tm.writeCDCInsert(stmt.TableName, stmt.NewValues)
	case protocol.StatementUpdate:
		return tm.writeCDCUpdate(stmt.TableName, stmt.RowKey, stmt.NewValues)
	case protocol.StatementDelete:
		return tm.writeCDCDelete(stmt.TableName, stmt.RowKey, stmt.OldValues)
	default:
		return fmt.Errorf("unsupported statement type for CDC: %v", stmt.Type)
	}
}

// writeCDCInsert performs INSERT OR REPLACE using CDC row data
func (tm *MVCCTransactionManager) writeCDCInsert(tableName string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to insert")
	}

	// Build INSERT OR REPLACE statement
	columns := make([]string, 0, len(newValues))
	placeholders := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues))

	// Collect columns (order doesn't matter for INSERT OR REPLACE with all columns)
	for col := range newValues {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")

		// Deserialize value from JSON
		var value interface{}
		if err := msgpack.Unmarshal(newValues[col], &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	sql := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := tm.db.Exec(sql, values...)
	return err
}

// writeCDCUpdate performs UPDATE using CDC row data
func (tm *MVCCTransactionManager) writeCDCUpdate(tableName string, rowKey string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to update")
	}

	// Get table schema to build proper WHERE clause
	schema, err := tm.schemaProvider.GetTableSchema(tableName)
	if err != nil {
		return fmt.Errorf("failed to get schema for table %s: %w", tableName, err)
	}

	// Build UPDATE statement with SET clause
	setClauses := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues)+len(schema.PrimaryKeys))

	for col, valBytes := range newValues {
		// Skip PK columns in SET clause (they're in WHERE clause)
		isPK := false
		for _, pkCol := range schema.PrimaryKeys {
			if col == pkCol {
				isPK = true
				break
			}
		}
		if isPK {
			continue
		}

		setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))

		// Deserialize value from JSON
		var value interface{}
		if err := msgpack.Unmarshal(valBytes, &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	// Build WHERE clause using primary key columns
	// For both single and composite PK: extract PK values from newValues
	// (newValues contains both SET columns and PK columns from WHERE clause)
	whereClauses := make([]string, 0, len(schema.PrimaryKeys))

	for _, pkCol := range schema.PrimaryKeys {
		pkValue, ok := newValues[pkCol]
		if !ok {
			return fmt.Errorf("primary key column %s not found in CDC data", pkCol)
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

		// Deserialize PK value
		var value interface{}
		if err := msgpack.Unmarshal(pkValue, &value); err != nil {
			return fmt.Errorf("failed to deserialize PK value for column %s: %w", pkCol, err)
		}
		values = append(values, value)
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		tableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))

	_, err = tm.db.Exec(sql, values...)
	return err
}

// writeCDCDelete performs DELETE using CDC row data
func (tm *MVCCTransactionManager) writeCDCDelete(tableName string, rowKey string, oldValues map[string][]byte) error {
	if rowKey == "" {
		return fmt.Errorf("empty row key for delete")
	}

	// Get table schema to build proper WHERE clause
	schema, err := tm.schemaProvider.GetTableSchema(tableName)
	if err != nil {
		return fmt.Errorf("failed to get schema for table %s: %w", tableName, err)
	}

	// Build WHERE clause using primary key columns
	whereClauses := make([]string, 0, len(schema.PrimaryKeys))
	values := make([]interface{}, 0, len(schema.PrimaryKeys))

	if len(schema.PrimaryKeys) == 1 {
		// Single PK: row key is the value directly
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", schema.PrimaryKeys[0]))
		values = append(values, rowKey)
	} else {
		// Composite PK: extract PK values from oldValues
		for _, pkCol := range schema.PrimaryKeys {
			pkValue, ok := oldValues[pkCol]
			if !ok {
				return fmt.Errorf("primary key column %s not found in CDC old values", pkCol)
			}

			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

			// Deserialize PK value
			var value interface{}
			if err := msgpack.Unmarshal(pkValue, &value); err != nil {
				return fmt.Errorf("failed to deserialize PK value for column %s: %w", pkCol, err)
			}
			values = append(values, value)
		}
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s",
		tableName,
		strings.Join(whereClauses, " AND "))

	_, err = tm.db.Exec(sql, values...)
	return err
}
