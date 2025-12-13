package db

import (
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
	ID         uint64
	NodeID     uint64
	StartTS    hlc.Timestamp
	CommitTS   hlc.Timestamp
	Status     TxnStatus
	Statements []protocol.Statement
	mu         sync.RWMutex
}

// MinAppliedTxnIDFunc returns the minimum last_applied_txn_id across all peers for a database
// Used for GC coordination to prevent deleting logs needed by lagging peers
type MinAppliedTxnIDFunc func(database string) (uint64, error)

// ClusterMinWatermarkFunc returns the minimum applied seq_num across all alive nodes
// Used for GC coordination via gossip protocol (no extra RPC calls needed)
type ClusterMinWatermarkFunc func() uint64

// TransactionManager manages distributed transactions
// All transaction state is stored in MetaStore (PebbleDB) - no in-memory caching
type TransactionManager struct {
	db                     *sql.DB   // User database for data operations
	metaStore              MetaStore // MetaStore for transaction metadata
	clock                  *hlc.Clock
	schemaCache            *SchemaCache // Schema cache for table metadata
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

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(db *sql.DB, metaStore MetaStore, clock *hlc.Clock, schemaCache *SchemaCache) *TransactionManager {
	// Import config values (with fallback to defaults if config not loaded)
	gcInterval := 30 * time.Second
	gcThreshold := 1 * time.Hour
	gcMinRetention := 1 * time.Hour
	gcMaxRetention := 4 * time.Hour
	heartbeatTimeout := 10 * time.Second

	// Try to use config values if available
	if cfg.Config != nil {
		heartbeatTimeout = time.Duration(cfg.Config.Transaction.HeartbeatTimeoutSeconds) * time.Second
		gcMinRetention = time.Duration(cfg.Config.Replication.GCMinRetentionHours) * time.Hour
		gcMaxRetention = time.Duration(cfg.Config.Replication.GCMaxRetentionHours) * time.Hour
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

	op := StatementTypeToOpType(stmt.Type)

	// Persist the intent directly to MetaStore (durable storage)
	err := tm.metaStore.WriteIntent(txn.ID, intentType, tableName, intentKey,
		op, stmt.SQL, dataSnapshot, txn.StartTS, txn.NodeID)

	if err != nil {
		// Check if this is a write-write conflict
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "PRIMARY KEY constraint failed") {
			// Query to get the conflicting transaction
			existingIntent, queryErr := tm.metaStore.GetIntent(tableName, intentKey)
			if queryErr == nil && existingIntent != nil {
				// If same transaction already has intent on this row, update it
				// This happens when DELETE then INSERT on same row in same txn
				if existingIntent.TxnID == txn.ID {
					// Delete and re-insert to update
					_ = tm.metaStore.DeleteIntent(tableName, intentKey, txn.ID)
					updateErr := tm.metaStore.WriteIntent(txn.ID, intentType, tableName, intentKey,
						op, stmt.SQL, dataSnapshot, txn.StartTS, txn.NodeID)
					if updateErr != nil {
						return fmt.Errorf("failed to update write intent: %w", updateErr)
					}
					return nil
				}

				// Write-write conflict detected - return error for client to retry
				return fmt.Errorf("write-write conflict: row %s:%s locked by transaction %d (current txn: %d)",
					tableName, intentKey, existingIntent.TxnID, txn.ID)
			}
		}
		return fmt.Errorf("failed to persist write intent: %w", err)
	}

	return nil
}

// CommitTransaction commits the transaction using 2PC
// Phase 1: Validate all write intents still held (fetched from MetaStore)
// Phase 2: Get commit timestamp, apply CDC data, mark as COMMITTED, cleanup intents
// All data is loaded from MetaStore - no in-memory caching
func (tm *TransactionManager) CommitTransaction(txn *Transaction) error {
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
func (tm *TransactionManager) validateIntents(txn *Transaction) ([]*WriteIntentRecord, error) {
	intents, err := tm.metaStore.GetIntentsByTxn(txn.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch write intents: %w", err)
	}

	for _, intent := range intents {
		valid, err := tm.metaStore.ValidateIntent(intent.TableName, intent.IntentKey, txn.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to validate intent: %w", err)
		}
		if !valid {
			return nil, fmt.Errorf("write intent lost for %s:%s - transaction aborted",
				intent.TableName, intent.IntentKey)
		}
	}

	return intents, nil
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

// applyDataChanges applies CDC entries or DDL statements from intents.
// CRITICAL: Also rebuilds txn.Statements from persistent storage for serialization
// in finalizeCommit. Without this, anti-entropy delta sync has no data to stream.
func (tm *TransactionManager) applyDataChanges(txn *Transaction, intents []*WriteIntentRecord) error {
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

	// Rebuild txn.Statements from CDC entries for persistence in TransactionRecord.
	// This is CRITICAL for anti-entropy: StreamChanges reads SerializedStatements to send
	// CDC data to lagging nodes. Without this, delta sync receives empty statements.
	txn.Statements = tm.rebuildStatementsFromCDC(cdcEntries, intents)
	log.Debug().
		Uint64("txn_id", txn.ID).
		Int("rebuilt_statements", len(txn.Statements)).
		Msg("applyDataChanges: rebuilt statements for TransactionRecord")

	// Apply DDL statements if no CDC entries
	if len(cdcEntries) == 0 && len(intents) > 0 {
		if err := tm.applyDDLIntents(txn.ID, intents); err != nil {
			return err
		}
	}

	return nil
}

// rebuildStatementsFromCDC reconstructs protocol.Statement slice from CDC entries
// and DDL intents. This data is serialized to TransactionRecord.SerializedStatements
// for anti-entropy streaming to lagging nodes.
func (tm *TransactionManager) rebuildStatementsFromCDC(cdcEntries []*IntentEntry, intents []*WriteIntentRecord) []protocol.Statement {
	statements := make([]protocol.Statement, 0, len(cdcEntries)+len(intents))

	log.Debug().
		Int("cdc_entries", len(cdcEntries)).
		Int("intents", len(intents)).
		Msg("rebuildStatementsFromCDC: starting rebuild")

	// Add DML statements from CDC entries
	for _, entry := range cdcEntries {
		stmt := protocol.Statement{
			TableName: entry.Table,
			IntentKey: entry.IntentKey,
			OldValues: entry.OldValues,
			NewValues: entry.NewValues,
			Type:      opCodeToStatementType(entry.Operation),
		}
		log.Debug().
			Str("table", entry.Table).
			Str("intent_key", entry.IntentKey).
			Int("old_values", len(entry.OldValues)).
			Int("new_values", len(entry.NewValues)).
			Int("stmt_type", int(stmt.Type)).
			Msg("rebuildStatementsFromCDC: added DML statement")
		statements = append(statements, stmt)
	}

	// Add DDL statements from intents (only if no CDC entries - DDL-only transactions)
	if len(cdcEntries) == 0 {
		// Filter and sort DDL intents by CreatedAt to preserve execution order
		ddlIntents := make([]*WriteIntentRecord, 0, len(intents))
		for _, intent := range intents {
			if intent.IntentType == IntentTypeDDL && intent.SQLStatement != "" {
				ddlIntents = append(ddlIntents, intent)
			}
		}

		// Sort by CreatedAt to ensure CREATE TABLE comes before CREATE INDEX, etc.
		sort.Slice(ddlIntents, func(i, j int) bool {
			return ddlIntents[i].CreatedAt < ddlIntents[j].CreatedAt
		})

		for _, intent := range ddlIntents {
			stmt := protocol.Statement{
				TableName: intent.TableName,
				SQL:       intent.SQLStatement,
				Type:      protocol.StatementDDL,
			}
			statements = append(statements, stmt)
		}
	}

	return statements
}

// applyCDCEntries applies CDC data entries to SQLite.
func (tm *TransactionManager) applyCDCEntries(txnID uint64, entries []*IntentEntry) error {
	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		stmt := protocol.Statement{
			TableName: entry.Table,
			IntentKey: entry.IntentKey,
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
// Only processes intents with IntentType == IntentTypeDDL.
// DML intents are handled via CDC entries in applyCDCEntries.
// CRITICAL: DDL statements must be executed in the order they were added to the transaction.
// For example, CREATE TABLE must execute before CREATE INDEX on that table.
func (tm *TransactionManager) applyDDLIntents(txnID uint64, intents []*WriteIntentRecord) error {
	// Filter DDL intents
	ddlIntents := make([]*WriteIntentRecord, 0, len(intents))
	for _, intent := range intents {
		if intent.IntentType == IntentTypeDDL && intent.SQLStatement != "" {
			ddlIntents = append(ddlIntents, intent)
		}
	}

	// Sort DDL intents by CreatedAt timestamp to preserve execution order
	// This ensures CREATE TABLE executes before CREATE INDEX, etc.
	sort.Slice(ddlIntents, func(i, j int) bool {
		return ddlIntents[i].CreatedAt < ddlIntents[j].CreatedAt
	})

	// Execute DDL statements in order
	for _, intent := range ddlIntents {
		if _, err := tm.db.Exec(intent.SQLStatement); err != nil {
			return fmt.Errorf("failed to execute DDL statement: %w", err)
		}

		log.Debug().Uint64("txn_id", txnID).Str("sql", intent.SQLStatement).Msg("DDL statement executed")
	}

	// Reload schema cache after DDL operations
	if len(ddlIntents) > 0 && tm.schemaCache != nil {
		if err := tm.reloadSchemaCache(); err != nil {
			log.Warn().Err(err).Uint64("txn_id", txnID).Msg("Failed to reload schema cache after DDL")
		}
	}

	return nil
}

// finalizeCommit marks the transaction as committed in MetaStore.
func (tm *TransactionManager) finalizeCommit(txn *Transaction) error {
	statementsJSON, err := encoding.Marshal(txn.Statements)
	if err != nil {
		return fmt.Errorf("failed to serialize statements: %w", err)
	}

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

	if err := tm.metaStore.CommitTransaction(txn.ID, txn.CommitTS, statementsJSON, dbName, tablesInvolved); err != nil {
		return fmt.Errorf("failed to mark transaction as committed: %w", err)
	}

	txn.Status = TxnStatusCommitted
	return nil
}

// cleanupAfterCommit performs synchronous cleanup to prevent goroutine explosion.
func (tm *TransactionManager) cleanupAfterCommit(txn *Transaction, intents []*WriteIntentRecord) {
	// Mark intents for cleanup first (fast path - allows immediate overwrite)
	if err := tm.metaStore.MarkIntentsForCleanup(txn.ID); err != nil {
		log.Warn().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to mark intents for cleanup")
	}

	// Delete intents and CDC entries
	if err := tm.metaStore.DeleteIntentsByTxn(txn.ID); err != nil {
		log.Warn().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to cleanup intents after commit")
	}
	if err := tm.metaStore.DeleteIntentEntries(txn.ID); err != nil {
		log.Warn().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to cleanup CDC entries after commit")
	}
}

// opCodeToStatementType converts OpType back to protocol.StatementType
func opCodeToStatementType(op uint8) protocol.StatementType {
	switch OpType(op) {
	case OpTypeInsert, OpTypeReplace:
		return protocol.StatementInsert
	case OpTypeUpdate:
		return protocol.StatementUpdate
	case OpTypeDelete:
		return protocol.StatementDelete
	default:
		return protocol.StatementInsert
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
	// PHASE 2: BACKGROUND - Skipped under load
	// ====================
	// Cleanup old transaction records.
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

	if oldTxnCount > 0 {
		log.Info().
			Int("old_txn_records", oldTxnCount).
			Msg("GC Phase 2: Cleaned up old data")
	}
}

// isMetaStoreBusy checks if MetaStore batch channel is under pressure
// Returns true if we should skip non-critical GC work
func (tm *TransactionManager) isMetaStoreBusy() bool {
	// Check if MetaStore supports load detection
	if loadChecker, ok := tm.metaStore.(interface{ IsBusy() bool }); ok {
		return loadChecker.IsBusy()
	}
	return false
}

// cleanupStaleTransactions aborts transactions that haven't had a heartbeat within the timeout
func (tm *TransactionManager) cleanupStaleTransactions() (int, error) {
	return tm.metaStore.CleanupStaleTransactions(tm.heartbeatTimeout)
}

// cleanupOldTransactionRecords removes old COMMITTED/ABORTED transaction records
// with GC safe point coordination to prevent deleting logs needed by lagging peers
// Uses belt-and-suspenders: both txn_id from anti-entropy and seq_num from gossip watermark
func (tm *TransactionManager) cleanupOldTransactionRecords() (int, error) {
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

// SerializeData helper for data snapshots
func SerializeData(data interface{}) ([]byte, error) {
	return encoding.Marshal(data)
}

// DeserializeData helper for data snapshots
func DeserializeData(data []byte, v interface{}) error {
	return encoding.Unmarshal(data, v)
}

// writeCDCData writes CDC row data directly to the base table
// This is the core CDC implementation - replicas receive row data, not SQL
func (tm *TransactionManager) writeCDCData(stmt protocol.Statement) error {
	switch stmt.Type {
	case protocol.StatementInsert, protocol.StatementReplace:
		return tm.writeCDCInsert(stmt.TableName, stmt.NewValues)
	case protocol.StatementUpdate:
		return tm.writeCDCUpdate(stmt.TableName, stmt.OldValues, stmt.NewValues)
	case protocol.StatementDelete:
		return tm.writeCDCDelete(stmt.TableName, stmt.IntentKey, stmt.OldValues)
	default:
		return fmt.Errorf("unsupported statement type for CDC: %v", stmt.Type)
	}
}

// writeCDCInsert performs INSERT OR REPLACE using CDC row data
func (tm *TransactionManager) writeCDCInsert(tableName string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to insert")
	}

	// Build INSERT OR REPLACE statement
	columns := make([]string, 0, len(newValues))
	placeholders := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues))

	for col := range newValues {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")

		var value interface{}
		if err := encoding.Unmarshal(newValues[col], &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := tm.db.Exec(sqlStmt, values...)
	return err
}

// writeCDCUpdate performs UPDATE using CDC row data
// Uses oldValues for WHERE clause (to find existing row) and newValues for SET clause
func (tm *TransactionManager) writeCDCUpdate(tableName string, oldValues, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to update")
	}

	if tm.schemaCache == nil {
		return fmt.Errorf("schema cache not initialized")
	}

	// Get table schema to build proper WHERE clause
	schema, err := tm.schemaCache.GetSchemaFor(tableName)
	if err != nil {
		return fmt.Errorf("failed to get schema for table %s: %w", tableName, err)
	}

	// Build UPDATE statement with SET clause using newValues
	setClauses := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues)+len(schema.PrimaryKeys))

	for col, valBytes := range newValues {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))

		var value interface{}
		if err := encoding.Unmarshal(valBytes, &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	// Build WHERE clause using primary key columns from oldValues
	// This is critical for PK changes: we need the OLD PK to find the row
	whereClauses := make([]string, 0, len(schema.PrimaryKeys))

	for _, pkCol := range schema.PrimaryKeys {
		// Use oldValues for WHERE clause if available, fallback to newValues
		pkBytes, ok := oldValues[pkCol]
		if !ok {
			// Fallback to newValues if oldValues doesn't have PK (shouldn't happen for UPDATEs)
			pkBytes, ok = newValues[pkCol]
			if !ok {
				return fmt.Errorf("primary key column %s not found in CDC data", pkCol)
			}
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

		var value interface{}
		if err := encoding.Unmarshal(pkBytes, &value); err != nil {
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
func (tm *TransactionManager) writeCDCDelete(tableName string, intentKey string, oldValues map[string][]byte) error {
	if len(oldValues) == 0 {
		return fmt.Errorf("empty old values for delete")
	}

	if tm.schemaCache == nil {
		return fmt.Errorf("schema cache not initialized")
	}

	// Get table schema to build proper WHERE clause
	schema, err := tm.schemaCache.GetSchemaFor(tableName)
	if err != nil {
		return fmt.Errorf("failed to get schema for table %s: %w", tableName, err)
	}

	// Build WHERE clause using primary key columns from oldValues
	// NOTE: Always extract PK values from oldValues, not from intentKey.
	// intentKey is in format "table:value" which includes the table prefix.
	whereClauses := make([]string, 0, len(schema.PrimaryKeys))
	values := make([]interface{}, 0, len(schema.PrimaryKeys))

	for _, pkCol := range schema.PrimaryKeys {
		pkValue, ok := oldValues[pkCol]
		if !ok {
			return fmt.Errorf("primary key column %s not found in CDC old values", pkCol)
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

		var value interface{}
		if err := encoding.Unmarshal(pkValue, &value); err != nil {
			return fmt.Errorf("failed to deserialize PK value for column %s: %w", pkCol, err)
		}
		values = append(values, value)
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s",
		tableName,
		strings.Join(whereClauses, " AND "))

	_, err = tm.db.Exec(sql, values...)
	return err
}
