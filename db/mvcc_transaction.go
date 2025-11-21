package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// MVCCTransaction represents a transaction with MVCC support
// Implements Percolator-style distributed transactions
type MVCCTransaction struct {
	ID           uint64
	NodeID       uint64
	StartTS      hlc.Timestamp
	CommitTS     hlc.Timestamp
	Status       string
	Statements   []protocol.Statement
	WriteIntents map[string]*WriteIntent // table:rowkey -> intent
	mu           sync.RWMutex
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

// MVCCTransactionManager manages MVCC transactions
type MVCCTransactionManager struct {
	db               *sql.DB
	clock            *hlc.Clock
	activeTxns       map[uint64]*MVCCTransaction
	mu               sync.RWMutex
	gcInterval       time.Duration
	gcThreshold      time.Duration
	heartbeatTimeout time.Duration
	stopGC           chan struct{}
	gcRunning        bool
}

// NewMVCCTransactionManager creates a new transaction manager
func NewMVCCTransactionManager(db *sql.DB, clock *hlc.Clock) *MVCCTransactionManager {
	// Import config values (with fallback to defaults if config not loaded)
	gcInterval := 30 * time.Second
	gcThreshold := 1 * time.Hour
	heartbeatTimeout := 10 * time.Second

	// Try to use config values if available
	if cfg.Config != nil {
		gcInterval = time.Duration(cfg.Config.MVCC.GCIntervalSeconds) * time.Second
		gcThreshold = time.Duration(cfg.Config.MVCC.GCRetentionHours) * time.Hour
		heartbeatTimeout = time.Duration(cfg.Config.MVCC.HeartbeatTimeoutSeconds) * time.Second
	}

	tm := &MVCCTransactionManager{
		db:               db,
		clock:            clock,
		activeTxns:       make(map[uint64]*MVCCTransaction),
		gcInterval:       gcInterval,
		gcThreshold:      gcThreshold,
		heartbeatTimeout: heartbeatTimeout,
		stopGC:           make(chan struct{}),
		gcRunning:        false,
	}

	// Start background garbage collection
	tm.StartGarbageCollection()

	return tm
}

// BeginTransaction starts a new MVCC transaction
func (tm *MVCCTransactionManager) BeginTransaction(nodeID uint64) (*MVCCTransaction, error) {
	startTS := tm.clock.Now()

	// Generate unique transaction ID from HLC timestamp
	// Use wall time + logical counter to ensure uniqueness for concurrent transactions
	// Add logical counter to wall time (logical is small, typically 0-10)
	txnID := uint64(startTS.WallTime) + uint64(startTS.Logical)

	txn := &MVCCTransaction{
		ID:           txnID,
		NodeID:       nodeID,
		StartTS:      startTS,
		Status:       TxnStatusPending,
		Statements:   make([]protocol.Statement, 0),
		WriteIntents: make(map[string]*WriteIntent),
	}

	// Persist transaction record
	_, err := tm.db.Exec(`
		INSERT INTO __marmot__txn_records
		(txn_id, node_id, status, start_ts_wall, start_ts_logical, created_at, last_heartbeat)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, txn.ID, nodeID, TxnStatusPending, startTS.WallTime, startTS.Logical,
		time.Now().UnixNano(), time.Now().UnixNano())

	if err != nil {
		return nil, fmt.Errorf("failed to create transaction record: %w", err)
	}

	tm.mu.Lock()
	tm.activeTxns[txn.ID] = txn
	tm.mu.Unlock()

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
func (tm *MVCCTransactionManager) WriteIntent(txn *MVCCTransaction, tableName, rowKey string,
	stmt protocol.Statement, dataSnapshot []byte) error {

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	// Create the write intent
	// Note: We rely on the PRIMARY KEY constraint to atomically detect conflicts
	// No pre-check needed - the database enforces exclusivity
	intent := &WriteIntent{
		TableName:    tableName,
		RowKey:       rowKey,
		TxnID:        txn.ID,
		Timestamp:    txn.StartTS,
		Operation:    statementTypeToOperation(stmt.Type),
		SQLStatement: stmt.SQL,
		DataSnapshot: dataSnapshot,
	}

	// Persist the intent - use plain INSERT to detect PRIMARY KEY conflicts atomically
	_, err := tm.db.Exec(`
		INSERT INTO __marmot__write_intents
		(table_name, row_key, txn_id, ts_wall, ts_logical, node_id, operation, sql_statement, data_snapshot, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tableName, rowKey, txn.ID, txn.StartTS.WallTime, txn.StartTS.Logical, txn.NodeID,
		statementTypeToOperation(stmt.Type), stmt.SQL, dataSnapshot, time.Now().UnixNano())

	if err != nil {
		// Check if this is a PRIMARY KEY constraint violation (concurrent intent)
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "PRIMARY KEY constraint failed") {
			// Another transaction already has an intent on this row
			// Query to get the conflicting transaction ID
			var conflictTxnID uint64
			queryErr := tm.db.QueryRow(`
				SELECT txn_id FROM __marmot__write_intents
				WHERE table_name = ? AND row_key = ?
			`, tableName, rowKey).Scan(&conflictTxnID)

			if queryErr == nil {
				return fmt.Errorf("write-write conflict: row %s:%s locked by transaction %d (current txn: %d)",
					tableName, rowKey, conflictTxnID, txn.ID)
			}
		}
		return fmt.Errorf("failed to persist write intent: %w", err)
	}

	// Store in transaction's intent map
	key := tableName + ":" + rowKey
	txn.WriteIntents[key] = intent

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

// cleanupIntent removes a stale write intent
func (tm *MVCCTransactionManager) cleanupIntent(intent *WriteIntent) error {
	_, err := tm.db.Exec(`
		DELETE FROM __marmot__write_intents
		WHERE table_name = ? AND row_key = ? AND txn_id = ?
	`, intent.TableName, intent.RowKey, intent.TxnID)
	return err
}

// CommitTransaction commits the transaction using 2PC
// Phase 1: Validate all write intents still held
// Phase 2: Get commit timestamp, mark as COMMITTED, async cleanup
func (tm *MVCCTransactionManager) CommitTransaction(txn *MVCCTransaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	// Phase 1: Validate all write intents
	for _, intent := range txn.WriteIntents {
		valid, err := tm.validateIntent(intent, txn.ID)
		if err != nil {
			return fmt.Errorf("failed to validate intent: %w", err)
		}
		if !valid {
			// Intent was stolen/removed - abort
			return fmt.Errorf("write intent lost for %s:%s - transaction aborted",
				intent.TableName, intent.RowKey)
		}
	}

	// Phase 2: Get commit timestamp (must be > start_ts)
	commitTS := tm.clock.Now()
	if hlc.Compare(commitTS, txn.StartTS) <= 0 {
		// Clock hasn't advanced - force it
		commitTS = tm.clock.Update(txn.StartTS)
		commitTS.Logical++
	}

	txn.CommitTS = commitTS

	// Execute DDL/DCL/Admin statements directly
	// These are not handled by MVCC versioning
	// Execute ALL statements (DML and DDL) directly on the base table
	// This ensures the base table reflects the latest committed state
	for _, stmt := range txn.Statements {
		if _, err := tm.db.Exec(stmt.SQL); err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	// Mark transaction as COMMITTED in transaction record
	_, err := tm.db.Exec(`
		UPDATE __marmot__txn_records
		SET status = ?, commit_ts_wall = ?, commit_ts_logical = ?, committed_at = ?
		WHERE txn_id = ?
	`, TxnStatusCommitted, commitTS.WallTime, commitTS.Logical, time.Now().UnixNano(), txn.ID)

	if err != nil {
		return fmt.Errorf("failed to mark transaction as committed: %w", err)
	}

	txn.Status = TxnStatusCommitted

	// Transaction is now COMMITTED
	// Async: Convert write intents to MVCC versions and cleanup
	go tm.finalizeCommit(txn)

	return nil
}

// validateIntent checks if the intent is still held by this transaction
func (tm *MVCCTransactionManager) validateIntent(intent *WriteIntent, txnID uint64) (bool, error) {
	var currentTxnID uint64
	err := tm.db.QueryRow(`
		SELECT txn_id FROM __marmot__write_intents
		WHERE table_name = ? AND row_key = ?
	`, intent.TableName, intent.RowKey).Scan(&currentTxnID)

	if err == sql.ErrNoRows {
		return false, nil // Intent disappeared
	}

	if err != nil {
		return false, err
	}

	return currentTxnID == txnID, nil
}

// finalizeCommit converts write intents to MVCC versions (async)
func (tm *MVCCTransactionManager) finalizeCommit(txn *MVCCTransaction) {
	// Convert each write intent to MVCC version
	for _, intent := range txn.WriteIntents {
		_, err := tm.db.Exec(`
			INSERT INTO __marmot__mvcc_versions
			(table_name, row_key, ts_wall, ts_logical, node_id, txn_id, operation, data_snapshot, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, intent.TableName, intent.RowKey, txn.CommitTS.WallTime, txn.CommitTS.Logical,
			txn.NodeID, txn.ID, intent.Operation, intent.DataSnapshot, time.Now().UnixNano())

		if err != nil {
			// Log error but continue - this is async cleanup
			log.Error().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to create MVCC version")
		}

		// Remove the write intent
		tm.cleanupIntent(intent)
	}

	// Remove from active transactions
	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()
}

// AbortTransaction aborts the transaction and cleans up write intents
func (tm *MVCCTransactionManager) AbortTransaction(txn *MVCCTransaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	// Mark as aborted
	_, err := tm.db.Exec(`
		UPDATE __marmot__txn_records
		SET status = ?
		WHERE txn_id = ?
	`, TxnStatusAborted, txn.ID)

	if err != nil {
		return fmt.Errorf("failed to mark transaction as aborted: %w", err)
	}

	txn.Status = TxnStatusAborted

	// Clean up all write intents
	for _, intent := range txn.WriteIntents {
		tm.cleanupIntent(intent)
	}

	// Remove from active transactions
	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()

	return nil
}

// GetTransaction retrieves an active transaction by ID
func (tm *MVCCTransactionManager) GetTransaction(txnID uint64) *MVCCTransaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.activeTxns[txnID]
}

// UpdateTransactionID updates the transaction ID in the active transactions map
// This is used when a replica receives a transaction with a coordinator-assigned ID
func (tm *MVCCTransactionManager) UpdateTransactionID(oldID, newID uint64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if txn, ok := tm.activeTxns[oldID]; ok {
		delete(tm.activeTxns, oldID)
		txn.ID = newID
		tm.activeTxns[newID] = txn
	}
}

// Heartbeat updates the last_heartbeat timestamp for a transaction
// This keeps long-running transactions alive and prevents them from being garbage collected
func (tm *MVCCTransactionManager) Heartbeat(txn *MVCCTransaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("cannot heartbeat non-pending transaction %d (status: %s)", txn.ID, txn.Status)
	}

	_, err := tm.db.Exec(`
		UPDATE __marmot__txn_records
		SET last_heartbeat = ?
		WHERE txn_id = ?
	`, time.Now().UnixNano(), txn.ID)

	if err != nil {
		return fmt.Errorf("failed to update heartbeat: %w", err)
	}

	return nil
}

// StartGarbageCollection starts the background garbage collection goroutine
func (tm *MVCCTransactionManager) StartGarbageCollection() {
	tm.mu.Lock()
	if tm.gcRunning {
		tm.mu.Unlock()
		return
	}
	tm.gcRunning = true
	tm.mu.Unlock()

	go tm.gcLoop()
}

// StopGarbageCollection stops the background garbage collection
func (tm *MVCCTransactionManager) StopGarbageCollection() {
	tm.mu.Lock()
	if !tm.gcRunning {
		tm.mu.Unlock()
		return
	}
	tm.mu.Unlock()

	close(tm.stopGC)
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

// runGarbageCollection performs garbage collection
func (tm *MVCCTransactionManager) runGarbageCollection() {
	// 1. Clean up stale transactions (timed out)
	staleCount, err := tm.cleanupStaleTransactions()
	if err != nil {
		log.Error().Err(err).Msg("GC: Failed to cleanup stale transactions")
	}

	// 2. Clean up old committed/aborted transaction records
	oldTxnCount, err := tm.cleanupOldTransactionRecords()
	if err != nil {
		log.Error().Err(err).Msg("GC: Failed to cleanup old transaction records")
	}

	// 3. Clean up old MVCC versions (keep last N versions per row)
	keepVersions := 10 // Default
	if cfg.Config != nil {
		keepVersions = cfg.Config.MVCC.VersionRetentionCount
	}
	oldVersionCount, err := tm.cleanupOldMVCCVersions(keepVersions)
	if err != nil {
		log.Error().Err(err).Msg("GC: Failed to cleanup old MVCC versions")
	}

	if staleCount > 0 || oldTxnCount > 0 || oldVersionCount > 0 {
		log.Info().
			Int("stale_txns", staleCount).
			Int("old_txn_records", oldTxnCount).
			Int("old_mvcc_versions", oldVersionCount).
			Msg("GC: Cleaned up old data")
	}
}

// cleanupStaleTransactions aborts transactions that haven't had a heartbeat within the timeout
func (tm *MVCCTransactionManager) cleanupStaleTransactions() (int, error) {
	cutoff := time.Now().Add(-tm.heartbeatTimeout).UnixNano()

	// Find stale PENDING transactions
	rows, err := tm.db.Query(`
		SELECT txn_id
		FROM __marmot__txn_records
		WHERE status = ? AND last_heartbeat < ?
	`, TxnStatusPending, cutoff)

	if err != nil {
		return 0, err
	}
	defer rows.Close()

	staleTxnIDs := []uint64{}
	for rows.Next() {
		var txnID uint64
		if err := rows.Scan(&txnID); err != nil {
			continue
		}
		staleTxnIDs = append(staleTxnIDs, txnID)
	}

	// Abort each stale transaction
	for _, txnID := range staleTxnIDs {
		// Mark as aborted
		_, err := tm.db.Exec(`
			UPDATE __marmot__txn_records
			SET status = ?
			WHERE txn_id = ?
		`, TxnStatusAborted, txnID)

		if err != nil {
			continue
		}

		// Clean up write intents
		_, _ = tm.db.Exec(`
			DELETE FROM __marmot__write_intents
			WHERE txn_id = ?
		`, txnID)

		// Remove from active transactions
		tm.mu.Lock()
		delete(tm.activeTxns, txnID)
		tm.mu.Unlock()
	}

	return len(staleTxnIDs), nil
}

// cleanupOldTransactionRecords removes old COMMITTED/ABORTED transaction records
func (tm *MVCCTransactionManager) cleanupOldTransactionRecords() (int, error) {
	cutoff := time.Now().Add(-tm.gcThreshold).UnixNano()

	result, err := tm.db.Exec(`
		DELETE FROM __marmot__txn_records
		WHERE (status = ? OR status = ?) AND created_at < ?
	`, TxnStatusCommitted, TxnStatusAborted, cutoff)

	if err != nil {
		return 0, err
	}

	rowsAffected, _ := result.RowsAffected()
	return int(rowsAffected), nil
}

// cleanupOldMVCCVersions removes old MVCC versions, keeping the latest N versions per row
func (tm *MVCCTransactionManager) cleanupOldMVCCVersions(keepVersions int) (int, error) {
	// For each (table_name, row_key), keep only the latest N versions
	// This is a simplified implementation - production would use a more efficient approach

	result, err := tm.db.Exec(`
		DELETE FROM __marmot__mvcc_versions
		WHERE rowid NOT IN (
			SELECT rowid
			FROM __marmot__mvcc_versions AS v1
			WHERE (
				SELECT COUNT(*)
				FROM __marmot__mvcc_versions AS v2
				WHERE v2.table_name = v1.table_name
				  AND v2.row_key = v1.row_key
				  AND (v2.ts_wall > v1.ts_wall OR
				       (v2.ts_wall = v1.ts_wall AND v2.ts_logical > v1.ts_logical))
			) < ?
		)
	`, keepVersions)

	if err != nil {
		return 0, err
	}

	rowsAffected, _ := result.RowsAffected()
	return int(rowsAffected), nil
}

// SerializeData helper for data snapshots
func SerializeData(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

// DeserializeData helper for data snapshots
func DeserializeData(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
