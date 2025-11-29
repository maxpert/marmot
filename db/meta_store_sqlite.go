package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteMetaStore implements MetaStore using SQLite
type SQLiteMetaStore struct {
	writeDB *sql.DB
	readDB  *sql.DB
	path    string
}

// Ensure SQLiteMetaStore implements MetaStore
var _ MetaStore = (*SQLiteMetaStore)(nil)

// NewSQLiteMetaStore creates a new SQLite-backed MetaStore
func NewSQLiteMetaStore(path string, busyTimeoutMS int) (*SQLiteMetaStore, error) {
	isMemoryDB := strings.Contains(path, ":memory:")

	// Write connection (1 connection)
	writeDSN := path
	if !isMemoryDB {
		if strings.Contains(writeDSN, "?") {
			writeDSN += fmt.Sprintf("&_journal_mode=WAL&_busy_timeout=%d&_txlock=immediate", busyTimeoutMS)
		} else {
			writeDSN += fmt.Sprintf("?_journal_mode=WAL&_busy_timeout=%d&_txlock=immediate", busyTimeoutMS)
		}
	}

	writeDB, err := sql.Open("sqlite3", writeDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open meta write database: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0)

	// Read connection pool (4 connections)
	readDSN := path
	if !isMemoryDB {
		if strings.Contains(readDSN, "?") {
			readDSN += fmt.Sprintf("&_journal_mode=WAL&_busy_timeout=%d", busyTimeoutMS)
		} else {
			readDSN += fmt.Sprintf("?_journal_mode=WAL&_busy_timeout=%d", busyTimeoutMS)
		}
	}

	readDB, err := sql.Open("sqlite3", readDSN)
	if err != nil {
		writeDB.Close()
		return nil, fmt.Errorf("failed to open meta read database: %w", err)
	}
	readDB.SetMaxOpenConns(4)
	readDB.SetMaxIdleConns(4)
	readDB.SetConnMaxLifetime(0)

	// Configure both connections
	for _, db := range []*sql.DB{writeDB, readDB} {
		if !isMemoryDB {
			if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
				writeDB.Close()
				readDB.Close()
				return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
			}
			if _, err := db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
				writeDB.Close()
				readDB.Close()
				return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
			}
			if _, err := db.Exec("PRAGMA cache_size=-16000"); err != nil {
				writeDB.Close()
				readDB.Close()
				return nil, fmt.Errorf("failed to set cache size: %w", err)
			}
			if _, err := db.Exec("PRAGMA temp_store=MEMORY"); err != nil {
				writeDB.Close()
				readDB.Close()
				return nil, fmt.Errorf("failed to set temp store: %w", err)
			}
		}
	}

	// Initialize schema
	for _, schema := range MetaSchemas() {
		if _, err := writeDB.Exec(schema); err != nil {
			writeDB.Close()
			readDB.Close()
			return nil, fmt.Errorf("failed to create meta schema: %w", err)
		}
	}

	return &SQLiteMetaStore{
		writeDB: writeDB,
		readDB:  readDB,
		path:    path,
	}, nil
}

// Close closes both database connections
func (s *SQLiteMetaStore) Close() error {
	var writeErr, readErr error
	if s.writeDB != nil {
		writeErr = s.writeDB.Close()
	}
	if s.readDB != nil {
		readErr = s.readDB.Close()
	}
	if writeErr != nil {
		return writeErr
	}
	return readErr
}

// WriteDB returns the write database connection
func (s *SQLiteMetaStore) WriteDB() *sql.DB {
	return s.writeDB
}

// ReadDB returns the read database connection pool
func (s *SQLiteMetaStore) ReadDB() *sql.DB {
	return s.readDB
}

// BeginTransaction creates a new transaction record with PENDING status
func (s *SQLiteMetaStore) BeginTransaction(txnID, nodeID uint64, startTS hlc.Timestamp) error {
	_, err := s.writeDB.Exec(`
		INSERT OR REPLACE INTO __marmot__txn_records
		(txn_id, node_id, status, start_ts_wall, start_ts_logical, created_at, last_heartbeat)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, txnID, nodeID, MetaTxnStatusPending, startTS.WallTime, startTS.Logical,
		time.Now().UnixNano(), time.Now().UnixNano())
	return err
}

// CommitTransaction marks a transaction as COMMITTED
// The seq_num is assigned atomically from the node_sequences table
func (s *SQLiteMetaStore) CommitTransaction(txnID uint64, commitTS hlc.Timestamp, statements []byte, dbName string) error {
	// Get the node_id from the transaction record to determine which sequence to use
	var nodeID uint64
	err := s.readDB.QueryRow(`SELECT node_id FROM __marmot__txn_records WHERE txn_id = ?`, txnID).Scan(&nodeID)
	if err != nil {
		return fmt.Errorf("failed to get node_id for txn %d: %w", txnID, err)
	}

	// Get next sequence number for this node
	seqNum, err := s.GetNextSeqNum(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get next seq_num for node %d: %w", nodeID, err)
	}

	_, err = s.writeDB.Exec(`
		UPDATE __marmot__txn_records
		SET status = ?, commit_ts_wall = ?, commit_ts_logical = ?, committed_at = ?,
		    statements_json = ?, database_name = ?, seq_num = ?
		WHERE txn_id = ?
	`, MetaTxnStatusCommitted, commitTS.WallTime, commitTS.Logical, time.Now().UnixNano(),
		string(statements), dbName, seqNum, txnID)
	return err
}

// AbortTransaction deletes a transaction record (clean up on abort)
func (s *SQLiteMetaStore) AbortTransaction(txnID uint64) error {
	_, err := s.writeDB.Exec(`DELETE FROM __marmot__txn_records WHERE txn_id = ?`, txnID)
	return err
}

// GetTransaction retrieves a transaction record by ID
func (s *SQLiteMetaStore) GetTransaction(txnID uint64) (*TransactionRecord, error) {
	row := s.readDB.QueryRow(`
		SELECT txn_id, node_id, COALESCE(seq_num, 0), status, start_ts_wall, start_ts_logical,
		       COALESCE(commit_ts_wall, 0), COALESCE(commit_ts_logical, 0),
		       created_at, COALESCE(committed_at, 0), COALESCE(last_heartbeat, 0),
		       COALESCE(tables_involved, ''), COALESCE(statements_json, ''),
		       COALESCE(database_name, '')
		FROM __marmot__txn_records
		WHERE txn_id = ?
	`, txnID)

	rec := &TransactionRecord{}
	var stmtsJSON string
	err := row.Scan(&rec.TxnID, &rec.NodeID, &rec.SeqNum, &rec.Status, &rec.StartTSWall, &rec.StartTSLogical,
		&rec.CommitTSWall, &rec.CommitTSLogical, &rec.CreatedAt, &rec.CommittedAt,
		&rec.LastHeartbeat, &rec.TablesInvolved, &stmtsJSON, &rec.DatabaseName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec.StatementsJSON = []byte(stmtsJSON)
	return rec, nil
}

// GetPendingTransactions retrieves all PENDING transactions
func (s *SQLiteMetaStore) GetPendingTransactions() ([]*TransactionRecord, error) {
	rows, err := s.readDB.Query(`
		SELECT txn_id, node_id, COALESCE(seq_num, 0), status, start_ts_wall, start_ts_logical,
		       COALESCE(commit_ts_wall, 0), COALESCE(commit_ts_logical, 0),
		       created_at, COALESCE(committed_at, 0), COALESCE(last_heartbeat, 0),
		       COALESCE(tables_involved, ''), COALESCE(statements_json, ''),
		       COALESCE(database_name, '')
		FROM __marmot__txn_records
		WHERE status = ?
	`, MetaTxnStatusPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*TransactionRecord
	for rows.Next() {
		rec := &TransactionRecord{}
		var stmtsJSON string
		if err := rows.Scan(&rec.TxnID, &rec.NodeID, &rec.SeqNum, &rec.Status, &rec.StartTSWall, &rec.StartTSLogical,
			&rec.CommitTSWall, &rec.CommitTSLogical, &rec.CreatedAt, &rec.CommittedAt,
			&rec.LastHeartbeat, &rec.TablesInvolved, &stmtsJSON, &rec.DatabaseName); err != nil {
			return nil, err
		}
		rec.StatementsJSON = []byte(stmtsJSON)
		records = append(records, rec)
	}
	return records, rows.Err()
}

// Heartbeat updates the last_heartbeat timestamp
func (s *SQLiteMetaStore) Heartbeat(txnID uint64) error {
	_, err := s.writeDB.Exec(`
		UPDATE __marmot__txn_records SET last_heartbeat = ? WHERE txn_id = ?
	`, time.Now().UnixNano(), txnID)
	return err
}

// WriteIntent creates a write intent (distributed lock)
func (s *SQLiteMetaStore) WriteIntent(txnID uint64, tableName, rowKey, op, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error {
	_, err := s.writeDB.Exec(`
		INSERT INTO __marmot__write_intents
		(table_name, row_key, txn_id, ts_wall, ts_logical, node_id, operation, sql_statement, data_snapshot, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tableName, rowKey, txnID, ts.WallTime, ts.Logical, nodeID, op, sqlStmt, data, time.Now().UnixNano())

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "PRIMARY KEY constraint failed") {
			var conflictTxnID uint64
			// Use writeDB here to get latest data - readDB may have stale WAL snapshot
			queryErr := s.writeDB.QueryRow(`
				SELECT txn_id FROM __marmot__write_intents WHERE table_name = ? AND row_key = ?
			`, tableName, rowKey).Scan(&conflictTxnID)

			if queryErr == nil {
				if conflictTxnID == txnID {
					// Same transaction, update the intent
					_, updateErr := s.writeDB.Exec(`
						UPDATE __marmot__write_intents
						SET operation = ?, sql_statement = ?, data_snapshot = ?, created_at = ?
						WHERE table_name = ? AND row_key = ? AND txn_id = ?
					`, op, sqlStmt, data, time.Now().UnixNano(), tableName, rowKey, txnID)
					return updateErr
				}
				return fmt.Errorf("write-write conflict: row %s:%s locked by transaction %d (current txn: %d)",
					tableName, rowKey, conflictTxnID, txnID)
			}
		}
		return fmt.Errorf("failed to persist write intent: %w", err)
	}
	return nil
}

// ValidateIntent checks if the intent is still held by the expected transaction
// Uses writeDB to get latest data - intent queries must be consistent with writes
func (s *SQLiteMetaStore) ValidateIntent(tableName, rowKey string, expectedTxnID uint64) (bool, error) {
	var currentTxnID uint64
	err := s.writeDB.QueryRow(`
		SELECT txn_id FROM __marmot__write_intents WHERE table_name = ? AND row_key = ?
	`, tableName, rowKey).Scan(&currentTxnID)

	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return currentTxnID == expectedTxnID, nil
}

// DeleteIntent removes a specific write intent
func (s *SQLiteMetaStore) DeleteIntent(tableName, rowKey string, txnID uint64) error {
	_, err := s.writeDB.Exec(`
		DELETE FROM __marmot__write_intents WHERE table_name = ? AND row_key = ? AND txn_id = ?
	`, tableName, rowKey, txnID)
	return err
}

// DeleteIntentsByTxn removes all write intents for a transaction
func (s *SQLiteMetaStore) DeleteIntentsByTxn(txnID uint64) error {
	_, err := s.writeDB.Exec(`DELETE FROM __marmot__write_intents WHERE txn_id = ?`, txnID)
	return err
}

// GetIntentsByTxn retrieves all write intents for a transaction
// Uses writeDB to get latest data - intent queries must be consistent with writes
func (s *SQLiteMetaStore) GetIntentsByTxn(txnID uint64) ([]*WriteIntentRecord, error) {
	rows, err := s.writeDB.Query(`
		SELECT table_name, row_key, txn_id, ts_wall, ts_logical, node_id, operation, sql_statement, data_snapshot, created_at
		FROM __marmot__write_intents WHERE txn_id = ?
	`, txnID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var intents []*WriteIntentRecord
	for rows.Next() {
		intent := &WriteIntentRecord{}
		if err := rows.Scan(&intent.TableName, &intent.RowKey, &intent.TxnID, &intent.TSWall, &intent.TSLogical,
			&intent.NodeID, &intent.Operation, &intent.SQLStatement, &intent.DataSnapshot, &intent.CreatedAt); err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

// GetIntent retrieves a specific write intent
// Uses writeDB to get latest data - intent queries must be consistent with writes
func (s *SQLiteMetaStore) GetIntent(tableName, rowKey string) (*WriteIntentRecord, error) {
	row := s.writeDB.QueryRow(`
		SELECT table_name, row_key, txn_id, ts_wall, ts_logical, node_id, operation, sql_statement, data_snapshot, created_at
		FROM __marmot__write_intents WHERE table_name = ? AND row_key = ?
	`, tableName, rowKey)

	intent := &WriteIntentRecord{}
	err := row.Scan(&intent.TableName, &intent.RowKey, &intent.TxnID, &intent.TSWall, &intent.TSLogical,
		&intent.NodeID, &intent.Operation, &intent.SQLStatement, &intent.DataSnapshot, &intent.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return intent, nil
}

// CreateMVCCVersion creates an MVCC version record
func (s *SQLiteMetaStore) CreateMVCCVersion(tableName, rowKey string, ts hlc.Timestamp, nodeID, txnID uint64, op string, data []byte) error {
	_, err := s.writeDB.Exec(`
		INSERT INTO __marmot__mvcc_versions
		(table_name, row_key, ts_wall, ts_logical, node_id, txn_id, operation, data_snapshot, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, tableName, rowKey, ts.WallTime, ts.Logical, nodeID, txnID, op, data, time.Now().UnixNano())
	return err
}

// GetLatestVersion retrieves the latest MVCC version for a row
func (s *SQLiteMetaStore) GetLatestVersion(tableName, rowKey string) (*MVCCVersionRecord, error) {
	row := s.readDB.QueryRow(`
		SELECT table_name, row_key, ts_wall, ts_logical, node_id, txn_id, operation, data_snapshot, created_at
		FROM __marmot__mvcc_versions
		WHERE table_name = ? AND row_key = ?
		ORDER BY ts_wall DESC, ts_logical DESC, node_id DESC
		LIMIT 1
	`, tableName, rowKey)

	ver := &MVCCVersionRecord{}
	err := row.Scan(&ver.TableName, &ver.RowKey, &ver.TSWall, &ver.TSLogical, &ver.NodeID,
		&ver.TxnID, &ver.Operation, &ver.DataSnapshot, &ver.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ver, nil
}

// GetReplicationState retrieves replication state for a peer
func (s *SQLiteMetaStore) GetReplicationState(peerNodeID uint64, dbName string) (*ReplicationStateRecord, error) {
	row := s.readDB.QueryRow(`
		SELECT peer_node_id, database_name, last_applied_txn_id, last_applied_ts_wall, last_applied_ts_logical, last_sync_time, sync_status
		FROM __marmot__replication_state
		WHERE peer_node_id = ? AND database_name = ?
	`, peerNodeID, dbName)

	state := &ReplicationStateRecord{}
	err := row.Scan(&state.PeerNodeID, &state.DatabaseName, &state.LastAppliedTxnID,
		&state.LastAppliedTSWall, &state.LastAppliedTSLogical, &state.LastSyncTime, &state.SyncStatus)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return state, nil
}

// UpdateReplicationState updates replication state for a peer
func (s *SQLiteMetaStore) UpdateReplicationState(peerNodeID uint64, dbName string, lastTxnID uint64, lastTS hlc.Timestamp) error {
	_, err := s.writeDB.Exec(`
		INSERT OR REPLACE INTO __marmot__replication_state
		(peer_node_id, database_name, last_applied_txn_id, last_applied_ts_wall, last_applied_ts_logical, last_sync_time, sync_status)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, peerNodeID, dbName, lastTxnID, lastTS.WallTime, lastTS.Logical, time.Now().UnixNano(), MetaSyncStatusSynced)
	return err
}

// GetMinAppliedTxnID returns the minimum applied txn_id across all peers for a database
func (s *SQLiteMetaStore) GetMinAppliedTxnID(dbName string) (uint64, error) {
	var minTxnID uint64
	err := s.readDB.QueryRow(`
		SELECT COALESCE(MIN(last_applied_txn_id), 0)
		FROM __marmot__replication_state
		WHERE database_name = ?
	`, dbName).Scan(&minTxnID)
	return minTxnID, err
}

// GetSchemaVersion retrieves the schema version for a database
func (s *SQLiteMetaStore) GetSchemaVersion(dbName string) (int64, error) {
	var version int64
	err := s.readDB.QueryRow(`
		SELECT schema_version FROM __marmot__schema_versions WHERE database_name = ?
	`, dbName).Scan(&version)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return version, err
}

// UpdateSchemaVersion updates the schema version for a database
func (s *SQLiteMetaStore) UpdateSchemaVersion(dbName string, version int64, ddlSQL string, txnID uint64) error {
	_, err := s.writeDB.Exec(`
		INSERT OR REPLACE INTO __marmot__schema_versions
		(database_name, schema_version, last_ddl_sql, last_ddl_txn_id, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`, dbName, version, ddlSQL, txnID, time.Now().UnixNano())
	return err
}

// TryAcquireDDLLock attempts to acquire a DDL lock for a database
func (s *SQLiteMetaStore) TryAcquireDDLLock(dbName string, nodeID uint64, leaseDuration time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	expiresAt := now + leaseDuration.Nanoseconds()

	// Try to insert new lock or update expired lock
	result, err := s.writeDB.Exec(`
		INSERT INTO __marmot__ddl_locks (database_name, locked_by_node_id, locked_at, lease_expires_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(database_name) DO UPDATE SET
			locked_by_node_id = excluded.locked_by_node_id,
			locked_at = excluded.locked_at,
			lease_expires_at = excluded.lease_expires_at
		WHERE lease_expires_at < ?
	`, dbName, nodeID, now, expiresAt, now)
	if err != nil {
		return false, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// ReleaseDDLLock releases a DDL lock
func (s *SQLiteMetaStore) ReleaseDDLLock(dbName string, nodeID uint64) error {
	_, err := s.writeDB.Exec(`
		DELETE FROM __marmot__ddl_locks WHERE database_name = ? AND locked_by_node_id = ?
	`, dbName, nodeID)
	return err
}

// WriteIntentEntry writes a CDC intent entry
func (s *SQLiteMetaStore) WriteIntentEntry(txnID, seq uint64, op uint8, table, rowKey string, oldVals, newVals []byte) error {
	_, err := s.writeDB.Exec(`
		INSERT INTO __marmot__intent_entries
		(txn_id, seq, operation, table_name, row_key, old_values, new_values, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, txnID, seq, op, table, rowKey, oldVals, newVals, time.Now().UnixNano())
	return err
}

// GetIntentEntries retrieves CDC intent entries for a transaction
// Uses writeDB to get latest data - intent queries must be consistent with writes
func (s *SQLiteMetaStore) GetIntentEntries(txnID uint64) ([]*IntentEntry, error) {
	rows, err := s.writeDB.Query(`
		SELECT txn_id, seq, operation, table_name, row_key, old_values, new_values, created_at
		FROM __marmot__intent_entries
		WHERE txn_id = ?
		ORDER BY seq
	`, txnID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*IntentEntry
	for rows.Next() {
		entry := &IntentEntry{}
		var oldJSON, newJSON []byte
		if err := rows.Scan(&entry.TxnID, &entry.Seq, &entry.Operation, &entry.Table,
			&entry.RowKey, &oldJSON, &newJSON, &entry.CreatedAt); err != nil {
			return nil, err
		}
		if oldJSON != nil {
			msgpack.Unmarshal(oldJSON, &entry.OldValues)
		}
		if newJSON != nil {
			msgpack.Unmarshal(newJSON, &entry.NewValues)
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// DeleteIntentEntries deletes CDC intent entries for a transaction
func (s *SQLiteMetaStore) DeleteIntentEntries(txnID uint64) error {
	_, err := s.writeDB.Exec(`DELETE FROM __marmot__intent_entries WHERE txn_id = ?`, txnID)
	return err
}

// CleanupStaleTransactions aborts transactions that haven't had a heartbeat within the timeout
// Also cleans up orphaned intents (intents whose transaction record doesn't exist)
func (s *SQLiteMetaStore) CleanupStaleTransactions(timeout time.Duration) (int, error) {
	cutoff := time.Now().Add(-timeout).UnixNano()

	// Part 1: Clean up stale PENDING transactions with old heartbeats
	rows, err := s.readDB.Query(`
		SELECT txn_id FROM __marmot__txn_records
		WHERE status = ? AND last_heartbeat < ?
	`, MetaTxnStatusPending, cutoff)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var staleTxnIDs []uint64
	for rows.Next() {
		var txnID uint64
		if err := rows.Scan(&txnID); err != nil {
			continue
		}
		staleTxnIDs = append(staleTxnIDs, txnID)
	}

	for _, txnID := range staleTxnIDs {
		if _, err := s.writeDB.Exec(`DELETE FROM __marmot__txn_records WHERE txn_id = ?`, txnID); err != nil {
			continue
		}
		s.writeDB.Exec(`DELETE FROM __marmot__write_intents WHERE txn_id = ?`, txnID)
		s.writeDB.Exec(`DELETE FROM __marmot__intent_entries WHERE txn_id = ?`, txnID)
	}

	// Part 2: Clean up orphaned intents (intents with no corresponding transaction record)
	// These can occur when a coordinator crashes before committing/aborting
	orphanedResult, err := s.writeDB.Exec(`
		DELETE FROM __marmot__write_intents
		WHERE created_at < ? AND txn_id NOT IN (
			SELECT txn_id FROM __marmot__txn_records
		)
	`, cutoff)
	orphanedCount := int64(0)
	if err == nil {
		orphanedCount, _ = orphanedResult.RowsAffected()
	}

	// Also clean orphaned intent_entries
	s.writeDB.Exec(`
		DELETE FROM __marmot__intent_entries
		WHERE created_at < ? AND txn_id NOT IN (
			SELECT txn_id FROM __marmot__txn_records
		)
	`, cutoff)

	totalCleaned := len(staleTxnIDs) + int(orphanedCount)
	if totalCleaned > 0 {
		log.Info().
			Int("stale_txns", len(staleTxnIDs)).
			Int64("orphaned_intents", orphanedCount).
			Msg("MetaStore GC: Cleaned up stale transactions and orphaned intents")
	}
	return totalCleaned, nil
}

// CleanupOldTransactionRecords removes old COMMITTED/ABORTED transaction records
// Uses belt-and-suspenders approach:
// - minAppliedTxnID: Minimum txn_id applied by all peers (from anti-entropy queries)
// - minAppliedSeqNum: Minimum seq_num from cluster watermark gossip protocol
// Records are only deleted if they pass BOTH constraints (when both are provided)
func (s *SQLiteMetaStore) CleanupOldTransactionRecords(minRetention, maxRetention time.Duration, minAppliedTxnID, minAppliedSeqNum uint64) (int, error) {
	now := time.Now()
	minRetentionCutoff := now.Add(-minRetention).UnixNano()
	maxRetentionCutoff := now.Add(-maxRetention).UnixNano()

	var result sql.Result
	var err error

	// Build query based on which constraints are available
	// Priority: Use both when available, fall back to single constraint, then time-only
	if minAppliedTxnID > 0 && minAppliedSeqNum > 0 {
		// Both constraints: require both txn_id AND seq_num to pass (belt-and-suspenders)
		result, err = s.writeDB.Exec(`
			DELETE FROM __marmot__txn_records
			WHERE (status = ? OR status = ?)
			  AND (
			    (created_at < ? AND txn_id < ? AND (seq_num IS NULL OR seq_num < ?))
			    OR created_at < ?
			  )
		`, MetaTxnStatusCommitted, MetaTxnStatusAborted,
			minRetentionCutoff, minAppliedTxnID, minAppliedSeqNum,
			maxRetentionCutoff)
	} else if minAppliedTxnID > 0 {
		// Only txn_id constraint
		result, err = s.writeDB.Exec(`
			DELETE FROM __marmot__txn_records
			WHERE (status = ? OR status = ?)
			  AND (
			    (created_at < ? AND txn_id < ?)
			    OR created_at < ?
			  )
		`, MetaTxnStatusCommitted, MetaTxnStatusAborted,
			minRetentionCutoff, minAppliedTxnID,
			maxRetentionCutoff)
	} else if minAppliedSeqNum > 0 {
		// Only seq_num constraint (from cluster watermark)
		result, err = s.writeDB.Exec(`
			DELETE FROM __marmot__txn_records
			WHERE (status = ? OR status = ?)
			  AND (
			    (created_at < ? AND (seq_num IS NULL OR seq_num < ?))
			    OR created_at < ?
			  )
		`, MetaTxnStatusCommitted, MetaTxnStatusAborted,
			minRetentionCutoff, minAppliedSeqNum,
			maxRetentionCutoff)
	} else {
		// No coordination constraints - use max retention only
		result, err = s.writeDB.Exec(`
			DELETE FROM __marmot__txn_records
			WHERE (status = ? OR status = ?) AND created_at < ?
		`, MetaTxnStatusCommitted, MetaTxnStatusAborted, maxRetentionCutoff)
	}

	if err != nil {
		return 0, err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		log.Info().Int64("deleted_records", rowsAffected).Msg("MetaStore GC: Cleaned up old transaction records")
	}
	return int(rowsAffected), nil
}

// CleanupOldMVCCVersions removes old MVCC versions, keeping the latest N versions per row
func (s *SQLiteMetaStore) CleanupOldMVCCVersions(keepVersions int) (int, error) {
	result, err := s.writeDB.Exec(`
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
	if rowsAffected > 0 {
		log.Info().Int64("deleted_versions", rowsAffected).Msg("MetaStore GC: Cleaned up old MVCC versions")
	}
	return int(rowsAffected), nil
}

// GetNextSeqNum atomically increments and returns the next sequence number for a node
func (s *SQLiteMetaStore) GetNextSeqNum(nodeID uint64) (uint64, error) {
	var seq uint64
	err := s.writeDB.QueryRow(`
		INSERT INTO __marmot__node_sequences (node_id, last_seq_num)
		VALUES (?, 1)
		ON CONFLICT(node_id) DO UPDATE SET last_seq_num = last_seq_num + 1
		RETURNING last_seq_num
	`, nodeID).Scan(&seq)
	return seq, err
}

// GetMaxSeqNum returns the maximum sequence number across all committed transactions
func (s *SQLiteMetaStore) GetMaxSeqNum() (uint64, error) {
	var seq sql.NullInt64
	err := s.readDB.QueryRow(`
		SELECT MAX(seq_num) FROM __marmot__txn_records
		WHERE status = ? AND seq_num IS NOT NULL
	`, MetaTxnStatusCommitted).Scan(&seq)
	if err != nil {
		return 0, err
	}
	if !seq.Valid {
		return 0, nil
	}
	return uint64(seq.Int64), nil
}

// GetMinAppliedSeqNum returns the minimum applied sequence number across all peers for a database
// This is used as the watermark for GC - we can't delete transactions with seq_num >= this value
func (s *SQLiteMetaStore) GetMinAppliedSeqNum(dbName string) (uint64, error) {
	// For now, this queries the minimum last_applied_txn_id as a proxy for sequence
	// TODO: Update replication_state table to track seq_num directly
	var minSeq sql.NullInt64
	err := s.readDB.QueryRow(`
		SELECT MIN(last_applied_txn_id) FROM __marmot__replication_state
		WHERE database_name = ?
	`, dbName).Scan(&minSeq)
	if err != nil {
		return 0, err
	}
	if !minSeq.Valid {
		return 0, nil
	}
	return uint64(minSeq.Int64), nil
}
