//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/rs/zerolog/log"
)

// =============================================================================
// Types
// =============================================================================

// EphemeralHookSession represents a CDC capture session with its own dedicated connection.
// The connection is held open for the duration of the session and closed on Commit/Rollback.
// CDC entries are stored in the per-database MetaStore's __marmot__intent_entries table.
type EphemeralHookSession struct {
	conn         *sql.Conn    // Dedicated user DB connection (closed on end)
	tx           *sql.Tx      // Active transaction on user DB
	metaStore    MetaStore    // MetaStore for intent entry storage
	txnID        uint64       // Transaction ID for intent entries
	seq          uint64       // Sequence counter for entries
	schemaCache  *SchemaCache // Shared schema cache
	lastInsertId int64        // Last insert ID from most recent insert
	mu           sync.Mutex

	conflictError error // Set if conflict detected during hook
}

// IntentEntry represents a CDC entry stored in the system database
type IntentEntry struct {
	TxnID     uint64
	Seq       uint64
	Operation uint8
	Table     string
	IntentKey string
	OldValues map[string][]byte
	NewValues map[string][]byte
	CreatedAt int64
}

// =============================================================================
// Constructor
// =============================================================================

// StartEphemeralSession creates a new CDC capture session with a dedicated connection.
// The session owns the connection and will close it when done.
// CDC entries are written to the per-database MetaStore during hooks.
//
// IMPORTANT: SchemaCache must be pre-populated for all tables that will be accessed.
// If a table's schema is not in the cache, its changes will be silently skipped.
// Call ReplicatedDatabase.ReloadSchema() before starting the session to ensure schemas are loaded.
func StartEphemeralSession(ctx context.Context, userDB *sql.DB, metaStore MetaStore, schemaCache *SchemaCache, txnID uint64) (*EphemeralHookSession, error) {
	conn, err := userDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	// Ensure schema cache is fresh before CDC capture
	err = conn.Raw(func(driverConn interface{}) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}
		if err := schemaCache.Reload(sqliteConn); err != nil {
			return fmt.Errorf("failed to reload schemas: %w", err)
		}
		return nil
	})
	if err != nil {
		conn.Close()
		return nil, err
	}

	session := &EphemeralHookSession{
		conn:        conn,
		metaStore:   metaStore,
		txnID:       txnID,
		seq:         0,
		schemaCache: schemaCache,
	}

	err = conn.Raw(func(driverConn interface{}) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}
		sqliteConn.RegisterPreUpdateHook(session.hookCallback)
		return nil
	})
	if err != nil {
		conn.Close()
		return nil, err
	}

	return session, nil
}

// =============================================================================
// EphemeralHookSession methods
// =============================================================================

// BeginTx starts a transaction on the session's connection
func (s *EphemeralHookSession) BeginTx(ctx context.Context) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	s.tx = tx
	return nil
}

// ExecContext executes a statement within the session's transaction
func (s *EphemeralHookSession) ExecContext(ctx context.Context, query string, args ...interface{}) error {
	if s.tx == nil {
		return fmt.Errorf("no active transaction")
	}
	result, err := s.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	if conflictErr := s.GetConflictError(); conflictErr != nil {
		return conflictErr
	}

	if id, err := result.LastInsertId(); err == nil && id != 0 {
		s.lastInsertId = id
	}
	return nil
}

// Commit commits the transaction and closes the connection.
// Note: CDC intent entries are NOT deleted here. They persist in MetaStore until
// the distributed transaction completes (commit or abort). Cleanup happens in
// TransactionManager.cleanupAfterCommit() or cleanupAfterAbort().
func (s *EphemeralHookSession) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var txErr error
	if s.tx != nil {
		txErr = s.tx.Commit()
	}

	s.cleanup()
	return txErr
}

// Rollback aborts the hookDB transaction, processes captured rows, and releases resources.
// Flow:
// 1. Rollback SQLite transaction (releases SQLite write lock)
// 2. Release hookDB connection immediately (minimize connection hold time)
// 3. ProcessCapturedRows (converts raw captures to IntentEntries) - uses only Pebble
// 4. Release row locks
//
// CRITICAL: hookDB connection is released BEFORE ProcessCapturedRows to avoid blocking
// other transactions during Pebble operations (which can be slow on cold start).
//
// CDC intent entries persist in MetaStore until the distributed transaction completes.
// Cleanup happens in TransactionManager.cleanupAfterCommit() or cleanupAfterAbort().
func (s *EphemeralHookSession) Rollback() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var txErr error
	if s.tx != nil {
		txErr = s.tx.Rollback()
	}

	// Release hookDB connection ASAP - ProcessCapturedRows only needs Pebble
	s.releaseConnection()

	if processErr := s.ProcessCapturedRows(); processErr != nil {
		log.Error().Err(processErr).Uint64("txn_id", s.txnID).Msg("Failed to process captured rows")
	}

	s.releaseRowLocks()
	return txErr
}

// ProcessCapturedRows iterates encoded captured rows from Pebble and acquires locks.
// Called AFTER transaction rollback when SQLite lock is released.
// Encoding happens in hookCallback, so this only does lock acquisition and conflict detection.
func (s *EphemeralHookSession) ProcessCapturedRows() error {
	cursor, err := s.metaStore.IterateCapturedRows(s.txnID)
	if err != nil {
		return fmt.Errorf("failed to iterate captured rows: %w", err)
	}
	defer cursor.Close()

	for cursor.Next() {
		rawSeq, data := cursor.Row()

		row, err := DecodeRow(data)
		if err != nil {
			return fmt.Errorf("failed to decode captured row: %w", err)
		}

		// Check DDL lock conflict
		if ddlTxn, err := s.metaStore.GetCDCTableDDLLock(row.Table); err == nil && ddlTxn != 0 && ddlTxn != s.txnID {
			s.conflictError = ErrCDCTableDDLInProgress{Table: row.Table, HeldByTxn: ddlTxn}
			return s.conflictError
		}

		// Acquire row lock
		if err := s.metaStore.AcquireCDCRowLock(s.txnID, row.Table, row.IntentKey); err != nil {
			s.conflictError = err
			return err
		}

		// Note: Do NOT delete captured rows here. They are retained for:
		// 1. GetIntentEntries reads them for replication preparation
		// 2. StreamCommittedTransactions reads them for delta sync
		// 3. CleanupOldTransactionRecords deletes them at GC time
		_ = rawSeq // Silence unused warning
	}

	return cursor.Err()
}

// GetIntentEntries reads all intent entries for this session from captured rows
func (s *EphemeralHookSession) GetIntentEntries() ([]*IntentEntry, error) {
	cursor, err := s.metaStore.IterateCapturedRows(s.txnID)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var entries []*IntentEntry
	for cursor.Next() {
		seq, data := cursor.Row()
		row, err := DecodeRow(data)
		if err != nil {
			continue
		}
		entries = append(entries, &IntentEntry{
			TxnID:     s.txnID,
			Seq:       seq,
			Operation: row.Op,
			Table:     row.Table,
			IntentKey: row.IntentKey,
			OldValues: row.OldValues,
			NewValues: row.NewValues,
		})
	}
	return entries, cursor.Err()
}

// GetTxnID returns the transaction ID for this session
func (s *EphemeralHookSession) GetTxnID() uint64 {
	return s.txnID
}

// GetLastInsertId returns the last insert ID from the most recent insert
func (s *EphemeralHookSession) GetLastInsertId() int64 {
	return s.lastInsertId
}

// GetConflictError returns any conflict error that occurred during CDC capture.
// Caller should check this after executing statements to detect row conflicts.
func (s *EphemeralHookSession) GetConflictError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conflictError
}

// ClearConflictError clears the conflict error (for retry scenarios)
func (s *EphemeralHookSession) ClearConflictError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conflictError = nil
}

// releaseConnection unregisters the hook and closes the SQLite connection.
// This releases the hookDB connection back to the pool ASAP.
// Called before ProcessCapturedRows to minimize hookDB hold time.
func (s *EphemeralHookSession) releaseConnection() {
	if s.conn == nil {
		return
	}

	s.conn.Raw(func(driverConn interface{}) error {
		if sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn); ok {
			sqliteConn.RegisterPreUpdateHook(nil)
		}
		return nil
	})

	s.conn.Close()
	s.conn = nil
}

// releaseRowLocks releases CDC row locks after processing is complete.
// Called after ProcessCapturedRows - row locks are only needed during capture.
func (s *EphemeralHookSession) releaseRowLocks() {
	if s.metaStore != nil && s.txnID != 0 {
		if err := s.metaStore.ReleaseCDCRowLocksByTxn(s.txnID); err != nil {
			log.Error().Err(err).Uint64("txn_id", s.txnID).Msg("Failed to release CDC row locks")
		}
	}
}

// cleanup releases all resources. Safe to call multiple times.
func (s *EphemeralHookSession) cleanup() {
	s.releaseConnection()
	s.releaseRowLocks()
}

// hookCallback is called by SQLite before each row modification.
// Encodes row data immediately with schema instead of deferring to ProcessCapturedRows.
func (s *EphemeralHookSession) hookCallback(data sqlite3.SQLitePreUpdateData) {
	if strings.HasPrefix(data.TableName, "__marmot") {
		return
	}

	// Get schema - skip if not cached (DDL operations)
	schema, err := s.schemaCache.GetSchemaFor(data.TableName)
	if err != nil {
		return // Table not in schema cache, skip CDC
	}

	// Determine operation type
	var opType uint8
	switch data.Op {
	case sqlite3.SQLITE_INSERT:
		opType = uint8(OpTypeInsert)
	case sqlite3.SQLITE_UPDATE:
		opType = uint8(OpTypeUpdate)
	case sqlite3.SQLITE_DELETE:
		opType = uint8(OpTypeDelete)
	default:
		return // Unknown op
	}

	colCount := data.Count()

	// Extract old/new values
	var oldVals, newVals map[string][]byte
	var intentKey string

	if data.Op == sqlite3.SQLITE_DELETE || data.Op == sqlite3.SQLITE_UPDATE {
		rawOld := make([]interface{}, colCount)
		if data.Old(rawOld...) == nil {
			oldVals = encodeValuesWithSchema(schema.Columns, rawOld)
			if data.Op == sqlite3.SQLITE_DELETE {
				pkValues := extractPKFromValues(schema, rawOld, data.OldRowID)
				intentKey = filter.SerializeIntentKey(data.TableName, schema.PrimaryKeys, pkValues)
			}
		}
	}

	if data.Op == sqlite3.SQLITE_INSERT || data.Op == sqlite3.SQLITE_UPDATE {
		rawNew := make([]interface{}, colCount)
		if data.New(rawNew...) == nil {
			newVals = encodeValuesWithSchema(schema.Columns, rawNew)
			pkValues := extractPKFromValues(schema, rawNew, data.NewRowID)
			intentKey = filter.SerializeIntentKey(data.TableName, schema.PrimaryKeys, pkValues)
		}
	}

	row := &EncodedCapturedRow{
		Table:     data.TableName,
		Op:        opType,
		IntentKey: intentKey,
		OldValues: oldVals,
		NewValues: newVals,
	}

	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.mu.Unlock()

	rowData, _ := EncodeRow(row)
	s.metaStore.WriteCapturedRow(s.txnID, seq, rowData)
}

// =============================================================================
// Utility functions
// =============================================================================

// extractPKFromValues extracts PK values from raw values slice using schema indices.
func extractPKFromValues(schema *TableSchema, values []interface{}, rowID int64) map[string][]byte {
	pkValues := make(map[string][]byte)
	for i, idx := range schema.PKIndices {
		colName := schema.PrimaryKeys[i]
		if idx == -1 {
			pkValues[colName] = []byte(fmt.Sprintf("%d", rowID))
		} else if idx < len(values) && values[idx] != nil {
			pkValues[colName] = pkValueToBytes(values[idx])
		}
	}
	return pkValues
}

// encodeValuesWithSchema converts []interface{} to map[string][]byte using schema column names.
func encodeValuesWithSchema(columns []string, values []interface{}) map[string][]byte {
	result := make(map[string][]byte, len(columns))
	for i, col := range columns {
		if i < len(values) && values[i] != nil {
			if encoded := encodeValue(values[i]); encoded != nil {
				result[col] = encoded
			}
		}
	}
	return result
}

// encodeValue encodes a single value to msgpack bytes.
func encodeValue(v interface{}) []byte {
	if v == nil {
		return nil
	}
	data, err := encoding.Marshal(v)
	if err != nil {
		return nil
	}
	return data
}

// pkValueToBytes converts a primary key value to its string byte representation.
// Used for intent key generation where values must match AST-based path format.
func pkValueToBytes(v interface{}) []byte {
	switch val := v.(type) {
	case int64:
		return []byte(fmt.Sprintf("%d", val))
	case float64:
		if val == float64(int64(val)) {
			return []byte(fmt.Sprintf("%d", int64(val)))
		}
		return []byte(fmt.Sprintf("%v", val))
	case string:
		return []byte(val)
	case []byte:
		return val
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}
