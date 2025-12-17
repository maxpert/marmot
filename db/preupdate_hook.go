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

// CapturedRow stores raw row data captured during hookCallback.
// This is a raw copy of sqlite3.SQLitePreUpdateData - no processing.
type CapturedRow struct {
	Table     string        `msgpack:"t"`
	Op        int           `msgpack:"o"` // Raw sqlite3 op (18=INSERT, 23=UPDATE, 9=DELETE)
	OldRowID  int64         `msgpack:"or"`
	NewRowID  int64         `msgpack:"nr"`
	OldValues []interface{} `msgpack:"ov,omitempty"`
	NewValues []interface{} `msgpack:"nv,omitempty"`
}

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

// Rollback aborts the hookDB transaction, processes captured rows, and closes the connection.
// Flow:
// 1. Rollback SQLite transaction (releases SQLite write lock)
// 2. ProcessCapturedRows (converts raw captures to IntentEntries)
// 3. cleanup() releases row locks and closes connection
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

	if processErr := s.ProcessCapturedRows(); processErr != nil {
		log.Error().Err(processErr).Uint64("txn_id", s.txnID).Msg("Failed to process captured rows")
	}

	s.cleanup()
	return txErr
}

// ProcessCapturedRows iterates raw captured rows from Pebble, processes each one, and cleans up.
// Called AFTER transaction rollback when SQLite lock is released.
// All schema lookup, conflict detection, and lock acquisition happens here.
func (s *EphemeralHookSession) ProcessCapturedRows() error {
	cursor, err := s.metaStore.IterateCapturedRows(s.txnID)
	if err != nil {
		return fmt.Errorf("failed to iterate captured rows: %w", err)
	}
	defer cursor.Close()

	var intentSeq uint64
	for cursor.Next() {
		rawSeq, data := cursor.Row()

		var row CapturedRow
		if err := encoding.Unmarshal(data, &row); err != nil {
			return fmt.Errorf("failed to unmarshal captured row: %w", err)
		}

		intentSeq++
		if err := s.processSingleRow(&row, intentSeq); err != nil {
			s.conflictError = err
			return err
		}

		if err := s.metaStore.DeleteCapturedRow(s.txnID, rawSeq); err != nil {
			return fmt.Errorf("failed to delete captured row: %w", err)
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	return nil
}

// GetIntentEntries reads all intent entries for this session from the MetaStore
func (s *EphemeralHookSession) GetIntentEntries() ([]*IntentEntry, error) {
	return s.metaStore.GetIntentEntries(s.txnID)
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

// cleanup unregisters the hook and closes the connection
func (s *EphemeralHookSession) cleanup() {
	if s.conn == nil {
		return
	}

	if s.metaStore != nil && s.txnID != 0 {
		if err := s.metaStore.ReleaseCDCRowLocksByTxn(s.txnID); err != nil {
			log.Error().Err(err).Uint64("txn_id", s.txnID).Msg("Failed to release CDC row locks during cleanup")
		}
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

// hookCallback is called by SQLite before each row modification.
// BRAIN DEAD: raw copy of sqlite3.SQLitePreUpdateData to Pebble.
func (s *EphemeralHookSession) hookCallback(data sqlite3.SQLitePreUpdateData) {
	if strings.HasPrefix(data.TableName, "__marmot") {
		return
	}

	colCount := data.Count()
	row := CapturedRow{
		Table:    data.TableName,
		Op:       data.Op,
		OldRowID: data.OldRowID,
		NewRowID: data.NewRowID,
	}

	oldVals := make([]interface{}, colCount)
	if data.Old(oldVals...) == nil {
		row.OldValues = oldVals
	}
	newVals := make([]interface{}, colCount)
	if data.New(newVals...) == nil {
		row.NewValues = newVals
	}

	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.mu.Unlock()

	rowData, _ := encoding.Marshal(&row)
	s.metaStore.WriteCapturedRow(s.txnID, seq, rowData)
}

// =============================================================================
// Utility functions
// =============================================================================

// processSingleRow processes one captured row: DDL check, schema lookup, lock acquisition, IntentEntry write.
func (s *EphemeralHookSession) processSingleRow(row *CapturedRow, seq uint64) error {
	// Fail fast if table has DDL lock from another transaction
	if ddlTxn, err := s.metaStore.GetCDCTableDDLLock(row.Table); err == nil && ddlTxn != 0 && ddlTxn != s.txnID {
		return ErrCDCTableDDLInProgress{Table: row.Table, HeldByTxn: ddlTxn}
	}

	var opType uint8
	switch row.Op {
	case sqlite3.SQLITE_INSERT:
		opType = uint8(OpTypeInsert)
	case sqlite3.SQLITE_UPDATE:
		opType = uint8(OpTypeUpdate)
	case sqlite3.SQLITE_DELETE:
		opType = uint8(OpTypeDelete)
	default:
		return fmt.Errorf("unknown op type: %d", row.Op)
	}

	schema, err := s.schemaCache.GetSchemaFor(row.Table)
	if err != nil {
		return fmt.Errorf("schema not found for table %s: %w", row.Table, err)
	}

	var intentKey string
	switch row.Op {
	case sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE:
		pkValues := extractPKFromValues(schema, row.NewValues, row.NewRowID)
		intentKey = filter.SerializeIntentKey(row.Table, schema.PrimaryKeys, pkValues)
	case sqlite3.SQLITE_DELETE:
		pkValues := extractPKFromValues(schema, row.OldValues, row.OldRowID)
		intentKey = filter.SerializeIntentKey(row.Table, schema.PrimaryKeys, pkValues)
	}

	if err := s.metaStore.AcquireCDCRowLock(s.txnID, row.Table, intentKey); err != nil {
		return err
	}

	var oldVals, newVals map[string][]byte
	if row.OldValues != nil {
		oldVals = encodeValuesWithSchema(schema.Columns, row.OldValues)
	}
	if row.NewValues != nil {
		newVals = encodeValuesWithSchema(schema.Columns, row.NewValues)
	}

	return s.metaStore.WriteIntentEntry(s.txnID, seq, opType, row.Table, intentKey, oldVals, newVals)
}

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
