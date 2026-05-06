//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

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
	captureMu    sync.Mutex

	conflictError        error // Set if conflict detected during hook
	capturedRows         []capturedRow
	capturedBytes        int
	capturedRowsMax      int
	usePersistentCapture bool

	intentEntries    []*IntentEntry
	intentEntriesErr error
}

type capturedRow struct {
	seq  uint64
	data []byte
}

const defaultCapturedRowsBudget = 64 * 1024 * 1024

// IntentEntry represents a CDC entry stored in the system database
type IntentEntry struct {
	TxnID     uint64
	Seq       uint64
	Operation uint8
	Table     string
	IntentKey []byte
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
// SchemaCache is initialized on session start when empty and then read from cache
// during capture. If a table is not in cache, CDC for that row is skipped.
func StartEphemeralSession(ctx context.Context, userDB *sql.DB, metaStore MetaStore, schemaCache *SchemaCache, txnID uint64) (*EphemeralHookSession, error) {
	conn, err := userDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	session := &EphemeralHookSession{
		conn:            conn,
		metaStore:       metaStore,
		txnID:           txnID,
		seq:             0,
		schemaCache:     schemaCache,
		capturedRows:    make([]capturedRow, 0, 4),
		capturedRowsMax: defaultCapturedRowsBudget,
	}

	if schemaCache != nil && schemaCache.IsEmpty() {
		err = conn.Raw(func(driverConn interface{}) error {
			sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
			if !ok {
				return fmt.Errorf("unexpected driver connection type: %T", driverConn)
			}
			return schemaCache.Reload(sqliteConn)
		})
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to load schema cache: %w", err)
		}
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
	var txErr error
	if s.tx != nil {
		txErr = s.tx.Commit()
		s.tx = nil
	}
	s.mu.Unlock()

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
	// Keep lock scope small: avoid calling ProcessCapturedRows under mutex.
	// ProcessCapturedRows performs cursor scanning and lock checks, which can block.
	var txErr error
	s.mu.Lock()
	if s.tx != nil {
		txErr = s.tx.Rollback()
		s.tx = nil
	}
	s.mu.Unlock()

	// Release hookDB connection ASAP - ProcessCapturedRows only needs Pebble
	s.releaseConnection()

	if processErr := s.ProcessCapturedRows(); processErr != nil {
		log.Error().Err(processErr).Uint64("txn_id", s.txnID).Msg("Failed to process captured rows")
	}

	s.releaseRowLocks()
	return txErr
}

// ProcessCapturedRows iterates captured rows and acquires locks.
// Called AFTER transaction rollback when SQLite lock is released.
// Encoding happens in hookCallback, so this does lock acquisition and conflict detection.
func (s *EphemeralHookSession) ProcessCapturedRows() error {
	s.mu.Lock()
	if s.intentEntries != nil || s.intentEntriesErr != nil {
		err := s.intentEntriesErr
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	entries, err := s.collectCapturedRows(true)
	if err != nil {
		s.mu.Lock()
		s.intentEntriesErr = err
		s.conflictError = err
		s.mu.Unlock()
		s.clearCapturedRows()
		return err
	}

	s.mu.Lock()
	s.intentEntries = entries
	s.intentEntriesErr = nil
	s.mu.Unlock()
	s.clearCapturedRows()

	return nil
}

func (s *EphemeralHookSession) collectCapturedRows(collectOnly bool) ([]*IntentEntry, error) {
	rows, err := s.captureSnapshot()
	if err != nil {
		return nil, err
	}

	entries := make([]*IntentEntry, 0, len(rows))
	for _, rowRef := range rows {
		row, err := DecodeRow(rowRef.data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode captured row: %w", err)
		}

		if collectOnly {
			// Check DDL lock conflict
			if ddlTxn, err := s.metaStore.GetCDCTableDDLLock(row.Table); err == nil && ddlTxn != 0 && ddlTxn != s.txnID {
				return nil, ErrCDCTableDDLInProgress{Table: row.Table, HeldByTxn: ddlTxn}
			}

			// Acquire row lock
			if err := s.metaStore.AcquireCDCRowLock(s.txnID, row.Table, string(row.IntentKey)); err != nil {
				return nil, err
			}
		}

		entries = append(entries, &IntentEntry{
			TxnID:     s.txnID,
			Seq:       rowRef.seq,
			Operation: row.Op,
			Table:     row.Table,
			IntentKey: row.IntentKey,
			OldValues: row.OldValues,
			NewValues: row.NewValues,
		})
	}
	return entries, nil
}

func (s *EphemeralHookSession) getSchemaForTable(tableName string) (*TableSchema, error) {
	if s.schemaCache == nil {
		return nil, fmt.Errorf("schema cache unavailable")
	}
	return s.schemaCache.GetSchemaFor(tableName)
}

// GetIntentEntries reads all intent entries for this session from captured rows
func (s *EphemeralHookSession) GetIntentEntries() ([]*IntentEntry, error) {
	if err := s.GetConflictError(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.intentEntries != nil || s.intentEntriesErr != nil {
		cached := s.intentEntries
		err := s.intentEntriesErr
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return cached, nil
	}
	s.mu.Unlock()

	entries, err := s.collectCapturedRows(false)
	if err != nil {
		s.mu.Lock()
		s.intentEntriesErr = err
		s.mu.Unlock()
		s.clearCapturedRows()
		return nil, err
	}

	s.mu.Lock()
	s.intentEntries = entries
	s.intentEntriesErr = nil
	s.mu.Unlock()
	s.clearCapturedRows()

	return entries, nil
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
	s.clearCapturedRows()
	s.mu.Lock()
	s.intentEntries = nil
	s.intentEntriesErr = nil
	s.conflictError = nil
	s.mu.Unlock()
}

func (s *EphemeralHookSession) clearCapturedRows() {
	s.captureMu.Lock()
	s.capturedRows = nil
	s.capturedBytes = 0
	s.captureMu.Unlock()
}

// hookCallback is called by SQLite before each row modification.
// Encodes row data immediately with schema instead of deferring to ProcessCapturedRows.
func (s *EphemeralHookSession) hookCallback(data sqlite3.SQLitePreUpdateData) {
	if strings.HasPrefix(data.TableName, "__marmot") {
		return
	}

	// Get schema from cache; missing entries are treated as non-capturable rows.
	schema, err := s.getSchemaForTable(data.TableName)
	if err != nil {
		log.Warn().Err(err).Uint64("txn_id", s.txnID).Str("table", data.TableName).Msg("hookCallback: schema not found, skipping CDC")
		return
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
	var intentKey []byte

	if data.Op == sqlite3.SQLITE_DELETE || data.Op == sqlite3.SQLITE_UPDATE {
		rawOld := make([]interface{}, colCount)
		if data.Old(rawOld...) == nil {
			oldVals = encodeValuesWithSchema(schema.Columns, rawOld)
			if data.Op == sqlite3.SQLITE_DELETE {
				pkValues := extractPKFromValues(schema, rawOld, data.OldRowID)
				intentKey = filter.EncodeIntentKeyWithPrefix(schema.IntentKeyPrefix, pkValues)
			}
		}
	}

	if data.Op == sqlite3.SQLITE_INSERT || data.Op == sqlite3.SQLITE_UPDATE {
		rawNew := make([]interface{}, colCount)
		if data.New(rawNew...) == nil {
			newVals = encodeValuesWithSchema(schema.Columns, rawNew)
			pkValues := extractPKFromValues(schema, rawNew, data.NewRowID)
			intentKey = filter.EncodeIntentKeyWithPrefix(schema.IntentKeyPrefix, pkValues)
		}
	}

	row := &EncodedCapturedRow{
		Table:     data.TableName,
		Op:        opType,
		IntentKey: intentKey,
		OldValues: oldVals,
		NewValues: newVals,
	}

	seq := atomic.AddUint64(&s.seq, 1)

	rowData, err := EncodeRow(row)
	if err != nil {
		log.Warn().Err(err).Uint64("txn_id", s.txnID).Str("table", row.Table).Msg("hookCallback: failed to marshal row")
		return
	}

	if err := s.captureRow(seq, rowData); err != nil {
		log.Warn().Err(err).Uint64("txn_id", s.txnID).Uint64("seq", seq).Str("table", row.Table).Msg("hookCallback: failed to capture row")
	}
}

func (s *EphemeralHookSession) captureRow(seq uint64, encoded []byte) error {
	if s.metaStore == nil {
		return nil
	}
	if s.GetConflictError() != nil {
		return s.GetConflictError()
	}

	dataCopy := append([]byte(nil), encoded...)
	s.captureMu.Lock()
	if s.usePersistentCapture {
		s.captureMu.Unlock()
		return s.persistCapturedRow(seq, dataCopy)
	}

	if s.capturedRowsMax > 0 && s.capturedBytes+len(dataCopy) > s.capturedRowsMax {
		pending := make([]capturedRow, len(s.capturedRows))
		copy(pending, s.capturedRows)
		s.capturedRows = nil
		s.capturedBytes = 0
		s.usePersistentCapture = true
		s.captureMu.Unlock()

		if err := s.persistCapturedRows(pending); err != nil {
			s.setConflictError(err)
			return err
		}
		return s.persistCapturedRow(seq, dataCopy)
	}

	s.capturedRows = append(s.capturedRows, capturedRow{seq: seq, data: dataCopy})
	s.capturedBytes += len(dataCopy)
	s.captureMu.Unlock()
	return nil
}

func (s *EphemeralHookSession) persistCapturedRows(rows []capturedRow) error {
	if len(rows) == 0 {
		return nil
	}
	for _, row := range rows {
		if err := s.metaStore.WriteCapturedRow(s.txnID, row.seq, row.data); err != nil {
			return err
		}
	}
	return nil
}

func (s *EphemeralHookSession) persistCapturedRow(seq uint64, encoded []byte) error {
	if err := s.metaStore.WriteCapturedRow(s.txnID, seq, encoded); err != nil {
		s.setConflictError(err)
		return err
	}
	return nil
}

func (s *EphemeralHookSession) captureSnapshot() ([]capturedRow, error) {
	s.captureMu.Lock()
	memoryRows := make([]capturedRow, len(s.capturedRows))
	copy(memoryRows, s.capturedRows)
	s.captureMu.Unlock()

	persistedRows, err := s.loadPersistedRows()
	if err != nil {
		return nil, err
	}

	if len(persistedRows) == 0 {
		return memoryRows, nil
	}
	if len(memoryRows) == 0 {
		return persistedRows, nil
	}

	combined := make([]capturedRow, 0, len(memoryRows)+len(persistedRows))
	combined = append(combined, persistedRows...)
	combined = append(combined, memoryRows...)

	sort.Slice(combined, func(i, j int) bool {
		return combined[i].seq < combined[j].seq
	})
	return combined, nil
}

func (s *EphemeralHookSession) loadPersistedRows() ([]capturedRow, error) {
	cursor, err := s.metaStore.IterateCapturedRows(s.txnID)
	if err != nil {
		return nil, fmt.Errorf("failed to iterate captured rows: %w", err)
	}
	defer cursor.Close()

	rows := make([]capturedRow, 0)
	for cursor.Next() {
		seq, data := cursor.Row()
		rows = append(rows, capturedRow{
			seq:  seq,
			data: append([]byte(nil), data...),
		})
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *EphemeralHookSession) setConflictError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.conflictError = err
	s.mu.Unlock()
}

// =============================================================================
// Utility functions
// =============================================================================

// extractPKFromValues extracts PK values from raw values slice using schema indices.
// Returns typed PK values in PK declaration order for binary encoding.
func extractPKFromValues(schema *TableSchema, values []interface{}, rowID int64) []filter.TypedPKValue {
	pkValues := make([]filter.TypedPKValue, len(schema.PKIndices))
	for i, idx := range schema.PKIndices {
		if idx == -1 {
			// rowid: always int64
			pkValues[i] = filter.TypedPKValue{
				Type:  filter.PKTypeInt64,
				Value: filter.EncodeInt64(rowID),
			}
		} else if idx < len(values) && values[idx] != nil {
			pkValues[i] = valueToTypedPK(values[idx])
		} else {
			// NULL PK value
			pkValues[i] = filter.TypedPKValue{Type: filter.PKTypeNull}
		}
	}
	return pkValues
}

// encodeValuesWithSchema converts []interface{} to map[string][]byte using schema column names.
func encodeValuesWithSchema(columns []string, values []interface{}) map[string][]byte {
	result := make(map[string][]byte, len(columns))
	for i, col := range columns {
		if i >= len(values) {
			continue
		}
		if encoded := encodeValue(values[i]); encoded != nil {
			result[col] = encoded
		}
	}
	return result
}

// encodeValue encodes a single value to msgpack bytes.
func encodeValue(v interface{}) []byte {
	data, err := encoding.Marshal(v)
	if err != nil {
		return nil
	}
	return data
}

// valueToTypedPK converts a raw SQLite value to a typed PK value for binary encoding.
// Returns appropriate type tag and encoded value bytes.
func valueToTypedPK(v interface{}) filter.TypedPKValue {
	switch val := v.(type) {
	case int64:
		return filter.TypedPKValue{
			Type:  filter.PKTypeInt64,
			Value: filter.EncodeInt64(val),
		}
	case float64:
		return filter.TypedPKValue{
			Type:  filter.PKTypeFloat64,
			Value: filter.EncodeFloat64(val),
		}
	case string:
		return filter.TypedPKValue{
			Type:  filter.PKTypeString,
			Value: []byte(val),
		}
	case []byte:
		return filter.TypedPKValue{
			Type:  filter.PKTypeBytes,
			Value: val,
		}
	default:
		// Fallback: convert to string
		return filter.TypedPKValue{
			Type:  filter.PKTypeString,
			Value: []byte(fmt.Sprintf("%v", val)),
		}
	}
}
