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
// CDC rows are captured as canonical msgpack row records. Normal transactions
// keep them in bounded session memory until SQLite rollback releases the writer;
// oversized transactions spill directly to the CDC segment log.
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

	lastProcessedSeq uint64 // high-water mark for captureAndLockNewRows
	eagerCaptureUsed bool   // true once captureAndLockNewRows has run at least once
}

type capturedRow struct {
	seq  uint64
	data []byte
}

const defaultCapturedRowsBudget = 64 * 1024 * 1024

// IntentEntry represents a CDC entry stored in the system database
type IntentEntry struct {
	TxnID        uint64
	Seq          uint64
	Operation    uint8
	Table        string
	IntentKey    []byte
	OldValues    map[string][]byte
	NewValues    map[string][]byte
	EncodedRow   []byte
	EncodedCodec uint32
	CreatedAt    int64
}

// =============================================================================
// Constructor
// =============================================================================

// StartEphemeralSession creates a new CDC capture session with a dedicated connection.
// The session owns the connection and will close it when done.
// CDC entries are captured during hooks and processed after rollback.
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

// ExecContext executes a statement within the session's transaction, and
// returns the real rows-affected count SQLite reports for it. Callers that
// need locks/CDC entries acquired incrementally (eager pinned-session
// execution) must follow this with captureAndLockNewRows; callers that defer
// all processing to Rollback (the autocommit ExecuteLocalWithHooks flow) do
// not need to.
func (s *EphemeralHookSession) ExecContext(ctx context.Context, query string, args ...interface{}) (int64, error) {
	if s.tx == nil {
		return 0, fmt.Errorf("no active transaction")
	}
	result, err := s.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	if conflictErr := s.GetConflictError(); conflictErr != nil {
		return 0, conflictErr
	}

	if id, err := result.LastInsertId(); err == nil && id != 0 {
		s.lastInsertId = id
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

// QueryContext runs a read on the session's still-open transaction, so it
// observes uncommitted writes made earlier in the same transaction.
func (s *EphemeralHookSession) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if s.tx == nil {
		return nil, fmt.Errorf("no active transaction")
	}
	return s.tx.QueryContext(ctx, query, args...)
}

// captureAndLockNewRows processes any rows captured (via hookCallback) since
// the last call, converting them to IntentEntry and acquiring their CDC row
// locks immediately - unlike the existing one-shot ProcessCapturedRows/
// GetIntentEntries path (used by the autocommit ExecuteLocalWithHooks flow),
// which defers lock acquisition until the whole hookDB transaction rolls
// back. A pinned session calls this after every statement so a conflicting
// transaction sees the lock from statement time, not just from COMMIT.
func (s *EphemeralHookSession) captureAndLockNewRows() error {
	rows, err := s.captureSnapshot()
	if err != nil {
		return err
	}

	s.mu.Lock()
	lastSeq := s.lastProcessedSeq
	s.mu.Unlock()

	newEntries := make([]*IntentEntry, 0)
	maxSeq := lastSeq
	for _, rowRef := range rows {
		if rowRef.seq <= lastSeq {
			continue
		}
		row, err := DecodeRow(rowRef.data)
		if err != nil {
			return fmt.Errorf("failed to decode captured row: %w", err)
		}

		if ddlTxn, err := s.metaStore.GetCDCTableDDLLock(row.Table); err == nil && ddlTxn != 0 && ddlTxn != s.txnID {
			return ErrCDCTableDDLInProgress{Table: row.Table, HeldByTxn: ddlTxn}
		}
		if err := s.metaStore.AcquireCDCRowLock(s.txnID, row.Table, string(row.IntentKey)); err != nil {
			return err
		}

		newEntries = append(newEntries, &IntentEntry{
			TxnID:        s.txnID,
			Seq:          rowRef.seq,
			Operation:    row.Op,
			Table:        row.Table,
			IntentKey:    row.IntentKey,
			OldValues:    row.OldValues,
			NewValues:    row.NewValues,
			EncodedRow:   rowRef.data,
			EncodedCodec: encodedCapturedRowCodecMsgpack,
		})
		if rowRef.seq > maxSeq {
			maxSeq = rowRef.seq
		}
	}

	s.mu.Lock()
	s.intentEntries = append(s.intentEntries, newEntries...)
	s.lastProcessedSeq = maxSeq
	s.eagerCaptureUsed = true
	s.mu.Unlock()
	return nil
}

// CapturedIntentEntries returns the entries accumulated so far via
// captureAndLockNewRows, without triggering any collection. Used by the
// pinned-session wrapper to build CDC entries for 2PC at COMMIT time, before
// the session is rolled back.
func (s *EphemeralHookSession) CapturedIntentEntries() []*IntentEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.intentEntries
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
// 3. ProcessCapturedRows (converts raw captures to IntentEntries)
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

	// Release hookDB connection ASAP; ProcessCapturedRows does not need SQLite.
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
	if s.eagerCaptureUsed {
		// Entries and locks are already correct via captureAndLockNewRows;
		// re-running collectCapturedRows would re-acquire locks already held
		// and double-append entries.
		s.mu.Unlock()
		return nil
	}
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
			TxnID:        s.txnID,
			Seq:          rowRef.seq,
			Operation:    row.Op,
			Table:        row.Table,
			IntentKey:    row.IntentKey,
			OldValues:    row.OldValues,
			NewValues:    row.NewValues,
			EncodedRow:   rowRef.data,
			EncodedCodec: encodedCapturedRowCodecMsgpack,
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
	if s.eagerCaptureUsed {
		// Entries already accumulated via captureAndLockNewRows - "eager mode
		// ran and captured zero rows" is a valid final state, not "not yet
		// collected", so skip the collectCapturedRows(false) fallback branch
		// entirely even when s.intentEntries is nil.
		entries := s.intentEntries
		s.mu.Unlock()
		return entries, nil
	}
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

	// The schema cache holds every non-internal table and is reloaded after DDL,
	// so a miss means this row cannot be captured. Dropping it would silently
	// lose the write, so fail the statement instead.
	schema, err := s.getSchemaForTable(data.TableName)
	if err != nil {
		log.Error().Err(err).Uint64("txn_id", s.txnID).Str("table", data.TableName).Msg("hookCallback: schema unavailable, cannot capture CDC")
		s.setConflictError(fmt.Errorf("cannot capture CDC for table %s: %w", data.TableName, err))
		return
	}

	// go-sqlite3's preupdate hook segfaults reading a VIRTUAL generated column's
	// value: sqlite3_preupdate_new/old returns a NULL sqlite3_value* for it, and
	// row() dereferences that pointer unconditionally (verified directly against
	// go-sqlite3 v1.14.24; there is no way to skip just that column's index with
	// the API as vendored). Refuse capture rather than crash the process.
	// STORED generated columns are unaffected and are not in schema.VirtualColumns.
	if len(schema.VirtualColumns) > 0 {
		s.setConflictError(fmt.Errorf(
			"cannot capture CDC for table %s: table has GENERATED ALWAYS AS (...) VIRTUAL "+
				"column(s) %s, which go-sqlite3's preupdate hook cannot safely read - "+
				"use STORED instead of VIRTUAL",
			data.TableName, strings.Join(schema.VirtualColumns, ", ")))
		return
	}

	// Tables with no explicit PRIMARY KEY replicate their identity via SQLite's
	// rowid (schema.PKIndices == [-1]). A user-declared column that shadows one
	// of SQLite's rowid aliases would collide with the synthetic "rowid" CDC key
	// used below, so refuse to capture rather than silently corrupting identity.
	if isRowidSentinelSchema(schema) {
		if shadow := findShadowedRowidColumn(schema.Columns); shadow != "" {
			s.setConflictError(fmt.Errorf(
				"cannot capture CDC for table %s: column %q shadows SQLite's rowid alias; "+
					"tables without an explicit PRIMARY KEY must not declare a column named "+
					"rowid, oid, or _rowid_ - add an explicit PRIMARY KEY instead",
				data.TableName, shadow))
			return
		}
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
			oldVals, err = encodeValuesWithSchema(schema, rawOld)
			if err != nil {
				s.setConflictError(fmt.Errorf("cannot capture CDC for table %s: %w", data.TableName, err))
				return
			}
			if isRowidSentinelSchema(schema) {
				if encoded := encodeValue(data.OldRowID); encoded != nil {
					oldVals[rowidColumnKey] = encoded
				}
			}
			if data.Op == sqlite3.SQLITE_DELETE {
				pkValues := extractPKFromValues(schema, rawOld, data.OldRowID)
				intentKey = filter.EncodeIntentKeyWithPrefix(schema.IntentKeyPrefix, pkValues)
			}
		}
	}

	if data.Op == sqlite3.SQLITE_INSERT || data.Op == sqlite3.SQLITE_UPDATE {
		rawNew := make([]interface{}, colCount)
		if data.New(rawNew...) == nil {
			newVals, err = encodeValuesWithSchema(schema, rawNew)
			if err != nil {
				s.setConflictError(fmt.Errorf("cannot capture CDC for table %s: %w", data.TableName, err))
				return
			}
			if isRowidSentinelSchema(schema) {
				if encoded := encodeValue(data.NewRowID); encoded != nil {
					newVals[rowidColumnKey] = encoded
				}
			}
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

// rowidColumnKey is the CDC map key used to carry a rowid-sentinel table's
// identity (SQLite's implicit rowid) through capture and apply, so replicas
// converge on the origin's rowid instead of assigning their own.
const rowidColumnKey = "rowid"

// reservedRowidAliases are SQLite's built-in names for the rowid column.
// A user-declared column sharing one of these names would collide with
// rowidColumnKey in the CDC maps, so tables relying on the rowid sentinel
// must not declare any of them.
var reservedRowidAliases = [...]string{"rowid", "oid", "_rowid_"}

// isRowidSentinelSchema reports whether a table has no explicit PRIMARY KEY,
// meaning its replication identity is SQLite's rowid (schema.PKIndices == [-1]).
func isRowidSentinelSchema(schema *TableSchema) bool {
	return len(schema.PKIndices) == 1 && schema.PKIndices[0] == -1
}

// findShadowedRowidColumn returns the first declared column name that
// case-insensitively matches one of SQLite's rowid aliases, or "" if none do.
func findShadowedRowidColumn(columns []string) string {
	for _, col := range columns {
		for _, alias := range reservedRowidAliases {
			if strings.EqualFold(col, alias) {
				return col
			}
		}
	}
	return ""
}

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
//
// SQLite's preupdate hook hands back TEXT and BLOB storage classes identically
// as Go []byte (see go-sqlite3's row(): both SQLITE_BLOB and SQLITE_TEXT go
// through sqlite3_value_bytes/GoBytes), so nothing about the raw value itself
// says which one it is. schema.BlobAffinityCols (precomputed at schema load
// from the declared column type) resolves that by column AFFINITY, not
// declared type name: per SQLite's dynamic typing (sqlite.org/datatype3.html
// #3.1), only BLOB affinity never coerces a stored value - every other
// affinity (TEXT, INTEGER, REAL, NUMERIC) converts a value that looks
// numeric on INSERT, but a TEXT value that doesn't parse as a number is left
// alone. So a []byte captured for a NUMERIC/INTEGER/REAL/TEXT-affinity
// column is TEXT storage class in the overwhelming common case (numbers
// arrive from the hook as int64/float64 already, never []byte) and is
// converted to string here, written as msgpack Str. A []byte for a
// BLOB-affinity column is left as-is and written as msgpack Bin.
// unmarshalCDCValue's strict decode preserves that choice on the way back
// out, so BLOB columns round trip as []byte -> sqlite3_bind_blob instead of
// being coerced to text.
//
// This is a deliberate, accepted lesser evil, not a complete solution: SQLite
// never coerces a genuine BLOB storage class value either, regardless of the
// column's declared affinity (e.g. inserted via a literal blob or an explicit
// CAST(... AS BLOB) into a NUMERIC/TEXT/etc-affinity column). Such a value
// would incorrectly round-trip as a string here, since nothing in the raw
// []byte or the static schema distinguishes it from ordinary TEXT storage
// class. This is intentionally the rarer case: BLOB-affinity columns are
// overwhelmingly used for genuine binary data (password hashes, UUIDs - the
// motivating bug), while non-BLOB-affinity columns overwhelmingly hold text
// or numbers, so defaulting non-BLOB affinities to string protects the
// common case in both directions.
//
// Columns are looked up by their true position (schema.ColumnPositions), not
// by their index within schema.Columns: whenever the table has a generated
// (STORED) column, schema.Columns excludes it but the preupdate hook's raw
// values array does not skip its slot, so index-in-Columns and
// index-into-values diverge (see loadSchema).
//
// Returns an error - rather than silently dropping data - if a column's
// position falls outside the captured values (a stale schema relative to
// this row) or if any value fails to encode, since a partial CDC row is
// worse than a loud failure.
func encodeValuesWithSchema(schema *TableSchema, values []interface{}) (map[string][]byte, error) {
	columns := schema.Columns
	result := make(map[string][]byte, len(columns))
	for i, col := range columns {
		pos := i
		if i < len(schema.ColumnPositions) {
			pos = schema.ColumnPositions[i]
		}
		if pos < 0 || pos >= len(values) {
			return nil, fmt.Errorf("column %s: position %d out of range for %d captured values (stale schema?)", col, pos, len(values))
		}

		v := values[pos]
		isBlobAffinity := i < len(schema.BlobAffinityCols) && schema.BlobAffinityCols[i]
		if b, ok := v.([]byte); ok && !isBlobAffinity {
			v = string(b)
		}
		encoded, err := encoding.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to encode column %s: %w", col, err)
		}
		result[col] = encoded
	}
	return result, nil
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
