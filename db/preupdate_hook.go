//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/rs/zerolog/log"
)

// OpInsertInt, OpUpdateInt, OpDeleteInt are defined in meta_schema.go
// to be available regardless of build tags

// interfacePtrPool pools *interface{} values to reduce allocations in hookCallback.
// Each row modification needs N pointers where N = column count.
var interfacePtrPool = sync.Pool{
	New: func() interface{} {
		v := new(interface{})
		return v
	},
}

// TableSchema represents a lightweight schema for CDC hooks.
// Contains only the minimal information needed for preupdate hook callbacks.
type TableSchema struct {
	Columns     []string
	PrimaryKeys []string
	PKIndices   []int
}

// SchemaCache provides thread-safe caching of table schemas.
// Schemas are loaded from DB via Reload() and accessed via GetSchemaFor().
type SchemaCache struct {
	mu    sync.RWMutex
	cache map[string]*TableSchema
}

// NewSchemaCache creates a new schema cache
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		cache: make(map[string]*TableSchema),
	}
}

// GetSchemaFor returns the cached schema for a table.
// Returns error if schema is not cached.
func (c *SchemaCache) GetSchemaFor(tableName string) (*TableSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.cache[tableName]
	if !ok {
		return nil, ErrSchemaCacheMiss{Table: tableName}
	}
	return schema, nil
}

// Reload reloads all table schemas from the database connection.
// This should be called after DDL operations or snapshot apply.
func (c *SchemaCache) Reload(conn *sqlite3.SQLiteConn) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get list of all user tables (exclude internal tables)
	rows, err := conn.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '__marmot%'", nil)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	dest := make([]driver.Value, 1)
	for {
		if err := rows.Next(dest); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read table names: %w", err)
		}
		if name, ok := dest[0].(string); ok {
			tableNames = append(tableNames, name)
		}
	}

	// Load schema for each table
	newCache := make(map[string]*TableSchema)
	for _, tableName := range tableNames {
		schema, err := loadSchema(conn, tableName)
		if err != nil {
			log.Warn().Err(err).Str("table", tableName).Msg("Failed to load schema during reload")
			continue
		}
		newCache[tableName] = schema
	}

	// Replace cache atomically
	c.cache = newCache
	log.Debug().Int("tables", len(newCache)).Msg("SchemaCache reloaded")
	return nil
}

// Clear clears all cached schemas.
// Used when database connections are closed or invalid.
func (c *SchemaCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*TableSchema)
}

// LoadTable loads schema for a single table into the cache.
// Used by tests and for on-demand schema loading.
func (c *SchemaCache) LoadTable(conn *sqlite3.SQLiteConn, tableName string) error {
	schema, err := loadSchema(conn, tableName)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.cache[tableName] = schema
	c.mu.Unlock()

	return nil
}

// Update directly updates the cache with a schema.
// Used by tests for manual schema setup.
func (c *SchemaCache) Update(tableName string, schema *TableSchema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[tableName] = schema
}

// loadSchema fetches schema from DB using the raw SQLite connection
func loadSchema(conn *sqlite3.SQLiteConn, tableName string) (*TableSchema, error) {
	rows, err := conn.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName), nil)
	if err != nil {
		return nil, fmt.Errorf("query table_info: %w", err)
	}
	defer rows.Close()

	schema := &TableSchema{
		Columns:     make([]string, 0),
		PrimaryKeys: make([]string, 0),
		PKIndices:   make([]int, 0),
	}

	// table_info returns: cid, name, type, notnull, dflt_value, pk
	dest := make([]driver.Value, 6)
	colIndex := 0
	for {
		if err := rows.Next(dest); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read table_info row: %w", err)
		}

		name, _ := dest[1].(string)
		pk, _ := dest[5].(int64)

		schema.Columns = append(schema.Columns, name)
		if pk > 0 {
			schema.PrimaryKeys = append(schema.PrimaryKeys, name)
			schema.PKIndices = append(schema.PKIndices, colIndex)
		}
		colIndex++
	}

	// If no PK columns found, assume rowid is the PK
	if len(schema.PrimaryKeys) == 0 {
		schema.PrimaryKeys = []string{"rowid"}
		schema.PKIndices = []int{-1} // -1 indicates rowid
	}

	return schema, nil
}

// EphemeralHookSession represents a CDC capture session with its own dedicated connection.
// The connection is held open for the duration of the session and closed on Commit/Rollback.
// CDC entries are stored in the per-database MetaStore's __marmot__intent_entries table.
type EphemeralHookSession struct {
	conn         *sql.Conn                           // Dedicated user DB connection (closed on end)
	tx           *sql.Tx                             // Active transaction on user DB
	metaStore    MetaStore                           // MetaStore for intent entry storage
	txnID        uint64                              // Transaction ID for intent entries
	seq          uint64                              // Sequence counter for entries
	collectors   map[string]*filter.KeyHashCollector // table -> key hash collector
	schemaCache  *SchemaCache                        // Shared schema cache
	lastInsertId int64                               // Last insert ID from most recent insert
	mu           sync.Mutex

	// CDC conflict detection
	conflictError error // Set if conflict detected during hook
}

// StartEphemeralSession creates a new CDC capture session with a dedicated connection.
// The session owns the connection and will close it when done.
// CDC entries are written to the per-database MetaStore during hooks.
//
// IMPORTANT: SchemaCache must be pre-populated for all tables that will be accessed.
// If a table's schema is not in the cache, its changes will be silently skipped.
// Call ReplicatedDatabase.ReloadSchema() before starting the session to ensure schemas are loaded.
func StartEphemeralSession(ctx context.Context, userDB *sql.DB, metaStore MetaStore, schemaCache *SchemaCache, txnID uint64) (*EphemeralHookSession, error) {
	// Get a dedicated connection for this session
	conn, err := userDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	session := &EphemeralHookSession{
		conn:        conn,
		metaStore:   metaStore,
		txnID:       txnID,
		seq:         0,
		collectors:  make(map[string]*filter.KeyHashCollector),
		schemaCache: schemaCache,
	}

	// Register preupdate hook inside Raw() callback
	err = conn.Raw(func(driverConn interface{}) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}

		// Register the preupdate hook
		sqliteConn.RegisterPreUpdateHook(session.hookCallback)
		return nil
	})
	if err != nil {
		conn.Close()
		return nil, err
	}

	return session, nil
}

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

	// Check for CDC conflict errors that occurred during hook callback
	if conflictErr := s.GetConflictError(); conflictErr != nil {
		return conflictErr
	}

	// Capture last insert ID (ignore error - not all statements produce one)
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

	// Unregister hook and close connection
	s.cleanup()

	return txErr
}

// Rollback aborts the hookDB transaction and closes the connection.
// Note: CDC intent entries are NOT deleted here. They persist in MetaStore until
// the distributed transaction completes. This is intentional because Rollback()
// is called in executeStatements() to release the hookDB connection BEFORE 2PC,
// but the CDC data is still needed for applyDataChanges() during commit phase.
// Cleanup happens in TransactionManager.cleanupAfterCommit() or cleanupAfterAbort().
func (s *EphemeralHookSession) Rollback() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var txErr error
	if s.tx != nil {
		txErr = s.tx.Rollback()
	}

	// Unregister hook and close connection
	s.cleanup()

	return txErr
}

// FlushIntentLog is a no-op for SQLite-based storage.
// SQLite WAL mode handles durability automatically.
// Kept for API compatibility.
func (s *EphemeralHookSession) FlushIntentLog() error {
	return nil
}

// cleanup unregisters the hook and closes the connection
func (s *EphemeralHookSession) cleanup() {
	if s.conn == nil {
		return
	}

	// Release all CDC row locks held by this transaction
	if s.metaStore != nil && s.txnID != 0 {
		if err := s.metaStore.ReleaseCDCRowLocksByTxn(s.txnID); err != nil {
			log.Error().Err(err).Uint64("txn_id", s.txnID).Msg("Failed to release CDC row locks during cleanup")
		}
	}

	// Unregister hook inside Raw() callback
	s.conn.Raw(func(driverConn interface{}) error {
		if sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn); ok {
			sqliteConn.RegisterPreUpdateHook(nil)
		}
		return nil
	})

	s.conn.Close()
	s.conn = nil
}

// hookCallback is called by SQLite before each row modification
// NOTE: This is called from within SQLite's transaction processing,
// so we cannot call back into the database connection (would deadlock).
func (s *EphemeralHookSession) hookCallback(data sqlite3.SQLitePreUpdateData) {
	// Skip internal tables
	if strings.HasPrefix(data.TableName, "__marmot") {
		return
	}

	// Get table schema from cache
	// NOTE: We cannot load on-demand here because we're inside a hook callback.
	// The schema MUST be pre-populated via Reload() or the table will be skipped.
	schema, err := s.schemaCache.GetSchemaFor(data.TableName)
	if err != nil {
		// Schema not in cache - this is a critical issue that will cause replication to fail
		// Set conflict error so caller knows to reload schema
		if _, ok := err.(ErrSchemaCacheMiss); ok {
			log.Warn().Str("table", data.TableName).Msg("CDC: schema not cached - changes will be lost")
			s.conflictError = err
		}
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get column count
	colCount := data.Count()

	// Prepare destinations for column values using pooled pointers
	oldDest := make([]interface{}, colCount)
	newDest := make([]interface{}, colCount)
	for i := 0; i < colCount; i++ {
		oldDest[i] = interfacePtrPool.Get().(*interface{})
		newDest[i] = interfacePtrPool.Get().(*interface{})
	}
	// Defer returning pointers to pool
	defer func() {
		for i := 0; i < colCount; i++ {
			if p, ok := oldDest[i].(*interface{}); ok {
				*p = nil // Clear before returning
				interfacePtrPool.Put(p)
			}
			if p, ok := newDest[i].(*interface{}); ok {
				*p = nil // Clear before returning
				interfacePtrPool.Put(p)
			}
		}
	}()

	var operation uint8
	var oldValues, newValues map[string][]byte
	var intentKey string

	// Determine operation type and capture values
	switch data.Op {
	case sqlite3.SQLITE_INSERT:
		operation = uint8(OpTypeInsert)
		if err := data.New(newDest...); err == nil {
			newValues = s.buildValueMap(schema.Columns, newDest)
		}
		pkValues := s.extractPKValuesFromDest(schema, newDest, data.NewRowID)
		intentKey = s.serializePK(data.TableName, schema, pkValues)

	case sqlite3.SQLITE_UPDATE:
		operation = uint8(OpTypeUpdate)
		if err := data.Old(oldDest...); err == nil {
			oldValues = s.buildValueMap(schema.Columns, oldDest)
		}
		if err := data.New(newDest...); err == nil {
			newValues = s.buildValueMap(schema.Columns, newDest)
		}
		// Track both old and new PK for updates
		oldPK := s.extractPKValuesFromDest(schema, oldDest, data.OldRowID)
		newPK := s.extractPKValuesFromDest(schema, newDest, data.NewRowID)
		oldKey := s.serializePK(data.TableName, schema, oldPK)
		newKey := s.serializePK(data.TableName, schema, newPK)
		intentKey = newKey

		// Ensure collector exists for this table
		if _, ok := s.collectors[data.TableName]; !ok {
			s.collectors[data.TableName] = filter.NewKeyHashCollector()
		}
		s.collectors[data.TableName].AddIntentKey(oldKey)
		if oldKey != newKey {
			s.collectors[data.TableName].AddIntentKey(newKey)
		}

	case sqlite3.SQLITE_DELETE:
		operation = uint8(OpTypeDelete)
		if err := data.Old(oldDest...); err == nil {
			oldValues = s.buildValueMap(schema.Columns, oldDest)
		}
		pkValues := s.extractPKValuesFromDest(schema, oldDest, data.OldRowID)
		intentKey = s.serializePK(data.TableName, schema, pkValues)

	default:
		return
	}

	// Ensure collector exists for this table (for non-UPDATE operations)
	if data.Op != sqlite3.SQLITE_UPDATE {
		if _, ok := s.collectors[data.TableName]; !ok {
			s.collectors[data.TableName] = filter.NewKeyHashCollector()
		}
		s.collectors[data.TableName].AddIntentKey(intentKey)
	}

	// CDC conflict detection: check for DDL lock and acquire row lock
	if ddlTxn, err := s.metaStore.GetCDCTableDDLLock(data.TableName); err == nil && ddlTxn != 0 && ddlTxn != s.txnID {
		log.Debug().
			Uint64("txn_id", s.txnID).
			Uint64("ddl_txn", ddlTxn).
			Str("table", data.TableName).
			Msg("CDC: DML blocked - DDL table lock exists")
		s.conflictError = ErrCDCTableDDLInProgress{Table: data.TableName, HeldByTxn: ddlTxn}
		return
	}

	// Acquire row lock
	if err := s.metaStore.AcquireCDCRowLock(s.txnID, data.TableName, intentKey); err != nil {
		s.conflictError = err
		return
	}

	// Serialize values to msgpack using pooled encoder
	var oldMsgpack, newMsgpack []byte
	if oldValues != nil {
		oldMsgpack, _ = encoding.Marshal(oldValues)
	}
	if newValues != nil {
		newMsgpack, _ = encoding.Marshal(newValues)
	}

	// Increment sequence
	s.seq++

	// Write to MetaStore (separate database file, so this is safe during hook)
	s.metaStore.WriteIntentEntry(s.txnID, s.seq, operation, data.TableName, intentKey, oldMsgpack, newMsgpack)
}

// buildValueMap converts column values to a map
func (s *EphemeralHookSession) buildValueMap(columns []string, values []interface{}) map[string][]byte {
	result := make(map[string][]byte, len(columns))
	for i, col := range columns {
		if i >= len(values) {
			break
		}
		val := values[i]
		if ptr, ok := val.(*interface{}); ok {
			val = *ptr
		}
		if val != nil {
			result[col] = s.serializeValue(val)
		}
	}
	return result
}

// serializeValue converts a value to msgpack bytes for CDC replication.
// Msgpack preserves type information (int64 stays int64, not float64).
// SQLite preupdate hook returns TEXT columns as []byte - we convert to string
// so msgpack encodes as str (not bin), ensuring correct type on decode.
// Returns nil only for nil input; Marshal errors are logged but non-fatal
// since we're on a hot path and partial CDC is better than blocking writes.
func (s *EphemeralHookSession) serializeValue(v interface{}) []byte {
	if v == nil {
		return nil
	}
	// SQLite preupdate hook returns TEXT as []byte - convert to string
	// so msgpack uses str format (not bin) for proper type preservation
	if b, ok := v.([]byte); ok {
		v = string(b)
	}
	data, err := encoding.Marshal(v)
	if err != nil {
		// Log but don't fail - partial CDC is better than blocking writes
		// This should never happen with valid SQLite values
		return nil
	}
	return data
}

// extractPKValuesFromDest extracts primary key values from a destination slice.
// PK values are converted to string representation for intent key generation,
// ensuring compatibility with AST-based path which uses string values.
func (s *EphemeralHookSession) extractPKValuesFromDest(schema *TableSchema, dest []interface{}, rowID int64) map[string][]byte {
	pkValues := make(map[string][]byte)

	for i, idx := range schema.PKIndices {
		colName := schema.PrimaryKeys[i]
		if idx == -1 {
			// rowid case
			pkValues[colName] = []byte(fmt.Sprintf("%d", rowID))
		} else if idx < len(dest) {
			val := dest[idx]
			if ptr, ok := val.(*interface{}); ok {
				val = *ptr
			}
			if val != nil {
				// Convert to string representation for intent key compatibility
				// This ensures hook and AST paths produce identical keys
				pkValues[colName] = pkValueToBytes(val)
			}
		}
	}

	return pkValues
}

// pkValueToBytes converts a primary key value to its string byte representation.
// Used for intent key generation where values must match AST-based path format.
func pkValueToBytes(v interface{}) []byte {
	switch val := v.(type) {
	case int64:
		return []byte(fmt.Sprintf("%d", val))
	case float64:
		// Check if float is actually an integer
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

// serializePK creates a deterministic string key from PK values
func (s *EphemeralHookSession) serializePK(table string, schema *TableSchema, pkValues map[string][]byte) string {
	return filter.SerializeIntentKey(table, schema.PrimaryKeys, pkValues)
}

// GetRowCounts returns the number of affected rows per table
func (s *EphemeralHookSession) GetRowCounts() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	counts := make(map[string]int64)
	for table, collector := range s.collectors {
		counts[table] = int64(collector.Count())
	}
	return counts
}

// GetKeyHashes returns XXH64 hashes of affected intent keys per table.
// Returns nil for tables exceeding maxRows.
func (s *EphemeralHookSession) GetKeyHashes(maxRows int) map[string][]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	hashes := make(map[string][]uint64)
	for table, collector := range s.collectors {
		count := collector.Count()
		if maxRows > 0 && count > maxRows {
			// Exceeds max - return empty slice to signal MVCC fallback
			hashes[table] = nil
			continue
		}
		hashes[table] = collector.Keys()
	}
	return hashes
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
