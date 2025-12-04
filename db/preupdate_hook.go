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

// SchemaCache provides thread-safe caching of table schemas.
// Schemas are preloaded before transactions to avoid DB queries during hook callbacks.
type SchemaCache struct {
	mu    sync.RWMutex
	cache map[string]*tableSchema
}

type tableSchema struct {
	columns   []string
	pkColumns []string
	pkIndices []int
}

// NewSchemaCache creates a new schema cache
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		cache: make(map[string]*tableSchema),
	}
}

// Get retrieves a cached schema (nil if not cached)
func (c *SchemaCache) Get(tableName string) *tableSchema {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache[tableName]
}

// Set stores a schema in the cache
func (c *SchemaCache) Set(tableName string, schema *tableSchema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[tableName] = schema
}

// Invalidate removes a table from the cache (call after DDL)
func (c *SchemaCache) Invalidate(tableName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, tableName)
}

// InvalidateAll clears the entire cache
func (c *SchemaCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*tableSchema)
}

// loadSchema fetches schema from DB using the raw SQLite connection
func loadSchema(conn *sqlite3.SQLiteConn, tableName string) (*tableSchema, error) {
	rows, err := conn.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName), nil)
	if err != nil {
		return nil, fmt.Errorf("query table_info: %w", err)
	}
	defer rows.Close()

	schema := &tableSchema{
		columns:   make([]string, 0),
		pkColumns: make([]string, 0),
		pkIndices: make([]int, 0),
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

		schema.columns = append(schema.columns, name)
		if pk > 0 {
			schema.pkColumns = append(schema.pkColumns, name)
			schema.pkIndices = append(schema.pkIndices, colIndex)
		}
		colIndex++
	}

	// If no PK columns found, assume rowid is the PK
	if len(schema.pkColumns) == 0 {
		schema.pkColumns = []string{"rowid"}
		schema.pkIndices = []int{-1} // -1 indicates rowid
	}

	return schema, nil
}

// EphemeralHookSession represents a CDC capture session with its own dedicated connection.
// The connection is held open for the duration of the session and closed on Commit/Rollback.
// CDC entries are stored in the per-database MetaStore's __marmot__intent_entries table.
type EphemeralHookSession struct {
	conn        *sql.Conn                           // Dedicated user DB connection (closed on end)
	tx          *sql.Tx                             // Active transaction on user DB
	metaStore   MetaStore                           // MetaStore for intent entry storage
	txnID       uint64                              // Transaction ID for intent entries
	seq         uint64                              // Sequence counter for entries
	collectors  map[string]*filter.KeyHashCollector // table -> key hash collector
	schemaCache *SchemaCache                        // Shared schema cache
	mu          sync.Mutex
}

// StartEphemeralSession creates a new CDC capture session with a dedicated connection.
// The session owns the connection and will close it when done.
// CDC entries are written to the per-database MetaStore during hooks.
func StartEphemeralSession(ctx context.Context, userDB *sql.DB, metaStore MetaStore, schemaCache *SchemaCache, txnID uint64, tables []string) (*EphemeralHookSession, error) {
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

	// Register hook and preload schemas inside Raw() callback
	err = conn.Raw(func(driverConn interface{}) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}

		// Preload schemas for affected tables
		for _, tableName := range tables {
			if schemaCache.Get(tableName) == nil {
				schema, err := loadSchema(sqliteConn, tableName)
				if err != nil {
					return fmt.Errorf("failed to load schema for %s: %w", tableName, err)
				}
				schemaCache.Set(tableName, schema)
			}
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
func (s *EphemeralHookSession) ExecContext(ctx context.Context, query string) error {
	if s.tx == nil {
		return fmt.Errorf("no active transaction")
	}
	_, err := s.tx.ExecContext(ctx, query)
	return err
}

// Commit commits the transaction and closes the connection
func (s *EphemeralHookSession) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var txErr error
	if s.tx != nil {
		txErr = s.tx.Commit()
	}

	// Unregister hook and close connection
	s.cleanup()

	// Delete intent entries from MetaStore (fast indexed delete)
	s.metaStore.DeleteIntentEntries(s.txnID)

	return txErr
}

// Rollback aborts the transaction and closes the connection
func (s *EphemeralHookSession) Rollback() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var txErr error
	if s.tx != nil {
		txErr = s.tx.Rollback()
	}

	// Unregister hook and close connection
	s.cleanup()

	// Delete intent entries from MetaStore
	s.metaStore.DeleteIntentEntries(s.txnID)

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
func (s *EphemeralHookSession) hookCallback(data sqlite3.SQLitePreUpdateData) {
	// Skip internal tables
	if strings.HasPrefix(data.TableName, "__marmot") {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get table schema from cache
	schema := s.schemaCache.Get(data.TableName)
	if schema == nil {
		return // Schema not preloaded - skip
	}

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
	var rowKey string

	// Determine operation type and capture values
	switch data.Op {
	case sqlite3.SQLITE_INSERT:
		operation = uint8(OpTypeInsert)
		if err := data.New(newDest...); err == nil {
			newValues = s.buildValueMap(schema.columns, newDest)
		}
		pkValues := s.extractPKValuesFromDest(schema, newDest, data.NewRowID)
		rowKey = s.serializePK(data.TableName, schema, pkValues)

	case sqlite3.SQLITE_UPDATE:
		operation = uint8(OpTypeUpdate)
		if err := data.Old(oldDest...); err == nil {
			oldValues = s.buildValueMap(schema.columns, oldDest)
		}
		if err := data.New(newDest...); err == nil {
			newValues = s.buildValueMap(schema.columns, newDest)
		}
		// Track both old and new PK for updates
		oldPK := s.extractPKValuesFromDest(schema, oldDest, data.OldRowID)
		newPK := s.extractPKValuesFromDest(schema, newDest, data.NewRowID)
		oldKey := s.serializePK(data.TableName, schema, oldPK)
		newKey := s.serializePK(data.TableName, schema, newPK)
		rowKey = newKey

		// Ensure collector exists for this table
		if _, ok := s.collectors[data.TableName]; !ok {
			s.collectors[data.TableName] = filter.NewKeyHashCollector()
		}
		s.collectors[data.TableName].AddRowKey(oldKey)
		if oldKey != newKey {
			s.collectors[data.TableName].AddRowKey(newKey)
		}

	case sqlite3.SQLITE_DELETE:
		operation = uint8(OpTypeDelete)
		if err := data.Old(oldDest...); err == nil {
			oldValues = s.buildValueMap(schema.columns, oldDest)
		}
		pkValues := s.extractPKValuesFromDest(schema, oldDest, data.OldRowID)
		rowKey = s.serializePK(data.TableName, schema, pkValues)

	default:
		return
	}

	// Ensure collector exists for this table (for non-UPDATE operations)
	if data.Op != sqlite3.SQLITE_UPDATE {
		if _, ok := s.collectors[data.TableName]; !ok {
			s.collectors[data.TableName] = filter.NewKeyHashCollector()
		}
		s.collectors[data.TableName].AddRowKey(rowKey)
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
	s.metaStore.WriteIntentEntry(s.txnID, s.seq, operation, data.TableName, rowKey, oldMsgpack, newMsgpack)
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
// PK values are converted to string representation for row key generation,
// ensuring compatibility with AST-based path which uses string values.
func (s *EphemeralHookSession) extractPKValuesFromDest(schema *tableSchema, dest []interface{}, rowID int64) map[string][]byte {
	pkValues := make(map[string][]byte)

	for i, idx := range schema.pkIndices {
		colName := schema.pkColumns[i]
		if idx == -1 {
			// rowid case
			pkValues[colName] = []byte(fmt.Sprintf("%d", rowID))
		} else if idx < len(dest) {
			val := dest[idx]
			if ptr, ok := val.(*interface{}); ok {
				val = *ptr
			}
			if val != nil {
				// Convert to string representation for row key compatibility
				// This ensures hook and AST paths produce identical keys
				pkValues[colName] = pkValueToBytes(val)
			}
		}
	}

	return pkValues
}

// pkValueToBytes converts a primary key value to its string byte representation.
// Used for row key generation where values must match AST-based path format.
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
func (s *EphemeralHookSession) serializePK(table string, schema *tableSchema, pkValues map[string][]byte) string {
	return filter.SerializeRowKey(table, schema.pkColumns, pkValues)
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

// GetKeyHashes returns XXH64 hashes of affected row keys per table.
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
	RowKey    string
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
