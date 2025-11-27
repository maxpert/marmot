//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/maxpert/marmot/protocol/intentlog"
)

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
type EphemeralHookSession struct {
	conn        *sql.Conn                             // Dedicated DB connection (closed on end)
	tx          *sql.Tx                               // Active transaction
	log         *intentlog.Log                        // Intent log for this session
	builders    map[string]*filter.BloomFilterBuilder // table -> builder
	schemaCache *SchemaCache                          // Shared schema cache
	mu          sync.Mutex
}

// StartEphemeralSession creates a new CDC capture session with a dedicated connection.
// The session owns the connection and will close it when done.
func StartEphemeralSession(ctx context.Context, db *sql.DB, schemaCache *SchemaCache, txnID uint64, dataDir string, tables []string) (*EphemeralHookSession, error) {
	// Get a dedicated connection for this session
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	// Create intent log
	log, err := intentlog.New(txnID, dataDir)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create intent log: %w", err)
	}

	session := &EphemeralHookSession{
		conn:        conn,
		log:         log,
		builders:    make(map[string]*filter.BloomFilterBuilder),
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
		log.Delete()
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

	// Close intent log (no fsync - already flushed if needed for multi-row)
	// Then delete the log file - it's no longer needed after commit
	if s.log != nil {
		s.log.Close()
		s.log.Delete()
	}

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

	// Close and delete intent log on rollback
	if s.log != nil {
		s.log.Close()
		s.log.Delete()
	}

	return txErr
}

// FlushIntentLog fsyncs the intent log to disk.
// Call this ONLY for multi-row operations before starting 2PC.
// Single-row operations skip fsync for performance.
func (s *EphemeralHookSession) FlushIntentLog() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.log != nil {
		return s.log.Flush()
	}
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

	// Prepare destinations for column values
	oldDest := make([]interface{}, colCount)
	newDest := make([]interface{}, colCount)
	for i := 0; i < colCount; i++ {
		oldDest[i] = new(interface{})
		newDest[i] = new(interface{})
	}

	entry := &intentlog.Entry{
		Table: data.TableName,
	}

	// Determine operation type
	switch data.Op {
	case sqlite3.SQLITE_INSERT:
		entry.Operation = intentlog.OpInsert
		if err := data.New(newDest...); err == nil {
			entry.NewValues = s.buildValueMap(schema.columns, newDest)
		}
	case sqlite3.SQLITE_UPDATE:
		entry.Operation = intentlog.OpUpdate
		if err := data.Old(oldDest...); err == nil {
			entry.OldValues = s.buildValueMap(schema.columns, oldDest)
		}
		if err := data.New(newDest...); err == nil {
			entry.NewValues = s.buildValueMap(schema.columns, newDest)
		}
	case sqlite3.SQLITE_DELETE:
		entry.Operation = intentlog.OpDelete
		if err := data.Old(oldDest...); err == nil {
			entry.OldValues = s.buildValueMap(schema.columns, oldDest)
		}
	default:
		return
	}

	// Ensure builder exists for this table
	if _, ok := s.builders[data.TableName]; !ok {
		s.builders[data.TableName] = filter.NewBloomFilterBuilder()
	}

	// Build row key from primary key values
	if data.Op == sqlite3.SQLITE_UPDATE {
		// Track both old and new PK for updates
		oldPK := s.extractPKValuesFromDest(schema, oldDest, data.OldRowID)
		newPK := s.extractPKValuesFromDest(schema, newDest, data.NewRowID)
		oldKey := s.serializePK(data.TableName, schema, oldPK)
		newKey := s.serializePK(data.TableName, schema, newPK)

		entry.RowKey = newKey
		s.builders[data.TableName].AddRowKey(oldKey)
		if oldKey != newKey {
			s.builders[data.TableName].AddRowKey(newKey)
		}
	} else {
		pkValues := s.extractPKValues(schema, oldDest, newDest, data.Op, data.OldRowID, data.NewRowID)
		entry.RowKey = s.serializePK(data.TableName, schema, pkValues)
		s.builders[data.TableName].AddRowKey(entry.RowKey)
	}

	// Append to intent log
	s.log.Append(entry)
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

// serializeValue converts a value to bytes with deterministic encoding
func (s *EphemeralHookSession) serializeValue(v interface{}) []byte {
	switch val := v.(type) {
	case nil:
		return nil
	case []byte:
		return val
	case string:
		return []byte(val)
	case int64:
		return []byte(strconv.FormatInt(val, 10))
	case int:
		return []byte(strconv.Itoa(val))
	case float64:
		return []byte(strconv.FormatFloat(val, 'g', -1, 64))
	case float32:
		return []byte(strconv.FormatFloat(float64(val), 'g', -1, 32))
	case bool:
		if val {
			return []byte("1")
		}
		return []byte("0")
	default:
		data, _ := json.Marshal(val)
		return data
	}
}

// extractPKValuesFromDest extracts primary key values from a destination slice
func (s *EphemeralHookSession) extractPKValuesFromDest(schema *tableSchema, dest []interface{}, rowID int64) map[string][]byte {
	pkValues := make(map[string][]byte)

	for i, idx := range schema.pkIndices {
		colName := schema.pkColumns[i]
		if idx == -1 {
			pkValues[colName] = []byte(fmt.Sprintf("%d", rowID))
		} else if idx < len(dest) {
			val := dest[idx]
			if ptr, ok := val.(*interface{}); ok {
				val = *ptr
			}
			if val != nil {
				pkValues[colName] = s.serializeValue(val)
			}
		}
	}

	return pkValues
}

// extractPKValues extracts primary key values based on operation type
func (s *EphemeralHookSession) extractPKValues(schema *tableSchema, oldDest, newDest []interface{}, op int, oldRowID, newRowID int64) map[string][]byte {
	if op == sqlite3.SQLITE_DELETE {
		return s.extractPKValuesFromDest(schema, oldDest, oldRowID)
	}
	return s.extractPKValuesFromDest(schema, newDest, newRowID)
}

// serializePK creates a deterministic string key from PK values
func (s *EphemeralHookSession) serializePK(table string, schema *tableSchema, pkValues map[string][]byte) string {
	return filter.SerializeRowKey(table, schema.pkColumns, pkValues)
}

// BuildFilters builds Bloom filters for each affected table
func (s *EphemeralHookSession) BuildFilters() map[string]*filter.BloomFilter {
	s.mu.Lock()
	defer s.mu.Unlock()

	filters := make(map[string]*filter.BloomFilter)
	for table, builder := range s.builders {
		filters[table] = builder.Build()
	}
	return filters
}

// GetRowCounts returns the number of affected rows per table
func (s *EphemeralHookSession) GetRowCounts() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	counts := make(map[string]int64)
	for table, builder := range s.builders {
		counts[table] = int64(builder.Count())
	}
	return counts
}

// GetIntentLog returns the intent log for streaming
func (s *EphemeralHookSession) GetIntentLog() *intentlog.Log {
	return s.log
}
