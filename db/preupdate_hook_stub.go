//go:build !sqlite_preupdate_hook
// +build !sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/mattn/go-sqlite3"
)

var ErrPreupdateHookNotEnabled = errors.New("preupdate hook requires build tag: sqlite_preupdate_hook")

// TableSchema represents a lightweight schema for CDC hooks (stub).
type TableSchema struct {
	Columns     []string
	PrimaryKeys []string
	PKIndices   []int
}

// SchemaCache is a stub when preupdate hook is not enabled
type SchemaCache struct {
	mu    sync.RWMutex
	cache map[string]*TableSchema
}

// NewSchemaCache creates a stub schema cache
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		cache: make(map[string]*TableSchema),
	}
}

// GetSchemaFor returns cached schema information for a table.
func (c *SchemaCache) GetSchemaFor(tableName string) (*TableSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	schema, ok := c.cache[tableName]
	if !ok {
		return nil, ErrSchemaCacheMiss{Table: tableName}
	}
	return schema, nil
}

// Reload loads schema metadata from SQLite using PRAGMA table_info.
// Even without preupdate hooks, this enables CDC applier paths in batch commit/replay.
func (c *SchemaCache) Reload(conn interface{}) error {
	sqliteConn, ok := conn.(*sqlite3.SQLiteConn)
	if !ok {
		return fmt.Errorf("unexpected connection type %T", conn)
	}

	rows, err := sqliteConn.Query(
		"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '__marmot%'",
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	newCache := make(map[string]*TableSchema)
	dest := make([]driver.Value, 1)
	for {
		if err := rows.Next(dest); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read table names: %w", err)
		}
		tableName, ok := dest[0].(string)
		if !ok || tableName == "" {
			continue
		}
		schema, err := loadStubSchema(sqliteConn, tableName)
		if err != nil {
			return err
		}
		newCache[tableName] = schema
	}

	c.mu.Lock()
	c.cache = newCache
	c.mu.Unlock()
	return nil
}

// Clear is a no-op (stub)
func (c *SchemaCache) Clear() {}

// LoadTable loads a single table schema into cache.
func (c *SchemaCache) LoadTable(conn interface{}, tableName string) error {
	sqliteConn, ok := conn.(*sqlite3.SQLiteConn)
	if !ok {
		return fmt.Errorf("unexpected connection type %T", conn)
	}
	schema, err := loadStubSchema(sqliteConn, tableName)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.cache[tableName] = schema
	c.mu.Unlock()
	return nil
}

func loadStubSchema(conn *sqlite3.SQLiteConn, tableName string) (*TableSchema, error) {
	rows, err := conn.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName), nil)
	if err != nil {
		return nil, fmt.Errorf("query table_info for %s: %w", tableName, err)
	}
	defer rows.Close()

	schema := &TableSchema{
		Columns:     make([]string, 0),
		PrimaryKeys: make([]string, 0),
		PKIndices:   make([]int, 0),
	}

	type pkInfo struct {
		name  string
		order int
		index int
	}
	var pkCols []pkInfo
	colIndex := 0

	dest := make([]driver.Value, 6)
	for {
		if err := rows.Next(dest); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read table_info for %s: %w", tableName, err)
		}

		name, _ := dest[1].(string)
		pk, _ := dest[5].(int64)
		if name == "" {
			continue
		}

		schema.Columns = append(schema.Columns, name)
		if pk > 0 {
			pkCols = append(pkCols, pkInfo{name: name, order: int(pk), index: colIndex})
		}
		colIndex++
	}

	// Stable ordering for composite primary keys.
	for i := 0; i < len(pkCols)-1; i++ {
		for j := i + 1; j < len(pkCols); j++ {
			if pkCols[i].order > pkCols[j].order {
				pkCols[i], pkCols[j] = pkCols[j], pkCols[i]
			}
		}
	}
	for _, pk := range pkCols {
		schema.PrimaryKeys = append(schema.PrimaryKeys, pk.name)
		schema.PKIndices = append(schema.PKIndices, pk.index)
	}

	// Rowid sentinel when table has no explicit PK.
	if len(schema.PrimaryKeys) == 0 {
		schema.PrimaryKeys = []string{"rowid"}
		schema.PKIndices = []int{-1}
	}

	return schema, nil
}

// EphemeralHookSession is a stub when preupdate hook is not enabled
type EphemeralHookSession struct{}

// StartEphemeralSession returns an error when preupdate hook is not enabled
func StartEphemeralSession(ctx context.Context, userDB *sql.DB, metaStore MetaStore, schemaCache *SchemaCache, txnID uint64) (*EphemeralHookSession, error) {
	return nil, ErrPreupdateHookNotEnabled
}

// BeginTx returns an error (stub)
func (s *EphemeralHookSession) BeginTx(ctx context.Context) error {
	return ErrPreupdateHookNotEnabled
}

// ExecContext returns an error (stub)
func (s *EphemeralHookSession) ExecContext(ctx context.Context, query string, args ...interface{}) error {
	return ErrPreupdateHookNotEnabled
}

// Commit returns an error (stub)
func (s *EphemeralHookSession) Commit() error {
	return ErrPreupdateHookNotEnabled
}

// Rollback returns an error (stub)
func (s *EphemeralHookSession) Rollback() error {
	return ErrPreupdateHookNotEnabled
}

// IntentEntry represents a CDC entry (stub)
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

// GetIntentEntries returns nil (stub)
func (s *EphemeralHookSession) GetIntentEntries() ([]*IntentEntry, error) {
	return nil, ErrPreupdateHookNotEnabled
}

// GetTxnID returns 0 (stub)
func (s *EphemeralHookSession) GetTxnID() uint64 {
	return 0
}

// GetLastInsertId returns 0 (stub)
func (s *EphemeralHookSession) GetLastInsertId() int64 {
	return 0
}

// GetConflictError returns nil (stub - no CDC in stub mode)
func (s *EphemeralHookSession) GetConflictError() error {
	return nil
}

// ClearConflictError is a no-op (stub)
func (s *EphemeralHookSession) ClearConflictError() {
}
