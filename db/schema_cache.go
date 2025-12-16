//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"database/sql/driver"
	"fmt"
	"io"
	"sync"

	"github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

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

	newCache := make(map[string]*TableSchema)
	for _, tableName := range tableNames {
		schema, err := loadSchema(conn, tableName)
		if err != nil {
			log.Warn().Err(err).Str("table", tableName).Msg("Failed to load schema during reload")
			continue
		}
		newCache[tableName] = schema
	}

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

	if len(schema.PrimaryKeys) == 0 {
		schema.PrimaryKeys = []string{"rowid"}
		schema.PKIndices = []int{-1}
	}

	return schema, nil
}
