//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/rs/zerolog/log"
)

// TableSchema represents complete table metadata for Marmot.
// This is the CANONICAL schema type - all packages should use this or adapters.
//
// HOT PATH FIELDS (used by preupdate hook - must be fast):
//   - Columns: Column names for value encoding
//   - PrimaryKeys: PK column names for intent key generation
//   - PKIndices: Indices into Columns for PKs (-1 for rowid)
//
// COLD PATH FIELDS (used by CDC publisher, SQL transpilation):
//   - FullColumns: Complete column metadata including types
//   - TableName: Table name for schema version calculation
//   - AutoIncrementCol: Single INTEGER PK column (rowid alias)
//
// DO NOT create alternative schema types in other packages.
// Use view methods (ToPublisherSchema, GetColumnTypes) or adapters.
type TableSchema struct {
	// Hot path fields - preupdate hook performance critical
	Columns         []string // Column names in declaration order
	PrimaryKeys     []string // PK column names in PK order
	PKIndices       []int    // Indices into Columns for PKs (-1 for rowid)
	IntentKeyPrefix []byte   // Precomputed: version(1) + uvarint(tableLen) + table

	// Cold path fields - populated for CDC publisher, transpilation
	FullColumns      []ColumnSchema // Full column metadata
	TableName        string         // Table name (for version calculation)
	AutoIncrementCol string         // Single INTEGER PK column (empty if none)
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

// loadSchema fetches schema from DB using the raw SQLite connection.
// Extracts ALL 6 columns from PRAGMA table_info:
//   - cid: column index
//   - name: column name
//   - type: column type affinity
//   - notnull: 1 if NOT NULL constraint
//   - dflt_value: default value (ignored)
//   - pk: primary key order (1-based, 0 if not PK)
func loadSchema(conn *sqlite3.SQLiteConn, tableName string) (*TableSchema, error) {
	rows, err := conn.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName), nil)
	if err != nil {
		return nil, fmt.Errorf("query table_info: %w", err)
	}
	defer rows.Close()

	schema := &TableSchema{
		TableName:   tableName,
		Columns:     make([]string, 0),
		PrimaryKeys: make([]string, 0),
		PKIndices:   make([]int, 0),
		FullColumns: make([]ColumnSchema, 0),
	}

	// Track PK columns with their order for proper sorting
	type pkInfo struct {
		name  string
		order int
		index int // column index
	}
	var pkColumns []pkInfo

	dest := make([]driver.Value, 6)
	colIndex := 0
	for {
		if err := rows.Next(dest); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read table_info row: %w", err)
		}

		// Extract all 6 PRAGMA columns
		name, _ := dest[1].(string)
		colType, _ := dest[2].(string)
		notNull, _ := dest[3].(int64)
		pk, _ := dest[5].(int64)

		// Hot path fields
		schema.Columns = append(schema.Columns, name)

		// Cold path fields - full column metadata
		col := ColumnSchema{
			Name:     name,
			Type:     colType,
			Nullable: notNull == 0,
			IsPK:     pk > 0,
			PKOrder:  int(pk),
		}
		schema.FullColumns = append(schema.FullColumns, col)

		// Track PK columns for sorting
		if pk > 0 {
			pkColumns = append(pkColumns, pkInfo{
				name:  name,
				order: int(pk),
				index: colIndex,
			})
		}
		colIndex++
	}

	// Sort PKs by their order in composite key
	if len(pkColumns) > 0 {
		for i := 0; i < len(pkColumns)-1; i++ {
			for j := i + 1; j < len(pkColumns); j++ {
				if pkColumns[i].order > pkColumns[j].order {
					pkColumns[i], pkColumns[j] = pkColumns[j], pkColumns[i]
				}
			}
		}
		for _, pk := range pkColumns {
			schema.PrimaryKeys = append(schema.PrimaryKeys, pk.name)
			schema.PKIndices = append(schema.PKIndices, pk.index)
		}
	}

	// Handle tables with no explicit PK (use rowid)
	if len(schema.PrimaryKeys) == 0 {
		schema.PrimaryKeys = []string{"rowid"}
		schema.PKIndices = []int{-1}
	}

	// Detect auto-increment: single INTEGER PRIMARY KEY = rowid alias
	if len(pkColumns) == 1 {
		pkName := pkColumns[0].name
		for _, col := range schema.FullColumns {
			if col.Name == pkName {
				// INTEGER PRIMARY KEY is SQLite's rowid alias (auto-increment)
				// Also accept BIGINT for Marmot's transformed auto-increment columns
				upperType := strings.ToUpper(col.Type)
				if upperType == "INTEGER" || upperType == "BIGINT" {
					schema.AutoIncrementCol = pkName
				}
				break
			}
		}
	}

	// Build precomputed intent key prefix for binary encoding
	schema.IntentKeyPrefix = filter.BuildIntentKeyPrefix(tableName)

	return schema, nil
}
