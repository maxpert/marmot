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
	"github.com/maxpert/marmot/protocol/determinism"
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
	Columns          []string // Column names in declaration order, excluding generated (VIRTUAL/STORED) columns
	ColumnPositions  []int    // Parallel to Columns: each column's TRUE ordinal position (PRAGMA table_xinfo cid), i.e. its index into the preupdate hook's raw value array. NOT the same as the index into Columns whenever the table has any generated column - see loadSchema.
	PrimaryKeys      []string // PK column names in PK order
	PKIndices        []int    // Indices into the preupdate hook's raw value array for PKs (-1 for rowid); same true-cid space as ColumnPositions
	IntentKeyPrefix  []byte   // Precomputed: version(1) + uvarint(tableLen) + table
	BlobAffinityCols []bool   // Parallel to Columns: true where the declared type has BLOB affinity.
	// CDC capture cannot tell TEXT from BLOB storage class by value alone (the
	// preupdate hook hands both back as Go []byte), so this precomputed lookup
	// lets encodeValuesWithSchema pick msgpack Bin (BLOB affinity) vs. Str
	// (every other affinity) without parsing the declared type on every row.
	// Only BLOB affinity is singled out - per SQLite's affinity rules, it is
	// the only one that never coerces a value on INSERT, so it is the only
	// affinity where a captured []byte is more likely to be genuine BLOB
	// storage class than TEXT. See encodeValuesWithSchema in preupdate_hook.go.

	// VirtualColumns lists GENERATED ALWAYS AS (...) VIRTUAL column names.
	// go-sqlite3's preupdate hook segfaults reading a virtual column's value
	// (sqlite3_preupdate_new/old returns NULL for it), so hookCallback must
	// refuse to capture CDC for tables that have any. STORED generated columns
	// are unaffected and are not included here.
	VirtualColumns []string

	// Cold path fields - populated for CDC publisher, transpilation
	FullColumns      []ColumnSchema // Full column metadata
	TableName        string         // Table name (for version calculation)
	AutoIncrementCol string         // Single INTEGER PK column (empty if none)

	// For determinism checking - cold path
	CreateSQL string   // Original CREATE TABLE statement from sqlite_master
	Triggers  []string // CREATE TRIGGER statements affecting this table
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

	// Query triggers for each table
	for tableName, schema := range newCache {
		triggerRows, err := conn.Query(
			"SELECT sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
			[]driver.Value{tableName})
		if err != nil {
			continue
		}

		dest := make([]driver.Value, 1)
		for {
			if err := triggerRows.Next(dest); err != nil {
				break
			}
			if sqlText, ok := dest[0].(string); ok && sqlText != "" {
				schema.Triggers = append(schema.Triggers, sqlText)
			}
		}
		triggerRows.Close()
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

// IsEmpty reports whether the cache currently has any table schema entries.
func (c *SchemaCache) IsEmpty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache) == 0
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
// Uses PRAGMA table_xinfo (a superset of table_info) rather than table_info:
//   - cid: column's TRUE ordinal position in the table, including hidden
//     (generated) columns. This is the index the preupdate hook's
//     Old()/New() use, and it does NOT match table_info's cid, which
//     silently renumbers columns after excluding hidden ones (verified
//     directly: a table with a generated column between two normal columns
//     gets cid 0,1,2 from table_info but the normal columns' true positions
//     are 0,2 - table_info's cid is useless for aligning against the
//     preupdate hook's raw value array whenever any generated column exists).
//   - name, type, notnull, pk: same as table_info.
//   - hidden: 0 normal, 1 a virtual table's own hidden pseudocolumn (e.g.
//     FTS5's table-name/rank columns), 2 GENERATED ALWAYS ... VIRTUAL,
//     3 ... STORED.
//
// Only hidden == 0 columns end up in Columns/FullColumns/PK tracking - the
// same set table_info already produced (it silently excludes every hidden
// kind), so this is not a behavior change for what counts as a "real"
// column. hidden == 3 (STORED) is excluded because SQLite rejects an
// explicit INSERT/UPDATE of a generated column ("cannot INSERT into
// generated column", verified directly) and its value is deterministically
// recomputed by SQLite from a row's other captured columns on every replica,
// so capturing and applying it would be both wrong and unnecessary.
// hidden == 1 pseudocolumns are query-only with no real storage to capture.
// VIRTUAL (hidden == 2) column names are recorded into VirtualColumns
// instead: go-sqlite3's preupdate hook cannot safely read their value at all
// (see hookCallback), so any table with one refuses CDC capture entirely
// rather than attempting a partial (and equally wrong) capture.
func loadSchema(conn *sqlite3.SQLiteConn, tableName string) (*TableSchema, error) {
	rows, err := conn.Query(fmt.Sprintf("PRAGMA table_xinfo(%s)", tableName), nil)
	if err != nil {
		return nil, fmt.Errorf("query table_xinfo: %w", err)
	}
	defer rows.Close()

	schema := &TableSchema{
		TableName:        tableName,
		Columns:          make([]string, 0),
		ColumnPositions:  make([]int, 0),
		PrimaryKeys:      make([]string, 0),
		PKIndices:        make([]int, 0),
		FullColumns:      make([]ColumnSchema, 0),
		BlobAffinityCols: make([]bool, 0),
	}

	// Track PK columns with their order for proper sorting
	type pkInfo struct {
		name  string
		order int
		index int // true cid: position into the preupdate hook's raw value array
	}
	var pkColumns []pkInfo

	const (
		hiddenNone    = 0
		hiddenVirtual = 2
	)

	dest := make([]driver.Value, 7)
	for {
		if err := rows.Next(dest); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read table_xinfo row: %w", err)
		}

		cid, _ := dest[0].(int64)
		name, _ := dest[1].(string)
		colType, _ := dest[2].(string)
		notNull, _ := dest[3].(int64)
		pk, _ := dest[5].(int64)
		hidden, _ := dest[6].(int64)

		if hidden == hiddenVirtual {
			schema.VirtualColumns = append(schema.VirtualColumns, name)
			continue
		}
		if hidden != hiddenNone {
			// hidden == 3 (GENERATED ... STORED): recomputed by SQLite itself
			// from the row's other columns, so it is neither captured nor
			// applied (see loadSchema's doc comment).
			// hidden == 1 (a virtual table's own HIDDEN column, e.g. FTS5's
			// table-name/rank pseudocolumns): query-only, no real storage to
			// capture. Matches table_info, which already excluded these.
			continue
		}
		// hidden == 0 (normal column): falls through and is captured below.

		// Hot path fields
		schema.Columns = append(schema.Columns, name)
		schema.ColumnPositions = append(schema.ColumnPositions, int(cid))
		schema.BlobAffinityCols = append(schema.BlobAffinityCols, isBlobAffinity(colType))

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
				index: int(cid),
			})
		}
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

	// Query CREATE TABLE statement from sqlite_master for determinism checking
	createRows, err := conn.Query(
		"SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
		[]driver.Value{tableName})
	if err == nil {
		defer createRows.Close()
		dest := make([]driver.Value, 1)
		if createRows.Next(dest) == nil {
			if sqlText, ok := dest[0].(string); ok {
				schema.CreateSQL = sqlText
			}
		}
	}

	return schema, nil
}

// isBlobAffinity reports whether a SQLite declared column type has BLOB
// affinity, using the exact precedence order from
// https://sqlite.org/datatype3.html#determination_of_column_affinity:
//  1. Contains "INT" -> INTEGER affinity (checked first, so e.g. "POINT"
//     is INTEGER, not BLOB, even though it contains none of the BLOB
//     markers - this rule must run before any of the others).
//  2. Contains "CHAR", "CLOB", or "TEXT" -> TEXT affinity.
//  3. Contains "BLOB", or no declared type at all -> BLOB affinity.
//  4. Contains "REAL", "FLOA", or "DOUB" -> REAL affinity.
//  5. Otherwise -> NUMERIC affinity.
//
// Only step 3 returns true here; every other affinity returns false. This
// intentionally is not a general affinity classifier (steps 4-5 are folded
// into a single "false"), because encodeValuesWithSchema only needs the
// BLOB/not-BLOB distinction.
func isBlobAffinity(declType string) bool {
	d := strings.ToUpper(declType)
	switch {
	case strings.Contains(d, "INT"):
		return false
	case strings.Contains(d, "CHAR"), strings.Contains(d, "CLOB"), strings.Contains(d, "TEXT"):
		return false
	case strings.Contains(d, "BLOB"), d == "":
		return true
	default:
		return false
	}
}

// BuildDeterminismSchema creates a determinism.Schema from cached table metadata.
// This is used for checking if DML statements are deterministic before execution.
func (c *SchemaCache) BuildDeterminismSchema() *determinism.Schema {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema := determinism.NewSchema()
	for _, tableSchema := range c.cache {
		if tableSchema.CreateSQL != "" {
			schema.AddTable(tableSchema.CreateSQL)
		}
		for _, triggerSQL := range tableSchema.Triggers {
			schema.AddTrigger(triggerSQL)
		}
	}
	return schema
}
