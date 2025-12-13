package protocol

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/protocol/filter"
)

// TableSchema represents the schema of a table for SQL transpilation and intent key generation.
// This is different from db.TableSchema which is optimized for CDC preupdate hooks.
//
// NOTE: For new code, prefer using db.SchemaCache for accessing table schemas.
// SchemaProvider is kept for backward compatibility with existing code that uses SQL queries.
type TableSchema struct {
	TableName        string
	PrimaryKeys      []string // Column names that form the primary key
	SchemaVersion    uint64   // Version from table metadata
	ColumnTypes      map[string]string
	AutoIncrementCol string // Column name if single INTEGER PRIMARY KEY (SQLite rowid alias)
}

// Querier is an interface for database query operations.
// Both *sql.DB and *sql.Tx implement this interface.
type Querier interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// SchemaProvider queries table schema information from SQLite.
// SQLite already caches this information, so we don't need our own cache.
//
// DEPRECATED: New code should use db.SchemaCache instead, which provides
// efficient schema caching for CDC operations. SchemaProvider is kept for
// backward compatibility with code that needs to query schemas via SQL.
type SchemaProvider struct {
	querier Querier
	mu      sync.RWMutex
}

// NewSchemaProvider creates a new schema provider from *sql.DB.
//
// DEPRECATED: Use db.SchemaCache for new code.
func NewSchemaProvider(db *sql.DB) *SchemaProvider {
	return &SchemaProvider{
		querier: db,
	}
}

// NewSchemaProviderFromQuerier creates a schema provider from any Querier.
//
// DEPRECATED: Use db.SchemaCache for new code.
func NewSchemaProviderFromQuerier(q Querier) *SchemaProvider {
	return &SchemaProvider{
		querier: q,
	}
}

// GetTableSchema queries SQLite for table schema.
// This is fast because SQLite caches schema information internally.
//
// DEPRECATED: Use db.SchemaCache.GetSchemaFor() for new code.
func (sp *SchemaProvider) GetTableSchema(tableName string) (*TableSchema, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if sp.querier == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	// Query PRAGMA table_info to get column information
	// This includes primary key information (pk column indicates PK order)
	rows, err := sp.querier.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to query table info: %w", err)
	}
	defer rows.Close()

	schema := &TableSchema{
		TableName:   tableName,
		PrimaryKeys: make([]string, 0),
		ColumnTypes: make(map[string]string),
	}

	type pkColumn struct {
		name  string
		order int
	}
	pkColumns := make([]pkColumn, 0)

	for rows.Next() {
		var cid int
		var name string
		var colType string
		var notNull int
		var dfltValue sql.NullString
		var pk int

		err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		schema.ColumnTypes[name] = colType

		// pk > 0 means this column is part of primary key
		// pk value indicates the order in composite key
		if pk > 0 {
			pkColumns = append(pkColumns, pkColumn{name: name, order: pk})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column info: %w", err)
	}

	// Sort PK columns by their order in the composite key
	sort.Slice(pkColumns, func(i, j int) bool {
		return pkColumns[i].order < pkColumns[j].order
	})

	// Extract just the column names in order
	for _, pk := range pkColumns {
		schema.PrimaryKeys = append(schema.PrimaryKeys, pk.name)
	}

	if len(schema.PrimaryKeys) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", tableName)
	}

	// Detect auto-increment: single INTEGER or BIGINT PRIMARY KEY
	// - INTEGER PRIMARY KEY: SQLite rowid alias (native auto-increment)
	// - BIGINT PRIMARY KEY: Marmot's transformed auto-increment columns
	//   (INT AUTO_INCREMENT is transformed to BIGINT by IntToBigintRule for 64-bit HLC IDs)
	if len(pkColumns) == 1 {
		pkName := pkColumns[0].name
		pkType := strings.ToUpper(schema.ColumnTypes[pkName])
		if pkType == "INTEGER" || pkType == "BIGINT" {
			schema.AutoIncrementCol = pkName
		}
	}

	// Get schema version from table metadata if available
	// For now, we'll use a hash of the schema as version
	schema.SchemaVersion = sp.calculateSchemaVersion(schema)

	return schema, nil
}

// calculateSchemaVersion creates a deterministic version number from schema.
// This ensures all nodes agree on schema version.
func (sp *SchemaProvider) calculateSchemaVersion(schema *TableSchema) uint64 {
	// Create a deterministic string representation of schema
	var b strings.Builder
	b.WriteString(schema.TableName)
	b.WriteString(":")

	// Sort primary keys by name for determinism
	sortedPKs := make([]string, len(schema.PrimaryKeys))
	copy(sortedPKs, schema.PrimaryKeys)
	sort.Strings(sortedPKs)

	for _, pk := range sortedPKs {
		b.WriteString(pk)
		b.WriteString(",")
	}

	// Hash it to get a version number
	hash := sha256.Sum256([]byte(b.String()))
	// Use first 8 bytes as uint64
	version := uint64(0)
	for i := 0; i < 8; i++ {
		version = (version << 8) | uint64(hash[i])
	}

	return version
}

// ErrMissingPrimaryKey is returned when PK columns are not in the values map
// This typically happens for INSERT statements with auto-increment PKs
var ErrMissingPrimaryKey = fmt.Errorf("primary key columns not present in values")

// GenerateIntentKey generates a deterministic intent key from primary key values.
// This is used by the AST-based path where values may be JSON-encoded.
// Delegates to filter.SerializeIntentKey after extracting JSON values.
//
// Returns ErrMissingPrimaryKey if any PK column is not present in values,
// or if the PK value is "0" (MySQL auto-increment placeholder).
// This is expected for INSERT statements with auto-increment PKs.
func GenerateIntentKey(schema *TableSchema, values map[string][]byte) (string, error) {
	if len(schema.PrimaryKeys) == 0 {
		return "", fmt.Errorf("no primary keys defined for table %s", schema.TableName)
	}

	// Check that all PK columns are present in values
	// If not, return ErrMissingPrimaryKey (e.g., for auto-increment PKs)
	for _, pkCol := range schema.PrimaryKeys {
		val, ok := values[pkCol]
		if !ok {
			return "", ErrMissingPrimaryKey
		}
		// MySQL treats id=0 as "use auto-increment" - treat it the same as missing
		// This is for compatibility with MySQL clients that use INSERT ... VALUES (0, ...)
		if isZeroValue(val) {
			return "", ErrMissingPrimaryKey
		}
	}

	// Extract JSON values and convert to raw bytes for serialization
	processedValues := make(map[string][]byte, len(values))
	for col, val := range values {
		if val == nil {
			continue
		}
		// Extract string from potentially JSON-encoded value
		processedValues[col] = []byte(extractStringValue(val))
	}

	return filter.SerializeIntentKey(schema.TableName, schema.PrimaryKeys, processedValues), nil
}

// extractStringValue extracts a string from a byte slice, handling msgpack encoding
func extractStringValue(pkValue []byte) string {
	// Try to unmarshal as msgpack string first
	var val string
	if err := encoding.Unmarshal(pkValue, &val); err != nil {
		// If not msgpack, use raw bytes as string
		val = string(pkValue)
	}
	return val
}

// isZeroValue checks if a byte slice represents a zero value (integer 0)
// MySQL uses id=0 to indicate "use auto-increment next value"
// Values may be msgpack-encoded strings or raw bytes
func isZeroValue(val []byte) bool {
	if len(val) == 0 {
		return false
	}
	// Try to unmarshal as msgpack string first
	var s string
	if err := encoding.Unmarshal(val, &s); err != nil {
		// If not msgpack, use raw bytes as string
		s = string(val)
	}
	return s == "0"
}

// ValidateIntentKey validates that an intent key matches expected schema version
func ValidateIntentKey(schema *TableSchema, intentKey string, expectedVersion uint64) error {
	if schema.SchemaVersion != expectedVersion {
		return fmt.Errorf("schema version mismatch: have %d, expected %d",
			schema.SchemaVersion, expectedVersion)
	}

	if intentKey == "" {
		return fmt.Errorf("empty intent key")
	}

	return nil
}

// GetAutoIncrementColumn returns the auto-increment column name for a table.
// Returns empty string if the table has no auto-increment column.
// A table has auto-increment if it has exactly one primary key column of type INTEGER.
func (sp *SchemaProvider) GetAutoIncrementColumn(tableName string) (string, error) {
	schema, err := sp.GetTableSchema(tableName)
	if err != nil {
		return "", err
	}
	return schema.AutoIncrementCol, nil
}
