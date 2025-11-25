package protocol

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// TableSchema represents the schema of a table
type TableSchema struct {
	TableName     string
	PrimaryKeys   []string // Column names that form the primary key
	SchemaVersion uint64   // Version from table metadata
	ColumnTypes   map[string]string
}

// SchemaProvider queries table schema information from SQLite
// SQLite already caches this information, so we don't need our own cache
type SchemaProvider struct {
	db *sql.DB
	mu sync.RWMutex
}

// NewSchemaProvider creates a new schema provider
func NewSchemaProvider(db *sql.DB) *SchemaProvider {
	return &SchemaProvider{
		db: db,
	}
}

// GetTableSchema queries SQLite for table schema
// This is fast because SQLite caches schema information internally
func (sp *SchemaProvider) GetTableSchema(tableName string) (*TableSchema, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if sp.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	// Query PRAGMA table_info to get column information
	// This includes primary key information (pk column indicates PK order)
	rows, err := sp.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
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

	// Get schema version from table metadata if available
	// For now, we'll use a hash of the schema as version
	schema.SchemaVersion = sp.calculateSchemaVersion(schema)

	return schema, nil
}

// calculateSchemaVersion creates a deterministic version number from schema
// This ensures all nodes agree on schema version
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

// GenerateRowKey generates a deterministic row key from primary key values
// For composite keys: sorts PK columns by name, then hashes concatenated values
// This ensures all nodes generate the same row key for the same row
func GenerateRowKey(schema *TableSchema, values map[string][]byte) (string, error) {
	if len(schema.PrimaryKeys) == 0 {
		return "", fmt.Errorf("no primary keys defined for table %s", schema.TableName)
	}

	// For single-column PK, use the value directly (after JSON unmarshal)
	if len(schema.PrimaryKeys) == 1 {
		pkCol := schema.PrimaryKeys[0]
		pkValue, ok := values[pkCol]
		if !ok {
			return "", fmt.Errorf("primary key column %s not found in values", pkCol)
		}

		// Unmarshal JSON to get actual value
		var val string
		if err := json.Unmarshal(pkValue, &val); err != nil {
			// If not JSON, use raw bytes
			val = string(pkValue)
		}

		return val, nil
	}

	// For composite keys: sort PK columns by name for determinism
	sortedPKs := make([]string, len(schema.PrimaryKeys))
	copy(sortedPKs, schema.PrimaryKeys)
	sort.Strings(sortedPKs)

	// Build composite key: concatenate sorted PK values
	var keyBuilder strings.Builder
	for i, pkCol := range sortedPKs {
		pkValue, ok := values[pkCol]
		if !ok {
			return "", fmt.Errorf("primary key column %s not found in values", pkCol)
		}

		// Unmarshal JSON to get actual value
		var val string
		if err := json.Unmarshal(pkValue, &val); err != nil {
			// If not JSON, use raw bytes as hex
			val = hex.EncodeToString(pkValue)
		}

		if i > 0 {
			keyBuilder.WriteString(":")
		}
		keyBuilder.WriteString(val)
	}

	compositeKey := keyBuilder.String()

	// Hash the composite key for consistent length and format
	hash := sha256.Sum256([]byte(compositeKey))
	return hex.EncodeToString(hash[:]), nil
}

// ValidateRowKey validates that a row key matches expected schema version
func ValidateRowKey(schema *TableSchema, rowKey string, expectedVersion uint64) error {
	if schema.SchemaVersion != expectedVersion {
		return fmt.Errorf("schema version mismatch: have %d, expected %d",
			schema.SchemaVersion, expectedVersion)
	}

	if rowKey == "" {
		return fmt.Errorf("empty row key")
	}

	return nil
}
