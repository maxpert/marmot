//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"crypto/sha256"
	"sort"
	"strings"

	"github.com/maxpert/marmot/publisher"
)

// ColumnSchema represents full metadata for a single column.
// This is the canonical column type - all other packages should use or adapt this.
//
// DO NOT create alternative column types in other packages.
// If you need a different shape, use view methods or adapters.
type ColumnSchema struct {
	Name     string // Column name
	Type     string // SQLite type affinity (TEXT, INTEGER, REAL, BLOB, NUMERIC)
	Nullable bool   // true if NULL allowed (notnull column == 0)
	IsPK     bool   // true if part of primary key (pk > 0)
	PKOrder  int    // 1-based order in composite PK (0 if not PK)
}

// ToPublisherSchema converts db.TableSchema to publisher.TableSchema.
// This is used by CDC publisher when transforming events.
func (ts *TableSchema) ToPublisherSchema() publisher.TableSchema {
	result := publisher.TableSchema{
		Columns: make([]publisher.ColumnInfo, 0, len(ts.FullColumns)),
	}

	// Use FullColumns if available (populated by loadSchema)
	if len(ts.FullColumns) > 0 {
		for _, col := range ts.FullColumns {
			result.Columns = append(result.Columns, publisher.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
				IsPK:     col.IsPK,
			})
		}
		return result
	}

	// Fallback for manually created schemas (tests)
	for i, colName := range ts.Columns {
		isPK := false
		for _, pkIdx := range ts.PKIndices {
			if pkIdx == i {
				isPK = true
				break
			}
		}

		result.Columns = append(result.Columns, publisher.ColumnInfo{
			Name:     colName,
			Type:     "", // Not available in lightweight schema
			Nullable: true,
			IsPK:     isPK,
		})
	}

	return result
}

// GetColumnTypes returns a map of column names to types.
// This is used for SQL transpilation in protocol layer.
func (ts *TableSchema) GetColumnTypes() map[string]string {
	result := make(map[string]string, len(ts.FullColumns))
	for _, col := range ts.FullColumns {
		result[col.Name] = col.Type
	}
	return result
}

// GetAutoIncrementCol returns the auto-increment column name if present.
// Returns empty string if no auto-increment column exists.
//
// Auto-increment detection rules (applied in loadSchema):
// - Must be single primary key column
// - Must be INTEGER or BIGINT type (SQLite rowid alias)
func (ts *TableSchema) GetAutoIncrementCol() string {
	return ts.AutoIncrementCol
}

// CalculateSchemaVersion creates a deterministic version number from schema.
// This ensures all nodes agree on schema version for intent key validation.
//
// Algorithm:
// 1. Create string: "tablename:pk1,pk2,pk3"
// 2. Sort PKs for determinism
// 3. SHA256 hash
// 4. Extract first 8 bytes as uint64
func (ts *TableSchema) CalculateSchemaVersion(tableName string) uint64 {
	var b strings.Builder
	b.WriteString(tableName)
	b.WriteString(":")

	// Sort primary keys for determinism
	sortedPKs := make([]string, len(ts.PrimaryKeys))
	copy(sortedPKs, ts.PrimaryKeys)
	sort.Strings(sortedPKs)

	for _, pk := range sortedPKs {
		b.WriteString(pk)
		b.WriteString(",")
	}

	// Hash to get version number
	hash := sha256.Sum256([]byte(b.String()))

	// Use first 8 bytes as uint64
	version := uint64(0)
	for i := 0; i < 8; i++ {
		version = (version << 8) | uint64(hash[i])
	}

	return version
}
