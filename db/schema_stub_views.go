//go:build !sqlite_preupdate_hook
// +build !sqlite_preupdate_hook

package db

import (
	"crypto/sha256"
	"sort"
	"strings"

	"github.com/maxpert/marmot/publisher"
)

// ToPublisherSchema converts the lightweight stub schema into publisher schema.
func (ts *TableSchema) ToPublisherSchema() publisher.TableSchema {
	result := publisher.TableSchema{
		Columns: make([]publisher.ColumnInfo, 0, len(ts.Columns)),
	}

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
			Type:     "",
			Nullable: true,
			IsPK:     isPK,
		})
	}

	return result
}

// GetColumnTypes returns an empty map in stub mode (no type metadata available).
func (ts *TableSchema) GetColumnTypes() map[string]string {
	return map[string]string{}
}

// GetAutoIncrementCol returns empty in stub mode.
func (ts *TableSchema) GetAutoIncrementCol() string {
	return ""
}

// CalculateSchemaVersion creates deterministic version based on table + primary keys.
func (ts *TableSchema) CalculateSchemaVersion(tableName string) uint64 {
	var b strings.Builder
	b.WriteString(tableName)
	b.WriteString(":")

	sortedPKs := make([]string, len(ts.PrimaryKeys))
	copy(sortedPKs, ts.PrimaryKeys)
	sort.Strings(sortedPKs)
	for _, pk := range sortedPKs {
		b.WriteString(pk)
		b.WriteString(",")
	}

	hash := sha256.Sum256([]byte(b.String()))
	version := uint64(0)
	for i := 0; i < 8; i++ {
		version = (version << 8) | uint64(hash[i])
	}
	return version
}
