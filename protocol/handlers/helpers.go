package handlers

import (
	"strings"
)

// toInt64 safely converts interface{} to int64
func toInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

// sqliteTypeMap maps SQLite type keywords to MySQL types (order matters for prefix matching)
var sqliteTypeMap = []struct {
	keyword string
	mysql   string
}{
	// INT variants (more specific first)
	{"TINYINT", "tinyint"},
	{"SMALLINT", "smallint"},
	{"MEDIUMINT", "mediumint"},
	{"BIGINT", "bigint"},
	{"INT", "int"},
	// Text types
	{"VARCHAR", "varchar(255)"},
	{"CHAR", "text"},
	{"TEXT", "text"},
	// Binary
	{"BLOB", "blob"},
	// Floating point
	{"DOUBLE", "double"},
	{"REAL", "float"},
	{"FLOAT", "float"},
	// Decimal
	{"DECIMAL", "decimal(10,0)"},
	{"NUMERIC", "decimal(10,0)"},
	// Date/time (more specific first)
	{"DATETIME", "datetime"},
	{"DATE", "date"},
	{"TIME", "time"},
	// Boolean
	{"BOOLEAN", "tinyint(1)"},
	{"BOOL", "tinyint(1)"},
}

// sqliteToMySQLType converts SQLite type to MySQL type
func sqliteToMySQLType(sqliteType string) string {
	upper := strings.ToUpper(sqliteType)
	for _, m := range sqliteTypeMap {
		if strings.Contains(upper, m.keyword) {
			return m.mysql
		}
	}
	return "varchar(255)"
}

// extractDataType extracts the base data type from a column type string
// e.g., "varchar(255)" -> "varchar", "int" -> "int"
func extractDataType(columnType string) string {
	if idx := strings.Index(columnType, "("); idx != -1 {
		return columnType[:idx]
	}
	return columnType
}
