package handlers

import (
	"fmt"
	"regexp"
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

// sqliteToMySQLType converts SQLite type to MySQL type
func sqliteToMySQLType(sqliteType string) string {
	upper := strings.ToUpper(sqliteType)

	// Handle common SQLite types
	if strings.Contains(upper, "INT") {
		if strings.Contains(upper, "TINYINT") {
			return "tinyint"
		} else if strings.Contains(upper, "SMALLINT") {
			return "smallint"
		} else if strings.Contains(upper, "MEDIUMINT") {
			return "mediumint"
		} else if strings.Contains(upper, "BIGINT") {
			return "bigint"
		}
		return "int"
	}

	if strings.Contains(upper, "CHAR") || strings.Contains(upper, "TEXT") {
		if strings.Contains(upper, "VARCHAR") {
			return "varchar(255)"
		}
		return "text"
	}

	if strings.Contains(upper, "BLOB") {
		return "blob"
	}

	if strings.Contains(upper, "REAL") || strings.Contains(upper, "FLOAT") {
		return "float"
	}

	if strings.Contains(upper, "DOUBLE") {
		return "double"
	}

	if strings.Contains(upper, "DECIMAL") || strings.Contains(upper, "NUMERIC") {
		return "decimal(10,0)"
	}

	if strings.Contains(upper, "DATE") {
		if strings.Contains(upper, "DATETIME") {
			return "datetime"
		}
		return "date"
	}

	if strings.Contains(upper, "TIME") {
		return "time"
	}

	// Default
	return "varchar(255)"
}

// extractDataType extracts the base data type from a column type string
func extractDataType(columnType string) string {
	// Remove size specifications like varchar(255) -> varchar
	re := regexp.MustCompile(`(\w+)(\(.*\))?`)
	matches := re.FindStringSubmatch(columnType)
	if len(matches) > 1 {
		return matches[1]
	}
	return columnType
}

// extractWhereValue extracts a value from a WHERE clause
// e.g., "WHERE TABLE_SCHEMA='mydb'" -> "mydb"
func extractWhereValue(query, columnName string) string {
	// Simple regex to extract value from WHERE clause
	// Handles: WHERE col='value' or WHERE col="value" or WHERE col=value
	pattern := fmt.Sprintf(`(?i)%s\s*=\s*['"]?([^'"\s,)]+)['"]?`, regexp.QuoteMeta(columnName))
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(query)
	if len(matches) > 1 {
		return strings.Trim(matches[1], `'"`)
	}
	return ""
}
