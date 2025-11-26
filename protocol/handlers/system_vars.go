package handlers

import (
	"strings"

	"github.com/maxpert/marmot/protocol"
)

// SystemVarConfig configures system variable responses
type SystemVarConfig struct {
	ReadOnly       bool   // true for replica, false for coordinator
	VersionComment string // e.g., "Marmot" or "Marmot Read-Only Replica"
	ConnID         uint64 // Session connection ID
	CurrentDB      string // Current database name
}

// HandleSystemQuery handles MySQL system variable queries
func HandleSystemQuery(query string, config SystemVarConfig) (*protocol.ResultSet, error) {
	queryUpper := strings.ToUpper(query)

	// Handle simple variable lookups first (replica style)
	if result := handleSimpleSystemVariable(queryUpper, config); result != nil {
		return result, nil
	}

	// Handle SELECT-style queries (coordinator style)
	return handleSelectSystemVariables(query, config)
}

// handleSimpleSystemVariable handles direct variable lookups (e.g., SELECT @@VERSION)
func handleSimpleSystemVariable(queryUpper string, config SystemVarConfig) *protocol.ResultSet {
	// Handle DATABASE() function
	if strings.Contains(queryUpper, "DATABASE()") {
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{{Name: "DATABASE()"}},
			Rows:    [][]interface{}{{config.CurrentDB}},
		}
	}

	// Build variables map with appropriate read-only values
	readOnlyVal := 0
	if config.ReadOnly {
		readOnlyVal = 1
	}

	versionComment := config.VersionComment
	if versionComment == "" {
		if config.ReadOnly {
			versionComment = "Marmot Read-Only Replica"
		} else {
			versionComment = "Marmot"
		}
	}

	version := "8.0.32-Marmot"
	if config.ReadOnly {
		version = "8.0.32-Marmot-Replica"
	}

	// Common system variables
	variables := map[string]interface{}{
		"@@VERSION":                          version,
		"@@VERSION_COMMENT":                  versionComment,
		"@@TX_ISOLATION":                     "REPEATABLE-READ",
		"@@TRANSACTION_ISOLATION":            "REPEATABLE-READ",
		"@@SESSION.TRANSACTION_ISOLATION":    "REPEATABLE-READ",
		"@@SESSION.TX_ISOLATION":             "REPEATABLE-READ",
		"@@SESSION.AUTO_INCREMENT_INCREMENT": 1,
		"@@SESSION.AUTOCOMMIT":               1,
		"@@AUTOCOMMIT":                       1,
		"@@MAX_ALLOWED_PACKET":               67108864,
		"@@SQL_MODE":                         "TRADITIONAL",
		"@@SESSION.SQL_MODE":                 "TRADITIONAL",
		"@@GLOBAL.SQL_MODE":                  "TRADITIONAL",
		"@@CHARACTER_SET_CLIENT":             "utf8mb4",
		"@@CHARACTER_SET_CONNECTION":         "utf8mb4",
		"@@CHARACTER_SET_RESULTS":            "utf8mb4",
		"@@CHARACTER_SET_SERVER":             "utf8mb4",
		"@@COLLATION_CONNECTION":             "utf8mb4_general_ci",
		"@@COLLATION_SERVER":                 "utf8mb4_general_ci",
		"@@INIT_CONNECT":                     "",
		"@@INTERACTIVE_TIMEOUT":              28800,
		"@@LICENSE":                          "MIT",
		"@@LOWER_CASE_TABLE_NAMES":           0,
		"@@NET_BUFFER_LENGTH":                16384,
		"@@NET_WRITE_TIMEOUT":                60,
		"@@QUERY_CACHE_SIZE":                 1048576,
		"@@QUERY_CACHE_TYPE":                 "OFF",
		"@@SYSTEM_TIME_ZONE":                 "UTC",
		"@@TIME_ZONE":                        "SYSTEM",
		"@@TX_READ_ONLY":                     readOnlyVal,
		"@@SESSION.TX_READ_ONLY":             readOnlyVal,
		"@@GLOBAL.READ_ONLY":                 readOnlyVal,
		"@@READ_ONLY":                        readOnlyVal,
		"@@WAIT_TIMEOUT":                     28800,
		"@@SESSION.WAIT_TIMEOUT":             28800,
		"@@GLOBAL.MAX_ALLOWED_PACKET":        67108864,
		"@@SESSION.MAX_ALLOWED_PACKET":       67108864,
		"@@SESSION.NET_READ_TIMEOUT":         30,
		"@@SESSION.NET_WRITE_TIMEOUT":        60,
		"@@INNODB_READ_ONLY":                 readOnlyVal,
		"@@PERFORMANCE_SCHEMA":               0,
		"@@GLOBAL.PERFORMANCE_SCHEMA":        0,
		"@@GLOBAL.TRANSACTION_ISOLATION":     "REPEATABLE-READ",
		"@@GLOBAL.GTID_MODE":                 "OFF",
		"@@GTID_MODE":                        "OFF",
		"@@SERVER_ID":                        1,
		"@@SESSION.PSEUDO_THREAD_ID":         config.ConnID,
		"@@GLOBAL.INNODB_VERSION":            "8.0.32",
		"@@DEFAULT_STORAGE_ENGINE":           "SQLite",
		"@@SESSION.DEFAULT_STORAGE_ENGINE":   "SQLite",
		"@@HAVE_OPENSSL":                     "YES",
		"@@HAVE_SSL":                         "YES",
		"@@SESSION.TRANSACTION_READ_ONLY":    readOnlyVal,
		"@@GLOBAL.SUPER_READ_ONLY":           readOnlyVal,
		"@@SESSION.SUPER_READ_ONLY":          readOnlyVal,
		"@@GLOBAL.INNODB_READ_ONLY":          readOnlyVal,
	}

	for varName, value := range variables {
		if strings.Contains(queryUpper, varName) {
			return &protocol.ResultSet{
				Columns: []protocol.ColumnDef{{Name: varName}},
				Rows:    [][]interface{}{{value}},
			}
		}
	}

	return nil
}

// handleSelectSystemVariables handles SELECT-style queries with column aliases
func handleSelectSystemVariables(query string, config SystemVarConfig) (*protocol.ResultSet, error) {
	// Extract the SELECT portion (remove comments and LIMIT)
	queryUpper := strings.ToUpper(query)
	selectIdx := strings.Index(queryUpper, "SELECT")
	if selectIdx == -1 {
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{{Name: "Value", Type: 0xFD}},
			Rows:    [][]interface{}{{""}},
		}, nil
	}

	// Find FROM or LIMIT or end of string
	fromIdx := strings.Index(queryUpper[selectIdx:], "FROM")
	limitIdx := strings.Index(queryUpper[selectIdx:], "LIMIT")
	endIdx := len(query)

	if fromIdx != -1 {
		endIdx = selectIdx + fromIdx
	} else if limitIdx != -1 {
		endIdx = selectIdx + limitIdx
	}

	// Extract column list
	columnsPart := strings.TrimSpace(query[selectIdx+6 : endIdx])

	// Split by comma
	parts := strings.Split(columnsPart, ",")

	var columns []protocol.ColumnDef
	var values []interface{}

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Extract alias using AS or just the last word
		var colName string
		if strings.Contains(strings.ToUpper(part), " AS ") {
			asIdx := strings.LastIndex(strings.ToUpper(part), " AS ")
			colName = strings.TrimSpace(part[asIdx+4:])
		} else {
			// Use the variable name itself
			words := strings.Fields(part)
			if len(words) > 0 {
				colName = words[len(words)-1]
			} else {
				colName = "value"
			}
		}

		// Clean up column name (remove backticks, quotes)
		colName = strings.Trim(colName, "`'\"")

		columns = append(columns, protocol.ColumnDef{Name: colName, Type: 0xFD})

		// Return appropriate values for known variables
		value := getSystemVariableValue(part, config)
		values = append(values, value)
	}

	if len(columns) == 0 {
		columns = []protocol.ColumnDef{{Name: "Value", Type: 0xFD}}
		values = []interface{}{""}
	}

	return &protocol.ResultSet{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
}

// getSystemVariableValue returns the value for a system variable expression
func getSystemVariableValue(varExpr string, config SystemVarConfig) interface{} {
	varExpr = strings.ToLower(varExpr)

	// Common system variables
	if strings.Contains(varExpr, "version") {
		if config.ReadOnly {
			return "8.0.0-marmot-replica"
		}
		return "8.0.0-marmot"
	}
	if strings.Contains(varExpr, "database()") {
		return config.CurrentDB
	}
	if strings.Contains(varExpr, "autocommit") {
		return 1
	}
	if strings.Contains(varExpr, "auto_increment") {
		return 1
	}
	if strings.Contains(varExpr, "sql_mode") {
		return "STRICT_TRANS_TABLES"
	}
	if strings.Contains(varExpr, "character_set") || strings.Contains(varExpr, "charset") {
		return "utf8mb4"
	}
	if strings.Contains(varExpr, "collation") {
		return "utf8mb4_general_ci"
	}
	if strings.Contains(varExpr, "system_time_zone") {
		return "UTC"
	}
	if strings.Contains(varExpr, "time_zone") {
		return "SYSTEM"
	}
	if strings.Contains(varExpr, "interactive_timeout") {
		return 28800
	}
	if strings.Contains(varExpr, "wait_timeout") {
		return 28800
	}
	if strings.Contains(varExpr, "net_write_timeout") {
		return 60
	}
	if strings.Contains(varExpr, "timeout") {
		return 28800
	}
	if strings.Contains(varExpr, "max_allowed_packet") {
		return 67108864
	}
	if strings.Contains(varExpr, "lower_case_table_names") {
		return 0
	}
	if strings.Contains(varExpr, "transaction_isolation") || strings.Contains(varExpr, "tx_isolation") {
		return "REPEATABLE-READ"
	}
	if strings.Contains(varExpr, "tx_read_only") || strings.Contains(varExpr, "read_only") {
		if config.ReadOnly {
			return 1
		}
		return 0
	}
	if strings.Contains(varExpr, "performance_schema") {
		return 0
	}
	if strings.Contains(varExpr, "query_cache") {
		return 0
	}
	if strings.Contains(varExpr, "license") {
		return "MIT"
	}
	if strings.Contains(varExpr, "init_connect") {
		return ""
	}

	// Default: return empty string for unknown variables
	return ""
}
