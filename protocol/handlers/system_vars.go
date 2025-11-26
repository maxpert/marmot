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

// HandleSystemVariableQuery handles system variable queries using parsed Statement
// This is the preferred entry point - uses pre-extracted SystemVarNames from AST
func HandleSystemVariableQuery(stmt protocol.Statement, config SystemVarConfig) (*protocol.ResultSet, error) {
	// Use pre-extracted variable names from AST parsing
	if len(stmt.SystemVarNames) == 0 {
		// Fallback: return empty result
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{{Name: "Value", Type: 0xFD}},
			Rows:    [][]interface{}{{""}},
		}, nil
	}

	// Build result from pre-extracted variable names
	var columns []protocol.ColumnDef
	var values []interface{}

	for _, varName := range stmt.SystemVarNames {
		value := getSystemVariableValueByName(varName, config)
		columns = append(columns, protocol.ColumnDef{Name: varName, Type: 0xFD})
		values = append(values, value)
	}

	return &protocol.ResultSet{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
}

// HandleSystemQuery handles MySQL system variable queries
// Deprecated: use HandleSystemVariableQuery with parsed Statement instead
func HandleSystemQuery(query string, config SystemVarConfig) (*protocol.ResultSet, error) {
	queryUpper := strings.ToUpper(query)

	// Handle simple variable lookups first (replica style)
	if result := handleSimpleSystemVariable(queryUpper, config); result != nil {
		return result, nil
	}

	// Handle SELECT-style queries (coordinator style)
	return handleSelectSystemVariables(query, config)
}

// getSystemVariableValueByName returns value for a normalized variable name (e.g., "VERSION", "DATABASE()")
func getSystemVariableValueByName(varName string, config SystemVarConfig) interface{} {
	varUpper := strings.ToUpper(varName)

	// Read-only value
	readOnlyVal := 0
	if config.ReadOnly {
		readOnlyVal = 1
	}

	// Lookup table for known system variables
	switch varUpper {
	case "VERSION":
		if config.ReadOnly {
			return "8.0.32-Marmot-Replica"
		}
		return "8.0.32-Marmot"
	case "VERSION_COMMENT":
		if config.VersionComment != "" {
			return config.VersionComment
		}
		if config.ReadOnly {
			return "Marmot Read-Only Replica"
		}
		return "Marmot"
	case "DATABASE()", "SCHEMA()":
		return config.CurrentDB
	case "AUTOCOMMIT", "SESSION.AUTOCOMMIT":
		return 1
	case "AUTO_INCREMENT_INCREMENT", "SESSION.AUTO_INCREMENT_INCREMENT":
		return 1
	case "SQL_MODE", "SESSION.SQL_MODE", "GLOBAL.SQL_MODE":
		return "TRADITIONAL"
	case "TX_ISOLATION", "SESSION.TX_ISOLATION", "TRANSACTION_ISOLATION",
		"SESSION.TRANSACTION_ISOLATION", "GLOBAL.TRANSACTION_ISOLATION":
		return "REPEATABLE-READ"
	case "CHARACTER_SET_CLIENT", "CHARACTER_SET_CONNECTION",
		"CHARACTER_SET_RESULTS", "CHARACTER_SET_SERVER":
		return "utf8mb4"
	case "COLLATION_CONNECTION", "COLLATION_SERVER":
		return "utf8mb4_general_ci"
	case "TIME_ZONE":
		return "SYSTEM"
	case "SYSTEM_TIME_ZONE":
		return "UTC"
	case "INTERACTIVE_TIMEOUT", "WAIT_TIMEOUT", "SESSION.WAIT_TIMEOUT":
		return 28800
	case "NET_WRITE_TIMEOUT", "SESSION.NET_WRITE_TIMEOUT":
		return 60
	case "NET_READ_TIMEOUT", "SESSION.NET_READ_TIMEOUT":
		return 30
	case "MAX_ALLOWED_PACKET", "SESSION.MAX_ALLOWED_PACKET", "GLOBAL.MAX_ALLOWED_PACKET":
		return 67108864
	case "NET_BUFFER_LENGTH":
		return 16384
	case "LOWER_CASE_TABLE_NAMES":
		return 0
	case "TX_READ_ONLY", "SESSION.TX_READ_ONLY", "READ_ONLY", "GLOBAL.READ_ONLY",
		"SESSION.TRANSACTION_READ_ONLY", "INNODB_READ_ONLY", "GLOBAL.INNODB_READ_ONLY",
		"SUPER_READ_ONLY", "GLOBAL.SUPER_READ_ONLY", "SESSION.SUPER_READ_ONLY":
		return readOnlyVal
	case "PERFORMANCE_SCHEMA", "GLOBAL.PERFORMANCE_SCHEMA":
		return 0
	case "QUERY_CACHE_SIZE":
		return 1048576
	case "QUERY_CACHE_TYPE":
		return "OFF"
	case "LICENSE":
		return "MIT"
	case "INIT_CONNECT":
		return ""
	case "SERVER_ID":
		return 1
	case "PSEUDO_THREAD_ID", "SESSION.PSEUDO_THREAD_ID":
		return config.ConnID
	case "GLOBAL.GTID_MODE", "GTID_MODE":
		return "OFF"
	case "GLOBAL.INNODB_VERSION":
		return "8.0.32"
	case "DEFAULT_STORAGE_ENGINE", "SESSION.DEFAULT_STORAGE_ENGINE":
		return "SQLite"
	case "HAVE_OPENSSL", "HAVE_SSL":
		return "YES"
	case "CONNECTION_ID()":
		return config.ConnID
	case "USER()", "CURRENT_USER()", "SESSION_USER()", "SYSTEM_USER()":
		return "root@localhost"
	default:
		// Unknown variable - return empty string
		return ""
	}
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
