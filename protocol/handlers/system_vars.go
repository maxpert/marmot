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
	FoundRowsCount int64  // Number of rows found by last SELECT (for FOUND_ROWS())
	LastInsertId   int64  // Last auto-generated ID from INSERT (for LAST_INSERT_ID())
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
	case "VERSION", "VERSION()":
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
	case "FOUND_ROWS()":
		return config.FoundRowsCount
	case "LAST_INSERT_ID()":
		return config.LastInsertId
	default:
		// Unknown variable - return empty string
		return ""
	}
}
