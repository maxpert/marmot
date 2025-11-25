package replica

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"

	"github.com/rs/zerolog/log"
)

// ReadOnlyHandler implements protocol.ConnectionHandler for read-only replicas
// It rejects all mutations and executes reads locally
type ReadOnlyHandler struct {
	dbManager *db.DatabaseManager
	clock     *hlc.Clock
	replica   *Replica
}

// NewReadOnlyHandler creates a new read-only handler
func NewReadOnlyHandler(dbManager *db.DatabaseManager, clock *hlc.Clock, replica *Replica) *ReadOnlyHandler {
	return &ReadOnlyHandler{
		dbManager: dbManager,
		clock:     clock,
		replica:   replica,
	}
}

// HandleQuery processes a SQL query (read-only)
func (h *ReadOnlyHandler) HandleQuery(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	log.Debug().
		Uint64("conn_id", session.ConnID).
		Str("database", session.CurrentDatabase).
		Str("query", query).
		Msg("Handling query (read-only)")

	// Intercept MySQL system variable queries
	if strings.Contains(query, "@@") || strings.Contains(strings.ToUpper(query), "DATABASE()") {
		return h.handleSystemQuery(session, query)
	}

	stmt := protocol.ParseStatement(query)

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Int("stmt_type", int(stmt.Type)).
		Str("table", stmt.TableName).
		Str("database", stmt.Database).
		Msg("Parsed statement")

	// Reject all mutations with MySQL read-only error
	if protocol.IsMutation(stmt) {
		log.Debug().
			Uint64("conn_id", session.ConnID).
			Int("stmt_type", int(stmt.Type)).
			Msg("Rejecting mutation on read-only replica")
		return nil, protocol.ErrReadOnly()
	}

	// Reject transaction control for writes (BEGIN is ok for reads, but COMMIT on writes is not)
	// Allow read-only transactions for compatibility
	if protocol.IsTransactionControl(stmt) {
		// Allow BEGIN/START TRANSACTION (for read-only transactions)
		// Allow COMMIT/ROLLBACK (no-op for read-only)
		switch stmt.Type {
		case protocol.StatementBegin:
			// For read-only replica, we don't need real transaction tracking
			// Just set a flag for client compatibility
			return nil, nil
		case protocol.StatementCommit, protocol.StatementRollback:
			return nil, nil
		default:
			// Savepoints, XA transactions - reject
			return nil, protocol.ErrReadOnly()
		}
	}

	// Handle SET commands as no-op (return OK)
	if stmt.Type == protocol.StatementSet {
		return nil, nil
	}

	// Handle database management commands (read-only)
	switch stmt.Type {
	case protocol.StatementShowDatabases:
		return h.handleShowDatabases()
	case protocol.StatementUseDatabase:
		return h.handleUseDatabase(session, stmt.Database)
	}

	// Handle metadata queries
	switch stmt.Type {
	case protocol.StatementShowTables:
		return h.handleShowTables(session, stmt)
	case protocol.StatementShowColumns:
		return h.handleShowColumns(session, stmt)
	case protocol.StatementShowCreateTable:
		return h.handleShowCreateTable(session, stmt)
	case protocol.StatementShowIndexes:
		return h.handleShowIndexes(session, stmt)
	case protocol.StatementShowTableStatus:
		return h.handleShowTableStatus(session, stmt)
	case protocol.StatementInformationSchema:
		return h.handleInformationSchema(session, query)
	}

	// Set database context from session if not specified in statement
	if stmt.Database == "" {
		stmt.Database = session.CurrentDatabase
	}

	// Execute read locally
	return h.executeLocalRead(stmt)
}

// handleSystemQuery handles MySQL system variable queries
func (h *ReadOnlyHandler) handleSystemQuery(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	upper := strings.ToUpper(query)

	// Handle DATABASE() function
	if strings.Contains(upper, "DATABASE()") {
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{{Name: "DATABASE()"}},
			Rows:    [][]interface{}{{session.CurrentDatabase}},
		}, nil
	}

	// Handle common system variables
	variables := map[string]interface{}{
		"@@VERSION":                          "8.0.32-Marmot-Replica",
		"@@VERSION_COMMENT":                  "Marmot Read-Only Replica",
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
		"@@LICENSE":                          "Apache-2.0",
		"@@LOWER_CASE_TABLE_NAMES":           0,
		"@@NET_BUFFER_LENGTH":                16384,
		"@@NET_WRITE_TIMEOUT":                60,
		"@@QUERY_CACHE_SIZE":                 1048576,
		"@@QUERY_CACHE_TYPE":                 "OFF",
		"@@SYSTEM_TIME_ZONE":                 "UTC",
		"@@TIME_ZONE":                        "SYSTEM",
		"@@TX_READ_ONLY":                     1, // Read-only replica
		"@@SESSION.TX_READ_ONLY":             1,
		"@@GLOBAL.READ_ONLY":                 1, // Read-only replica
		"@@READ_ONLY":                        1,
		"@@WAIT_TIMEOUT":                     28800,
		"@@SESSION.WAIT_TIMEOUT":             28800,
		"@@GLOBAL.MAX_ALLOWED_PACKET":        67108864,
		"@@SESSION.MAX_ALLOWED_PACKET":       67108864,
		"@@SESSION.NET_READ_TIMEOUT":         30,
		"@@SESSION.NET_WRITE_TIMEOUT":        60,
		"@@INNODB_READ_ONLY":                 1, // Read-only replica
		"@@PERFORMANCE_SCHEMA":               0,
		"@@GLOBAL.PERFORMANCE_SCHEMA":        0,
		"@@GLOBAL.TRANSACTION_ISOLATION":     "REPEATABLE-READ",
		"@@GLOBAL.GTID_MODE":                 "OFF",
		"@@GTID_MODE":                        "OFF",
		"@@SERVER_ID":                        1,
		"@@SESSION.PSEUDO_THREAD_ID":         session.ConnID,
		"@@GLOBAL.INNODB_VERSION":            "8.0.32",
		"@@DEFAULT_STORAGE_ENGINE":           "SQLite",
		"@@SESSION.DEFAULT_STORAGE_ENGINE":   "SQLite",
		"@@HAVE_OPENSSL":                     "YES",
		"@@HAVE_SSL":                         "YES",
		"@@SESSION.TRANSACTION_READ_ONLY":    1, // Read-only replica
		"@@GLOBAL.SUPER_READ_ONLY":           1, // Read-only replica
		"@@SESSION.SUPER_READ_ONLY":          1,
		"@@GLOBAL.INNODB_READ_ONLY":          1,
	}

	for varName, value := range variables {
		if strings.Contains(upper, varName) {
			return &protocol.ResultSet{
				Columns: []protocol.ColumnDef{{Name: varName}},
				Rows:    [][]interface{}{{value}},
			}, nil
		}
	}

	// Default: return empty string for unknown variables
	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{{Name: "Value"}},
		Rows:    [][]interface{}{{""}},
	}, nil
}

// handleShowDatabases returns list of databases
func (h *ReadOnlyHandler) handleShowDatabases() (*protocol.ResultSet, error) {
	dbs := h.dbManager.ListDatabases()
	rows := make([][]interface{}, 0, len(dbs))
	for _, dbName := range dbs {
		rows = append(rows, []interface{}{dbName})
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{{Name: "Database"}},
		Rows:    rows,
	}, nil
}

// handleUseDatabase switches the current database context
func (h *ReadOnlyHandler) handleUseDatabase(session *protocol.ConnectionSession, dbName string) (*protocol.ResultSet, error) {
	if !h.dbManager.DatabaseExists(dbName) {
		return nil, fmt.Errorf("ERROR 1049 (42000): Unknown database '%s'", dbName)
	}
	session.CurrentDatabase = dbName
	return nil, nil
}

// handleShowTables returns list of tables in current database
func (h *ReadOnlyHandler) handleShowTables(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}
	if dbName == "" {
		return nil, fmt.Errorf("ERROR 1046 (3D000): No database selected")
	}

	sqlDB, err := h.dbManager.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	rows, err := sqlDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__marmot__%' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &protocol.ResultSet{
		Columns: []protocol.ColumnDef{{Name: fmt.Sprintf("Tables_in_%s", dbName)}},
		Rows:    make([][]interface{}, 0),
	}

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, []interface{}{tableName})
	}

	return result, nil
}

// handleShowColumns returns columns for a table
func (h *ReadOnlyHandler) handleShowColumns(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}
	if dbName == "" {
		return nil, fmt.Errorf("ERROR 1046 (3D000): No database selected")
	}

	sqlDB, err := h.dbManager.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	rows, err := sqlDB.Query(fmt.Sprintf("PRAGMA table_info(%s)", stmt.TableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Field"},
			{Name: "Type"},
			{Name: "Null"},
			{Name: "Key"},
			{Name: "Default"},
			{Name: "Extra"},
		},
		Rows: make([][]interface{}, 0),
	}

	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var dfltValue sql.NullString
		var pk int

		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return nil, err
		}

		nullStr := "YES"
		if notNull == 1 {
			nullStr = "NO"
		}

		keyStr := ""
		if pk > 0 {
			keyStr = "PRI"
		}

		defaultVal := interface{}(nil)
		if dfltValue.Valid {
			defaultVal = dfltValue.String
		}

		result.Rows = append(result.Rows, []interface{}{
			name, colType, nullStr, keyStr, defaultVal, "",
		})
	}

	return result, nil
}

// handleShowCreateTable returns CREATE TABLE statement
func (h *ReadOnlyHandler) handleShowCreateTable(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}
	if dbName == "" {
		return nil, fmt.Errorf("ERROR 1046 (3D000): No database selected")
	}

	sqlDB, err := h.dbManager.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	var createSQL string
	err = sqlDB.QueryRow("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", stmt.TableName).Scan(&createSQL)
	if err != nil {
		return nil, fmt.Errorf("ERROR 1146 (42S02): Table '%s.%s' doesn't exist", dbName, stmt.TableName)
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Table"},
			{Name: "Create Table"},
		},
		Rows: [][]interface{}{{stmt.TableName, createSQL}},
	}, nil
}

// handleShowIndexes returns indexes for a table
func (h *ReadOnlyHandler) handleShowIndexes(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}
	if dbName == "" {
		return nil, fmt.Errorf("ERROR 1046 (3D000): No database selected")
	}

	sqlDB, err := h.dbManager.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	rows, err := sqlDB.Query(fmt.Sprintf("PRAGMA index_list(%s)", stmt.TableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Table"},
			{Name: "Non_unique"},
			{Name: "Key_name"},
			{Name: "Seq_in_index"},
			{Name: "Column_name"},
			{Name: "Collation"},
			{Name: "Cardinality"},
			{Name: "Sub_part"},
			{Name: "Packed"},
			{Name: "Null"},
			{Name: "Index_type"},
			{Name: "Comment"},
			{Name: "Index_comment"},
		},
		Rows: make([][]interface{}, 0),
	}

	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin, partial string

		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			continue
		}

		nonUnique := 1
		if unique == 1 {
			nonUnique = 0
		}

		result.Rows = append(result.Rows, []interface{}{
			stmt.TableName, nonUnique, name, 1, "", "A", nil, nil, nil, "YES", "BTREE", "", "",
		})
	}

	return result, nil
}

// handleShowTableStatus returns table status
func (h *ReadOnlyHandler) handleShowTableStatus(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}
	if dbName == "" {
		return nil, fmt.Errorf("ERROR 1046 (3D000): No database selected")
	}

	sqlDB, err := h.dbManager.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	// Get list of tables
	rows, err := sqlDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__marmot__%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Name"},
			{Name: "Engine"},
			{Name: "Version"},
			{Name: "Row_format"},
			{Name: "Rows"},
			{Name: "Avg_row_length"},
			{Name: "Data_length"},
			{Name: "Max_data_length"},
			{Name: "Index_length"},
			{Name: "Data_free"},
			{Name: "Auto_increment"},
			{Name: "Create_time"},
			{Name: "Update_time"},
			{Name: "Check_time"},
			{Name: "Collation"},
			{Name: "Checksum"},
			{Name: "Create_options"},
			{Name: "Comment"},
		},
		Rows: make([][]interface{}, 0),
	}

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		result.Rows = append(result.Rows, []interface{}{
			tableName, "SQLite", 10, "Dynamic", 0, 0, 0, 0, 0, 0, nil, nil, nil, nil, "utf8mb4_general_ci", nil, "", "",
		})
	}

	return result, nil
}

// handleInformationSchema handles information_schema queries
func (h *ReadOnlyHandler) handleInformationSchema(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	// For now, return empty result set for information_schema queries
	// This provides basic compatibility with MySQL clients
	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{},
		Rows:    [][]interface{}{},
	}, nil
}

// executeLocalRead executes a read query locally
func (h *ReadOnlyHandler) executeLocalRead(stmt protocol.Statement) (*protocol.ResultSet, error) {
	if stmt.Database == "" {
		return nil, fmt.Errorf("ERROR 1046 (3D000): No database selected")
	}

	sqlDB, err := h.dbManager.GetDatabaseConnection(stmt.Database)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := sqlDB.QueryContext(ctx, stmt.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column info
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := &protocol.ResultSet{
		Columns: make([]protocol.ColumnDef, len(columns)),
		Rows:    make([][]interface{}, 0),
	}

	for i, col := range columns {
		result.Columns[i] = protocol.ColumnDef{Name: col}
	}

	// Scan rows
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Convert sql.RawBytes to string for proper MySQL encoding
		row := make([]interface{}, len(columns))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}

		result.Rows = append(result.Rows, row)
	}

	return result, rows.Err()
}
