package coordinator

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// execMetadataQuery executes a metadata query and returns the response
func (h *CoordinatorHandler) execMetadataQuery(dbName, query string, args ...interface{}) (*ReadResponse, error) {
	req := &ReadRequest{
		Query:       query,
		Args:        args,
		SnapshotTS:  h.clock.Now(),
		Consistency: protocol.ConsistencyLocalOne, // Metadata queries always local
		Database:    dbName,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := h.readCoord.ReadTransaction(ctx, req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	return resp, nil
}

// handleShowTables returns list of tables in the specified database
func (h *CoordinatorHandler) handleShowTables(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}

	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	log.Debug().
		Str("database", dbName).
		Msg("Handling SHOW TABLES")

	// Query SQLite's sqlite_master table through read coordinator
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	resp, err := h.execMetadataQuery(dbName, query)
	if err != nil {
		return nil, err
	}

	var tables [][]interface{}
	for _, row := range resp.Rows {
		if tableName, ok := row["name"]; ok {
			tables = append(tables, []interface{}{tableName})
		}
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: fmt.Sprintf("Tables_in_%s", dbName), Type: 0xFD},
		},
		Rows: tables,
	}, nil
}

// handleShowColumns returns column information for a table
func (h *CoordinatorHandler) handleShowColumns(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}

	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	if stmt.TableName == "" {
		return nil, fmt.Errorf("no table specified")
	}

	log.Debug().
		Str("database", dbName).
		Str("table", stmt.TableName).
		Msg("Handling SHOW COLUMNS")

	// Use PRAGMA table_info to get column information
	query := fmt.Sprintf("PRAGMA table_info(%s)", stmt.TableName)
	resp, err := h.execMetadataQuery(dbName, query)
	if err != nil {
		return nil, err
	}

	var columns [][]interface{}
	for _, row := range resp.Rows {
		name, _ := row["name"].(string)
		colType, _ := row["type"].(string)
		notNullInt, _ := toInt64(row["notnull"])
		pkInt, _ := toInt64(row["pk"])
		dfltValue := row["dflt_value"]

		// Convert SQLite types to MySQL-compatible format
		mysqlType := sqliteToMySQLType(colType)

		// Determine NULL/NOT NULL
		nullable := "YES"
		if notNullInt == 1 {
			nullable = "NO"
		}

		// Determine if it's a key
		key := ""
		if pkInt > 0 {
			key = "PRI"
		}

		// Build MySQL-compatible SHOW COLUMNS result
		columns = append(columns, []interface{}{
			name,        // Field
			mysqlType,   // Type
			nullable,    // Null
			key,         // Key
			dfltValue,   // Default
			"",          // Extra
		})
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Field", Type: 0xFD},
			{Name: "Type", Type: 0xFD},
			{Name: "Null", Type: 0xFD},
			{Name: "Key", Type: 0xFD},
			{Name: "Default", Type: 0xFD},
			{Name: "Extra", Type: 0xFD},
		},
		Rows: columns,
	}, nil
}

// handleShowCreateTable returns the CREATE TABLE statement
func (h *CoordinatorHandler) handleShowCreateTable(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}

	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	if stmt.TableName == "" {
		return nil, fmt.Errorf("no table specified")
	}

	log.Debug().
		Str("database", dbName).
		Str("table", stmt.TableName).
		Msg("Handling SHOW CREATE TABLE")

	// Query sqlite_master for the CREATE TABLE statement
	query := "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
	resp, err := h.execMetadataQuery(dbName, query, stmt.TableName)
	if err != nil {
		return nil, err
	}

	if len(resp.Rows) == 0 {
		return nil, fmt.Errorf("table not found: %s", stmt.TableName)
	}

	createSQL, _ := resp.Rows[0]["sql"].(string)

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Table", Type: 0xFD},
			{Name: "Create Table", Type: 0xFD},
		},
		Rows: [][]interface{}{
			{stmt.TableName, createSQL},
		},
	}, nil
}

// handleShowIndexes returns index information for a table
func (h *CoordinatorHandler) handleShowIndexes(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}

	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	if stmt.TableName == "" {
		return nil, fmt.Errorf("no table specified")
	}

	log.Debug().
		Str("database", dbName).
		Str("table", stmt.TableName).
		Msg("Handling SHOW INDEXES")

	// Use PRAGMA index_list to get index information
	query := fmt.Sprintf("PRAGMA index_list(%s)", stmt.TableName)
	resp, err := h.execMetadataQuery(dbName, query)
	if err != nil {
		return nil, err
	}

	var indexes [][]interface{}
	for _, row := range resp.Rows {
		name, _ := row["name"].(string)
		uniqueInt, _ := toInt64(row["unique"])

		nonUnique := 1
		if uniqueInt == 1 {
			nonUnique = 0
		}

		// Get index columns
		indexQuery := fmt.Sprintf("PRAGMA index_info(%s)", name)
		indexResp, err := h.execMetadataQuery(dbName, indexQuery)
		if err != nil {
			continue
		}

		var columnName string
		if len(indexResp.Rows) > 0 {
			columnName, _ = indexResp.Rows[0]["name"].(string)
		}

		indexes = append(indexes, []interface{}{
			stmt.TableName, // Table
			nonUnique,      // Non_unique
			name,           // Key_name
			1,              // Seq_in_index
			columnName,     // Column_name
			"A",            // Collation
			nil,            // Cardinality
			nil,            // Sub_part
			nil,            // Packed
			"",             // Null
			"BTREE",        // Index_type
			"",             // Comment
			"",             // Index_comment
		})
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Table", Type: 0xFD},
			{Name: "Non_unique", Type: 0x03},
			{Name: "Key_name", Type: 0xFD},
			{Name: "Seq_in_index", Type: 0x03},
			{Name: "Column_name", Type: 0xFD},
			{Name: "Collation", Type: 0xFD},
			{Name: "Cardinality", Type: 0x03},
			{Name: "Sub_part", Type: 0x03},
			{Name: "Packed", Type: 0xFD},
			{Name: "Null", Type: 0xFD},
			{Name: "Index_type", Type: 0xFD},
			{Name: "Comment", Type: 0xFD},
			{Name: "Index_comment", Type: 0xFD},
		},
		Rows: indexes,
	}, nil
}

// handleShowTableStatus returns table status information (for DBeaver compatibility)
func (h *CoordinatorHandler) handleShowTableStatus(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	dbName := stmt.Database
	if dbName == "" {
		dbName = session.CurrentDatabase
	}

	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	log.Debug().
		Str("database", dbName).
		Str("table", stmt.TableName).
		Msg("Handling SHOW TABLE STATUS")

	// Build query to get table info
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
	if stmt.TableName != "" {
		query = fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name = '%s'", stmt.TableName)
	}

	resp, err := h.execMetadataQuery(dbName, query)
	if err != nil {
		return nil, err
	}

	var tables [][]interface{}
	for _, row := range resp.Rows {
		tableName, _ := row["name"].(string)

		// Return MySQL-compatible table status
		tables = append(tables, []interface{}{
			tableName,                // Name
			"InnoDB",                 // Engine
			10,                       // Version
			"Dynamic",                // Row_format
			0,                        // Rows
			0,                        // Avg_row_length
			0,                        // Data_length
			0,                        // Max_data_length
			0,                        // Index_length
			0,                        // Data_free
			nil,                      // Auto_increment
			nil,                      // Create_time
			nil,                      // Update_time
			nil,                      // Check_time
			"utf8mb4_general_ci",     // Collation
			nil,                      // Checksum
			"",                       // Create_options
			"",                       // Comment
		})
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Name", Type: 0xFD},
			{Name: "Engine", Type: 0xFD},
			{Name: "Version", Type: 0x03},
			{Name: "Row_format", Type: 0xFD},
			{Name: "Rows", Type: 0x08},
			{Name: "Avg_row_length", Type: 0x08},
			{Name: "Data_length", Type: 0x08},
			{Name: "Max_data_length", Type: 0x08},
			{Name: "Index_length", Type: 0x08},
			{Name: "Data_free", Type: 0x08},
			{Name: "Auto_increment", Type: 0x08},
			{Name: "Create_time", Type: 0x0C},
			{Name: "Update_time", Type: 0x0C},
			{Name: "Check_time", Type: 0x0C},
			{Name: "Collation", Type: 0xFD},
			{Name: "Checksum", Type: 0x08},
			{Name: "Create_options", Type: 0xFD},
			{Name: "Comment", Type: 0xFD},
		},
		Rows: tables,
	}, nil
}

// handleInformationSchema handles INFORMATION_SCHEMA queries
func (h *CoordinatorHandler) handleInformationSchema(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	queryUpper := strings.ToUpper(query)

	log.Debug().
		Str("query", query).
		Msg("Handling INFORMATION_SCHEMA query")

	// Detect which INFORMATION_SCHEMA table is being queried
	if strings.Contains(queryUpper, "INFORMATION_SCHEMA.TABLES") {
		return h.handleInformationSchemaTables(session, query)
	} else if strings.Contains(queryUpper, "INFORMATION_SCHEMA.COLUMNS") {
		return h.handleInformationSchemaColumns(session, query)
	} else if strings.Contains(queryUpper, "INFORMATION_SCHEMA.SCHEMATA") {
		return h.handleInformationSchemaSchemata(session, query)
	} else if strings.Contains(queryUpper, "INFORMATION_SCHEMA.STATISTICS") {
		return h.handleInformationSchemaStatistics(session, query)
	}

	// Unsupported INFORMATION_SCHEMA table - return empty result
	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{},
		Rows:    [][]interface{}{},
	}, nil
}

// handleInformationSchemaTables handles queries to INFORMATION_SCHEMA.TABLES
func (h *CoordinatorHandler) handleInformationSchemaTables(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	// Extract database name from WHERE clause if present
	dbName := extractWhereValue(query, "TABLE_SCHEMA")
	if dbName == "" {
		dbName = session.CurrentDatabase
	}

	if dbName == "" {
		// Return all tables from all databases
		return h.getAllTablesFromAllDatabases()
	}

	log.Debug().
		Str("database", dbName).
		Msg("Querying INFORMATION_SCHEMA.TABLES")

	// Query SQLite's sqlite_master table
	sqlQuery := "SELECT name, type FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	resp, err := h.execMetadataQuery(dbName, sqlQuery)
	if err != nil {
		return nil, err
	}

	var tables [][]interface{}
	for _, row := range resp.Rows {
		tableName, _ := row["name"].(string)

		tables = append(tables, []interface{}{
			"def",        // TABLE_CATALOG
			dbName,       // TABLE_SCHEMA
			tableName,    // TABLE_NAME
			"BASE TABLE", // TABLE_TYPE
			"InnoDB",     // ENGINE
			10,           // VERSION
			"Dynamic",    // ROW_FORMAT
			nil,          // TABLE_ROWS
			nil,          // AVG_ROW_LENGTH
			nil,          // DATA_LENGTH
			nil,          // MAX_DATA_LENGTH
			nil,          // INDEX_LENGTH
			nil,          // DATA_FREE
			nil,          // AUTO_INCREMENT
			nil,          // CREATE_TIME
			nil,          // UPDATE_TIME
			nil,          // CHECK_TIME
			"utf8mb4_general_ci", // TABLE_COLLATION
			nil,          // CHECKSUM
			"",           // CREATE_OPTIONS
			"",           // TABLE_COMMENT
		})
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "TABLE_CATALOG", Type: 0xFD},
			{Name: "TABLE_SCHEMA", Type: 0xFD},
			{Name: "TABLE_NAME", Type: 0xFD},
			{Name: "TABLE_TYPE", Type: 0xFD},
			{Name: "ENGINE", Type: 0xFD},
			{Name: "VERSION", Type: 0x03},
			{Name: "ROW_FORMAT", Type: 0xFD},
			{Name: "TABLE_ROWS", Type: 0x08},
			{Name: "AVG_ROW_LENGTH", Type: 0x08},
			{Name: "DATA_LENGTH", Type: 0x08},
			{Name: "MAX_DATA_LENGTH", Type: 0x08},
			{Name: "INDEX_LENGTH", Type: 0x08},
			{Name: "DATA_FREE", Type: 0x08},
			{Name: "AUTO_INCREMENT", Type: 0x08},
			{Name: "CREATE_TIME", Type: 0x0C},
			{Name: "UPDATE_TIME", Type: 0x0C},
			{Name: "CHECK_TIME", Type: 0x0C},
			{Name: "TABLE_COLLATION", Type: 0xFD},
			{Name: "CHECKSUM", Type: 0x08},
			{Name: "CREATE_OPTIONS", Type: 0xFD},
			{Name: "TABLE_COMMENT", Type: 0xFD},
		},
		Rows: tables,
	}, nil
}

// handleInformationSchemaColumns handles queries to INFORMATION_SCHEMA.COLUMNS
func (h *CoordinatorHandler) handleInformationSchemaColumns(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	// Extract database and table names from WHERE clause
	dbName := extractWhereValue(query, "TABLE_SCHEMA")
	tableName := extractWhereValue(query, "TABLE_NAME")

	if dbName == "" {
		dbName = session.CurrentDatabase
	}

	if dbName == "" {
		return nil, fmt.Errorf("no database specified")
	}

	log.Debug().
		Str("database", dbName).
		Str("table", tableName).
		Msg("Querying INFORMATION_SCHEMA.COLUMNS")

	var columns [][]interface{}

	if tableName != "" {
		// Query specific table
		cols, err := h.getTableColumns(dbName, tableName)
		if err != nil {
			return nil, err
		}
		columns = append(columns, cols...)
	} else {
		// Query all tables
		tablesQuery := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
		resp, err := h.execMetadataQuery(dbName, tablesQuery)
		if err != nil {
			return nil, err
		}

		for _, row := range resp.Rows {
			tblName, _ := row["name"].(string)
			cols, err := h.getTableColumns(dbName, tblName)
			if err != nil {
				continue
			}
			columns = append(columns, cols...)
		}
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "TABLE_CATALOG", Type: 0xFD},
			{Name: "TABLE_SCHEMA", Type: 0xFD},
			{Name: "TABLE_NAME", Type: 0xFD},
			{Name: "COLUMN_NAME", Type: 0xFD},
			{Name: "ORDINAL_POSITION", Type: 0x03},
			{Name: "COLUMN_DEFAULT", Type: 0xFD},
			{Name: "IS_NULLABLE", Type: 0xFD},
			{Name: "DATA_TYPE", Type: 0xFD},
			{Name: "CHARACTER_MAXIMUM_LENGTH", Type: 0x08},
			{Name: "CHARACTER_OCTET_LENGTH", Type: 0x08},
			{Name: "NUMERIC_PRECISION", Type: 0x08},
			{Name: "NUMERIC_SCALE", Type: 0x08},
			{Name: "DATETIME_PRECISION", Type: 0x08},
			{Name: "CHARACTER_SET_NAME", Type: 0xFD},
			{Name: "COLLATION_NAME", Type: 0xFD},
			{Name: "COLUMN_TYPE", Type: 0xFD},
			{Name: "COLUMN_KEY", Type: 0xFD},
			{Name: "EXTRA", Type: 0xFD},
			{Name: "PRIVILEGES", Type: 0xFD},
			{Name: "COLUMN_COMMENT", Type: 0xFD},
		},
		Rows: columns,
	}, nil
}

// handleInformationSchemaSchemata handles queries to INFORMATION_SCHEMA.SCHEMATA
func (h *CoordinatorHandler) handleInformationSchemaSchemata(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	// Extract schema name from WHERE clause if present
	schemaName := extractWhereValue(query, "SCHEMA_NAME")

	log.Debug().
		Str("schema", schemaName).
		Msg("Querying INFORMATION_SCHEMA.SCHEMATA")

	if h.dbManager == nil {
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{
				{Name: "CATALOG_NAME", Type: 0xFD},
				{Name: "SCHEMA_NAME", Type: 0xFD},
				{Name: "DEFAULT_CHARACTER_SET_NAME", Type: 0xFD},
				{Name: "DEFAULT_COLLATION_NAME", Type: 0xFD},
				{Name: "SQL_PATH", Type: 0xFD},
			},
			Rows: [][]interface{}{},
		}, nil
	}

	databases := h.dbManager.ListDatabases()

	var schemas [][]interface{}
	for _, dbName := range databases {
		// Skip system database
		if dbName == SystemDatabaseName {
			continue
		}

		// Filter by schema name if specified
		if schemaName != "" && dbName != schemaName {
			continue
		}

		schemas = append(schemas, []interface{}{
			"def",                // CATALOG_NAME
			dbName,               // SCHEMA_NAME
			"utf8mb4",            // DEFAULT_CHARACTER_SET_NAME
			"utf8mb4_general_ci", // DEFAULT_COLLATION_NAME
			nil,                  // SQL_PATH
		})
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "CATALOG_NAME", Type: 0xFD},
			{Name: "SCHEMA_NAME", Type: 0xFD},
			{Name: "DEFAULT_CHARACTER_SET_NAME", Type: 0xFD},
			{Name: "DEFAULT_COLLATION_NAME", Type: 0xFD},
			{Name: "SQL_PATH", Type: 0xFD},
		},
		Rows: schemas,
	}, nil
}

// handleInformationSchemaStatistics handles queries to INFORMATION_SCHEMA.STATISTICS
func (h *CoordinatorHandler) handleInformationSchemaStatistics(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	dbName := extractWhereValue(query, "TABLE_SCHEMA")
	tableName := extractWhereValue(query, "TABLE_NAME")

	if dbName == "" {
		dbName = session.CurrentDatabase
	}

	if dbName == "" || tableName == "" {
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{
				{Name: "TABLE_CATALOG", Type: 0xFD},
				{Name: "TABLE_SCHEMA", Type: 0xFD},
				{Name: "TABLE_NAME", Type: 0xFD},
				{Name: "NON_UNIQUE", Type: 0x03},
				{Name: "INDEX_SCHEMA", Type: 0xFD},
				{Name: "INDEX_NAME", Type: 0xFD},
				{Name: "SEQ_IN_INDEX", Type: 0x03},
				{Name: "COLUMN_NAME", Type: 0xFD},
			},
			Rows: [][]interface{}{},
		}, nil
	}

	// Use SHOW INDEXES handler
	stmt := protocol.Statement{
		Database:  dbName,
		TableName: tableName,
	}

	result, err := h.handleShowIndexes(session, stmt)
	if err != nil {
		return nil, err
	}

	// Convert to INFORMATION_SCHEMA.STATISTICS format
	var statistics [][]interface{}
	for _, row := range result.Rows {
		if len(row) >= 5 {
			statistics = append(statistics, []interface{}{
				"def",  // TABLE_CATALOG
				dbName, // TABLE_SCHEMA
				row[0], // TABLE_NAME
				row[1], // NON_UNIQUE
				dbName, // INDEX_SCHEMA
				row[2], // INDEX_NAME
				row[3], // SEQ_IN_INDEX
				row[4], // COLUMN_NAME
			})
		}
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "TABLE_CATALOG", Type: 0xFD},
			{Name: "TABLE_SCHEMA", Type: 0xFD},
			{Name: "TABLE_NAME", Type: 0xFD},
			{Name: "NON_UNIQUE", Type: 0x03},
			{Name: "INDEX_SCHEMA", Type: 0xFD},
			{Name: "INDEX_NAME", Type: 0xFD},
			{Name: "SEQ_IN_INDEX", Type: 0x03},
			{Name: "COLUMN_NAME", Type: 0xFD},
		},
		Rows: statistics,
	}, nil
}

// Helper functions

// getTableColumns retrieves column information for a specific table
func (h *CoordinatorHandler) getTableColumns(dbName, tableName string) ([][]interface{}, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	resp, err := h.execMetadataQuery(dbName, query)
	if err != nil {
		return nil, err
	}

	var columns [][]interface{}
	for _, row := range resp.Rows {
		cid, _ := toInt64(row["cid"])
		name, _ := row["name"].(string)
		colType, _ := row["type"].(string)
		notNullInt, _ := toInt64(row["notnull"])
		pkInt, _ := toInt64(row["pk"])
		dfltValue := row["dflt_value"]

		// Convert SQLite types to MySQL-compatible format
		mysqlType := sqliteToMySQLType(colType)
		dataType := extractDataType(mysqlType)

		nullable := "YES"
		if notNullInt == 1 {
			nullable = "NO"
		}

		key := ""
		if pkInt > 0 {
			key = "PRI"
		}

		defaultVal := interface{}(nil)
		if dfltValue != nil {
			if strVal, ok := dfltValue.(string); ok {
				defaultVal = strVal
			}
		}

		columns = append(columns, []interface{}{
			"def",                    // TABLE_CATALOG
			dbName,                   // TABLE_SCHEMA
			tableName,                // TABLE_NAME
			name,                     // COLUMN_NAME
			cid + 1,                  // ORDINAL_POSITION
			defaultVal,               // COLUMN_DEFAULT
			nullable,                 // IS_NULLABLE
			dataType,                 // DATA_TYPE
			nil,                      // CHARACTER_MAXIMUM_LENGTH
			nil,                      // CHARACTER_OCTET_LENGTH
			nil,                      // NUMERIC_PRECISION
			nil,                      // NUMERIC_SCALE
			nil,                      // DATETIME_PRECISION
			"utf8mb4",                // CHARACTER_SET_NAME
			"utf8mb4_general_ci",     // COLLATION_NAME
			mysqlType,                // COLUMN_TYPE
			key,                      // COLUMN_KEY
			"",                       // EXTRA
			"select,insert,update,references", // PRIVILEGES
			"",                       // COLUMN_COMMENT
		})
	}

	return columns, nil
}

// getAllTablesFromAllDatabases returns tables from all databases
func (h *CoordinatorHandler) getAllTablesFromAllDatabases() (*protocol.ResultSet, error) {
	if h.dbManager == nil {
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{},
			Rows:    [][]interface{}{},
		}, nil
	}

	databases := h.dbManager.ListDatabases()
	var allTables [][]interface{}

	for _, dbName := range databases {
		if dbName == SystemDatabaseName {
			continue
		}

		query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
		resp, err := h.execMetadataQuery(dbName, query)
		if err != nil {
			continue
		}

		for _, row := range resp.Rows {
			tableName, _ := row["name"].(string)

			allTables = append(allTables, []interface{}{
				"def",        // TABLE_CATALOG
				dbName,       // TABLE_SCHEMA
				tableName,    // TABLE_NAME
				"BASE TABLE", // TABLE_TYPE
			})
		}
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "TABLE_CATALOG", Type: 0xFD},
			{Name: "TABLE_SCHEMA", Type: 0xFD},
			{Name: "TABLE_NAME", Type: 0xFD},
			{Name: "TABLE_TYPE", Type: 0xFD},
		},
		Rows: allTables,
	}, nil
}

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
