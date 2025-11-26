package handlers

import (
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// HandleInformationSchema handles INFORMATION_SCHEMA queries using pre-parsed filter values
func (m *MetadataHandler) HandleInformationSchema(currentDB string, stmt protocol.Statement) (*protocol.ResultSet, error) {
	log.Debug().
		Int("is_table_type", int(stmt.ISTableType)).
		Str("filter_schema", stmt.ISFilter.SchemaName).
		Str("filter_table", stmt.ISFilter.TableName).
		Msg("Handling INFORMATION_SCHEMA query")

	// Route based on pre-parsed table type (no string matching needed)
	switch stmt.ISTableType {
	case protocol.ISTableTables:
		return m.handleInformationSchemaTables(currentDB, stmt.ISFilter)
	case protocol.ISTableColumns:
		return m.handleInformationSchemaColumns(currentDB, stmt.ISFilter)
	case protocol.ISTableSchemata:
		return m.handleInformationSchemaSchemata(stmt.ISFilter)
	case protocol.ISTableStatistics:
		return m.handleInformationSchemaStatistics(currentDB, stmt.ISFilter)
	default:
		// Unsupported INFORMATION_SCHEMA table - return empty result
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{},
			Rows:    [][]interface{}{},
		}, nil
	}
}

// handleInformationSchemaTables handles queries to INFORMATION_SCHEMA.TABLES
func (m *MetadataHandler) handleInformationSchemaTables(currentDB string, filter protocol.InformationSchemaFilter) (*protocol.ResultSet, error) {
	// Use filter value or fall back to current database
	dbName := filter.SchemaName
	if dbName == "" {
		dbName = currentDB
	}

	if dbName == "" {
		// Return all tables from all databases
		return m.getAllTablesFromAllDatabases()
	}

	log.Debug().
		Str("database", dbName).
		Msg("Querying INFORMATION_SCHEMA.TABLES")

	sqlDB, err := m.db.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	// Query SQLite's sqlite_master table
	rows, err := sqlDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__marmot__%' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables [][]interface{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		tables = append(tables, []interface{}{
			"def",                // TABLE_CATALOG
			dbName,               // TABLE_SCHEMA
			tableName,            // TABLE_NAME
			"BASE TABLE",         // TABLE_TYPE
			"SQLite",             // ENGINE
			10,                   // VERSION
			"Dynamic",            // ROW_FORMAT
			nil,                  // TABLE_ROWS
			nil,                  // AVG_ROW_LENGTH
			nil,                  // DATA_LENGTH
			nil,                  // MAX_DATA_LENGTH
			nil,                  // INDEX_LENGTH
			nil,                  // DATA_FREE
			nil,                  // AUTO_INCREMENT
			nil,                  // CREATE_TIME
			nil,                  // UPDATE_TIME
			nil,                  // CHECK_TIME
			"utf8mb4_general_ci", // TABLE_COLLATION
			nil,                  // CHECKSUM
			"",                   // CREATE_OPTIONS
			"",                   // TABLE_COMMENT
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
func (m *MetadataHandler) handleInformationSchemaColumns(currentDB string, filter protocol.InformationSchemaFilter) (*protocol.ResultSet, error) {
	// Use filter values or fall back to current database
	dbName := filter.SchemaName
	tableName := filter.TableName

	if dbName == "" {
		dbName = currentDB
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
		cols, err := m.getTableColumns(dbName, tableName)
		if err != nil {
			return nil, err
		}
		columns = append(columns, cols...)
	} else {
		// Query all tables
		sqlDB, err := m.db.GetDatabaseConnection(dbName)
		if err != nil {
			return nil, err
		}

		rows, err := sqlDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__marmot__%'")
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var tblName string
			if err := rows.Scan(&tblName); err != nil {
				continue
			}
			cols, err := m.getTableColumns(dbName, tblName)
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
func (m *MetadataHandler) handleInformationSchemaSchemata(filter protocol.InformationSchemaFilter) (*protocol.ResultSet, error) {
	schemaName := filter.SchemaName

	log.Debug().
		Str("schema", schemaName).
		Msg("Querying INFORMATION_SCHEMA.SCHEMATA")

	databases := m.db.ListDatabases()

	var schemas [][]interface{}
	for _, dbName := range databases {
		// Skip system database
		if dbName == m.systemDatabaseName {
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
func (m *MetadataHandler) handleInformationSchemaStatistics(currentDB string, filter protocol.InformationSchemaFilter) (*protocol.ResultSet, error) {
	dbName := filter.SchemaName
	tableName := filter.TableName

	if dbName == "" {
		dbName = currentDB
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
	result, err := m.HandleShowIndexes(dbName, tableName)
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

// getTableColumns retrieves column information for a specific table
func (m *MetadataHandler) getTableColumns(dbName, tableName string) ([][]interface{}, error) {
	sqlDB, err := m.db.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	rows, err := sqlDB.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns [][]interface{}
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var dfltValue sql.NullString
		var pk int

		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			continue
		}

		// Convert SQLite types to MySQL-compatible format
		mysqlType := sqliteToMySQLType(colType)
		dataType := extractDataType(mysqlType)

		nullable := "YES"
		if notNull == 1 {
			nullable = "NO"
		}

		key := ""
		if pk > 0 {
			key = "PRI"
		}

		defaultVal := interface{}(nil)
		if dfltValue.Valid {
			defaultVal = dfltValue.String
		}

		columns = append(columns, []interface{}{
			"def",                             // TABLE_CATALOG
			dbName,                            // TABLE_SCHEMA
			tableName,                         // TABLE_NAME
			name,                              // COLUMN_NAME
			cid + 1,                           // ORDINAL_POSITION
			defaultVal,                        // COLUMN_DEFAULT
			nullable,                          // IS_NULLABLE
			dataType,                          // DATA_TYPE
			nil,                               // CHARACTER_MAXIMUM_LENGTH
			nil,                               // CHARACTER_OCTET_LENGTH
			nil,                               // NUMERIC_PRECISION
			nil,                               // NUMERIC_SCALE
			nil,                               // DATETIME_PRECISION
			"utf8mb4",                         // CHARACTER_SET_NAME
			"utf8mb4_general_ci",              // COLLATION_NAME
			mysqlType,                         // COLUMN_TYPE
			key,                               // COLUMN_KEY
			"",                                // EXTRA
			"select,insert,update,references", // PRIVILEGES
			"",                                // COLUMN_COMMENT
		})
	}

	return columns, rows.Err()
}

// getAllTablesFromAllDatabases returns tables from all databases
func (m *MetadataHandler) getAllTablesFromAllDatabases() (*protocol.ResultSet, error) {
	databases := m.db.ListDatabases()
	var allTables [][]interface{}

	for _, dbName := range databases {
		if dbName == m.systemDatabaseName {
			continue
		}

		sqlDB, err := m.db.GetDatabaseConnection(dbName)
		if err != nil {
			continue
		}

		rows, err := sqlDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__marmot__%'")
		if err != nil {
			continue
		}

		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				continue
			}

			allTables = append(allTables, []interface{}{
				"def",        // TABLE_CATALOG
				dbName,       // TABLE_SCHEMA
				tableName,    // TABLE_NAME
				"BASE TABLE", // TABLE_TYPE
			})
		}
		rows.Close()
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
