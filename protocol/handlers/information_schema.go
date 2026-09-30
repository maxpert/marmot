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

	var (
		rows [][]interface{}
		err  error
	)
	// Route based on pre-parsed table type (no string matching needed)
	switch stmt.ISTableType {
	case protocol.ISTableTables:
		rows, err = m.informationSchemaTables(currentDB, stmt.ISFilter)
	case protocol.ISTableColumns:
		rows, err = m.informationSchemaColumns(currentDB, stmt.ISFilter)
	case protocol.ISTableSchemata:
		rows = m.informationSchemaSchemata(stmt.ISFilter)
	case protocol.ISTableStatistics:
		rows, err = m.informationSchemaStatistics(currentDB, stmt.ISFilter)
	default:
		// Unsupported INFORMATION_SCHEMA table - return empty result
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{},
			Rows:    [][]interface{}{},
		}, nil
	}
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = [][]interface{}{}
	}
	return &protocol.ResultSet{
		Columns: protocol.InformationSchemaColumns(stmt.ISTableType),
		Rows:    rows,
	}, nil
}

// visibleDatabase reports whether a client may see name in
// INFORMATION_SCHEMA: it exists and is not Marmot's system database. A schema
// a client cannot see has no tables, exactly as MySQL answers for one that
// does not exist.
func (m *MetadataHandler) visibleDatabase(name string) bool {
	return name != "" && name != m.systemDatabaseName && m.db.DatabaseExists(name)
}

// informationSchemaTables answers INFORMATION_SCHEMA.TABLES: every table of
// the filtered schema (else the session's, else every schema).
func (m *MetadataHandler) informationSchemaTables(currentDB string, filter protocol.InformationSchemaFilter) ([][]interface{}, error) {
	dbName := filter.SchemaName
	if dbName == "" {
		dbName = currentDB
	}

	schemas := []string{dbName}
	if dbName == "" {
		schemas = m.db.ListDatabases()
	}

	var rows [][]interface{}
	for _, schema := range schemas {
		if !m.visibleDatabase(schema) {
			continue
		}
		tables, err := m.userTables(schema, filter.TableName)
		if err != nil {
			return nil, err
		}
		for _, tableName := range tables {
			rows = append(rows, informationSchemaTablesRow(schema, tableName))
		}
	}
	return rows, nil
}

// informationSchemaTablesRow is one INFORMATION_SCHEMA.TABLES row, in
// protocol.InformationSchemaColumns(ISTableTables) order.
func informationSchemaTablesRow(schema, tableName string) []interface{} {
	return []interface{}{
		"def",                // TABLE_CATALOG
		schema,               // TABLE_SCHEMA
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
	}
}

// informationSchemaColumns answers INFORMATION_SCHEMA.COLUMNS for the
// filtered table, or for every table of the schema.
func (m *MetadataHandler) informationSchemaColumns(currentDB string, filter protocol.InformationSchemaFilter) ([][]interface{}, error) {
	dbName := filter.SchemaName
	if dbName == "" {
		dbName = currentDB
	}
	if dbName == "" {
		return nil, fmt.Errorf("no database specified")
	}
	if !m.visibleDatabase(dbName) {
		return nil, nil
	}

	log.Debug().
		Str("database", dbName).
		Str("table", filter.TableName).
		Msg("Querying INFORMATION_SCHEMA.COLUMNS")

	// The table list is read to the end before any column is read, so no
	// query runs while another holds a connection.
	tables, err := m.userTables(dbName, filter.TableName)
	if err != nil {
		return nil, err
	}
	var rows [][]interface{}
	for _, tableName := range tables {
		cols, err := m.tableColumns(dbName, tableName)
		if err != nil {
			return nil, err
		}
		rows = append(rows, cols...)
	}
	return rows, nil
}

// informationSchemaSchemata answers INFORMATION_SCHEMA.SCHEMATA.
func (m *MetadataHandler) informationSchemaSchemata(filter protocol.InformationSchemaFilter) [][]interface{} {
	var rows [][]interface{}
	for _, dbName := range m.db.ListDatabases() {
		if dbName == m.systemDatabaseName {
			continue
		}
		if filter.SchemaName != "" && dbName != filter.SchemaName {
			continue
		}
		rows = append(rows, []interface{}{
			"def",                // CATALOG_NAME
			dbName,               // SCHEMA_NAME
			"utf8mb4",            // DEFAULT_CHARACTER_SET_NAME
			"utf8mb4_general_ci", // DEFAULT_COLLATION_NAME
			nil,                  // SQL_PATH
		})
	}
	return rows
}

// informationSchemaStatistics answers INFORMATION_SCHEMA.STATISTICS for the
// filtered table; without a table it answers no rows.
func (m *MetadataHandler) informationSchemaStatistics(currentDB string, filter protocol.InformationSchemaFilter) ([][]interface{}, error) {
	dbName := filter.SchemaName
	if dbName == "" {
		dbName = currentDB
	}
	if filter.TableName == "" || !m.visibleDatabase(dbName) {
		return nil, nil
	}

	result, err := m.HandleShowIndexes(dbName, filter.TableName)
	if err != nil {
		return nil, err
	}

	var rows [][]interface{}
	for _, row := range result.Rows {
		rows = append(rows, []interface{}{
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
	return rows, nil
}

// tableColumns returns a table's INFORMATION_SCHEMA.COLUMNS rows.
func (m *MetadataHandler) tableColumns(dbName, tableName string) ([][]interface{}, error) {
	infos, err := m.columnInfos(dbName, tableName)
	if err != nil {
		return nil, err
	}

	columns := make([][]interface{}, 0, len(infos))
	for _, info := range infos {
		mysqlType := sqliteToMySQLType(info.declType)
		columns = append(columns, []interface{}{
			"def",                             // TABLE_CATALOG
			dbName,                            // TABLE_SCHEMA
			tableName,                         // TABLE_NAME
			info.name,                         // COLUMN_NAME
			info.cid + 1,                      // ORDINAL_POSITION
			info.defaultValue(),               // COLUMN_DEFAULT
			info.nullable(),                   // IS_NULLABLE
			extractDataType(mysqlType),        // DATA_TYPE
			nil,                               // CHARACTER_MAXIMUM_LENGTH
			nil,                               // CHARACTER_OCTET_LENGTH
			nil,                               // NUMERIC_PRECISION
			nil,                               // NUMERIC_SCALE
			nil,                               // DATETIME_PRECISION
			"utf8mb4",                         // CHARACTER_SET_NAME
			"utf8mb4_general_ci",              // COLLATION_NAME
			mysqlType,                         // COLUMN_TYPE
			info.key(),                        // COLUMN_KEY
			"",                                // EXTRA
			"select,insert,update,references", // PRIVILEGES
			"",                                // COLUMN_COMMENT
		})
	}
	return columns, nil
}

// columnInfo is one row of SQLite's table_info pragma.
type columnInfo struct {
	cid      int
	name     string
	declType string
	notNull  int
	dflt     sql.NullString
	pk       int
}

func (c columnInfo) nullable() string {
	if c.notNull == 1 {
		return "NO"
	}
	return "YES"
}

func (c columnInfo) key() string {
	if c.pk > 0 {
		return "PRI"
	}
	return ""
}

func (c columnInfo) defaultValue() interface{} {
	if c.dflt.Valid {
		return c.dflt.String
	}
	return nil
}
