package handlers

import (
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/query/transform/intmarker"
	"github.com/rs/zerolog/log"
)

// userTablesQuery selects the names of a database's user tables: Marmot's own
// bookkeeping tables and SQLite's are never shown to a client.
const userTablesQuery = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__marmot__%'"

// DatabaseProvider abstracts database access for metadata queries.
//
// Metadata is read through the database's read pool, never its write handle:
// the write handle has one connection, and a metadata read waiting on it would
// wait behind every write, or behind itself.
type DatabaseProvider interface {
	ListDatabases() []string
	DatabaseExists(name string) bool
	GetDatabaseReadConnection(name string) (*sql.DB, error)
}

// MetadataHandler provides shared SHOW command implementations
type MetadataHandler struct {
	db                 DatabaseProvider
	systemDatabaseName string
}

// NewMetadataHandler creates a new MetadataHandler
func NewMetadataHandler(db DatabaseProvider, systemDatabaseName string) *MetadataHandler {
	return &MetadataHandler{
		db:                 db,
		systemDatabaseName: systemDatabaseName,
	}
}

// HandleShowDatabases returns list of databases
func (m *MetadataHandler) HandleShowDatabases() (*protocol.ResultSet, error) {
	dbs := m.db.ListDatabases()
	rows := make([][]interface{}, 0, len(dbs))
	for _, dbName := range dbs {
		rows = append(rows, []interface{}{dbName})
	}

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{{Name: "Database"}},
		Rows:    rows,
	}, nil
}

// HandleUseDatabase switches the current database context
func (m *MetadataHandler) HandleUseDatabase(dbName string) error {
	if !m.db.DatabaseExists(dbName) {
		return fmt.Errorf("ERROR 1049 (42000): Unknown database '%s'", dbName)
	}
	return nil
}

// HandleShowTables returns list of tables in the specified database
// The likeFilter parameter filters tables by name (empty string = no filter)
func (m *MetadataHandler) HandleShowTables(dbName string, likeFilter string) (*protocol.ResultSet, error) {
	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	log.Debug().Str("database", dbName).Str("filter", likeFilter).Msg("Handling SHOW TABLES")

	sqlDB, err := m.db.GetDatabaseReadConnection(dbName)
	if err != nil {
		return nil, err
	}

	// MySQL LIKE is case-insensitive by default, as SQLite's LIKE is, and both
	// use the same % and _ wildcards.
	query := userTablesQuery
	var args []interface{}
	if likeFilter != "" {
		query += " AND name LIKE ?"
		args = append(args, likeFilter)
	}
	query += " ORDER BY name"

	tables, err := queryStrings(sqlDB, query, args...)
	if err != nil {
		return nil, err
	}

	result := &protocol.ResultSet{
		Columns: []protocol.ColumnDef{{Name: fmt.Sprintf("Tables_in_%s", dbName)}},
		Rows:    make([][]interface{}, 0, len(tables)),
	}
	for _, tableName := range tables {
		result.Rows = append(result.Rows, []interface{}{tableName})
	}
	return result, nil
}

// HandleShowColumns returns column information for a table
func (m *MetadataHandler) HandleShowColumns(dbName, tableName string) (*protocol.ResultSet, error) {
	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	if tableName == "" {
		return nil, fmt.Errorf("no table specified")
	}

	log.Debug().
		Str("database", dbName).
		Str("table", tableName).
		Msg("Handling SHOW COLUMNS")

	infos, err := m.columnInfos(dbName, tableName)
	if err != nil {
		return nil, err
	}

	result := &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Field", Type: 0xFD},
			{Name: "Type", Type: 0xFD},
			{Name: "Null", Type: 0xFD},
			{Name: "Key", Type: 0xFD},
			{Name: "Default", Type: 0xFD},
			{Name: "Extra", Type: 0xFD},
		},
		Rows: make([][]interface{}, 0, len(infos)),
	}
	for _, info := range infos {
		result.Rows = append(result.Rows, []interface{}{
			info.name, sqliteToMySQLType(info.declType), info.nullable(), info.key(), info.defaultValue(), "",
		})
	}
	return result, nil
}

// HandleShowCreateTable returns the CREATE TABLE statement
func (m *MetadataHandler) HandleShowCreateTable(dbName, tableName string) (*protocol.ResultSet, error) {
	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	if tableName == "" {
		return nil, fmt.Errorf("no table specified")
	}

	log.Debug().
		Str("database", dbName).
		Str("table", tableName).
		Msg("Handling SHOW CREATE TABLE")

	sqlDB, err := m.db.GetDatabaseReadConnection(dbName)
	if err != nil {
		return nil, err
	}

	var createSQL string
	err = sqlDB.QueryRow("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", tableName).Scan(&createSQL)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("table not found: %s", tableName)
		}
		return nil, err
	}

	// sqlite_master.sql is returned verbatim, so it would carry the width
	// marker the transpiler writes beside narrow integer types. The marker is
	// Marmot's own bookkeeping; a client must never see it, and a tool that
	// round-trips SHOW CREATE TABLE output must not be handed a comment it
	// would then feed back in.
	//
	// This is the ONLY client-facing reader of the DDL text: every other
	// sqlite_master query in protocol/handlers selects `name`, and the column
	// types shown by SHOW COLUMNS and information_schema.COLUMNS come from
	// PRAGMA table_info, which reports plain INTEGER with the marker
	// normalised away.
	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Table", Type: 0xFD},
			{Name: "Create Table", Type: 0xFD},
		},
		Rows: [][]interface{}{
			{tableName, intmarker.Strip(createSQL)},
		},
	}, nil
}

// HandleShowIndexes returns index information for a table
func (m *MetadataHandler) HandleShowIndexes(dbName, tableName string) (*protocol.ResultSet, error) {
	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	if tableName == "" {
		return nil, fmt.Errorf("no table specified")
	}

	log.Debug().
		Str("database", dbName).
		Str("table", tableName).
		Msg("Handling SHOW INDEXES")

	sqlDB, err := m.db.GetDatabaseReadConnection(dbName)
	if err != nil {
		return nil, err
	}

	// The index list is read to the end before any index's columns are read,
	// so no query runs while another holds a connection.
	type indexEntry struct {
		name   string
		unique int
	}
	var indexes []indexEntry
	err = func() error {
		rows, err := sqlDB.Query("SELECT name, \"unique\" FROM pragma_index_list(?)", tableName)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e indexEntry
			if err := rows.Scan(&e.name, &e.unique); err != nil {
				return err
			}
			indexes = append(indexes, e)
		}
		return rows.Err()
	}()
	if err != nil {
		return nil, err
	}

	result := &protocol.ResultSet{
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
		Rows: make([][]interface{}, 0, len(indexes)),
	}

	for _, idx := range indexes {
		nonUnique := 1
		if idx.unique == 1 {
			nonUnique = 0
		}

		// An expression index has no column name; it shows an empty one.
		var columnName sql.NullString
		err := sqlDB.QueryRow("SELECT name FROM pragma_index_info(?) ORDER BY seqno LIMIT 1", idx.name).Scan(&columnName)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}

		result.Rows = append(result.Rows, []interface{}{
			tableName, nonUnique, idx.name, 1, columnName.String, "A", nil, nil, nil, "YES", "BTREE", "", "",
		})
	}

	return result, nil
}

// HandleShowTableStatus returns table status information
func (m *MetadataHandler) HandleShowTableStatus(dbName, tableName string) (*protocol.ResultSet, error) {
	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	log.Debug().
		Str("database", dbName).
		Str("table", tableName).
		Msg("Handling SHOW TABLE STATUS")

	sqlDB, err := m.db.GetDatabaseReadConnection(dbName)
	if err != nil {
		return nil, err
	}

	// Build query to get table info
	query := userTablesQuery
	var args []interface{}
	if tableName != "" {
		query = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"
		args = append(args, tableName)
	}

	rows, err := sqlDB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &protocol.ResultSet{
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
		Rows: make([][]interface{}, 0),
	}

	for rows.Next() {
		var tblName string
		if err := rows.Scan(&tblName); err != nil {
			continue
		}

		result.Rows = append(result.Rows, []interface{}{
			tblName,              // Name
			"SQLite",             // Engine
			10,                   // Version
			"Dynamic",            // Row_format
			0,                    // Rows
			0,                    // Avg_row_length
			0,                    // Data_length
			0,                    // Max_data_length
			0,                    // Index_length
			0,                    // Data_free
			nil,                  // Auto_increment
			nil,                  // Create_time
			nil,                  // Update_time
			nil,                  // Check_time
			"utf8mb4_general_ci", // Collation
			nil,                  // Checksum
			"",                   // Create_options
			"",                   // Comment
		})
	}

	return result, rows.Err()
}

// userTables lists a database's user tables by name, or only name when it is
// set.
func (m *MetadataHandler) userTables(dbName, name string) ([]string, error) {
	sqlDB, err := m.db.GetDatabaseReadConnection(dbName)
	if err != nil {
		return nil, err
	}
	query := userTablesQuery
	var args []interface{}
	if name != "" {
		query += " AND name = ?"
		args = append(args, name)
	}
	return queryStrings(sqlDB, query+" ORDER BY name", args...)
}

// columnInfos reads a table's columns from SQLite's table_info pragma. The
// table name is bound, never spliced into the statement.
func (m *MetadataHandler) columnInfos(dbName, tableName string) ([]columnInfo, error) {
	sqlDB, err := m.db.GetDatabaseReadConnection(dbName)
	if err != nil {
		return nil, err
	}
	rows, err := sqlDB.Query(`SELECT cid, name, type, "notnull", dflt_value, pk FROM pragma_table_info(?)`, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var infos []columnInfo
	for rows.Next() {
		var c columnInfo
		if err := rows.Scan(&c.cid, &c.name, &c.declType, &c.notNull, &c.dflt, &c.pk); err != nil {
			return nil, err
		}
		infos = append(infos, c)
	}
	return infos, rows.Err()
}

// queryStrings runs a query selecting one text column and reads it to the
// end. The rows are closed before it returns, so the connection is free for
// the caller's next query: metadata handlers never issue a query while
// another one's rows are open.
func queryStrings(db *sql.DB, query string, args ...interface{}) ([]string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
