package handlers

import (
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// DatabaseProvider abstracts database access for metadata queries
type DatabaseProvider interface {
	ListDatabases() []string
	DatabaseExists(name string) bool
	GetDatabaseConnection(name string) (*sql.DB, error)
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

	sqlDB, err := m.db.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	// Build query with optional LIKE filter
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__marmot__%'"
	var args []interface{}
	if likeFilter != "" {
		// SQLite uses GLOB for case-sensitive matching or LIKE for case-insensitive
		// MySQL LIKE is case-insensitive by default, so we use SQLite LIKE
		// Convert MySQL LIKE pattern (% and _) to be used directly (SQLite supports same syntax)
		query += " AND name LIKE ?"
		args = append(args, likeFilter)
	}
	query += " ORDER BY name"

	rows, err := sqlDB.Query(query, args...)
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

	return result, rows.Err()
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

	sqlDB, err := m.db.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	rows, err := sqlDB.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Field", Type: 0xFD},
			{Name: "Type", Type: 0xFD},
			{Name: "Null", Type: 0xFD},
			{Name: "Key", Type: 0xFD},
			{Name: "Default", Type: 0xFD},
			{Name: "Extra", Type: 0xFD},
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

		// Convert SQLite types to MySQL-compatible format
		mysqlType := sqliteToMySQLType(colType)

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
			name, mysqlType, nullStr, keyStr, defaultVal, "",
		})
	}

	return result, rows.Err()
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

	sqlDB, err := m.db.GetDatabaseConnection(dbName)
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

	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Table", Type: 0xFD},
			{Name: "Create Table", Type: 0xFD},
		},
		Rows: [][]interface{}{
			{tableName, createSQL},
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

	sqlDB, err := m.db.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	rows, err := sqlDB.Query(fmt.Sprintf("PRAGMA index_list(%s)", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

		// Get index column info
		var columnName string
		indexRows, err := sqlDB.Query(fmt.Sprintf("PRAGMA index_info(%s)", name))
		if err == nil {
			if indexRows.Next() {
				var seqno, cid int
				var colName sql.NullString
				if err := indexRows.Scan(&seqno, &cid, &colName); err == nil && colName.Valid {
					columnName = colName.String
				}
			}
			indexRows.Close()
		}

		result.Rows = append(result.Rows, []interface{}{
			tableName, nonUnique, name, 1, columnName, "A", nil, nil, nil, "YES", "BTREE", "", "",
		})
	}

	return result, rows.Err()
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

	sqlDB, err := m.db.GetDatabaseConnection(dbName)
	if err != nil {
		return nil, err
	}

	// Build query to get table info
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__marmot__%'"
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
