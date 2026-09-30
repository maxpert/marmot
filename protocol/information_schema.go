package protocol

import "slices"

// Column types used by the INFORMATION_SCHEMA column sets.
const (
	isTypeString   byte = 0xFD // MYSQL_TYPE_VAR_STRING
	isTypeLong     byte = 0x03 // MYSQL_TYPE_LONG
	isTypeLongLong byte = 0x08 // MYSQL_TYPE_LONGLONG
	isTypeDatetime byte = 0x0C // MYSQL_TYPE_DATETIME
)

var (
	isTablesColumns = []ColumnDef{
		{Name: "TABLE_CATALOG", Type: isTypeString},
		{Name: "TABLE_SCHEMA", Type: isTypeString},
		{Name: "TABLE_NAME", Type: isTypeString},
		{Name: "TABLE_TYPE", Type: isTypeString},
		{Name: "ENGINE", Type: isTypeString},
		{Name: "VERSION", Type: isTypeLong},
		{Name: "ROW_FORMAT", Type: isTypeString},
		{Name: "TABLE_ROWS", Type: isTypeLongLong},
		{Name: "AVG_ROW_LENGTH", Type: isTypeLongLong},
		{Name: "DATA_LENGTH", Type: isTypeLongLong},
		{Name: "MAX_DATA_LENGTH", Type: isTypeLongLong},
		{Name: "INDEX_LENGTH", Type: isTypeLongLong},
		{Name: "DATA_FREE", Type: isTypeLongLong},
		{Name: "AUTO_INCREMENT", Type: isTypeLongLong},
		{Name: "CREATE_TIME", Type: isTypeDatetime},
		{Name: "UPDATE_TIME", Type: isTypeDatetime},
		{Name: "CHECK_TIME", Type: isTypeDatetime},
		{Name: "TABLE_COLLATION", Type: isTypeString},
		{Name: "CHECKSUM", Type: isTypeLongLong},
		{Name: "CREATE_OPTIONS", Type: isTypeString},
		{Name: "TABLE_COMMENT", Type: isTypeString},
	}

	isColumnsColumns = []ColumnDef{
		{Name: "TABLE_CATALOG", Type: isTypeString},
		{Name: "TABLE_SCHEMA", Type: isTypeString},
		{Name: "TABLE_NAME", Type: isTypeString},
		{Name: "COLUMN_NAME", Type: isTypeString},
		{Name: "ORDINAL_POSITION", Type: isTypeLong},
		{Name: "COLUMN_DEFAULT", Type: isTypeString},
		{Name: "IS_NULLABLE", Type: isTypeString},
		{Name: "DATA_TYPE", Type: isTypeString},
		{Name: "CHARACTER_MAXIMUM_LENGTH", Type: isTypeLongLong},
		{Name: "CHARACTER_OCTET_LENGTH", Type: isTypeLongLong},
		{Name: "NUMERIC_PRECISION", Type: isTypeLongLong},
		{Name: "NUMERIC_SCALE", Type: isTypeLongLong},
		{Name: "DATETIME_PRECISION", Type: isTypeLongLong},
		{Name: "CHARACTER_SET_NAME", Type: isTypeString},
		{Name: "COLLATION_NAME", Type: isTypeString},
		{Name: "COLUMN_TYPE", Type: isTypeString},
		{Name: "COLUMN_KEY", Type: isTypeString},
		{Name: "EXTRA", Type: isTypeString},
		{Name: "PRIVILEGES", Type: isTypeString},
		{Name: "COLUMN_COMMENT", Type: isTypeString},
	}

	isSchemataColumns = []ColumnDef{
		{Name: "CATALOG_NAME", Type: isTypeString},
		{Name: "SCHEMA_NAME", Type: isTypeString},
		{Name: "DEFAULT_CHARACTER_SET_NAME", Type: isTypeString},
		{Name: "DEFAULT_COLLATION_NAME", Type: isTypeString},
		{Name: "SQL_PATH", Type: isTypeString},
	}

	isStatisticsColumns = []ColumnDef{
		{Name: "TABLE_CATALOG", Type: isTypeString},
		{Name: "TABLE_SCHEMA", Type: isTypeString},
		{Name: "TABLE_NAME", Type: isTypeString},
		{Name: "NON_UNIQUE", Type: isTypeLong},
		{Name: "INDEX_SCHEMA", Type: isTypeString},
		{Name: "INDEX_NAME", Type: isTypeString},
		{Name: "SEQ_IN_INDEX", Type: isTypeLong},
		{Name: "COLUMN_NAME", Type: isTypeString},
	}
)

// InformationSchemaColumns returns the columns Marmot answers an
// INFORMATION_SCHEMA table with, or nil for a table it does not serve. The
// text query, the prepared statement's execution and its PREPARE description
// all use this one set, so the three can never disagree.
func InformationSchemaColumns(table InformationSchemaTableType) []ColumnDef {
	switch table {
	case ISTableTables:
		return slices.Clone(isTablesColumns)
	case ISTableColumns:
		return slices.Clone(isColumnsColumns)
	case ISTableSchemata:
		return slices.Clone(isSchemataColumns)
	case ISTableStatistics:
		return slices.Clone(isStatisticsColumns)
	default:
		return nil
	}
}
