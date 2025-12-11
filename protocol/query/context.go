package query

import (
	"strings"

	"github.com/maxpert/marmot/protocol/query/transform"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Dialect represents the SQL dialect of a query (MySQL or SQLite).
type Dialect int

const (
	DialectUnknown Dialect = iota
	DialectMySQL
	DialectSQLite
)

// Transformation records a transformation rule that was applied to a query.
type Transformation struct {
	Rule   string
	Method string
	Before string
	After  string
}

// StatementType categorizes SQL statements for execution routing and validation.
type StatementType int

const (
	StatementUnknown StatementType = iota // 0 - means not yet classified
	StatementInsert
	StatementReplace
	StatementUpdate
	StatementDelete
	StatementLoadData
	StatementDDL
	StatementDCL
	StatementBegin
	StatementCommit
	StatementRollback
	StatementSavepoint
	StatementXA
	StatementLock
	StatementSelect
	StatementAdmin
	StatementSet
	StatementShowDatabases
	StatementUseDatabase
	StatementCreateDatabase
	StatementDropDatabase
	StatementShowTables
	StatementShowColumns
	StatementShowCreateTable
	StatementShowIndexes
	StatementShowTableStatus
	StatementInformationSchema
	StatementUnsupported
	StatementSystemVariable // SELECT @@version, SELECT DATABASE(), etc.
	StatementVirtualTable   // SELECT * FROM MARMOT_CLUSTER_NODES, etc.
)

// InformationSchemaTableType identifies which INFORMATION_SCHEMA table is being queried.
type InformationSchemaTableType int

const (
	ISTableUnknown    InformationSchemaTableType = iota
	ISTableTables                                // INFORMATION_SCHEMA.TABLES
	ISTableColumns                               // INFORMATION_SCHEMA.COLUMNS
	ISTableSchemata                              // INFORMATION_SCHEMA.SCHEMATA
	ISTableStatistics                            // INFORMATION_SCHEMA.STATISTICS
)

// VirtualTableType identifies which Marmot virtual table is being queried.
type VirtualTableType int

const (
	VirtualTableUnknown      VirtualTableType = iota
	VirtualTableClusterNodes                  // MARMOT_CLUSTER_NODES or MARMOT.CLUSTER_NODES
)

// InformationSchemaFilter holds extracted WHERE clause values for INFORMATION_SCHEMA queries.
type InformationSchemaFilter struct {
	SchemaName string // From TABLE_SCHEMA = 'x' or SCHEMA_NAME = 'x'
	TableName  string // From TABLE_NAME = 'x'
	ColumnName string // From COLUMN_NAME = 'x'
}

// QueryInput holds the input parameters for a query.
type QueryInput struct {
	SQL        string
	Parameters []interface{}
	Dialect    Dialect
}

// QueryOutput holds the results of query processing.
type QueryOutput struct {
	Statements    []TranspiledStatement
	StatementType StatementType
	Database      string
	IsValid       bool
	ValidationErr error
}

// TranspiledStatement represents a single transpiled SQL statement with parameters.
type TranspiledStatement struct {
	SQL             string
	Params          []interface{}
	RequiresPrepare bool
}

// MySQLParseState holds MySQL-specific parsing state and metadata.
type MySQLParseState struct {
	AST              sqlparser.Statement
	Transformations  []Transformation
	WasCached        bool
	ISTableType      InformationSchemaTableType
	ISFilter         InformationSchemaFilter
	VirtualTableType VirtualTableType
	SystemVarNames   []string
	// TableName extracted from SQL for:
	// - Metadata queries (SHOW COLUMNS, SHOW CREATE TABLE, etc.)
	// - DML queries (INSERT, UPDATE, DELETE) for CDC schema preloading
	TableName string
}

// QueryContext holds all state for processing a single query through the pipeline.
type QueryContext struct {
	Input          QueryInput
	Output         QueryOutput
	MySQLState     *MySQLParseState // nil for SQLite dialect
	SchemaLookup   func(table string) string
	SchemaProvider transform.SchemaProvider
}

// IsMutation returns true if the statement type is a mutation operation.
func (t StatementType) IsMutation() bool {
	switch t {
	case StatementInsert, StatementReplace, StatementUpdate, StatementDelete,
		StatementLoadData, StatementDDL, StatementDCL, StatementAdmin,
		StatementCreateDatabase, StatementDropDatabase:
		return true
	}
	return false
}

// IsReadOnly returns true if the statement type is read-only.
func (t StatementType) IsReadOnly() bool {
	switch t {
	case StatementSelect, StatementShowDatabases, StatementShowTables,
		StatementShowColumns, StatementShowCreateTable, StatementShowIndexes,
		StatementShowTableStatus, StatementInformationSchema:
		return true
	}
	return false
}

// NewContext creates a new QueryContext for the given SQL and parameters.
// It automatically detects the SQL dialect.
func NewContext(sql string, params []interface{}) *QueryContext {
	dialect := detectDialect(sql)
	ctx := &QueryContext{
		Input: QueryInput{
			SQL:        sql,
			Parameters: params,
			Dialect:    dialect,
		},
		Output: QueryOutput{
			Statements: []TranspiledStatement{},
		},
	}

	// Initialize MySQLState only for MySQL dialect
	if dialect == DialectMySQL {
		ctx.MySQLState = &MySQLParseState{}
	}

	return ctx
}

// detectDialect analyzes SQL to determine if it's SQLite or MySQL dialect.
// It strips comments and looks for dialect-specific keywords and syntax patterns.
func detectDialect(sql string) Dialect {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	for strings.HasPrefix(upper, "/*") {
		if idx := strings.Index(upper, "*/"); idx >= 0 {
			upper = strings.TrimSpace(upper[idx+2:])
		} else {
			break
		}
	}
	for strings.HasPrefix(upper, "--") || strings.HasPrefix(upper, "#") {
		if idx := strings.Index(upper, "\n"); idx >= 0 {
			upper = strings.TrimSpace(upper[idx+1:])
		} else {
			return DialectMySQL
		}
	}

	if strings.HasPrefix(upper, "PRAGMA ") ||
		strings.HasPrefix(upper, "ATTACH ") ||
		strings.HasPrefix(upper, "DETACH ") ||
		strings.HasPrefix(upper, "VACUUM") ||
		(strings.HasPrefix(upper, "ANALYZE") && !strings.Contains(upper, "TABLE")) {
		return DialectSQLite
	}

	if strings.Contains(upper, "INSERT OR IGNORE") ||
		strings.Contains(upper, "INSERT OR REPLACE") ||
		strings.Contains(upper, "INSERT OR ABORT") ||
		strings.Contains(upper, "INSERT OR FAIL") ||
		strings.Contains(upper, "INSERT OR ROLLBACK") {
		return DialectSQLite
	}

	return DialectMySQL
}
