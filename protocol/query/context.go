package query

import (
	"strings"

	"github.com/maxpert/marmot/common"
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

// StatementCode categorizes SQL statements for execution routing and validation.
// Type alias to common.StatementCode for backward compatibility.
type StatementCode = common.StatementCode

// Const aliases for backward compatibility with existing code.
const (
	StatementUnknown           = common.StatementUnknown
	StatementInsert            = common.StatementInsert
	StatementReplace           = common.StatementReplace
	StatementUpdate            = common.StatementUpdate
	StatementDelete            = common.StatementDelete
	StatementLoadData          = common.StatementLoadData
	StatementDDL               = common.StatementDDL
	StatementDCL               = common.StatementDCL
	StatementBegin             = common.StatementBegin
	StatementCommit            = common.StatementCommit
	StatementRollback          = common.StatementRollback
	StatementSavepoint         = common.StatementSavepoint
	StatementXA                = common.StatementXA
	StatementLock              = common.StatementLock
	StatementSelect            = common.StatementSelect
	StatementAdmin             = common.StatementAdmin
	StatementSet               = common.StatementSet
	StatementShowDatabases     = common.StatementShowDatabases
	StatementUseDatabase       = common.StatementUseDatabase
	StatementCreateDatabase    = common.StatementCreateDatabase
	StatementDropDatabase      = common.StatementDropDatabase
	StatementShowTables        = common.StatementShowTables
	StatementShowColumns       = common.StatementShowColumns
	StatementShowCreateTable   = common.StatementShowCreateTable
	StatementShowIndexes       = common.StatementShowIndexes
	StatementShowTableStatus   = common.StatementShowTableStatus
	StatementInformationSchema = common.StatementInformationSchema
	StatementUnsupported       = common.StatementUnsupported
	StatementSystemVariable    = common.StatementSystemVariable
	StatementVirtualTable      = common.StatementVirtualTable
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
	StatementType StatementCode
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

	// SkipTranspilation bypasses MySQL→SQLite transpilation when true.
	// SQL is passed through unchanged, only statement type classification is performed.
	SkipTranspilation bool
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
