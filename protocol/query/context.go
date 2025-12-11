package query

import (
	"strings"

	"github.com/maxpert/marmot/protocol/query/transform"
	rqlitesql "github.com/rqlite/sql"
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
	StatementInsert StatementType = iota
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

// QueryContext holds all state for processing a single query through the pipeline.
type QueryContext struct {
	OriginalSQL string
	Parameters  []interface{}

	AST           sqlparser.Statement // Vitess AST (for MySQL dialect)
	SQLiteAST     rqlitesql.Statement // rqlite AST (for SQLite dialect)
	StatementType StatementType
	TableName     string
	Database      string

	SourceDialect  Dialect
	NeedsTranspile bool

	TranspiledSQL        string                          // Primary transpiled statement (backwards compat)
	TranspiledStatements []transform.TranspiledStatement // All statements (main + additional like CREATE INDEX)
	Transformations      []Transformation
	WasCached            bool

	IsValid       bool
	ValidationErr error

	IsMutation      bool
	IsReadOnly      bool
	RequiresPrepare bool

	// InformationSchema filter values extracted from WHERE clause (for INFORMATION_SCHEMA queries)
	ISFilter InformationSchemaFilter

	// InformationSchema table type (which table is being queried)
	ISTableType InformationSchemaTableType

	// Virtual table type (for MARMOT_* virtual tables)
	VirtualTableType VirtualTableType

	// System variable metadata (for @@var and DATABASE() queries)
	SystemVarNames []string // List of system variables referenced (e.g., ["version", "sql_mode"])

	// HasFoundRowsCalc indicates SQL_CALC_FOUND_ROWS was in the original query
	// and COUNT(*) OVER() was appended. Execution layer should extract the count.
	HasFoundRowsCalc bool

	ExecutionErr error
	RowsAffected int64
	ResultSet    interface{}

	// SchemaLookup returns the auto-increment column name for a table, or empty string if none.
	// This is set by the handler before processing to enable schema-based ID injection.
	SchemaLookup func(table string) string

	// SchemaProvider returns full schema info for conflict target detection and ID injection.
	// Takes (database, table) and returns SchemaInfo or nil.
	SchemaProvider transform.SchemaProvider
}

// NewContext creates a new QueryContext for the given SQL and parameters.
// It automatically detects the SQL dialect and sets the NeedsTranspile flag.
func NewContext(sql string, params []interface{}) *QueryContext {
	ctx := &QueryContext{
		OriginalSQL: sql,
		Parameters:  params,
	}

	ctx.SourceDialect = detectDialect(sql)
	ctx.NeedsTranspile = (ctx.SourceDialect == DialectMySQL)

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
