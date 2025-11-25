package query

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type Dialect int

const (
	DialectUnknown Dialect = iota
	DialectMySQL
	DialectSQLite
)

type Transformation struct {
	Rule   string
	Method string
	Before string
	After  string
}

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
)

type QueryContext struct {
	OriginalSQL string
	Parameters  []interface{}

	AST           sqlparser.Statement
	StatementType StatementType
	TableName     string
	Database      string

	SourceDialect  Dialect
	NeedsTranspile bool

	TranspiledSQL   string
	Transformations []Transformation
	WasCached       bool

	IsValid       bool
	ValidationErr error

	IsMutation      bool
	IsReadOnly      bool
	RequiresPrepare bool

	// CDC (Change Data Capture) data extracted from AST immediately after parsing
	// This is populated BEFORE transpilation to ensure AST validity
	CDCRowKey    string
	CDCOldValues map[string][]byte
	CDCNewValues map[string][]byte

	ExecutionErr error
	RowsAffected int64
	ResultSet    interface{}
}

func NewContext(sql string, params []interface{}) *QueryContext {
	ctx := &QueryContext{
		OriginalSQL: sql,
		Parameters:  params,
	}

	ctx.SourceDialect = detectDialect(sql)
	ctx.NeedsTranspile = (ctx.SourceDialect == DialectMySQL)

	return ctx
}

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
