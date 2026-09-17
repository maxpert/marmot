package protocol

import (
	"regexp"
	"strings"

	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol/query"
	"github.com/maxpert/marmot/protocol/query/transform"
	"github.com/rs/zerolog/log"
)

// truncateSQLForLog returns first n chars of SQL for logging
func truncateSQLForLog(sql string, n int) string {
	if len(sql) <= n {
		return sql
	}
	return sql[:n] + "..."
}

// errorString returns the error message if err is non-nil, empty string otherwise.
func errorString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

var (
	// Consistency hint pattern: /*+ CONSISTENCY(LEVEL) */
	consistencyHintPattern = regexp.MustCompile(`(?i)/\*\+\s*CONSISTENCY\s*\(\s*(\w+)\s*\)\s*\*/`)

	// Patterns used by Vitess pre-parser for MySQL-specific syntax
	savepointPattern        = regexp.MustCompile(`(?i)^\s*SAVEPOINT\s+`)
	releaseSavepointPattern = regexp.MustCompile(`(?i)^\s*RELEASE\s+SAVEPOINT\s+`)
	setTransactionPattern   = regexp.MustCompile(`(?i)^\s*SET\s+TRANSACTION\s+`)

	// XA Transaction control
	xaStartPattern    = regexp.MustCompile(`(?i)^\s*XA\s+START\s+`)
	xaEndPattern      = regexp.MustCompile(`(?i)^\s*XA\s+END\s+`)
	xaPreparePattern  = regexp.MustCompile(`(?i)^\s*XA\s+PREPARE\s+`)
	xaCommitPattern   = regexp.MustCompile(`(?i)^\s*XA\s+COMMIT\s+`)
	xaRollbackPattern = regexp.MustCompile(`(?i)^\s*XA\s+ROLLBACK\s+`)
	xaRecoverPattern  = regexp.MustCompile(`(?i)^\s*XA\s+RECOVER`)

	// Lock statements
	lockInstancePattern   = regexp.MustCompile(`(?i)^\s*LOCK\s+INSTANCE\s+FOR\s+BACKUP`)
	unlockInstancePattern = regexp.MustCompile(`(?i)^\s*UNLOCK\s+INSTANCE`)

	// Administrative statements
	installPluginPattern      = regexp.MustCompile(`(?i)^\s*INSTALL\s+PLUGIN\s+`)
	uninstallPluginPattern    = regexp.MustCompile(`(?i)^\s*UNINSTALL\s+PLUGIN\s+`)
	installComponentPattern   = regexp.MustCompile(`(?i)^\s*INSTALL\s+COMPONENT\s+`)
	uninstallComponentPattern = regexp.MustCompile(`(?i)^\s*UNINSTALL\s+COMPONENT\s+`)

	// Load XML pattern (MySQL specific)
	loadXMLPattern = regexp.MustCompile(`(?i)^\s*LOAD\s+XML\s+`)

	// DDL pattern for an object Vitess does not parse
	dropIndexPattern = regexp.MustCompile(`(?i)^\s*DROP\s+INDEX\s+`)

	// Vector index DDL patterns
	createVectorIndexPattern = regexp.MustCompile(`(?i)^\s*CREATE\s+VECTOR\s+INDEX\s+`)
	dropVectorIndexPattern   = regexp.MustCompile(`(?i)^\s*DROP\s+VECTOR\s+INDEX\s+`)
)

var globalPipeline *query.Pipeline

// InitializePipeline initializes the global query processing pipeline.
// idGen is optional - if nil, auto-increment ID injection is disabled.
func InitializePipeline(cacheSize int, idGen id.Generator) error {
	var err error
	globalPipeline, err = query.NewPipeline(cacheSize, idGen)
	return err
}

// SchemaLookupFunc returns the schema facts an INSERT needs for auto-increment
// id injection, or nil if the table is unknown.
type SchemaLookupFunc func(table string) *transform.SchemaInfo

// ParseOptions holds options for parsing SQL statements.
type ParseOptions struct {
	SchemaLookup      SchemaLookupFunc
	SchemaProvider    transform.SchemaProvider // For ON CONFLICT target resolution
	SkipTranspilation bool
	ExtractLiterals   bool // Enable literal extraction for parameterized execution
}

// ParseStatement analyzes a SQL statement and returns its type and metadata.
// This version does not perform auto-increment ID injection.
func ParseStatement(sql string) Statement {
	return ParseStatementWithSchema(sql, nil)
}

// ParseStatementWithOptions parses a SQL statement with the given options.
// Use this when you need to control transpilation behavior.
func ParseStatementWithOptions(sql string, opts ParseOptions) Statement {
	ctx := query.NewContext(sql, nil)
	ctx.SchemaLookup = opts.SchemaLookup
	ctx.SchemaProvider = opts.SchemaProvider
	ctx.SkipTranspilation = opts.SkipTranspilation
	ctx.ExtractLiterals = opts.ExtractLiterals

	if err := globalPipeline.Process(ctx); err != nil {
		log.Debug().
			Err(err).
			Str("sql_prefix", truncateSQLForLog(sql, 80)).
			Bool("skip_transpilation", opts.SkipTranspilation).
			Msg("PARSE: Pipeline processing failed")
		return Statement{
			SQL:          sql,
			Type:         StatementUnsupported,
			Error:        err.Error(),
			TranspileErr: ctx.Output.TranspileErr,
		}
	}

	// Extract transpiled SQL from first statement
	transpiledSQL := ""
	var extractedParams []interface{}
	var paramOrder []bool
	if len(ctx.Output.Statements) > 0 {
		transpiledSQL = ctx.Output.Statements[0].SQL
		if len(ctx.Output.Statements[0].Params) > 0 {
			extractedParams = ctx.Output.Statements[0].Params
		}
		paramOrder = ctx.Output.Statements[0].ParamOrder
	}

	stmt := Statement{
		SQL:             transpiledSQL,
		Type:            ctx.Output.StatementType,
		Database:        ctx.Output.Database,
		Error:           errorString(ctx.Output.ValidationErr),
		ExtractedParams: extractedParams,
		ParamOrder:      paramOrder,
	}

	// Extract MySQL-specific metadata (if available)
	if ctx.MySQLState != nil {
		applyMySQLStateToStatement(&stmt, ctx.MySQLState)
	}

	return stmt
}

// ParseStatementWithSchema analyzes a SQL statement with schema-based ID injection.
// If schemaLookup is provided, INSERT statements missing auto-increment columns
// will have HLC-based IDs injected.
func ParseStatementWithSchema(sql string, schemaLookup SchemaLookupFunc) Statement {
	ctx := query.NewContext(sql, nil)
	ctx.SchemaLookup = schemaLookup

	if err := globalPipeline.Process(ctx); err != nil {
		// Log parsing failures for debugging
		log.Debug().
			Err(err).
			Str("sql_prefix", truncateSQLForLog(sql, 80)).
			Msg("PARSE: Pipeline processing failed")
		return Statement{
			SQL:          sql,
			Type:         StatementUnsupported,
			Error:        err.Error(),
			TranspileErr: ctx.Output.TranspileErr,
		}
	}

	// Extract transpiled SQL from first statement
	transpiledSQL := ""
	var extractedParams []interface{}
	var paramOrder []bool
	if len(ctx.Output.Statements) > 0 {
		transpiledSQL = ctx.Output.Statements[0].SQL
		if len(ctx.Output.Statements[0].Params) > 0 {
			extractedParams = ctx.Output.Statements[0].Params
		}
		paramOrder = ctx.Output.Statements[0].ParamOrder
	}

	stmt := Statement{
		SQL:             transpiledSQL,
		Type:            ctx.Output.StatementType,
		Database:        ctx.Output.Database,
		Error:           errorString(ctx.Output.ValidationErr),
		ExtractedParams: extractedParams,
		ParamOrder:      paramOrder,
	}

	// Extract MySQL-specific metadata (if available)
	if ctx.MySQLState != nil {
		applyMySQLStateToStatement(&stmt, ctx.MySQLState)
	}

	return stmt
}

// ParseStatementsWithSchema parses a SQL statement and returns all transpiled statements.
// For DDL that generates multiple statements (e.g., CREATE TABLE with KEY definitions),
// each generated statement becomes a separate Statement in the slice.
// These should be added to the same transaction for atomic execution.
func ParseStatementsWithSchema(sql string, schemaLookup SchemaLookupFunc) []Statement {
	ctx := query.NewContext(sql, nil)
	ctx.SchemaLookup = schemaLookup

	if err := globalPipeline.Process(ctx); err != nil {
		log.Debug().
			Err(err).
			Str("sql_prefix", truncateSQLForLog(sql, 80)).
			Msg("PARSE: Pipeline processing failed")
		return []Statement{{
			SQL:          sql,
			Type:         StatementUnsupported,
			Error:        err.Error(),
			TranspileErr: ctx.Output.TranspileErr,
		}}
	}

	// Create a Statement for each transpiled statement
	stmts := make([]Statement, 0, len(ctx.Output.Statements))
	for _, ts := range ctx.Output.Statements {
		stmts = append(stmts, buildStatement(*ctx, ts))
	}
	return stmts
}

// buildStatement creates a Statement from context with specified TranspiledStatement
func buildStatement(ctx query.QueryContext, ts query.TranspiledStatement) Statement {
	var extractedParams []interface{}
	if len(ts.Params) > 0 {
		extractedParams = ts.Params
	}

	stmt := Statement{
		SQL:             ts.SQL,
		Type:            ctx.Output.StatementType,
		Database:        ctx.Output.Database,
		Error:           errorString(ctx.Output.ValidationErr),
		ExtractedParams: extractedParams,
		ParamOrder:      ts.ParamOrder,
	}

	// Extract MySQL-specific metadata (if available)
	if ctx.MySQLState != nil {
		applyMySQLStateToStatement(&stmt, ctx.MySQLState)
	}

	return stmt
}

func applyMySQLStateToStatement(stmt *Statement, state *query.MySQLParseState) {
	stmt.TableName = state.TableName
	stmt.ISFilter = InformationSchemaFilter{
		SchemaName: state.ISFilter.SchemaName,
		TableName:  state.ISFilter.TableName,
		ColumnName: state.ISFilter.ColumnName,
	}
	stmt.ISTableType = InformationSchemaTableType(state.ISTableType)
	stmt.VirtualTableType = VirtualTableType(state.VirtualTableType)
	stmt.SystemVarNames = state.SystemVarNames
	stmt.ShowFilter = state.ShowFilter
	stmt.VectorIndexName = state.VectorIndexName
	stmt.VectorColumnName = state.VectorColumnName
	stmt.VectorMetric = state.VectorMetric
	stmt.VectorDim = state.VectorDim
	stmt.VectorNlist = state.VectorNlist
	stmt.VectorNprobe = state.VectorNprobe
	stmt.VectorMaxNorm = state.VectorMaxNorm
	stmt.ParsedAST = state.AST
}

// NormalizeSQLForSQLite converts MySQL-style SQL to SQLite-compatible SQL
// This is the central place for all MySQL -> SQLite transformations
func NormalizeSQLForSQLite(sql string) string {
	// Convert backslash escapes to SQLite double-quote escapes
	// MySQL/Vitess uses \' but SQLite uses ''
	sql = strings.ReplaceAll(sql, `\'`, `''`)

	// Convert backslash-escaped double quotes if any
	sql = strings.ReplaceAll(sql, `\"`, `"`)

	// Convert backslash-escaped backslashes
	sql = strings.ReplaceAll(sql, `\\`, `\`)

	return sql
}

// ExtractConsistencyHint extracts consistency hint from SQL comment
// Example: /*+ CONSISTENCY(QUORUM) */ SELECT * FROM users
func ExtractConsistencyHint(sql string) (ConsistencyLevel, bool) {
	matches := consistencyHintPattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return ConsistencyQuorum, false // Fallback, caller should use config default
	}

	level, err := ParseConsistencyLevel(strings.ToUpper(matches[1]))
	if err != nil {
		return ConsistencyQuorum, false // Fallback, caller should use config default
	}

	return level, true
}

// IsMutation returns true if the statement is a write operation
func IsMutation(stmt Statement) bool {
	switch stmt.Type {
	case StatementInsert, StatementReplace, StatementUpdate, StatementDelete, StatementLoadData,
		StatementDDL, StatementDCL, StatementAdmin,
		StatementCreateDatabase, StatementDropDatabase,
		StatementCreateVectorIndex, StatementDropVectorIndex,
		StatementReindexVectorIndex:
		return true
	default:
		return false
	}
}

// IsDML returns true if the statement is a row-level DML operation
// (INSERT, UPDATE, DELETE, REPLACE) that requires intent key tracking
func IsDML(stmt Statement) bool {
	switch stmt.Type {
	case StatementInsert, StatementUpdate, StatementDelete, StatementReplace:
		return true
	default:
		return false
	}
}

// IsTransactionControl returns true if the statement is transaction control
func IsTransactionControl(stmt Statement) bool {
	switch stmt.Type {
	case StatementBegin, StatementCommit, StatementRollback, StatementSavepoint, StatementXA:
		return true
	default:
		return false
	}
}

// IsDDL returns true if the statement is a DDL operation
// (CREATE/ALTER/DROP TABLE, CREATE/DROP INDEX, etc.)
func IsDDL(stmt Statement) bool {
	return stmt.Type == StatementDDL
}
