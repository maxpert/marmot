package protocol

import (
	"regexp"
	"strings"

	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol/query"
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

	// DDL patterns for MySQL-specific objects not in Vitess
	createTriggerPattern       = regexp.MustCompile(`(?i)^\s*CREATE\s+TRIGGER\s+`)
	dropTriggerPattern         = regexp.MustCompile(`(?i)^\s*DROP\s+TRIGGER\s+`)
	createProcedurePattern     = regexp.MustCompile(`(?i)^\s*CREATE\s+PROCEDURE\s+`)
	dropProcedurePattern       = regexp.MustCompile(`(?i)^\s*DROP\s+PROCEDURE\s+`)
	alterProcedurePattern      = regexp.MustCompile(`(?i)^\s*ALTER\s+PROCEDURE\s+`)
	createFunctionPattern      = regexp.MustCompile(`(?i)^\s*CREATE\s+FUNCTION\s+`)
	dropFunctionPattern        = regexp.MustCompile(`(?i)^\s*DROP\s+FUNCTION\s+`)
	alterFunctionPattern       = regexp.MustCompile(`(?i)^\s*ALTER\s+FUNCTION\s+`)
	createEventPattern         = regexp.MustCompile(`(?i)^\s*CREATE\s+EVENT\s+`)
	dropEventPattern           = regexp.MustCompile(`(?i)^\s*DROP\s+EVENT\s+`)
	alterEventPattern          = regexp.MustCompile(`(?i)^\s*ALTER\s+EVENT\s+`)
	createTablespacePattern    = regexp.MustCompile(`(?i)^\s*CREATE\s+TABLESPACE\s+`)
	dropTablespacePattern      = regexp.MustCompile(`(?i)^\s*DROP\s+TABLESPACE\s+`)
	alterTablespacePattern     = regexp.MustCompile(`(?i)^\s*ALTER\s+TABLESPACE\s+`)
	createLogfileGroupPattern  = regexp.MustCompile(`(?i)^\s*CREATE\s+LOGFILE\s+GROUP\s+`)
	dropLogfileGroupPattern    = regexp.MustCompile(`(?i)^\s*DROP\s+LOGFILE\s+GROUP\s+`)
	alterLogfileGroupPattern   = regexp.MustCompile(`(?i)^\s*ALTER\s+LOGFILE\s+GROUP\s+`)
	createServerPattern        = regexp.MustCompile(`(?i)^\s*CREATE\s+SERVER\s+`)
	dropServerPattern          = regexp.MustCompile(`(?i)^\s*DROP\s+SERVER\s+`)
	alterServerPattern         = regexp.MustCompile(`(?i)^\s*ALTER\s+SERVER\s+`)
	createSpatialRefPattern    = regexp.MustCompile(`(?i)^\s*CREATE\s+SPATIAL\s+REFERENCE\s+SYSTEM\s+`)
	dropSpatialRefPattern      = regexp.MustCompile(`(?i)^\s*DROP\s+SPATIAL\s+REFERENCE\s+SYSTEM\s+`)
	createResourceGroupPattern = regexp.MustCompile(`(?i)^\s*CREATE\s+RESOURCE\s+GROUP\s+`)
	dropResourceGroupPattern   = regexp.MustCompile(`(?i)^\s*DROP\s+RESOURCE\s+GROUP\s+`)
	alterResourceGroupPattern  = regexp.MustCompile(`(?i)^\s*ALTER\s+RESOURCE\s+GROUP\s+`)
	dropIndexPattern           = regexp.MustCompile(`(?i)^\s*DROP\s+INDEX\s+`)
	renameTablePattern         = regexp.MustCompile(`(?i)^\s*RENAME\s+TABLE\s+`)

	// DCL patterns
	createUserPattern     = regexp.MustCompile(`(?i)^\s*CREATE\s+USER\s+`)
	dropUserPattern       = regexp.MustCompile(`(?i)^\s*DROP\s+USER\s+`)
	alterUserPattern      = regexp.MustCompile(`(?i)^\s*ALTER\s+USER\s+`)
	renameUserPattern     = regexp.MustCompile(`(?i)^\s*RENAME\s+USER\s+`)
	setPasswordPattern    = regexp.MustCompile(`(?i)^\s*SET\s+PASSWORD\s+`)
	grantPattern          = regexp.MustCompile(`(?i)^\s*GRANT\s+`)
	revokePattern         = regexp.MustCompile(`(?i)^\s*REVOKE\s+`)
	createRolePattern     = regexp.MustCompile(`(?i)^\s*CREATE\s+ROLE\s+`)
	dropRolePattern       = regexp.MustCompile(`(?i)^\s*DROP\s+ROLE\s+`)
	setRolePattern        = regexp.MustCompile(`(?i)^\s*SET\s+ROLE\s+`)
	setDefaultRolePattern = regexp.MustCompile(`(?i)^\s*SET\s+DEFAULT\s+ROLE\s+`)
)

var globalPipeline *query.Pipeline

// InitializePipeline initializes the global query processing pipeline.
// idGen is optional - if nil, auto-increment ID injection is disabled.
func InitializePipeline(cacheSize int, idGen id.Generator) error {
	var err error
	globalPipeline, err = query.NewPipeline(cacheSize, idGen)
	return err
}

// SchemaLookupFunc returns the auto-increment column name for a table, or empty string if none.
type SchemaLookupFunc func(table string) string

// ParseOptions holds options for parsing SQL statements.
type ParseOptions struct {
	SchemaLookup      SchemaLookupFunc
	SkipTranspilation bool
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
	ctx.SkipTranspilation = opts.SkipTranspilation

	if err := globalPipeline.Process(ctx); err != nil {
		log.Debug().
			Err(err).
			Str("sql_prefix", truncateSQLForLog(sql, 80)).
			Bool("skip_transpilation", opts.SkipTranspilation).
			Msg("PARSE: Pipeline processing failed")
		return Statement{
			SQL:   sql,
			Type:  StatementUnsupported,
			Error: err.Error(),
		}
	}

	// Extract transpiled SQL from first statement
	transpiledSQL := ""
	if len(ctx.Output.Statements) > 0 {
		transpiledSQL = ctx.Output.Statements[0].SQL
	}

	stmt := Statement{
		SQL:      transpiledSQL,
		Type:     ctx.Output.StatementType,
		Database: ctx.Output.Database,
		Error:    errorString(ctx.Output.ValidationErr),
	}

	// Extract MySQL-specific metadata (if available)
	if ctx.MySQLState != nil {
		stmt.TableName = ctx.MySQLState.TableName
		stmt.ISFilter = InformationSchemaFilter{
			SchemaName: ctx.MySQLState.ISFilter.SchemaName,
			TableName:  ctx.MySQLState.ISFilter.TableName,
			ColumnName: ctx.MySQLState.ISFilter.ColumnName,
		}
		stmt.ISTableType = InformationSchemaTableType(ctx.MySQLState.ISTableType)
		stmt.VirtualTableType = VirtualTableType(ctx.MySQLState.VirtualTableType)
		stmt.SystemVarNames = ctx.MySQLState.SystemVarNames
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
			SQL:   sql,
			Type:  StatementUnsupported,
			Error: err.Error(),
		}
	}

	// Extract transpiled SQL from first statement
	transpiledSQL := ""
	if len(ctx.Output.Statements) > 0 {
		transpiledSQL = ctx.Output.Statements[0].SQL
	}

	stmt := Statement{
		SQL:      transpiledSQL,
		Type:     ctx.Output.StatementType,
		Database: ctx.Output.Database,
		Error:    errorString(ctx.Output.ValidationErr),
	}

	// Extract MySQL-specific metadata (if available)
	if ctx.MySQLState != nil {
		stmt.TableName = ctx.MySQLState.TableName
		stmt.ISFilter = InformationSchemaFilter{
			SchemaName: ctx.MySQLState.ISFilter.SchemaName,
			TableName:  ctx.MySQLState.ISFilter.TableName,
			ColumnName: ctx.MySQLState.ISFilter.ColumnName,
		}
		stmt.ISTableType = InformationSchemaTableType(ctx.MySQLState.ISTableType)
		stmt.VirtualTableType = VirtualTableType(ctx.MySQLState.VirtualTableType)
		stmt.SystemVarNames = ctx.MySQLState.SystemVarNames
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
			SQL:   sql,
			Type:  StatementUnsupported,
			Error: err.Error(),
		}}
	}

	// Create a Statement for each transpiled statement
	stmts := make([]Statement, 0, len(ctx.Output.Statements))
	for _, ts := range ctx.Output.Statements {
		stmts = append(stmts, buildStatement(*ctx, ts.SQL))
	}
	return stmts
}

// buildStatement creates a Statement from context with specified SQL
func buildStatement(ctx query.QueryContext, sql string) Statement {
	stmt := Statement{
		SQL:      sql,
		Type:     ctx.Output.StatementType,
		Database: ctx.Output.Database,
		Error:    errorString(ctx.Output.ValidationErr),
	}

	// Extract MySQL-specific metadata (if available)
	if ctx.MySQLState != nil {
		stmt.TableName = ctx.MySQLState.TableName
		stmt.ISFilter = InformationSchemaFilter{
			SchemaName: ctx.MySQLState.ISFilter.SchemaName,
			TableName:  ctx.MySQLState.ISFilter.TableName,
			ColumnName: ctx.MySQLState.ISFilter.ColumnName,
		}
		stmt.ISTableType = InformationSchemaTableType(ctx.MySQLState.ISTableType)
		stmt.VirtualTableType = VirtualTableType(ctx.MySQLState.VirtualTableType)
		stmt.SystemVarNames = ctx.MySQLState.SystemVarNames
	}

	return stmt
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

// extractTableName extracts table name using a regex pattern
func extractTableName(sql, pattern string) string {
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(sql)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
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
		StatementCreateDatabase, StatementDropDatabase:
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
