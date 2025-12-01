package protocol

import (
	"regexp"
	"strings"

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

func InitializePipeline(cacheSize, poolSize int) error {
	var err error
	globalPipeline, err = query.NewPipeline(cacheSize, poolSize)
	return err
}

// ParseStatement analyzes a SQL statement and returns its type and metadata
// Now uses the new clean layered architecture with caching
func ParseStatement(sql string) Statement {
	ctx := query.NewContext(sql, nil)

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

	stmt := Statement{
		SQL:       ctx.TranspiledSQL,
		Type:      mapQueryTypeToProtocolType(ctx.StatementType),
		TableName: ctx.TableName,
		Database:  ctx.Database,
		Error: func() string {
			if ctx.ValidationErr != nil {
				return ctx.ValidationErr.Error()
			}
			return ""
		}(),
		// CDC fields (RowKey, OldValues, NewValues) are populated later by
		// handler.go after preupdate hooks capture row data
		// INFORMATION_SCHEMA filter values extracted from WHERE clause
		ISFilter: InformationSchemaFilter{
			SchemaName: ctx.ISFilter.SchemaName,
			TableName:  ctx.ISFilter.TableName,
			ColumnName: ctx.ISFilter.ColumnName,
		},
		// New fields to eliminate pre-parse string matching in handlers
		ISTableType:      InformationSchemaTableType(ctx.ISTableType),
		VirtualTableType: VirtualTableType(ctx.VirtualTableType),
		SystemVarNames:   ctx.SystemVarNames,
	}

	return stmt
}

// mapQueryTypeToProtocolType converts query.StatementType to protocol.StatementType
// These two enums have the same constant names but live in different packages
func mapQueryTypeToProtocolType(qt query.StatementType) StatementType {
	// Both enums are defined in the same order with the same names
	// query.StatementInsert = 0, protocol.StatementInsert = 0
	// query.StatementReplace = 1, protocol.StatementReplace = 1
	// etc.
	return StatementType(qt)
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
		return ConsistencyLocalOne, false
	}

	level, err := ParseConsistencyLevel(strings.ToUpper(matches[1]))
	if err != nil {
		return ConsistencyLocalOne, false
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
// (INSERT, UPDATE, DELETE, REPLACE) that requires row key tracking
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
