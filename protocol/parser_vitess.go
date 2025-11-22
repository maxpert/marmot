package protocol

import (
	"database/sql"
	"regexp"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Global parser instance (reused for efficiency)
var vitessParser *sqlparser.Parser

// SQLite syntax validator (in-memory, persistent)
var (
	sqliteSyntaxChecker *sql.DB
	sqliteCheckerMu     sync.Mutex
)

func init() {
	var err error
	vitessParser, err = sqlparser.New(sqlparser.Options{})
	if err != nil {
		panic("failed to initialize Vitess parser: " + err.Error())
	}

	// Initialize in-memory SQLite for syntax validation
	sqliteSyntaxChecker, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic("failed to initialize SQLite syntax checker: " + err.Error())
	}
}

// ValidateSQLiteSyntax checks if a query is valid SQLite syntax
// Returns nil if valid, error if invalid (only for true syntax errors)
func ValidateSQLiteSyntax(query string) error {
	sqliteCheckerMu.Lock()
	defer sqliteCheckerMu.Unlock()

	stmt, err := sqliteSyntaxChecker.Prepare(query)
	if err != nil {
		errStr := err.Error()
		// Ignore "no such table/column" errors - those are schema errors, not syntax errors
		// We only want to catch true syntax errors
		if strings.Contains(errStr, "no such table") ||
			strings.Contains(errStr, "no such column") ||
			strings.Contains(errStr, "no such index") ||
			strings.Contains(errStr, "no such view") ||
			strings.Contains(errStr, "no such trigger") {
			return nil // Schema error, not syntax error - allow it
		}
		return err
	}
	stmt.Close()
	return nil
}

// shouldValidateWithSQLite returns true if we should validate this SQL against SQLite
// Only validate DML statements - these are what get replicated and executed on SQLite
func shouldValidateWithSQLite(sql string) bool {
	// Strip leading comments
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
			return false
		}
	}

	// Only validate core DML that gets replicated
	return strings.HasPrefix(upper, "INSERT ") ||
		strings.HasPrefix(upper, "UPDATE ") ||
		strings.HasPrefix(upper, "DELETE ") ||
		strings.HasPrefix(upper, "SELECT ") ||
		strings.HasPrefix(upper, "REPLACE ")
}

// isNonPreparableStatement checks if a statement cannot be validated via Prepare()
// These include transaction control, session commands, and certain admin statements
func isNonPreparableStatement(sql string) bool {
	// Strip leading comments (/* ... */ and -- style)
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
			return true // Comment-only statement
		}
	}

	// Transaction control - can't be prepared
	if strings.HasPrefix(upper, "BEGIN") ||
		strings.HasPrefix(upper, "START TRANSACTION") ||
		strings.HasPrefix(upper, "COMMIT") ||
		strings.HasPrefix(upper, "ROLLBACK") ||
		strings.HasPrefix(upper, "END") ||
		strings.HasPrefix(upper, "SAVEPOINT") ||
		strings.HasPrefix(upper, "RELEASE") {
		return true
	}

	// Session/connection commands
	if strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "USE ") ||
		strings.HasPrefix(upper, "SHOW ") {
		return true
	}

	// Database management - syntax differs between MySQL and SQLite
	if strings.HasPrefix(upper, "CREATE DATABASE") ||
		strings.HasPrefix(upper, "CREATE SCHEMA") ||
		strings.HasPrefix(upper, "DROP DATABASE") ||
		strings.HasPrefix(upper, "DROP SCHEMA") ||
		strings.HasPrefix(upper, "ALTER DATABASE") ||
		strings.HasPrefix(upper, "ALTER SCHEMA") {
		return true
	}

	// DCL - user/privilege management
	if strings.HasPrefix(upper, "GRANT ") ||
		strings.HasPrefix(upper, "REVOKE ") ||
		strings.HasPrefix(upper, "CREATE USER") ||
		strings.HasPrefix(upper, "DROP USER") ||
		strings.HasPrefix(upper, "ALTER USER") ||
		strings.HasPrefix(upper, "RENAME USER") ||
		strings.HasPrefix(upper, "CREATE ROLE") ||
		strings.HasPrefix(upper, "DROP ROLE") ||
		strings.HasPrefix(upper, "SET ROLE") {
		return true
	}

	// DDL that SQLite doesn't support same way
	if strings.HasPrefix(upper, "CREATE PROCEDURE") ||
		strings.HasPrefix(upper, "DROP PROCEDURE") ||
		strings.HasPrefix(upper, "ALTER PROCEDURE") ||
		strings.HasPrefix(upper, "CREATE FUNCTION") ||
		strings.HasPrefix(upper, "DROP FUNCTION") ||
		strings.HasPrefix(upper, "ALTER FUNCTION") ||
		strings.HasPrefix(upper, "CREATE TRIGGER") ||
		strings.HasPrefix(upper, "DROP TRIGGER") ||
		strings.HasPrefix(upper, "CREATE EVENT") ||
		strings.HasPrefix(upper, "DROP EVENT") ||
		strings.HasPrefix(upper, "ALTER EVENT") ||
		strings.HasPrefix(upper, "CREATE SERVER") ||
		strings.HasPrefix(upper, "DROP SERVER") ||
		strings.HasPrefix(upper, "ALTER SERVER") ||
		strings.HasPrefix(upper, "CREATE LOGFILE") ||
		strings.HasPrefix(upper, "DROP LOGFILE") ||
		strings.HasPrefix(upper, "ALTER LOGFILE") ||
		strings.HasPrefix(upper, "CREATE TABLESPACE") ||
		strings.HasPrefix(upper, "DROP TABLESPACE") ||
		strings.HasPrefix(upper, "ALTER TABLESPACE") ||
		strings.HasPrefix(upper, "CREATE SPATIAL") ||
		strings.HasPrefix(upper, "DROP SPATIAL") ||
		strings.HasPrefix(upper, "CREATE RESOURCE") ||
		strings.HasPrefix(upper, "DROP RESOURCE") ||
		strings.HasPrefix(upper, "ALTER RESOURCE") ||
		strings.HasPrefix(upper, "ALTER VIEW") ||
		strings.HasPrefix(upper, "TRUNCATE ") ||
		strings.HasPrefix(upper, "LOAD DATA") ||
		strings.HasPrefix(upper, "LOAD XML") ||
		strings.HasPrefix(upper, "RENAME TABLE") {
		return true
	}

	// Admin commands
	if strings.HasPrefix(upper, "INSTALL ") ||
		strings.HasPrefix(upper, "UNINSTALL ") ||
		strings.HasPrefix(upper, "OPTIMIZE ") ||
		strings.HasPrefix(upper, "REPAIR ") ||
		strings.HasPrefix(upper, "ANALYZE ") ||
		strings.HasPrefix(upper, "CHECK ") ||
		strings.HasPrefix(upper, "FLUSH ") ||
		strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "PURGE ") ||
		strings.HasPrefix(upper, "KILL ") {
		return true
	}

	// EXPLAIN and PRAGMA
	if strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "PRAGMA") {
		return true
	}

	// XA transactions
	if strings.HasPrefix(upper, "XA ") {
		return true
	}

	// Lock statements
	if strings.HasPrefix(upper, "LOCK ") ||
		strings.HasPrefix(upper, "UNLOCK ") {
		return true
	}

	return false
}

// ParseStatementVitess uses Vitess sqlparser for accurate MySQL parsing
// First validates syntax against SQLite, then extracts metadata via Vitess
func ParseStatementVitess(sql string) Statement {
	sql = strings.TrimSpace(sql)

	stmt := Statement{SQL: sql}

	// Step 1: Validate DML syntax against SQLite first
	// Only validate INSERT/UPDATE/DELETE/SELECT - these are the replicated statements
	// Skip DDL and other statement types as they have MySQL/SQLite syntax differences
	if shouldValidateWithSQLite(sql) {
		if err := ValidateSQLiteSyntax(sql); err != nil {
			// Invalid SQLite syntax - reject early with clear error
			stmt.Type = StatementUnsupported
			stmt.Error = "invalid SQLite syntax: " + err.Error()
			return stmt
		}
	}

	// Step 2: Continue with Vitess parsing to extract metadata (table names, type, etc.)

	// Check for savepoint statements first (Vitess doesn't parse these properly)
	if savepointPattern.MatchString(sql) || releaseSavepointPattern.MatchString(sql) {
		stmt.Type = StatementSavepoint
		return stmt
	}

	// Check for SET TRANSACTION (should be treated as transaction control)
	if setTransactionPattern.MatchString(sql) {
		stmt.Type = StatementBegin
		return stmt
	}

	// Check for XA transactions (Vitess doesn't parse these)
	if xaStartPattern.MatchString(sql) || xaEndPattern.MatchString(sql) ||
		xaPreparePattern.MatchString(sql) || xaCommitPattern.MatchString(sql) ||
		xaRollbackPattern.MatchString(sql) || xaRecoverPattern.MatchString(sql) {
		stmt.Type = StatementXA
		return stmt
	}

	// Check for lock instance statements (Vitess doesn't parse these)
	if lockInstancePattern.MatchString(sql) || unlockInstancePattern.MatchString(sql) {
		stmt.Type = StatementLock
		return stmt
	}

	// Check for administrative statements (Vitess doesn't parse these)
	if installPluginPattern.MatchString(sql) || uninstallPluginPattern.MatchString(sql) ||
		installComponentPattern.MatchString(sql) || uninstallComponentPattern.MatchString(sql) {
		stmt.Type = StatementAdmin
		return stmt
	}

	// Check for LOAD XML (Vitess might not handle this)
	if loadXMLPattern.MatchString(sql) {
		stmt.Type = StatementLoadData
		return stmt
	}

	// Check for INSERT variants that Vitess might not parse correctly
	insertDelayedPattern := regexp.MustCompile(`(?i)^\s*INSERT\s+(DELAYED|LOW_PRIORITY|HIGH_PRIORITY)\s+`)
	if insertDelayedPattern.MatchString(sql) {
		stmt.Type = StatementInsert
		// Try to extract table name
		tablePattern := regexp.MustCompile(`(?i)INSERT\s+(DELAYED|LOW_PRIORITY|HIGH_PRIORITY)\s+INTO\s+(\w+)`)
		if matches := tablePattern.FindStringSubmatch(sql); len(matches) > 2 {
			stmt.TableName = matches[2]
		}
		return stmt
	}

	// Check for DDL statements that Vitess might not handle properly
	// Triggers
	if createTriggerPattern.MatchString(sql) || dropTriggerPattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Procedures
	if createProcedurePattern.MatchString(sql) || dropProcedurePattern.MatchString(sql) ||
		alterProcedurePattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Functions
	if createFunctionPattern.MatchString(sql) || dropFunctionPattern.MatchString(sql) ||
		alterFunctionPattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Events
	if createEventPattern.MatchString(sql) || dropEventPattern.MatchString(sql) ||
		alterEventPattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Tablespaces
	if createTablespacePattern.MatchString(sql) || dropTablespacePattern.MatchString(sql) ||
		alterTablespacePattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Logfile Groups
	if createLogfileGroupPattern.MatchString(sql) || dropLogfileGroupPattern.MatchString(sql) ||
		alterLogfileGroupPattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Servers
	if createServerPattern.MatchString(sql) || dropServerPattern.MatchString(sql) ||
		alterServerPattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Spatial Reference Systems
	if createSpatialRefPattern.MatchString(sql) || dropSpatialRefPattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Resource Groups
	if createResourceGroupPattern.MatchString(sql) || dropResourceGroupPattern.MatchString(sql) ||
		alterResourceGroupPattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Drop Index (Vitess might not handle this)
	if dropIndexPattern.MatchString(sql) {
		stmt.Type = StatementDDL
		return stmt
	}

	// Check for DCL statements (Vitess doesn't parse these)
	if createUserPattern.MatchString(sql) || dropUserPattern.MatchString(sql) ||
		alterUserPattern.MatchString(sql) || renameUserPattern.MatchString(sql) ||
		setPasswordPattern.MatchString(sql) {
		stmt.Type = StatementDCL
		return stmt
	}
	if grantPattern.MatchString(sql) || revokePattern.MatchString(sql) {
		stmt.Type = StatementDCL
		return stmt
	}
	if createRolePattern.MatchString(sql) || dropRolePattern.MatchString(sql) ||
		setRolePattern.MatchString(sql) || setDefaultRolePattern.MatchString(sql) {
		stmt.Type = StatementDCL
		return stmt
	}

	// Parse with Vitess
	parsed, err := vitessParser.Parse(sql)
	if err != nil {
		// Fallback: check regex patterns for statements Vitess can't parse
		fallback := checkRegexFallback(sql)
		if fallback.Type != StatementSelect {
			return fallback
		}
		// Unknown - treat as SELECT (safe default)
		stmt.Type = StatementSelect
		return stmt
	}

	// Map Vitess AST to StatementType and extract metadata FIRST
	switch parsed := parsed.(type) {
	case *sqlparser.Insert:
		if parsed.Action == sqlparser.ReplaceAct {
			stmt.Type = StatementReplace
		} else {
			stmt.Type = StatementInsert
		}
		// Extract table name from AliasedTableExpr
		if parsed.Table != nil {
			if tn, ok := parsed.Table.Expr.(sqlparser.TableName); ok {
				stmt.TableName = tn.Name.String()
				if tn.Qualifier.NotEmpty() {
					stmt.Database = tn.Qualifier.String()
				}
			}
		}

	case *sqlparser.Update:
		stmt.Type = StatementUpdate
		if len(parsed.TableExprs) > 0 {
			if aliased, ok := parsed.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := aliased.Expr.(sqlparser.TableName); ok {
					stmt.TableName = tn.Name.String()
					if tn.Qualifier.NotEmpty() {
						stmt.Database = tn.Qualifier.String()
					}
				}
			}
		}

	case *sqlparser.Delete:
		stmt.Type = StatementDelete
		if len(parsed.TableExprs) > 0 {
			if aliased, ok := parsed.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := aliased.Expr.(sqlparser.TableName); ok {
					stmt.TableName = tn.Name.String()
					if tn.Qualifier.NotEmpty() {
						stmt.Database = tn.Qualifier.String()
					}
				}
			}
		}

	case *sqlparser.Select:
		stmt.Type = StatementSelect
		// Check if this is an INFORMATION_SCHEMA query
		if isInformationSchemaQuery(parsed) {
			stmt.Type = StatementInformationSchema
		}
		// Extract database from CTEs first (WITH clause)
		if parsed.With != nil {
			for _, cte := range parsed.With.CTEs {
				if cteSelect, ok := cte.Subquery.(*sqlparser.Select); ok {
					if len(cteSelect.From) > 0 {
						db, table := extractDatabaseFromTableExpr(cteSelect.From[0])
						if db != "" {
							stmt.Database = db
							if table != "" {
								stmt.TableName = table
							}
							break // Use first database found
						}
					}
				}
			}
		}
		// If no database found in CTE, extract from main FROM clause
		if stmt.Database == "" && len(parsed.From) > 0 {
			db, table := extractDatabaseFromTableExpr(parsed.From[0])
			if db != "" {
				stmt.Database = db
			}
			if table != "" {
				stmt.TableName = table
			}
		}

	// DDL statements - now using specific types instead of generic DDL
	case *sqlparser.CreateTable:
		stmt.Type = StatementDDL
		stmt.TableName = parsed.Table.Name.String()
		if parsed.Table.Qualifier.NotEmpty() {
			stmt.Database = parsed.Table.Qualifier.String()
		}

	case *sqlparser.AlterTable:
		stmt.Type = StatementDDL
		stmt.TableName = parsed.Table.Name.String()
		if parsed.Table.Qualifier.NotEmpty() {
			stmt.Database = parsed.Table.Qualifier.String()
		}

	case *sqlparser.DropTable:
		stmt.Type = StatementDDL
		if len(parsed.FromTables) > 0 {
			stmt.TableName = parsed.FromTables[0].Name.String()
			if parsed.FromTables[0].Qualifier.NotEmpty() {
				stmt.Database = parsed.FromTables[0].Qualifier.String()
			}
		}

	case *sqlparser.CreateDatabase:
		stmt.Type = StatementCreateDatabase
		stmt.Database = parsed.DBName.String()

	case *sqlparser.DropDatabase:
		stmt.Type = StatementDropDatabase
		stmt.Database = parsed.DBName.String()

	case *sqlparser.AlterDatabase:
		stmt.Type = StatementDDL
		stmt.Database = parsed.DBName.String()

	// Handle RENAME TABLE specifically
	case *sqlparser.RenameTable:
		stmt.Type = StatementDDL
		if len(parsed.TablePairs) > 0 {
			stmt.TableName = parsed.TablePairs[0].FromTable.Name.String()
			if parsed.TablePairs[0].FromTable.Qualifier.NotEmpty() {
				stmt.Database = parsed.TablePairs[0].FromTable.Qualifier.String()
			}
		}

	// Handle other DDL types that might still use the DDLStatement interface
	case sqlparser.DDLStatement:
		// Generic DDL handling for types we don't explicitly handle above
		stmt.Type = StatementDDL
		table := parsed.GetTable()
		if !table.IsEmpty() {
			stmt.TableName = table.Name.String()
			if table.Qualifier.NotEmpty() {
				stmt.Database = table.Qualifier.String()
			}
		}

	case *sqlparser.Show:
		// Show.Internal contains the actual show type
		if showBasic, ok := parsed.Internal.(*sqlparser.ShowBasic); ok {
			// Check the command type
			switch showBasic.Command {
			case sqlparser.Database:
				stmt.Type = StatementShowDatabases
			case sqlparser.Table:
				stmt.Type = StatementShowTables
				// Extract database name if specified
				if showBasic.DbName.NotEmpty() {
					stmt.Database = showBasic.DbName.String()
				}
			case sqlparser.Column:
				stmt.Type = StatementShowColumns
				// Extract table name
				if !showBasic.Tbl.IsEmpty() {
					stmt.TableName = showBasic.Tbl.Name.String()
					if showBasic.Tbl.Qualifier.NotEmpty() {
						stmt.Database = showBasic.Tbl.Qualifier.String()
					}
				}
			case sqlparser.Index:
				stmt.Type = StatementShowIndexes
				// Extract table name
				if !showBasic.Tbl.IsEmpty() {
					stmt.TableName = showBasic.Tbl.Name.String()
					if showBasic.Tbl.Qualifier.NotEmpty() {
						stmt.Database = showBasic.Tbl.Qualifier.String()
					}
				}
			case sqlparser.TableStatus:
				stmt.Type = StatementShowTableStatus
				// Extract database name and filter
				if showBasic.DbName.NotEmpty() {
					stmt.Database = showBasic.DbName.String()
				}
				// Extract table name from LIKE filter if present
				if showBasic.Filter != nil && showBasic.Filter.Like != "" {
					stmt.TableName = showBasic.Filter.Like
				}
			default:
				// Other SHOW commands treated as SELECT
				stmt.Type = StatementSelect
			}
		} else if showCreate, ok := parsed.Internal.(*sqlparser.ShowCreate); ok {
			// SHOW CREATE TABLE
			stmt.Type = StatementShowCreateTable
			if !showCreate.Op.IsEmpty() {
				stmt.TableName = showCreate.Op.Name.String()
				if showCreate.Op.Qualifier.NotEmpty() {
					stmt.Database = showCreate.Op.Qualifier.String()
				}
			}
		} else {
			// Other SHOW variants treated as SELECT
			stmt.Type = StatementSelect
		}

	case *sqlparser.Use:
		stmt.Type = StatementUseDatabase
		stmt.Database = parsed.DBName.String()

	case *sqlparser.Begin:
		stmt.Type = StatementBegin

	case *sqlparser.Commit:
		stmt.Type = StatementCommit

	case *sqlparser.Rollback:
		stmt.Type = StatementRollback

	case *sqlparser.Savepoint:
		stmt.Type = StatementSavepoint

	case *sqlparser.Set:
		stmt.Type = StatementSet

	case *sqlparser.Load:
		stmt.Type = StatementLoadData

	case *sqlparser.OtherAdmin:
		stmt.Type = StatementAdmin

	case *sqlparser.LockTables:
		stmt.Type = StatementLock

	case *sqlparser.UnlockTables:
		stmt.Type = StatementLock

	default:
		// Unknown type - safe default
		stmt.Type = StatementSelect
	}

	// Strip database qualifiers from AST if any database was extracted
	// This ensures that "SELECT * FROM db.table" becomes "SELECT * FROM table"
	// since we route queries to the correct database file using stmt.Database
	if stmt.Database != "" {
		stripDatabaseQualifiers(parsed)
		stmt.SQL = sqlparser.String(parsed)
	}

	return stmt
}

// isInformationSchemaQuery checks if a SELECT statement queries INFORMATION_SCHEMA
func isInformationSchemaQuery(sel *sqlparser.Select) bool {
	// Check all table expressions in FROM clause
	for _, tableExpr := range sel.From {
		if aliased, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := aliased.Expr.(sqlparser.TableName); ok {
				// Check if database qualifier is INFORMATION_SCHEMA
				if tableName.Qualifier.NotEmpty() {
					qualifier := strings.ToUpper(tableName.Qualifier.String())
					if qualifier == "INFORMATION_SCHEMA" {
						return true
					}
				}
			}
		}
	}
	return false
}

// checkRegexFallback uses regex patterns to detect statement types when Vitess can't parse
func checkRegexFallback(sql string) Statement {
	stmt := Statement{SQL: sql}

	// Check for RENAME TABLE
	if renameTablePattern.MatchString(sql) {
		stmt.Type = StatementDDL
		stmt.TableName = extractTableName(sql, `(?i)RENAME\s+TABLE\s+(\w+)`)
		return stmt
	}

	// This fallback should only be called for statements Vitess can't parse
	// Default to SELECT
	stmt.Type = StatementSelect
	return stmt
}

// extractDatabaseFromTableExpr recursively extracts database and table names from any TableExpr
// This handles simple tables, JOINs, subqueries, CTEs, etc.
// Returns the first database qualifier found (for routing purposes)
func extractDatabaseFromTableExpr(expr sqlparser.TableExpr) (database string, table string) {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		// Simple table or subquery
		if tn, ok := e.Expr.(sqlparser.TableName); ok {
			table = tn.Name.String()
			if tn.Qualifier.NotEmpty() {
				database = tn.Qualifier.String()
			}
		}
	case *sqlparser.JoinTableExpr:
		// JOIN expression - extract from left side (primary table)
		database, table = extractDatabaseFromTableExpr(e.LeftExpr)
		// If no database found on left, try right side
		if database == "" {
			rightDB, rightTable := extractDatabaseFromTableExpr(e.RightExpr)
			if rightDB != "" {
				database = rightDB
				if table == "" {
					table = rightTable
				}
			}
		}
	case *sqlparser.ParenTableExpr:
		// Parenthesized table expression
		for _, t := range e.Exprs {
			db, tbl := extractDatabaseFromTableExpr(t)
			if db != "" {
				return db, tbl
			}
		}
	}
	return database, table
}

// stripDatabaseQualifiers removes database qualifiers from all table references in the AST
// This modifies the AST in-place to convert "db.table" to "table"
func stripDatabaseQualifiers(stmt sqlparser.Statement) {
	// Use Vitess's Rewrite function to traverse and modify AST
	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		switch n := cursor.Node().(type) {
		case sqlparser.TableName:
			// Clear the qualifier (database name) by creating a new TableName
			newTableName := sqlparser.TableName{
				Name:      n.Name,
				Qualifier: sqlparser.NewIdentifierCS(""),
			}
			cursor.Replace(newTableName)
		}
		return true
	}, nil)
}
