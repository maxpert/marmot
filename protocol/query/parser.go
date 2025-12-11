package query

import (
	"fmt"
	"regexp"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	savepointPattern        = regexp.MustCompile(`(?i)^\s*SAVEPOINT\s+`)
	releaseSavepointPattern = regexp.MustCompile(`(?i)^\s*RELEASE\s+SAVEPOINT\s+`)
	setTransactionPattern   = regexp.MustCompile(`(?i)^\s*SET\s+TRANSACTION\s+`)

	xaStartPattern    = regexp.MustCompile(`(?i)^\s*XA\s+START\s+`)
	xaEndPattern      = regexp.MustCompile(`(?i)^\s*XA\s+END\s+`)
	xaPreparePattern  = regexp.MustCompile(`(?i)^\s*XA\s+PREPARE\s+`)
	xaCommitPattern   = regexp.MustCompile(`(?i)^\s*XA\s+COMMIT\s+`)
	xaRollbackPattern = regexp.MustCompile(`(?i)^\s*XA\s+ROLLBACK\s+`)
	xaRecoverPattern  = regexp.MustCompile(`(?i)^\s*XA\s+RECOVER`)

	lockInstancePattern   = regexp.MustCompile(`(?i)^\s*LOCK\s+INSTANCE\s+FOR\s+BACKUP`)
	unlockInstancePattern = regexp.MustCompile(`(?i)^\s*UNLOCK\s+INSTANCE`)

	installPluginPattern      = regexp.MustCompile(`(?i)^\s*INSTALL\s+PLUGIN\s+`)
	uninstallPluginPattern    = regexp.MustCompile(`(?i)^\s*UNINSTALL\s+PLUGIN\s+`)
	installComponentPattern   = regexp.MustCompile(`(?i)^\s*INSTALL\s+COMPONENT\s+`)
	uninstallComponentPattern = regexp.MustCompile(`(?i)^\s*UNINSTALL\s+COMPONENT\s+`)

	loadXMLPattern = regexp.MustCompile(`(?i)^\s*LOAD\s+XML\s+`)

	insertDelayedPattern = regexp.MustCompile(`(?i)^\s*INSERT\s+(DELAYED|LOW_PRIORITY|HIGH_PRIORITY)\s+`)
	insertOrPattern      = regexp.MustCompile(`(?i)^\s*INSERT\s+OR\s+(IGNORE|REPLACE)\s+INTO\s+`)
)

type VitessParser struct {
	parser *sqlparser.Parser
}

func NewVitessParser() (*VitessParser, error) {
	p, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return nil, err
	}
	return &VitessParser{parser: p}, nil
}

func (p *VitessParser) Parse(ctx *QueryContext) error {
	if ctx.MySQLState == nil {
		return fmt.Errorf("VitessParser.Parse requires MySQL dialect context")
	}

	classifyByPattern(ctx)

	stmt, err := p.parser.Parse(ctx.Input.SQL)
	if err != nil {
		return err
	}

	ctx.MySQLState.AST = stmt
	classifyStatement(ctx, stmt)
	extractMetadata(ctx, stmt)

	return nil
}

func classifyByPattern(ctx *QueryContext) {
	sql := ctx.Input.SQL

	if savepointPattern.MatchString(sql) || releaseSavepointPattern.MatchString(sql) {
		ctx.Output.StatementType = StatementSavepoint
		return
	}

	if setTransactionPattern.MatchString(sql) {
		ctx.Output.StatementType = StatementBegin
		return
	}

	if xaStartPattern.MatchString(sql) || xaEndPattern.MatchString(sql) ||
		xaPreparePattern.MatchString(sql) || xaCommitPattern.MatchString(sql) ||
		xaRollbackPattern.MatchString(sql) || xaRecoverPattern.MatchString(sql) {
		ctx.Output.StatementType = StatementXA
		return
	}

	if lockInstancePattern.MatchString(sql) || unlockInstancePattern.MatchString(sql) {
		ctx.Output.StatementType = StatementLock
		return
	}

	if installPluginPattern.MatchString(sql) || uninstallPluginPattern.MatchString(sql) ||
		installComponentPattern.MatchString(sql) || uninstallComponentPattern.MatchString(sql) {
		ctx.Output.StatementType = StatementAdmin
		return
	}

	if loadXMLPattern.MatchString(sql) {
		ctx.Output.StatementType = StatementLoadData
		return
	}

	if insertDelayedPattern.MatchString(sql) {
		ctx.Output.StatementType = StatementInsert
		return
	}

	if matches := insertOrPattern.FindStringSubmatch(sql); len(matches) > 0 {
		if len(matches) > 1 && strings.ToUpper(matches[1]) == "REPLACE" {
			ctx.Output.StatementType = StatementReplace
		} else {
			ctx.Output.StatementType = StatementInsert
		}
		return
	}
}

func classifyStatement(ctx *QueryContext, stmt sqlparser.Statement) {
	if ctx.Output.StatementType != StatementUnknown {
		return
	}

	switch parsed := stmt.(type) {
	case *sqlparser.Insert:
		if parsed.Action == sqlparser.ReplaceAct {
			ctx.Output.StatementType = StatementReplace
		} else {
			ctx.Output.StatementType = StatementInsert
		}

	case *sqlparser.Update:
		ctx.Output.StatementType = StatementUpdate

	case *sqlparser.Delete:
		ctx.Output.StatementType = StatementDelete

	case *sqlparser.Select:
		ctx.Output.StatementType = StatementSelect
		// Check for system variables (@@var, DATABASE()) - these take priority
		if sysVars := extractSystemVariables(parsed); len(sysVars) > 0 {
			ctx.Output.StatementType = StatementSystemVariable
			ctx.MySQLState.SystemVarNames = sysVars
		} else if vtType := detectVirtualTable(parsed); vtType != VirtualTableUnknown {
			// Check for Marmot virtual tables
			ctx.Output.StatementType = StatementVirtualTable
			ctx.MySQLState.VirtualTableType = vtType
		} else if isTableType := detectInformationSchemaTable(parsed); isTableType != ISTableUnknown {
			// Check for INFORMATION_SCHEMA queries
			ctx.Output.StatementType = StatementInformationSchema
			ctx.MySQLState.ISTableType = isTableType
			ctx.MySQLState.ISFilter = extractInformationSchemaFilter(parsed)
		}

	case *sqlparser.CreateTable:
		ctx.Output.StatementType = StatementDDL

	case *sqlparser.AlterTable:
		ctx.Output.StatementType = StatementDDL

	case *sqlparser.DropTable:
		ctx.Output.StatementType = StatementDDL

	case *sqlparser.CreateDatabase:
		ctx.Output.StatementType = StatementCreateDatabase

	case *sqlparser.DropDatabase:
		ctx.Output.StatementType = StatementDropDatabase

	case *sqlparser.AlterDatabase:
		ctx.Output.StatementType = StatementDDL

	case *sqlparser.RenameTable:
		ctx.Output.StatementType = StatementDDL

	case sqlparser.DDLStatement:
		ctx.Output.StatementType = StatementDDL

	case *sqlparser.Show:
		classifyShowStatement(ctx, parsed)

	case *sqlparser.Use:
		ctx.Output.StatementType = StatementUseDatabase

	case *sqlparser.Begin:
		ctx.Output.StatementType = StatementBegin

	case *sqlparser.Commit:
		ctx.Output.StatementType = StatementCommit

	case *sqlparser.Rollback:
		ctx.Output.StatementType = StatementRollback

	case *sqlparser.Savepoint:
		ctx.Output.StatementType = StatementSavepoint

	case *sqlparser.Set:
		ctx.Output.StatementType = StatementSet

	case *sqlparser.Load:
		ctx.Output.StatementType = StatementLoadData

	case *sqlparser.OtherAdmin:
		ctx.Output.StatementType = StatementAdmin

	case *sqlparser.LockTables:
		ctx.Output.StatementType = StatementLock

	case *sqlparser.UnlockTables:
		ctx.Output.StatementType = StatementLock

	case *sqlparser.ExplainTab:
		ctx.Output.StatementType = StatementShowColumns
		if parsed.Table.Qualifier.NotEmpty() {
			ctx.Output.Database = parsed.Table.Qualifier.String()
		}

	case *sqlparser.Union:
		// Union queries - check left side for special detection
		// System vars/virtual tables in unions are rare edge cases
		// Default to regular SELECT for unions
		ctx.Output.StatementType = StatementSelect
		// Check if left side is a special query type
		if leftSelect, ok := parsed.Left.(*sqlparser.Select); ok {
			if sysVars := extractSystemVariables(leftSelect); len(sysVars) > 0 {
				ctx.Output.StatementType = StatementSystemVariable
				ctx.MySQLState.SystemVarNames = sysVars
				// Also check right side for additional system vars
				if rightSelect, ok := parsed.Right.(*sqlparser.Select); ok {
					ctx.MySQLState.SystemVarNames = append(ctx.MySQLState.SystemVarNames, extractSystemVariables(rightSelect)...)
				}
			} else if vtType := detectVirtualTable(leftSelect); vtType != VirtualTableUnknown {
				ctx.Output.StatementType = StatementVirtualTable
				ctx.MySQLState.VirtualTableType = vtType
			} else if isTableType := detectInformationSchemaTable(leftSelect); isTableType != ISTableUnknown {
				ctx.Output.StatementType = StatementInformationSchema
				ctx.MySQLState.ISTableType = isTableType
				ctx.MySQLState.ISFilter = extractInformationSchemaFilter(leftSelect)
			}
		}

	default:
		ctx.Output.StatementType = StatementSelect
	}
}

func classifyShowStatement(ctx *QueryContext, parsed *sqlparser.Show) {
	if showBasic, ok := parsed.Internal.(*sqlparser.ShowBasic); ok {
		switch showBasic.Command {
		case sqlparser.Database:
			ctx.Output.StatementType = StatementShowDatabases
		case sqlparser.Table:
			ctx.Output.StatementType = StatementShowTables
		case sqlparser.Column:
			ctx.Output.StatementType = StatementShowColumns
		case sqlparser.Index:
			ctx.Output.StatementType = StatementShowIndexes
		case sqlparser.TableStatus:
			ctx.Output.StatementType = StatementShowTableStatus
		default:
			ctx.Output.StatementType = StatementSelect
		}
	} else if _, ok := parsed.Internal.(*sqlparser.ShowCreate); ok {
		ctx.Output.StatementType = StatementShowCreateTable
	} else {
		ctx.Output.StatementType = StatementSelect
	}
}

// extractSystemVariables finds @@variable references and DATABASE() calls in SELECT
func extractSystemVariables(sel *sqlparser.Select) []string {
	var vars []string
	if sel.SelectExprs == nil {
		return vars
	}
	for _, expr := range sel.SelectExprs.Exprs {
		switch e := expr.(type) {
		case *sqlparser.AliasedExpr:
			vars = append(vars, extractVarsFromExpr(e.Expr)...)
		}
	}
	return vars
}

// extractVarsFromExpr recursively finds system variables in an expression
func extractVarsFromExpr(expr sqlparser.Expr) []string {
	var vars []string
	switch e := expr.(type) {
	case *sqlparser.Variable:
		// @@global.version, @@session.sql_mode, @@version
		vars = append(vars, strings.ToUpper(e.Name.String()))
	case *sqlparser.FuncExpr:
		// DATABASE(), VERSION(), USER(), etc.
		funcName := strings.ToUpper(e.Name.String())
		if funcName == "DATABASE" || funcName == "SCHEMA" ||
			funcName == "VERSION" || funcName == "USER" ||
			funcName == "CURRENT_USER" || funcName == "SESSION_USER" ||
			funcName == "SYSTEM_USER" || funcName == "CONNECTION_ID" ||
			funcName == "FOUND_ROWS" {
			vars = append(vars, funcName+"()")
		}
	case *sqlparser.BinaryExpr:
		vars = append(vars, extractVarsFromExpr(e.Left)...)
		vars = append(vars, extractVarsFromExpr(e.Right)...)
	case *sqlparser.CaseExpr:
		if e.Expr != nil {
			vars = append(vars, extractVarsFromExpr(e.Expr)...)
		}
		for _, when := range e.Whens {
			vars = append(vars, extractVarsFromExpr(when.Cond)...)
			vars = append(vars, extractVarsFromExpr(when.Val)...)
		}
		if e.Else != nil {
			vars = append(vars, extractVarsFromExpr(e.Else)...)
		}
	}
	return vars
}

// detectVirtualTable checks if query references Marmot virtual tables
func detectVirtualTable(sel *sqlparser.Select) VirtualTableType {
	for _, tableExpr := range sel.From {
		if aliased, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := aliased.Expr.(sqlparser.TableName); ok {
				name := strings.ToUpper(tableName.Name.String())
				qualifier := ""
				if tableName.Qualifier.NotEmpty() {
					qualifier = strings.ToUpper(tableName.Qualifier.String())
				}
				// MARMOT_CLUSTER_NODES or MARMOT.CLUSTER_NODES
				if name == "MARMOT_CLUSTER_NODES" ||
					(qualifier == "MARMOT" && name == "CLUSTER_NODES") {
					return VirtualTableClusterNodes
				}
			}
		}
	}
	return VirtualTableUnknown
}

// detectInformationSchemaTable detects which INFORMATION_SCHEMA table is being queried
func detectInformationSchemaTable(sel *sqlparser.Select) InformationSchemaTableType {
	for _, tableExpr := range sel.From {
		if aliased, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := aliased.Expr.(sqlparser.TableName); ok {
				if tableName.Qualifier.NotEmpty() {
					qualifier := strings.ToUpper(tableName.Qualifier.String())
					if qualifier == "INFORMATION_SCHEMA" {
						name := strings.ToUpper(tableName.Name.String())
						switch name {
						case "TABLES":
							return ISTableTables
						case "COLUMNS":
							return ISTableColumns
						case "SCHEMATA":
							return ISTableSchemata
						case "STATISTICS":
							return ISTableStatistics
						}
					}
				}
			}
		}
	}
	return ISTableUnknown
}

// extractInformationSchemaFilter extracts WHERE clause filter values from INFORMATION_SCHEMA queries
func extractInformationSchemaFilter(sel *sqlparser.Select) InformationSchemaFilter {
	filter := InformationSchemaFilter{}
	if sel.Where == nil {
		return filter
	}
	extractFiltersFromExpr(sel.Where.Expr, &filter)
	return filter
}

// extractFiltersFromExpr recursively walks WHERE expression to find equality comparisons
func extractFiltersFromExpr(expr sqlparser.Expr, filter *InformationSchemaFilter) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		extractFiltersFromExpr(e.Left, filter)
		extractFiltersFromExpr(e.Right, filter)
	case *sqlparser.OrExpr:
		// For OR expressions, we can't reliably extract filters
		// but we still walk both sides in case there's an AND somewhere
		extractFiltersFromExpr(e.Left, filter)
		extractFiltersFromExpr(e.Right, filter)
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualOp {
			extractEqualityFilter(e, filter)
		}
	}
}

// extractEqualityFilter extracts column = 'value' patterns
func extractEqualityFilter(cmp *sqlparser.ComparisonExpr, filter *InformationSchemaFilter) {
	// Get column name (could be on left or right side)
	var colName string
	var value string

	if col, ok := cmp.Left.(*sqlparser.ColName); ok {
		colName = strings.ToUpper(col.Name.String())
		value = extractStringValue(cmp.Right)
	} else if col, ok := cmp.Right.(*sqlparser.ColName); ok {
		colName = strings.ToUpper(col.Name.String())
		value = extractStringValue(cmp.Left)
	}

	if value == "" {
		return
	}

	switch colName {
	case "TABLE_SCHEMA", "SCHEMA_NAME":
		filter.SchemaName = value
	case "TABLE_NAME":
		filter.TableName = value
	case "COLUMN_NAME":
		filter.ColumnName = value
	}
}

// extractStringValue extracts string literal value from expression
func extractStringValue(expr sqlparser.Expr) string {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		// Remove surrounding quotes
		val := v.Val
		if len(val) >= 2 && (val[0] == '\'' || val[0] == '"') {
			return val[1 : len(val)-1]
		}
		return val
	}
	return ""
}

func extractMetadata(ctx *QueryContext, stmt sqlparser.Statement) {
	switch parsed := stmt.(type) {
	case *sqlparser.Insert:
		if parsed.Table != nil {
			if tn, ok := parsed.Table.Expr.(sqlparser.TableName); ok {
				ctx.MySQLState.TableName = tn.Name.String()
				if tn.Qualifier.NotEmpty() {
					ctx.Output.Database = tn.Qualifier.String()
				}
			}
		}

	case *sqlparser.Update:
		if len(parsed.TableExprs) > 0 {
			if aliased, ok := parsed.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := aliased.Expr.(sqlparser.TableName); ok {
					ctx.MySQLState.TableName = tn.Name.String()
					if tn.Qualifier.NotEmpty() {
						ctx.Output.Database = tn.Qualifier.String()
					}
				}
			}
		}

	case *sqlparser.Delete:
		if len(parsed.TableExprs) > 0 {
			if aliased, ok := parsed.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := aliased.Expr.(sqlparser.TableName); ok {
					ctx.MySQLState.TableName = tn.Name.String()
					if tn.Qualifier.NotEmpty() {
						ctx.Output.Database = tn.Qualifier.String()
					}
				}
			}
		}

	case *sqlparser.Select:
		if len(parsed.From) > 0 {
			db, _ := extractDatabaseFromTableExpr(parsed.From[0])
			if db != "" {
				ctx.Output.Database = db
			}
		}

	case *sqlparser.CreateTable:
		if parsed.Table.Qualifier.NotEmpty() {
			ctx.Output.Database = parsed.Table.Qualifier.String()
		}

	case *sqlparser.AlterTable:
		if parsed.Table.Qualifier.NotEmpty() {
			ctx.Output.Database = parsed.Table.Qualifier.String()
		}

	case *sqlparser.DropTable:
		if len(parsed.FromTables) > 0 {
			if parsed.FromTables[0].Qualifier.NotEmpty() {
				ctx.Output.Database = parsed.FromTables[0].Qualifier.String()
			}
		}

	case *sqlparser.CreateDatabase:
		ctx.Output.Database = parsed.DBName.String()

	case *sqlparser.DropDatabase:
		ctx.Output.Database = parsed.DBName.String()

	case *sqlparser.AlterDatabase:
		ctx.Output.Database = parsed.DBName.String()

	case *sqlparser.RenameTable:
		if len(parsed.TablePairs) > 0 {
			if parsed.TablePairs[0].FromTable.Qualifier.NotEmpty() {
				ctx.Output.Database = parsed.TablePairs[0].FromTable.Qualifier.String()
			}
		}

	case sqlparser.DDLStatement:
		table := parsed.GetTable()
		if !table.IsEmpty() {
			if table.Qualifier.NotEmpty() {
				ctx.Output.Database = table.Qualifier.String()
			}
		}

	case *sqlparser.Show:
		extractShowMetadata(ctx, parsed)

	case *sqlparser.Use:
		ctx.Output.Database = parsed.DBName.String()
	}
}

func extractShowMetadata(ctx *QueryContext, parsed *sqlparser.Show) {
	if showBasic, ok := parsed.Internal.(*sqlparser.ShowBasic); ok {
		if showBasic.DbName.NotEmpty() {
			ctx.Output.Database = showBasic.DbName.String()
		}
		if !showBasic.Tbl.IsEmpty() {
			ctx.MySQLState.TableName = showBasic.Tbl.Name.String()
			if showBasic.Tbl.Qualifier.NotEmpty() {
				ctx.Output.Database = showBasic.Tbl.Qualifier.String()
			}
		}
	} else if showCreate, ok := parsed.Internal.(*sqlparser.ShowCreate); ok {
		if !showCreate.Op.IsEmpty() {
			ctx.MySQLState.TableName = showCreate.Op.Name.String()
			if showCreate.Op.Qualifier.NotEmpty() {
				ctx.Output.Database = showCreate.Op.Qualifier.String()
			}
		}
	}
}

func extractDatabaseFromTableExpr(expr sqlparser.TableExpr) (database string, table string) {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tn, ok := e.Expr.(sqlparser.TableName); ok {
			table = tn.Name.String()
			if tn.Qualifier.NotEmpty() {
				database = tn.Qualifier.String()
			}
		}
	case *sqlparser.JoinTableExpr:
		database, table = extractDatabaseFromTableExpr(e.LeftExpr)
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
		for _, t := range e.Exprs {
			db, tbl := extractDatabaseFromTableExpr(t)
			if db != "" {
				return db, tbl
			}
		}
	}
	return database, table
}
