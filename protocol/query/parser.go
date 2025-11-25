package query

import (
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
	classifyByPattern(ctx)

	stmt, err := p.parser.Parse(ctx.OriginalSQL)
	if err != nil {
		return err
	}

	ctx.AST = stmt
	classifyStatement(ctx, stmt)
	extractMetadata(ctx, stmt)

	return nil
}

func classifyByPattern(ctx *QueryContext) {
	sql := ctx.OriginalSQL

	if savepointPattern.MatchString(sql) || releaseSavepointPattern.MatchString(sql) {
		ctx.StatementType = StatementSavepoint
		return
	}

	if setTransactionPattern.MatchString(sql) {
		ctx.StatementType = StatementBegin
		return
	}

	if xaStartPattern.MatchString(sql) || xaEndPattern.MatchString(sql) ||
		xaPreparePattern.MatchString(sql) || xaCommitPattern.MatchString(sql) ||
		xaRollbackPattern.MatchString(sql) || xaRecoverPattern.MatchString(sql) {
		ctx.StatementType = StatementXA
		return
	}

	if lockInstancePattern.MatchString(sql) || unlockInstancePattern.MatchString(sql) {
		ctx.StatementType = StatementLock
		return
	}

	if installPluginPattern.MatchString(sql) || uninstallPluginPattern.MatchString(sql) ||
		installComponentPattern.MatchString(sql) || uninstallComponentPattern.MatchString(sql) {
		ctx.StatementType = StatementAdmin
		return
	}

	if loadXMLPattern.MatchString(sql) {
		ctx.StatementType = StatementLoadData
		return
	}

	if insertDelayedPattern.MatchString(sql) {
		ctx.StatementType = StatementInsert
		tablePattern := regexp.MustCompile(`(?i)INSERT\s+(DELAYED|LOW_PRIORITY|HIGH_PRIORITY)\s+INTO\s+(\w+)`)
		if matches := tablePattern.FindStringSubmatch(sql); len(matches) > 2 {
			ctx.TableName = matches[2]
		}
		return
	}

	if matches := insertOrPattern.FindStringSubmatch(sql); len(matches) > 0 {
		if len(matches) > 1 && strings.ToUpper(matches[1]) == "REPLACE" {
			ctx.StatementType = StatementReplace
		} else {
			ctx.StatementType = StatementInsert
		}
		return
	}
}

func classifyStatement(ctx *QueryContext, stmt sqlparser.Statement) {
	if ctx.StatementType != 0 {
		return
	}

	switch parsed := stmt.(type) {
	case *sqlparser.Insert:
		if parsed.Action == sqlparser.ReplaceAct {
			ctx.StatementType = StatementReplace
		} else {
			ctx.StatementType = StatementInsert
		}

	case *sqlparser.Update:
		ctx.StatementType = StatementUpdate

	case *sqlparser.Delete:
		ctx.StatementType = StatementDelete

	case *sqlparser.Select:
		ctx.StatementType = StatementSelect
		if isInformationSchemaQuery(parsed) {
			ctx.StatementType = StatementInformationSchema
		}

	case *sqlparser.CreateTable:
		ctx.StatementType = StatementDDL

	case *sqlparser.AlterTable:
		ctx.StatementType = StatementDDL

	case *sqlparser.DropTable:
		ctx.StatementType = StatementDDL

	case *sqlparser.CreateDatabase:
		ctx.StatementType = StatementCreateDatabase

	case *sqlparser.DropDatabase:
		ctx.StatementType = StatementDropDatabase

	case *sqlparser.AlterDatabase:
		ctx.StatementType = StatementDDL

	case *sqlparser.RenameTable:
		ctx.StatementType = StatementDDL

	case sqlparser.DDLStatement:
		ctx.StatementType = StatementDDL

	case *sqlparser.Show:
		classifyShowStatement(ctx, parsed)

	case *sqlparser.Use:
		ctx.StatementType = StatementUseDatabase

	case *sqlparser.Begin:
		ctx.StatementType = StatementBegin

	case *sqlparser.Commit:
		ctx.StatementType = StatementCommit

	case *sqlparser.Rollback:
		ctx.StatementType = StatementRollback

	case *sqlparser.Savepoint:
		ctx.StatementType = StatementSavepoint

	case *sqlparser.Set:
		ctx.StatementType = StatementSet

	case *sqlparser.Load:
		ctx.StatementType = StatementLoadData

	case *sqlparser.OtherAdmin:
		ctx.StatementType = StatementAdmin

	case *sqlparser.LockTables:
		ctx.StatementType = StatementLock

	case *sqlparser.UnlockTables:
		ctx.StatementType = StatementLock

	default:
		ctx.StatementType = StatementSelect
	}
}

func classifyShowStatement(ctx *QueryContext, parsed *sqlparser.Show) {
	if showBasic, ok := parsed.Internal.(*sqlparser.ShowBasic); ok {
		switch showBasic.Command {
		case sqlparser.Database:
			ctx.StatementType = StatementShowDatabases
		case sqlparser.Table:
			ctx.StatementType = StatementShowTables
		case sqlparser.Column:
			ctx.StatementType = StatementShowColumns
		case sqlparser.Index:
			ctx.StatementType = StatementShowIndexes
		case sqlparser.TableStatus:
			ctx.StatementType = StatementShowTableStatus
		default:
			ctx.StatementType = StatementSelect
		}
	} else if _, ok := parsed.Internal.(*sqlparser.ShowCreate); ok {
		ctx.StatementType = StatementShowCreateTable
	} else {
		ctx.StatementType = StatementSelect
	}
}

func isInformationSchemaQuery(sel *sqlparser.Select) bool {
	for _, tableExpr := range sel.From {
		if aliased, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := aliased.Expr.(sqlparser.TableName); ok {
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

func extractMetadata(ctx *QueryContext, stmt sqlparser.Statement) {
	switch parsed := stmt.(type) {
	case *sqlparser.Insert:
		if parsed.Table != nil {
			if tn, ok := parsed.Table.Expr.(sqlparser.TableName); ok {
				ctx.TableName = tn.Name.String()
				if tn.Qualifier.NotEmpty() {
					ctx.Database = tn.Qualifier.String()
				}
			}
		}

	case *sqlparser.Update:
		if len(parsed.TableExprs) > 0 {
			if aliased, ok := parsed.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := aliased.Expr.(sqlparser.TableName); ok {
					ctx.TableName = tn.Name.String()
					if tn.Qualifier.NotEmpty() {
						ctx.Database = tn.Qualifier.String()
					}
				}
			}
		}

	case *sqlparser.Delete:
		if len(parsed.TableExprs) > 0 {
			if aliased, ok := parsed.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := aliased.Expr.(sqlparser.TableName); ok {
					ctx.TableName = tn.Name.String()
					if tn.Qualifier.NotEmpty() {
						ctx.Database = tn.Qualifier.String()
					}
				}
			}
		}

	case *sqlparser.Select:
		if len(parsed.From) > 0 {
			db, table := extractDatabaseFromTableExpr(parsed.From[0])
			if db != "" {
				ctx.Database = db
			}
			if table != "" {
				ctx.TableName = table
			}
		}

	case *sqlparser.CreateTable:
		ctx.TableName = parsed.Table.Name.String()
		if parsed.Table.Qualifier.NotEmpty() {
			ctx.Database = parsed.Table.Qualifier.String()
		}

	case *sqlparser.AlterTable:
		ctx.TableName = parsed.Table.Name.String()
		if parsed.Table.Qualifier.NotEmpty() {
			ctx.Database = parsed.Table.Qualifier.String()
		}

	case *sqlparser.DropTable:
		if len(parsed.FromTables) > 0 {
			ctx.TableName = parsed.FromTables[0].Name.String()
			if parsed.FromTables[0].Qualifier.NotEmpty() {
				ctx.Database = parsed.FromTables[0].Qualifier.String()
			}
		}

	case *sqlparser.CreateDatabase:
		ctx.Database = parsed.DBName.String()

	case *sqlparser.DropDatabase:
		ctx.Database = parsed.DBName.String()

	case *sqlparser.AlterDatabase:
		ctx.Database = parsed.DBName.String()

	case *sqlparser.RenameTable:
		if len(parsed.TablePairs) > 0 {
			ctx.TableName = parsed.TablePairs[0].FromTable.Name.String()
			if parsed.TablePairs[0].FromTable.Qualifier.NotEmpty() {
				ctx.Database = parsed.TablePairs[0].FromTable.Qualifier.String()
			}
		}

	case sqlparser.DDLStatement:
		table := parsed.GetTable()
		if !table.IsEmpty() {
			ctx.TableName = table.Name.String()
			if table.Qualifier.NotEmpty() {
				ctx.Database = table.Qualifier.String()
			}
		}

	case *sqlparser.Show:
		extractShowMetadata(ctx, parsed)

	case *sqlparser.Use:
		ctx.Database = parsed.DBName.String()
	}
}

func extractShowMetadata(ctx *QueryContext, parsed *sqlparser.Show) {
	if showBasic, ok := parsed.Internal.(*sqlparser.ShowBasic); ok {
		if showBasic.DbName.NotEmpty() {
			ctx.Database = showBasic.DbName.String()
		}
		if !showBasic.Tbl.IsEmpty() {
			ctx.TableName = showBasic.Tbl.Name.String()
			if showBasic.Tbl.Qualifier.NotEmpty() {
				ctx.Database = showBasic.Tbl.Qualifier.String()
			}
		}
	} else if showCreate, ok := parsed.Internal.(*sqlparser.ShowCreate); ok {
		if !showCreate.Op.IsEmpty() {
			ctx.TableName = showCreate.Op.Name.String()
			if showCreate.Op.Qualifier.NotEmpty() {
				ctx.Database = showCreate.Op.Qualifier.String()
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
