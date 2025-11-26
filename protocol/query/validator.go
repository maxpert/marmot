package query

import (
	"database/sql"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	rqlitesql "github.com/rqlite/sql"
)

type Validator struct {
	connPool chan *sql.DB
}

func NewValidator(poolSize int) (*Validator, error) {
	pool := make(chan *sql.DB, poolSize)
	for i := 0; i < poolSize; i++ {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			for j := 0; j < i; j++ {
				closeDB := <-pool
				closeDB.Close()
			}
			return nil, err
		}
		pool <- db
	}
	return &Validator{connPool: pool}, nil
}

func (v *Validator) Close() {
	close(v.connPool)
	for db := range v.connPool {
		db.Close()
	}
}

func (v *Validator) Validate(ctx *QueryContext) error {
	if !shouldValidate(ctx.StatementType) {
		ctx.IsValid = true
		return nil
	}

	db := <-v.connPool
	defer func() { v.connPool <- db }()

	stmt, err := db.Prepare(ctx.TranspiledSQL)
	if err != nil {
		if isSchemaError(err) {
			ctx.IsValid = true
			return nil
		}
		ctx.IsValid = false
		ctx.ValidationErr = err
		return err
	}
	stmt.Close()

	ctx.IsValid = true
	return nil
}

// ValidateAndClassify validates SQL using SQLite's parser and classifies the statement type
// Uses rqlite/sql parser for proper AST-based classification - no string matching
func (v *Validator) ValidateAndClassify(ctx *QueryContext) error {
	db := <-v.connPool
	defer func() { v.connPool <- db }()

	// First validate syntax using SQLite's Prepare
	stmt, err := db.Prepare(ctx.TranspiledSQL)
	if err != nil {
		if isSchemaError(err) {
			// Schema errors mean syntax is valid, just missing tables
			ctx.IsValid = true
			classifyFromAST(ctx)
			return nil
		}
		ctx.IsValid = false
		ctx.ValidationErr = err
		return err
	}
	stmt.Close()

	ctx.IsValid = true

	// Classify using rqlite/sql parser AST
	classifyFromAST(ctx)
	return nil
}

// classifyFromAST classifies statement type and extracts table name using rqlite/sql parser
// This provides proper AST-based classification without string matching
// Also populates ctx.SQLiteAST for use in CDC extraction
func classifyFromAST(ctx *QueryContext) {
	parser := rqlitesql.NewParser(strings.NewReader(ctx.TranspiledSQL))
	astStmt, err := parser.ParseStatement()
	if err != nil {
		// Parser failed - mark as unsupported
		ctx.StatementType = StatementUnsupported
		return
	}

	// Store the AST for CDC extraction
	ctx.SQLiteAST = astStmt

	switch s := astStmt.(type) {
	case *rqlitesql.InsertStatement:
		// Check for INSERT OR REPLACE or REPLACE statement
		// InsertOrReplace and Replace are Pos types - check if they're valid (non-zero)
		if s.InsertOrReplace.IsValid() || s.Replace.IsValid() {
			ctx.StatementType = StatementReplace
		} else {
			ctx.StatementType = StatementInsert
		}
		ctx.TableName = rqlitesql.IdentName(s.Table)

	case *rqlitesql.UpdateStatement:
		ctx.StatementType = StatementUpdate
		if s.Table != nil {
			ctx.TableName = s.Table.TableName()
		}

	case *rqlitesql.DeleteStatement:
		ctx.StatementType = StatementDelete
		if s.Table != nil {
			ctx.TableName = s.Table.TableName()
		}

	case *rqlitesql.SelectStatement:
		ctx.StatementType = StatementSelect

	case *rqlitesql.CreateTableStatement:
		ctx.StatementType = StatementDDL
		ctx.TableName = rqlitesql.IdentName(s.Name)

	case *rqlitesql.CreateIndexStatement:
		ctx.StatementType = StatementDDL

	case *rqlitesql.CreateViewStatement:
		ctx.StatementType = StatementDDL

	case *rqlitesql.CreateTriggerStatement:
		ctx.StatementType = StatementDDL

	case *rqlitesql.DropTableStatement:
		ctx.StatementType = StatementDDL

	case *rqlitesql.DropIndexStatement:
		ctx.StatementType = StatementDDL

	case *rqlitesql.DropViewStatement:
		ctx.StatementType = StatementDDL

	case *rqlitesql.DropTriggerStatement:
		ctx.StatementType = StatementDDL

	case *rqlitesql.AlterTableStatement:
		ctx.StatementType = StatementDDL

	case *rqlitesql.BeginStatement:
		ctx.StatementType = StatementBegin

	case *rqlitesql.CommitStatement:
		ctx.StatementType = StatementCommit

	case *rqlitesql.RollbackStatement:
		ctx.StatementType = StatementRollback

	case *rqlitesql.SavepointStatement:
		ctx.StatementType = StatementSavepoint

	case *rqlitesql.ReleaseStatement:
		ctx.StatementType = StatementSavepoint

	case *rqlitesql.AnalyzeStatement:
		ctx.StatementType = StatementAdmin

	case *rqlitesql.ExplainStatement:
		ctx.StatementType = StatementAdmin

	default:
		ctx.StatementType = StatementUnsupported
	}
}

func shouldValidate(stmtType StatementType) bool {
	switch stmtType {
	case StatementInsert, StatementReplace, StatementUpdate, StatementDelete, StatementSelect:
		return true
	default:
		return false
	}
}

func isSchemaError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "no such table") ||
		strings.Contains(errStr, "no such column") ||
		strings.Contains(errStr, "no such index") ||
		strings.Contains(errStr, "no such view") ||
		strings.Contains(errStr, "no such trigger")
}
