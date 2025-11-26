package query

import (
	"fmt"

	"github.com/maxpert/marmot/protocol/cdc"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Pipeline struct {
	parser     *VitessParser
	transpiler *Transpiler
	validator  *Validator
}

func NewPipeline(cacheSize, validatorPoolSize int) (*Pipeline, error) {
	p, err := NewVitessParser()
	if err != nil {
		return nil, err
	}

	t, err := NewTranspiler(cacheSize)
	if err != nil {
		return nil, err
	}

	v, err := NewValidator(validatorPoolSize)
	if err != nil {
		return nil, err
	}

	return &Pipeline{
		parser:     p,
		transpiler: t,
		validator:  v,
	}, nil
}

func (p *Pipeline) Close() {
	if p.validator != nil {
		p.validator.Close()
	}
}

func (p *Pipeline) Process(ctx *QueryContext) error {
	// SQLite dialect: Parse with rqlite/sql, extract CDC, validate
	if ctx.SourceDialect == DialectSQLite {
		ctx.TranspiledSQL = ctx.OriginalSQL

		// Parse with rqlite/sql parser and classify
		if err := p.validator.ValidateAndClassify(ctx); err != nil {
			return err
		}

		// Extract CDC data using SQLite AST (same as MySQL path)
		if err := extractSQLiteCDC(ctx); err != nil {
			return err
		}

		setExecutionFlags(ctx)
		return nil
	}

	// MySQL path: Parse with Vitess
	if err := p.parser.Parse(ctx); err != nil {
		return err
	}

	// Extract CDC data immediately after parsing, BEFORE transpilation
	// This ensures the AST is valid and matches the original MySQL SQL
	if err := extractCDC(ctx); err != nil {
		return err
	}

	if err := p.transpiler.Transpile(ctx); err != nil {
		return err
	}

	if ctx.Database != "" {
		stripDatabaseQualifiers(ctx)
	}

	if err := p.validator.Validate(ctx); err != nil {
		return err
	}

	setExecutionFlags(ctx)

	return nil
}

func setExecutionFlags(ctx *QueryContext) {
	switch ctx.StatementType {
	case StatementInsert, StatementReplace, StatementUpdate, StatementDelete,
		StatementLoadData, StatementDDL, StatementDCL, StatementAdmin,
		StatementCreateDatabase, StatementDropDatabase:
		ctx.IsMutation = true
		ctx.IsReadOnly = false
	case StatementSelect, StatementShowDatabases, StatementShowTables,
		StatementShowColumns, StatementShowCreateTable, StatementShowIndexes,
		StatementShowTableStatus, StatementInformationSchema:
		ctx.IsMutation = false
		ctx.IsReadOnly = true
	default:
		ctx.IsMutation = false
		ctx.IsReadOnly = false
	}
}

func stripDatabaseQualifiers(ctx *QueryContext) {
	if ctx.AST == nil {
		return
	}

	sqlparser.Rewrite(ctx.AST, func(cursor *sqlparser.Cursor) bool {
		switch n := cursor.Node().(type) {
		case sqlparser.TableName:
			newTableName := sqlparser.TableName{
				Name:      n.Name,
				Qualifier: sqlparser.NewIdentifierCS(""),
			}
			cursor.Replace(newTableName)
		}
		return true
	}, nil)

	ctx.TranspiledSQL = sqlparser.String(ctx.AST)
}

// extractCDC extracts CDC (Change Data Capture) row data from AST
// This must be called BEFORE transpilation to ensure AST validity
// For DML operations (INSERT/UPDATE/DELETE), CDC extraction is REQUIRED
// For multi-row INSERTs, extracts ALL rows into CDCRows
func extractCDC(ctx *QueryContext) error {
	// Only extract CDC for DML operations
	switch ctx.StatementType {
	case StatementInsert, StatementReplace, StatementUpdate, StatementDelete:
		// Require Vitess AST to be populated for DML
		if ctx.AST == nil {
			return fmt.Errorf("Vitess AST not populated for DML statement (required for CDC)")
		}

		// Extract ALL rows (handles multi-row INSERTs)
		rowDataList, err := cdc.ExtractAllRowData(ctx.AST)
		if err != nil {
			return err
		}

		// Convert to CDCRow slice
		ctx.CDCRows = make([]*CDCRow, len(rowDataList))
		for i, rd := range rowDataList {
			ctx.CDCRows[i] = &CDCRow{
				OldValues: rd.OldValues,
				NewValues: rd.NewValues,
			}
		}

		// For backwards compatibility, populate single-row fields with first row
		if len(rowDataList) > 0 {
			ctx.CDCOldValues = rowDataList[0].OldValues
			ctx.CDCNewValues = rowDataList[0].NewValues
		}
	}

	return nil
}

// extractSQLiteCDC extracts CDC (Change Data Capture) row data from SQLite AST
// This is the SQLite equivalent of extractCDC for statements parsed as SQLite dialect
// For DML operations (INSERT/UPDATE/DELETE), CDC extraction is REQUIRED
// For multi-row INSERTs, extracts ALL rows into CDCRows
func extractSQLiteCDC(ctx *QueryContext) error {
	// Only extract CDC for DML operations
	switch ctx.StatementType {
	case StatementInsert, StatementReplace, StatementUpdate, StatementDelete:
		// Require SQLite AST to be populated for DML
		if ctx.SQLiteAST == nil {
			return fmt.Errorf("SQLite AST not populated for DML statement (required for CDC)")
		}

		// Extract ALL rows (handles multi-row INSERTs)
		rowDataList, err := cdc.ExtractAllRowDataFromSQLite(ctx.SQLiteAST)
		if err != nil {
			return err
		}

		// Convert to CDCRow slice
		ctx.CDCRows = make([]*CDCRow, len(rowDataList))
		for i, rd := range rowDataList {
			ctx.CDCRows[i] = &CDCRow{
				OldValues: rd.OldValues,
				NewValues: rd.NewValues,
			}
		}

		// For backwards compatibility, populate single-row fields with first row
		if len(rowDataList) > 0 {
			ctx.CDCOldValues = rowDataList[0].OldValues
			ctx.CDCNewValues = rowDataList[0].NewValues
		}
	}

	return nil
}
