package query

import (
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
	// SQLite dialect: Parse with rqlite/sql and validate
	if ctx.SourceDialect == DialectSQLite {
		ctx.TranspiledSQL = ctx.OriginalSQL

		// Parse with rqlite/sql parser and classify
		if err := p.validator.ValidateAndClassify(ctx); err != nil {
			return err
		}

		setExecutionFlags(ctx)
		return nil
	}

	// MySQL path: Parse with Vitess
	if err := p.parser.Parse(ctx); err != nil {
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
