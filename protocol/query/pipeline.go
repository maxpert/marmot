package query

import (
	"strings"

	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol/query/transform"
	"github.com/rs/zerolog/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Pipeline coordinates query parsing and transpilation from MySQL to SQLite.
type Pipeline struct {
	parser     *VitessParser
	transpiler *Transpiler
}

// NewPipeline creates a new query processing pipeline.
// idGen is optional - if nil, auto-increment ID injection is disabled.
func NewPipeline(cacheSize int, idGen id.Generator) (*Pipeline, error) {
	p, err := NewVitessParser()
	if err != nil {
		return nil, err
	}

	t, err := NewTranspiler(cacheSize, idGen)
	if err != nil {
		return nil, err
	}

	return &Pipeline{
		parser:     p,
		transpiler: t,
	}, nil
}

// Close releases any resources held by the pipeline. Currently a no-op.
func (p *Pipeline) Close() {
	// No resources to clean up
}

// Process parses and transpiles a query, setting execution flags and metadata in the context.
// For SQLite dialect, queries pass through unchanged. For MySQL, they are parsed and transpiled.
func (p *Pipeline) Process(ctx *QueryContext) error {
	// SQLite dialect: pass through directly
	if ctx.SourceDialect == DialectSQLite {
		ctx.TranspiledSQL = ctx.OriginalSQL
		ctx.TranspiledStatements = []transform.TranspiledStatement{{SQL: ctx.OriginalSQL, Params: ctx.Parameters}}
		ctx.IsValid = true
		classifySQLiteStatement(ctx)
		setExecutionFlags(ctx)
		return nil
	}

	// MySQL path: Parse with Vitess
	if err := p.parser.Parse(ctx); err != nil {
		log.Debug().Err(err).Str("sql", ctx.OriginalSQL).Msg("Parse failed")
		return err
	}

	if err := p.transpiler.Transpile(ctx); err != nil {
		log.Debug().Err(err).Str("sql", ctx.OriginalSQL).Msg("Transpile failed")
		return err
	}

	if ctx.Database != "" {
		stripDatabaseQualifiers(ctx)
	}

	// Mark query as valid (validator removed - transpilation success means valid)
	ctx.IsValid = true

	setExecutionFlags(ctx)

	// Log transformations for debugging
	if len(ctx.Transformations) > 0 {
		log.Debug().
			Str("original", ctx.OriginalSQL).
			Str("transpiled", ctx.TranspiledSQL).
			Int("statement_count", len(ctx.TranspiledStatements)).
			Interface("transformations", ctx.Transformations).
			Msg("Query transpiled")
	}

	return nil
}

// setExecutionFlags sets IsMutation and IsReadOnly based on the statement type.
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

// stripDatabaseQualifiers removes database name qualifiers from table references.
// SQLite doesn't support database qualifiers in the same way as MySQL.
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

	// Re-serialize the main statement after stripping qualifiers.
	// Additional statements (like CREATE INDEX) are assumed not to have qualifiers.
	serializer := &transform.SQLiteSerializer{}
	ctx.TranspiledSQL = serializer.Serialize(ctx.AST)
	ctx.TranspiledStatements[0].SQL = ctx.TranspiledSQL
}

// classifySQLiteStatement classifies SQLite dialect statements based on prefix matching.
// This is a lightweight classification used when bypassing the MySQL parser.
func classifySQLiteStatement(ctx *QueryContext) {
	upper := strings.ToUpper(strings.TrimSpace(ctx.OriginalSQL))

	switch {
	case strings.HasPrefix(upper, "PRAGMA"):
		ctx.StatementType = StatementUnsupported
	case strings.HasPrefix(upper, "SELECT"):
		ctx.StatementType = StatementSelect
	case strings.HasPrefix(upper, "INSERT"):
		ctx.StatementType = StatementInsert
	case strings.HasPrefix(upper, "UPDATE"):
		ctx.StatementType = StatementUpdate
	case strings.HasPrefix(upper, "DELETE"):
		ctx.StatementType = StatementDelete
	case strings.HasPrefix(upper, "CREATE"):
		ctx.StatementType = StatementDDL
	case strings.HasPrefix(upper, "DROP"):
		ctx.StatementType = StatementDDL
	case strings.HasPrefix(upper, "ALTER"):
		ctx.StatementType = StatementDDL
	default:
		ctx.StatementType = StatementUnsupported
	}
}
