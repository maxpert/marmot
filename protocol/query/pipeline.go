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
	if ctx.Input.Dialect == DialectSQLite {
		ctx.Output.Statements = []TranspiledStatement{{SQL: ctx.Input.SQL, Params: ctx.Input.Parameters}}
		ctx.Output.IsValid = true
		classifySQLiteStatement(ctx)
		return nil
	}

	// MySQL path: Parse with Vitess
	if err := p.parser.Parse(ctx); err != nil {
		log.Debug().Err(err).Str("sql", ctx.Input.SQL).Msg("Parse failed")
		return err
	}

	if err := p.transpiler.Transpile(ctx); err != nil {
		log.Debug().Err(err).Str("sql", ctx.Input.SQL).Msg("Transpile failed")
		return err
	}

	if ctx.Output.Database != "" {
		stripDatabaseQualifiers(ctx)
	}

	// Mark query as valid (validator removed - transpilation success means valid)
	ctx.Output.IsValid = true

	// Log transformations for debugging
	if ctx.MySQLState != nil && len(ctx.MySQLState.Transformations) > 0 {
		log.Debug().
			Str("original", ctx.Input.SQL).
			Str("transpiled", ctx.Output.Statements[0].SQL).
			Int("statement_count", len(ctx.Output.Statements)).
			Interface("transformations", ctx.MySQLState.Transformations).
			Msg("Query transpiled")
	}

	return nil
}

// stripDatabaseQualifiers removes database name qualifiers from table references.
// SQLite doesn't support database qualifiers in the same way as MySQL.
func stripDatabaseQualifiers(ctx *QueryContext) {
	if ctx.MySQLState == nil || ctx.MySQLState.AST == nil {
		return
	}

	sqlparser.Rewrite(ctx.MySQLState.AST, func(cursor *sqlparser.Cursor) bool {
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
	sql := serializer.Serialize(ctx.MySQLState.AST)
	ctx.Output.Statements[0].SQL = sql
}

// classifySQLiteStatement classifies SQLite dialect statements based on prefix matching.
// This is a lightweight classification used when bypassing the MySQL parser.
func classifySQLiteStatement(ctx *QueryContext) {
	upper := strings.ToUpper(strings.TrimSpace(ctx.Input.SQL))

	switch {
	case strings.HasPrefix(upper, "PRAGMA"):
		ctx.Output.StatementType = StatementUnsupported
	case strings.HasPrefix(upper, "SELECT"):
		ctx.Output.StatementType = StatementSelect
	case strings.HasPrefix(upper, "INSERT"):
		ctx.Output.StatementType = StatementInsert
	case strings.HasPrefix(upper, "UPDATE"):
		ctx.Output.StatementType = StatementUpdate
	case strings.HasPrefix(upper, "DELETE"):
		ctx.Output.StatementType = StatementDelete
	case strings.HasPrefix(upper, "CREATE"):
		ctx.Output.StatementType = StatementDDL
	case strings.HasPrefix(upper, "DROP"):
		ctx.Output.StatementType = StatementDDL
	case strings.HasPrefix(upper, "ALTER"):
		ctx.Output.StatementType = StatementDDL
	default:
		ctx.Output.StatementType = StatementUnsupported
	}
}
