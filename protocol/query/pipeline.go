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
// When SkipTranspilation is true, SQL passes through unchanged with only classification performed.
func (p *Pipeline) Process(ctx *QueryContext) error {
	// Skip transpilation: pass through directly with classification only
	if ctx.SkipTranspilation {
		ctx.Output.Statements = []TranspiledStatement{{SQL: ctx.Input.SQL, Params: ctx.Input.Parameters}}
		ctx.Output.IsValid = true
		classifySQLiteStatement(ctx)
		return nil
	}

	// SQLite dialect: pass through directly
	if ctx.Input.Dialect == DialectSQLite {
		ctx.Output.Statements = []TranspiledStatement{{SQL: ctx.Input.SQL, Params: ctx.Input.Parameters}}
		ctx.Output.IsValid = true
		classifySQLiteStatement(ctx)
		return nil
	}

	// MySQL path: Preprocess to convert ANSI SQL quoted identifiers to MySQL backticks
	// Drupal and some other apps use "table_name" instead of `table_name`
	ctx.Input.SQL = convertANSIQuotesToBackticks(ctx.Input.SQL)

	// Parse with Vitess
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

// convertANSIQuotesToBackticks converts ANSI SQL double-quoted identifiers to MySQL backticks.
// For example: SELECT "column" FROM "table" becomes SELECT `column` FROM `table`
// This is needed for compatibility with Drupal and other apps that use ANSI SQL quoting.
// The function preserves:
//   - Content inside single-quoted strings
//   - Empty strings "" (converts to '' for MySQL compatibility)
//   - Strings that look like values (after = or DEFAULT keywords)
func convertANSIQuotesToBackticks(sql string) string {
	// Quick check: if no double quotes, return as-is
	if !strings.Contains(sql, "\"") {
		return sql
	}

	var result strings.Builder
	result.Grow(len(sql))

	inSingleQuote := false
	i := 0

	for i < len(sql) {
		c := sql[i]

		// Handle single quotes (string literals)
		if c == '\'' {
			if !inSingleQuote {
				inSingleQuote = true
			} else if i+1 < len(sql) && sql[i+1] == '\'' {
				// Escaped single quote ('') - write both and skip
				result.WriteString("''")
				i += 2
				continue
			} else {
				inSingleQuote = false
			}
			result.WriteByte(c)
			i++
			continue
		}

		// Inside single-quoted string - pass through unchanged
		if inSingleQuote {
			result.WriteByte(c)
			i++
			continue
		}

		// Handle double quotes outside of single-quoted strings
		if c == '"' {
			// Find the closing double quote
			end := i + 1
			for end < len(sql) && sql[end] != '"' {
				end++
			}

			if end < len(sql) {
				identifier := sql[i+1 : end]

				// Check if this is a string value context (after = or DEFAULT)
				// by looking at preceding non-whitespace characters
				isValueContext := false
				for j := i - 1; j >= 0; j-- {
					if sql[j] == ' ' || sql[j] == '\t' || sql[j] == '\n' || sql[j] == '\r' {
						continue
					}
					// Check for = sign or end of DEFAULT keyword
					if sql[j] == '=' {
						isValueContext = true
					} else if j >= 6 {
						preceding := strings.ToUpper(sql[j-6 : j+1])
						if strings.HasSuffix(preceding, "DEFAULT") {
							isValueContext = true
						}
					}
					break
				}

				// Empty string or value context - convert to single quotes
				if identifier == "" || isValueContext {
					result.WriteByte('\'')
					result.WriteString(identifier)
					result.WriteByte('\'')
				} else {
					// Identifier - convert to backticks
					result.WriteByte('`')
					result.WriteString(identifier)
					result.WriteByte('`')
				}
				i = end + 1
				continue
			}
		}

		result.WriteByte(c)
		i++
	}

	return result.String()
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
