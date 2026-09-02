package transform

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// AlterTableConstraintRule rewrites MySQL index-DDL AlterOptions - "ALTER TABLE t ADD
// [CONSTRAINT name] UNIQUE (cols)" and plain "ALTER TABLE t ADD INDEX ... (cols)" - into
// SQLite-compatible standalone CREATE [UNIQUE] INDEX statements.
//
// Vitess also parses bare "CREATE [UNIQUE] INDEX name ON table (cols)" into this exact same
// *AlterTable/AddIndexDefinition AST shape (see create_index_prefix in sql.y: it builds an
// AlterTable node with a single AddIndexDefinition option). So this rule is the single path
// that turns every MySQL index-DDL form - ALTER ADD CONSTRAINT/ADD INDEX and standalone
// CREATE INDEX alike - into SQLite syntax.
//
// SQLite's ALTER TABLE only supports RENAME TABLE/COLUMN, ADD COLUMN, and DROP COLUMN - there
// is no equivalent to MySQL's ADD CONSTRAINT/ADD INDEX alter options, and SQLite has no
// "ALTER TABLE ... ADD INDEX" at all. Without this rule the default serializer emits invalid
// SQLite syntax (e.g. "alter table t add index idx (col)") which SQLite's PREPARE step rejects.
type AlterTableConstraintRule struct {
}

func (r *AlterTableConstraintRule) Name() string {
	return "AlterTableConstraint"
}

func (r *AlterTableConstraintRule) Priority() int {
	return 10
}

func (r *AlterTableConstraintRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	alter, ok := stmt.(*sqlparser.AlterTable)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	var indexAdds []*sqlparser.IndexDefinition
	var remaining []sqlparser.AlterOption
	for _, opt := range alter.AlterOptions {
		addIdx, ok := opt.(*sqlparser.AddIndexDefinition)
		if !ok || addIdx.IndexDefinition == nil || addIdx.IndexDefinition.Info == nil {
			remaining = append(remaining, opt)
			continue
		}

		switch addIdx.IndexDefinition.Info.Type {
		case sqlparser.IndexTypeUnique, sqlparser.IndexTypeDefault:
			indexAdds = append(indexAdds, addIdx.IndexDefinition)
		default:
			// FULLTEXT/SPATIAL indexes have no SQLite equivalent; leave them for the
			// default serializer - unrelated pre-existing limitation, not this rule's concern.
			remaining = append(remaining, opt)
		}
	}

	if len(indexAdds) == 0 {
		return nil, ErrRuleNotApplicable
	}

	tableName := alter.Table.Name.String()
	var results []TranspiledStatement

	// If other alter options remain (e.g. ADD COLUMN alongside ADD CONSTRAINT), keep them
	// in the ALTER TABLE statement; the default serializer already handles those correctly.
	if len(remaining) > 0 {
		alter.AlterOptions = remaining
		results = append(results, TranspiledStatement{SQL: serializer.Serialize(alter), Params: params})
	}

	for _, idx := range indexAdds {
		results = append(results, TranspiledStatement{SQL: buildIndexSQL(tableName, idx), Params: nil})
	}

	return results, nil
}

// buildIndexSQL generates a SQLite `CREATE [UNIQUE] INDEX "name" ON "table" (cols)` statement
// for an extracted ADD CONSTRAINT/ADD INDEX alter option or a standalone CREATE INDEX. Names
// are double-quoted since MySQL identifiers may contain characters (e.g. hyphens) that are not
// valid in unquoted SQLite identifiers.
func buildIndexSQL(tableName string, idx *sqlparser.IndexDefinition) string {
	unique := idx.Info.Type == sqlparser.IndexTypeUnique

	name := idx.Info.ConstraintName.String()
	if name == "" {
		name = idx.Info.Name.String()
	}
	if name == "" {
		name = generatedIndexName(tableName, idx.Columns, unique)
	}

	var sb strings.Builder
	sb.WriteString("CREATE ")
	if unique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	sb.WriteString(QuoteIdentifier(name))
	sb.WriteString(" ON ")
	sb.WriteString(QuoteIdentifier(tableName))
	sb.WriteString(" (")
	for i, col := range idx.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(QuoteIdentifier(col.Column.String()))
	}
	sb.WriteString(")")

	return sb.String()
}

// generatedIndexName mirrors MySQL's own convention for an unnamed index/constraint:
// derive a deterministic name from the table and column names.
func generatedIndexName(tableName string, columns []*sqlparser.IndexColumn, unique bool) string {
	cols := make([]string, len(columns))
	for i, c := range columns {
		cols[i] = c.Column.String()
	}
	suffix := "idx"
	if unique {
		suffix = "unique"
	}
	return tableName + "_" + strings.Join(cols, "_") + "_" + suffix
}

// QuoteIdentifier double-quotes a SQLite identifier, escaping embedded quotes. Exported so
// other packages that build SQLite DDL text outside the AST/transform pipeline (e.g. the
// query package's DROP INDEX pattern-based extraction, which Vitess cannot parse at all)
// can safely quote identifiers using the same convention.
func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
