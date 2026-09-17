package rules

import (
	"strconv"
	"strings"

	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol/query/transform"
	"vitess.io/vitess/go/vt/sqlparser"
)

// SchemaLookup returns the schema facts an INSERT needs for id injection.
// It returns nil when the table is unknown.
type SchemaLookup func(table string) *transform.SchemaInfo

// AutoIncrementIDRule injects distributed IDs for auto-increment columns.
// Detection is based on the SQLite schema: a single INTEGER/BIGINT PRIMARY KEY.
//
// The governing principle is that SQLite must never assign a primary key,
// because two nodes assigning rowids independently both mint 1, 2, 3 and CDC
// replays the collision as INSERT OR REPLACE. The rule therefore substitutes a
// generated id wherever the id's position in the statement is knowable, and
// refuses the statement only where safe substitution is structurally
// impossible.
type AutoIncrementIDRule struct {
	generator id.Generator
}

// NewAutoIncrementIDRule creates a new rule with the given ID generator.
func NewAutoIncrementIDRule(gen id.Generator) *AutoIncrementIDRule {
	return &AutoIncrementIDRule{
		generator: gen,
	}
}

func (r *AutoIncrementIDRule) Name() string  { return "AutoIncrementID" }
func (r *AutoIncrementIDRule) Priority() int { return 5 }

// insertPlan is what analyzeInsert decided ApplyAST must do to an INSERT.
// Exactly one of appendColumn and valueIdx is meaningful: appendColumn names a
// column to add to an explicit column list (with one generated value per row),
// while valueIdx is the position in each VALUES tuple whose placeholder value
// is replaced by a generated id.
type insertPlan struct {
	insert       *sqlparser.Insert
	rows         sqlparser.Values
	appendColumn string
	valueIdx     int
}

// analyzeInsert decides what the rule must do with a statement, without
// mutating it. It returns (nil, nil) when there is nothing to do, a plan when
// the statement can be rewritten, and an error when the statement must be
// refused. NeedsIDInjection and ApplyAST both go through it so the transpiler's
// gate and the rewrite can never disagree.
func analyzeInsert(stmt sqlparser.Statement, schemaLookup SchemaLookup) (*insertPlan, error) {
	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return nil, nil
	}

	tableName := extractInsertTableName(insert)
	if tableName == "" {
		return nil, nil
	}

	info := schemaLookup(tableName)
	if !info.HasAutoIncrement() {
		return nil, nil
	}
	autoIncCol := info.AutoIncrementColumn
	colIdx := findColumnIndex(insert.Columns, autoIncCol)

	rows, isValues := insert.Rows.(sqlparser.Values)
	if !isValues {
		// The rows come from a query, not from literals this rule can rewrite:
		// sqlparser.InsertRows is only Values, *Select, *Union or
		// *ValuesStatement, and the last three all supply their values at
		// execution time. When the column list names the auto-increment
		// column the query supplies its values and the statement is left
		// alone; otherwise SQLite would assign the rowid itself, so the
		// statement is refused.
		// A table value constructor (VALUES ROW(...)) is refused in BOTH forms.
		// Its literals live inside a statement node this rule cannot index
		// positionally, so the column-listed form would otherwise pass the gate
		// with a NULL id and reach SQLite, which cannot parse VALUES ROW at all.
		if _, isRowConstructor := insert.Rows.(*sqlparser.ValuesStatement); isRowConstructor {
			return nil, transform.NewCodedError(transform.ErrCodeNotSupportedYet,
				"This version of MySQL doesn't yet support 'INSERT ... VALUES ROW(...) into table `%s`, which has AUTO_INCREMENT column `%s`'",
				tableName, autoIncCol)
		}
		if colIdx >= 0 {
			return nil, nil
		}
		return nil, transform.NewCodedError(transform.ErrCodeNotSupportedYet,
			"This version of MySQL doesn't yet support 'INSERT ... SELECT into table `%s` without a projected value for AUTO_INCREMENT column `%s`'",
			tableName, autoIncCol)
	}

	if colIdx >= 0 {
		// The column list names the auto-increment column: substitute wherever
		// a row left the value for the server to fill in.
		if rowsNeedID(rows, colIdx) {
			return &insertPlan{insert: insert, rows: rows, valueIdx: colIdx}, nil
		}
		return nil, nil
	}

	if len(insert.Columns) > 0 {
		// Explicit column list that omits the auto-increment column: add it,
		// with one generated id per row.
		return &insertPlan{insert: insert, rows: rows, appendColumn: autoIncCol, valueIdx: -1}, nil
	}

	// Column-less INSERT. Every column gets a value in the table's own order,
	// so the id sits at the auto-increment column's ordinal. A row of the
	// wrong length is SQLite's error to report, exactly as it is today, so
	// rows that cannot hold the ordinal are left untouched.
	ordinal := info.AutoIncrementOrdinal
	if ordinal < 0 {
		return nil, nil
	}
	if rowsNeedID(rows, ordinal) {
		return &insertPlan{insert: insert, rows: rows, valueIdx: ordinal}, nil
	}
	return nil, nil
}

// rowsNeedID reports whether any row leaves position idx for the server to
// fill in.
func rowsNeedID(rows sqlparser.Values, idx int) bool {
	for _, row := range rows {
		if idx < len(row) && needsIDInjection(row[idx]) {
			return true
		}
	}
	return false
}

// NeedsIDInjection reports whether the statement needs this rule to run. The
// transpiler uses it both to decide whether to call ApplyAST and to bypass its
// SQL cache, so a statement this rule would refuse must answer true as well.
func (r *AutoIncrementIDRule) NeedsIDInjection(stmt sqlparser.Statement, schemaLookup SchemaLookup) bool {
	if r.generator == nil || schemaLookup == nil {
		return false
	}
	plan, err := analyzeInsert(stmt, schemaLookup)
	return err != nil || plan != nil
}

// ApplyAST rewrites the statement in place, or returns an error when the
// statement must be refused. The error is returned to the caller verbatim; a
// *transform.CodedError carries the MySQL error code the client must see.
func (r *AutoIncrementIDRule) ApplyAST(stmt sqlparser.Statement, schemaLookup SchemaLookup) (sqlparser.Statement, bool, error) {
	if r.generator == nil || schemaLookup == nil {
		return stmt, false, nil
	}

	plan, err := analyzeInsert(stmt, schemaLookup)
	if err != nil {
		return stmt, false, err
	}
	if plan == nil {
		return stmt, false, nil
	}

	if plan.appendColumn != "" {
		plan.insert.Columns = append(plan.insert.Columns, sqlparser.NewIdentifierCI(plan.appendColumn))
		for i := range plan.rows {
			plan.rows[i] = append(plan.rows[i], r.newIDLiteral())
		}
		return plan.insert, true, nil
	}

	// analyzeInsert only returns a substitution plan when at least one row
	// needs one, so this loop always replaces something.
	for _, row := range plan.rows {
		if plan.valueIdx < len(row) && needsIDInjection(row[plan.valueIdx]) {
			row[plan.valueIdx] = r.newIDLiteral()
		}
	}
	return plan.insert, true, nil
}

// newIDLiteral mints one id and renders it as an integer literal.
func (r *AutoIncrementIDRule) newIDLiteral() *sqlparser.Literal {
	return sqlparser.NewIntLiteral(strconv.FormatUint(r.generator.NextID(), 10))
}

// extractInsertTableName extracts the table name from an INSERT statement.
func extractInsertTableName(insert *sqlparser.Insert) string {
	if insert.Table == nil {
		return ""
	}
	if tn, ok := insert.Table.Expr.(sqlparser.TableName); ok {
		return tn.Name.String()
	}
	return ""
}

// findColumnIndex returns the index of a column in the columns list, or -1 if not found.
func findColumnIndex(columns sqlparser.Columns, name string) int {
	nameLower := strings.ToLower(name)
	for i, col := range columns {
		if strings.ToLower(col.String()) == nameLower {
			return i
		}
	}
	return -1
}

// needsIDInjection reports whether an expression in the auto-increment
// column's position asks the server to supply the value. MySQL treats NULL,
// literal 0 and DEFAULT identically here.
func needsIDInjection(expr sqlparser.Expr) bool {
	switch v := expr.(type) {
	case *sqlparser.NullVal:
		return true
	case *sqlparser.Default:
		return true
	case *sqlparser.Literal:
		if v.Type == sqlparser.IntVal {
			return strings.TrimSpace(string(v.Val)) == "0"
		}
	}
	return false
}
