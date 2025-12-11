//go:build sqlite_preupdate_hook

package transform

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

// SQLCalcFoundRowsRule transforms MySQL SQL_CALC_FOUND_ROWS hint for SQLite compatibility.
//
// Key transformations:
//   - Strips SQL_CALC_FOUND_ROWS hint from SELECT statement
//   - Appends COUNT(*) OVER() AS __marmot_found_rows to the column list
//
// This enables FOUND_ROWS() support by:
//  1. Transpiler: adds window function to calculate total rows
//  2. Executor: extracts __marmot_found_rows value, stores in session
//  3. FOUND_ROWS() handler: returns stored value from session
//
// The rule is stateless and thread-safe.
type SQLCalcFoundRowsRule struct{}

func (r *SQLCalcFoundRowsRule) Name() string {
	return "SQLCalcFoundRows"
}

func (r *SQLCalcFoundRowsRule) Priority() int {
	return 10
}

func (r *SQLCalcFoundRowsRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	if !sel.SQLCalcFoundRows {
		return nil, ErrRuleNotApplicable
	}

	sel.SQLCalcFoundRows = false

	countOverExpr := &sqlparser.AliasedExpr{
		Expr: &sqlparser.CountStar{
			OverClause: &sqlparser.OverClause{
				WindowSpec: &sqlparser.WindowSpecification{},
			},
		},
		As: sqlparser.NewIdentifierCI(FoundRowsColumnName),
	}

	sel.SelectExprs.Exprs = append(sel.SelectExprs.Exprs, countOverExpr)

	sql := serializer.Serialize(sel)
	return []TranspiledStatement{{SQL: sql, Params: params}}, nil
}
