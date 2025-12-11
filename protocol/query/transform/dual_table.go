package transform

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// DualTableRule removes FROM dual from SELECT statements.
//
// MySQL uses the "dual" table as a dummy table for evaluating expressions
// without querying actual tables (e.g., SELECT 1 FROM dual, SELECT @@version FROM dual).
// SQLite doesn't need a FROM clause for such queries - it can evaluate expressions directly.
//
// This rule transforms:
//
//	SELECT 1 FROM dual           -> SELECT 1
//	SELECT @@version FROM dual   -> SELECT @@version
//	SELECT NOW() FROM dual       -> SELECT NOW()
type DualTableRule struct{}

func (r *DualTableRule) Name() string {
	return "DualTable"
}

func (r *DualTableRule) Priority() int {
	return 1
}

func (r *DualTableRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	if len(sel.From) != 1 {
		return nil, ErrRuleNotApplicable
	}

	aliased, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	tableName, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	if !strings.EqualFold(tableName.Name.String(), "dual") {
		return nil, ErrRuleNotApplicable
	}

	// Remove FROM dual - SQLite doesn't need it
	sel.From = nil

	sql := serializer.Serialize(sel)
	return []TranspiledStatement{{SQL: sql, Params: params}}, nil
}
