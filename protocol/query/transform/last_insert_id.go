package transform

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

// LastInsertIDRule transforms MySQL LAST_INSERT_ID() to SQLite's last_insert_rowid().
type LastInsertIDRule struct{}

func (r *LastInsertIDRule) Name() string {
	return "LastInsertID"
}

func (r *LastInsertIDRule) Priority() int {
	return 5 // Run early
}

func (r *LastInsertIDRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	modified := false

	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		funcExpr, ok := cursor.Node().(*sqlparser.FuncExpr)
		if !ok {
			return true
		}

		funcName := funcExpr.Name.Lowered()
		if funcName != "last_insert_id" {
			return true
		}

		// Replace LAST_INSERT_ID() with last_insert_rowid()
		cursor.Replace(&sqlparser.FuncExpr{
			Name: sqlparser.NewIdentifierCI("last_insert_rowid"),
		})
		modified = true
		return true
	}, nil)

	if !modified {
		return nil, ErrRuleNotApplicable
	}

	sql := serializer.Serialize(stmt)
	return []TranspiledStatement{{SQL: sql, Params: params}}, nil
}
