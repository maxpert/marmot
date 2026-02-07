package transform

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

// InsertOnDuplicateKeyRule transforms MySQL ON DUPLICATE KEY UPDATE to SQLite ON CONFLICT DO UPDATE.
//
// Key transformations:
//   - VALUES(col) references → excluded.col
//   - Conflict target: uses SchemaProvider to get PRIMARY KEY columns
//   - Falls back to first column in INSERT if schema unavailable
//
// The VALUES(col) → excluded.col transformation is performed during AST manipulation.
// This rule is stateless and thread-safe.
type InsertOnDuplicateKeyRule struct{}

func (r *InsertOnDuplicateKeyRule) Name() string {
	return "InsertOnDuplicateKey"
}

func (r *InsertOnDuplicateKeyRule) Priority() int {
	return 20
}

func (r *InsertOnDuplicateKeyRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	if len(insert.OnDup) == 0 {
		return nil, ErrRuleNotApplicable
	}

	tableName := sqlparser.GetTableName(insert.Table.Expr).String()
	conflictColumns, err := r.determineConflictTarget(insert, schema, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("insert on duplicate key: %w", err)
	}

	// Transform VALUES(col) → excluded.col in AST
	r.transformValuesExpressions(insert.OnDup)

	// Return metadata with conflict columns - let transpiler handle serialization
	return []TranspiledStatement{{
		SQL:    "",
		Params: params,
		Metadata: map[string]interface{}{
			"conflictColumns": conflictColumns,
		},
	}}, nil
}

func (r *InsertOnDuplicateKeyRule) determineConflictTarget(insert *sqlparser.Insert, schema SchemaProvider, database, tableName string) ([]string, error) {
	if schema != nil {
		if schemaInfo := schema(database, tableName); schemaInfo != nil {
			if conflictTarget := schemaInfo.ConflictTarget(); len(conflictTarget) > 0 {
				return conflictTarget, nil
			}
		}
	}

	if len(insert.Columns) > 0 {
		return []string{insert.Columns[0].String()}, nil
	}

	return nil, fmt.Errorf("cannot determine conflict target for table %s: no schema available and no columns specified in INSERT", tableName)
}

func (r *InsertOnDuplicateKeyRule) transformValuesExpressions(onDup sqlparser.OnDup) {
	for _, updateExpr := range onDup {
		updateExpr.Expr = r.transformValuesFunction(updateExpr.Expr)
	}
}

func (r *InsertOnDuplicateKeyRule) transformValuesFunction(expr sqlparser.Expr) sqlparser.Expr {
	switch e := expr.(type) {
	case *sqlparser.ValuesFuncExpr:
		colName := e.Name.Name.String()
		return &sqlparser.ColName{
			Qualifier: sqlparser.TableName{
				Name: sqlparser.NewIdentifierCS("excluded"),
			},
			Name: sqlparser.NewIdentifierCI(colName),
		}

	case *sqlparser.BinaryExpr:
		e.Left = r.transformValuesFunction(e.Left)
		e.Right = r.transformValuesFunction(e.Right)
		return e

	case *sqlparser.UnaryExpr:
		e.Expr = r.transformValuesFunction(e.Expr)
		return e

	default:
		return e
	}
}
