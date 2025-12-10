package rules

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type InsertOnDuplicateKeyRule struct{}

func (r *InsertOnDuplicateKeyRule) Name() string  { return "InsertOnDuplicateKey" }
func (r *InsertOnDuplicateKeyRule) Priority() int { return 6 }

func (r *InsertOnDuplicateKeyRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *InsertOnDuplicateKeyRule) ApplyPattern(sql string) (string, bool, error) {
	stmt, err := sqlparser.NewTestParser().Parse(sql)
	if err != nil {
		return sql, false, nil
	}

	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return sql, false, nil
	}

	if len(insert.OnDup) == 0 {
		return sql, false, nil
	}

	conflictColumn, err := r.determineConflictColumn(insert)
	if err != nil {
		return sql, false, err
	}

	transformedUpdates, err := r.transformUpdateExprs(insert.OnDup)
	if err != nil {
		return sql, false, err
	}

	sqliteSQL := r.buildSQLiteUpsert(insert, conflictColumn, transformedUpdates)

	return sqliteSQL, true, nil
}

func (r *InsertOnDuplicateKeyRule) determineConflictColumn(insert *sqlparser.Insert) (string, error) {
	if len(insert.Columns) == 0 {
		return "", fmt.Errorf("INSERT statement has no columns")
	}

	return insert.Columns[0].String(), nil
}

func (r *InsertOnDuplicateKeyRule) transformUpdateExprs(onDup sqlparser.OnDup) ([]string, error) {
	var result []string

	for _, updateExpr := range onDup {
		colName := updateExpr.Name.Name.String()
		transformedExpr := r.transformValuesFunction(updateExpr.Expr)
		result = append(result, fmt.Sprintf("%s = %s", colName, sqlparser.String(transformedExpr)))
	}

	return result, nil
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

func (r *InsertOnDuplicateKeyRule) buildSQLiteUpsert(insert *sqlparser.Insert, conflictColumn string, updates []string) string {
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(sqlparser.String(insert.Table))
	sb.WriteString(" (")
	for i, col := range insert.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.String())
	}
	sb.WriteString(") VALUES ")

	rows, ok := insert.Rows.(sqlparser.Values)
	if ok {
		for i, row := range rows {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(")
			for j, val := range row {
				if j > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(sqlparser.String(val))
			}
			sb.WriteString(")")
		}
	}

	sb.WriteString(" ON CONFLICT (")
	sb.WriteString(conflictColumn)
	sb.WriteString(") DO UPDATE SET ")
	sb.WriteString(strings.Join(updates, ", "))

	return sb.String()
}
