package rules

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type UpdateWithJoinRule struct{}

func (r *UpdateWithJoinRule) Name() string  { return "UpdateWithJoin" }
func (r *UpdateWithJoinRule) Priority() int { return 13 }

func (r *UpdateWithJoinRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *UpdateWithJoinRule) ApplyPattern(sql string) (string, bool, error) {
	stmt, err := sqlparser.NewTestParser().Parse(sql)
	if err != nil {
		return sql, false, nil
	}

	upd, ok := stmt.(*sqlparser.Update)
	if !ok {
		return sql, false, nil
	}

	if !r.hasJoin(upd.TableExprs) {
		return sql, false, nil
	}

	if len(upd.TableExprs) > 1 && r.isMultiTableUpdate(upd.TableExprs) {
		return sql, false, fmt.Errorf("multi-table UPDATE not supported: please use separate UPDATE statements for each table")
	}

	targetTable, targetAlias, err := r.extractTargetTable(upd)
	if err != nil {
		return sql, false, err
	}

	setClause, err := r.buildSetClause(upd.Exprs, targetTable, targetAlias, upd.TableExprs, upd.Where)
	if err != nil {
		return sql, false, err
	}

	whereInClause := r.buildWhereInClause(targetAlias, upd.TableExprs, upd.Where, upd.OrderBy, upd.Limit)

	sqliteSQL := fmt.Sprintf("UPDATE %s SET %s WHERE rowid IN (%s)", targetTable, setClause, whereInClause)

	return sqliteSQL, true, nil
}

func (r *UpdateWithJoinRule) hasJoin(tableExprs sqlparser.TableExprs) bool {
	for _, expr := range tableExprs {
		if _, isJoin := expr.(*sqlparser.JoinTableExpr); isJoin {
			return true
		}
	}
	return false
}

func (r *UpdateWithJoinRule) isMultiTableUpdate(tableExprs sqlparser.TableExprs) bool {
	count := 0
	for _, expr := range tableExprs {
		if _, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			count++
		}
	}
	return count > 1
}

func (r *UpdateWithJoinRule) extractTargetTable(upd *sqlparser.Update) (string, string, error) {
	if len(upd.TableExprs) == 0 {
		return "", "", fmt.Errorf("UPDATE statement has no table expressions")
	}

	tableName, alias, found := r.findFirstTable(upd.TableExprs)
	if !found {
		return "", "", fmt.Errorf("unable to find target table in UPDATE statement")
	}

	if alias == "" {
		alias = tableName
	}

	return tableName, alias, nil
}

func (r *UpdateWithJoinRule) findFirstTable(tableExprs sqlparser.TableExprs) (string, string, bool) {
	for _, expr := range tableExprs {
		switch e := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			tableName := sqlparser.GetTableName(e.Expr).String()
			alias := e.As.String()
			if alias == "" {
				alias = tableName
			}
			return tableName, alias, true

		case *sqlparser.JoinTableExpr:
			if tableName, alias, found := r.searchTableExpr(e.LeftExpr); found {
				return tableName, alias, true
			}
		}
	}
	return "", "", false
}

func (r *UpdateWithJoinRule) searchTableExpr(expr sqlparser.TableExpr) (string, string, bool) {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := sqlparser.GetTableName(e.Expr).String()
		alias := e.As.String()
		if alias == "" {
			alias = tableName
		}
		return tableName, alias, true

	case *sqlparser.JoinTableExpr:
		if tableName, alias, found := r.searchTableExpr(e.LeftExpr); found {
			return tableName, alias, true
		}
		if tableName, alias, found := r.searchTableExpr(e.RightExpr); found {
			return tableName, alias, true
		}
	}

	return "", "", false
}

func (r *UpdateWithJoinRule) buildSetClause(exprs sqlparser.UpdateExprs, targetTable, targetAlias string, tableExprs sqlparser.TableExprs, where *sqlparser.Where) (string, error) {
	var setParts []string

	for _, expr := range exprs {
		colName := expr.Name.Name.String()

		if r.needsSubquery(expr.Expr, targetAlias) {
			subquery := r.buildCorrelatedSubquery(expr.Expr, tableExprs, where, targetAlias)
			setParts = append(setParts, fmt.Sprintf("%s = (%s)", colName, subquery))
		} else {
			setParts = append(setParts, fmt.Sprintf("%s = %s", colName, sqlparser.String(expr.Expr)))
		}
	}

	return strings.Join(setParts, ", "), nil
}

func (r *UpdateWithJoinRule) needsSubquery(expr sqlparser.Expr, targetAlias string) bool {
	hasOtherTable := false

	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if col, ok := node.(*sqlparser.ColName); ok {
			qualifier := col.Qualifier.Name.String()
			if qualifier != "" && qualifier != targetAlias {
				hasOtherTable = true
				return false, nil
			}
		}
		return true, nil
	}, expr)

	return hasOtherTable
}

func (r *UpdateWithJoinRule) buildCorrelatedSubquery(expr sqlparser.Expr, tableExprs sqlparser.TableExprs, where *sqlparser.Where, targetAlias string) string {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(sqlparser.String(expr))
	sb.WriteString(" FROM ")

	for i, tableExpr := range tableExprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sqlparser.String(tableExpr))
	}

	sb.WriteString(" WHERE ")

	if where != nil {
		sb.WriteString(sqlparser.String(where.Expr))
		sb.WriteString(" AND ")
	}

	targetTable, _, _ := r.extractTargetTable(&sqlparser.Update{TableExprs: tableExprs})
	sb.WriteString(targetAlias)
	sb.WriteString(".rowid = ")
	sb.WriteString(targetTable)
	sb.WriteString(".rowid")

	sb.WriteString(" LIMIT 1")

	return sb.String()
}

func (r *UpdateWithJoinRule) buildWhereInClause(targetAlias string, tableExprs sqlparser.TableExprs, where *sqlparser.Where, orderBy sqlparser.OrderBy, limit *sqlparser.Limit) string {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(targetAlias)
	sb.WriteString(".rowid FROM ")

	for i, tableExpr := range tableExprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sqlparser.String(tableExpr))
	}

	if where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(sqlparser.String(where.Expr))
	}

	if orderBy != nil {
		sb.WriteString(" ")
		sb.WriteString(sqlparser.String(orderBy))
	}

	if limit != nil {
		sb.WriteString(" ")
		sb.WriteString(sqlparser.String(limit))
	}

	return sb.String()
}
