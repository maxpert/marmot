package rules

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type DeleteWithJoinRule struct{}

func (r *DeleteWithJoinRule) Name() string  { return "DeleteWithJoin" }
func (r *DeleteWithJoinRule) Priority() int { return 12 }

func (r *DeleteWithJoinRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *DeleteWithJoinRule) ApplyPattern(sql string) (string, bool, error) {
	stmt, err := sqlparser.NewTestParser().Parse(sql)
	if err != nil {
		return sql, false, nil
	}

	del, ok := stmt.(*sqlparser.Delete)
	if !ok {
		return sql, false, nil
	}

	if !r.hasJoin(del.TableExprs) {
		return sql, false, nil
	}

	if len(del.Targets) > 1 {
		return sql, false, fmt.Errorf("multi-table DELETE not supported: please use separate DELETE statements for each table")
	}

	targetTable, targetAlias, err := r.extractTargetTable(del)
	if err != nil {
		return sql, false, err
	}

	subquerySQL := r.buildSubquerySQL(del, targetAlias)
	sqliteSQL := fmt.Sprintf("DELETE FROM %s WHERE rowid IN (%s)", targetTable, subquerySQL)

	return sqliteSQL, true, nil
}

func (r *DeleteWithJoinRule) hasJoin(tableExprs sqlparser.TableExprs) bool {
	for _, expr := range tableExprs {
		if _, isJoin := expr.(*sqlparser.JoinTableExpr); isJoin {
			return true
		}
	}
	return false
}

func (r *DeleteWithJoinRule) extractTargetTable(del *sqlparser.Delete) (string, string, error) {
	if len(del.TableExprs) == 0 {
		return "", "", fmt.Errorf("DELETE statement has no table expressions")
	}

	var targetAlias string
	if len(del.Targets) == 1 {
		targetAlias = del.Targets[0].Name.String()
	}

	tableName, alias, err := r.findTableByAlias(del.TableExprs, targetAlias)
	if err != nil {
		return "", "", err
	}

	if alias == "" {
		alias = tableName
	}

	return tableName, alias, nil
}

func (r *DeleteWithJoinRule) findTableByAlias(tableExprs sqlparser.TableExprs, targetAlias string) (string, string, error) {
	for _, expr := range tableExprs {
		tableName, alias, found := r.searchTableExpr(expr, targetAlias)
		if found {
			return tableName, alias, nil
		}
	}

	return "", "", fmt.Errorf("unable to find table for alias %s", targetAlias)
}

func (r *DeleteWithJoinRule) searchTableExpr(expr sqlparser.TableExpr, targetAlias string) (string, string, bool) {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := sqlparser.GetTableName(e.Expr).String()
		alias := e.As.String()
		if alias == "" {
			alias = tableName
		}

		if targetAlias == "" || targetAlias == alias {
			return tableName, alias, true
		}

	case *sqlparser.JoinTableExpr:
		if tableName, alias, found := r.searchTableExpr(e.LeftExpr, targetAlias); found {
			return tableName, alias, true
		}
		if tableName, alias, found := r.searchTableExpr(e.RightExpr, targetAlias); found {
			return tableName, alias, true
		}
	}

	return "", "", false
}

func (r *DeleteWithJoinRule) buildSubquerySQL(del *sqlparser.Delete, targetAlias string) string {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(targetAlias)
	sb.WriteString(".rowid FROM ")

	for i, tableExpr := range del.TableExprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sqlparser.String(tableExpr))
	}

	if del.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(sqlparser.String(del.Where.Expr))
	}

	if del.OrderBy != nil {
		sb.WriteString(" ")
		sb.WriteString(sqlparser.String(del.OrderBy))
	}

	if del.Limit != nil {
		sb.WriteString(" ")
		sb.WriteString(sqlparser.String(del.Limit))
	}

	return sb.String()
}
