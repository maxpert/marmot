package transform

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// UpdateJoinRule converts UPDATE with JOIN to correlated subquery form.
//
// MySQL allows UPDATE with JOIN:
//
//	UPDATE t1 JOIN t2 ON t1.id = t2.ref_id SET t1.name = t2.name WHERE t2.active = 1
//
// SQLite requires:
//
//	UPDATE t1 SET name = (
//	  SELECT t2.name FROM t1 AS t1_sub JOIN t2 ON t1_sub.id = t2.ref_id
//	  WHERE t2.active = 1 AND t1_sub.rowid = t1.rowid LIMIT 1
//	) WHERE rowid IN (
//	  SELECT t1.rowid FROM t1 JOIN t2 ON t1.id = t2.ref_id WHERE t2.active = 1
//	)
//
// Multi-table UPDATE is not supported and returns an error.
type UpdateJoinRule struct{}

func (r *UpdateJoinRule) Name() string {
	return "UpdateJoin"
}

func (r *UpdateJoinRule) Priority() int {
	return 40
}

func (r *UpdateJoinRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	upd, ok := stmt.(*sqlparser.Update)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	if !HasJoin(upd.TableExprs) {
		return nil, ErrRuleNotApplicable
	}

	if len(upd.TableExprs) > 1 && r.isMultiTableUpdate(upd.TableExprs) {
		return nil, fmt.Errorf("multi-table UPDATE not supported: use separate UPDATE statements for each table")
	}

	targetTable, targetAlias, err := r.extractTargetTable(upd)
	if err != nil {
		return nil, fmt.Errorf("failed to extract target table: %w", err)
	}

	newExprs := r.buildSetExpressions(upd.Exprs, targetTable, targetAlias, upd.TableExprs, upd.Where)
	whereInClause := r.buildWhereInClause(targetAlias, upd.TableExprs, upd.Where, upd.OrderBy, upd.Limit)

	// MUTATE upd in place instead of creating new object
	upd.TableExprs = sqlparser.TableExprs{
		&sqlparser.AliasedTableExpr{
			Expr: sqlparser.TableName{
				Name: sqlparser.NewIdentifierCS(targetTable),
			},
		},
	}
	upd.Exprs = newExprs
	upd.Where = &sqlparser.Where{
		Type: sqlparser.WhereClause,
		Expr: &sqlparser.ComparisonExpr{
			Operator: sqlparser.InOp,
			Left: &sqlparser.ColName{
				Name: sqlparser.NewIdentifierCI("rowid"),
			},
			Right: whereInClause,
		},
	}
	upd.OrderBy = nil
	upd.Limit = nil

	// Serialize
	sql := serializer.Serialize(upd)

	return []TranspiledStatement{{SQL: sql, Params: params}}, nil
}

func (r *UpdateJoinRule) isMultiTableUpdate(tableExprs sqlparser.TableExprs) bool {
	count := 0
	for _, expr := range tableExprs {
		if _, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			count++
		}
	}
	return count > 1
}

func (r *UpdateJoinRule) extractTargetTable(upd *sqlparser.Update) (string, string, error) {
	if len(upd.TableExprs) == 0 {
		return "", "", fmt.Errorf("UPDATE has no table expressions")
	}

	tableName, alias, found := r.findFirstTable(upd.TableExprs)
	if !found {
		return "", "", fmt.Errorf("unable to find target table in UPDATE")
	}

	if alias == "" {
		alias = tableName
	}

	return tableName, alias, nil
}

func (r *UpdateJoinRule) findFirstTable(tableExprs sqlparser.TableExprs) (string, string, bool) {
	tableName, alias, err := FindTableByAlias(tableExprs, "")
	if err != nil {
		return "", "", false
	}
	return tableName, alias, true
}

func (r *UpdateJoinRule) buildSetExpressions(exprs sqlparser.UpdateExprs, targetTable, targetAlias string, tableExprs sqlparser.TableExprs, where *sqlparser.Where) sqlparser.UpdateExprs {
	var newExprs sqlparser.UpdateExprs

	for _, expr := range exprs {
		if r.needsSubquery(expr.Expr, targetAlias) {
			subquery := r.buildCorrelatedSubquery(expr.Expr, tableExprs, where, targetAlias, targetTable)
			newExprs = append(newExprs, &sqlparser.UpdateExpr{
				Name: expr.Name,
				Expr: subquery,
			})
		} else {
			newExprs = append(newExprs, expr)
		}
	}

	return newExprs
}

func (r *UpdateJoinRule) needsSubquery(expr sqlparser.Expr, targetAlias string) bool {
	hasOtherTable := false

	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if col, ok := node.(*sqlparser.ColName); ok {
			qualifier := col.Qualifier.Name.String()
			if qualifier != "" && !strings.EqualFold(qualifier, targetAlias) {
				hasOtherTable = true
				return false, nil
			}
		}
		return true, nil
	}, expr)

	return hasOtherTable
}

func (r *UpdateJoinRule) buildCorrelatedSubquery(expr sqlparser.Expr, tableExprs sqlparser.TableExprs, where *sqlparser.Where, targetAlias, targetTable string) *sqlparser.Subquery {
	var whereExpr sqlparser.Expr

	rowidCondition := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left: &sqlparser.ColName{
			Qualifier: sqlparser.TableName{
				Name: sqlparser.NewIdentifierCS(targetAlias),
			},
			Name: sqlparser.NewIdentifierCI("rowid"),
		},
		Right: &sqlparser.ColName{
			Qualifier: sqlparser.TableName{
				Name: sqlparser.NewIdentifierCS(targetTable),
			},
			Name: sqlparser.NewIdentifierCI("rowid"),
		},
	}

	if where != nil {
		whereExpr = &sqlparser.AndExpr{
			Left:  where.Expr,
			Right: rowidCondition,
		}
	} else {
		whereExpr = rowidCondition
	}

	sel := &sqlparser.Select{
		SelectExprs: &sqlparser.SelectExprs{
			Exprs: []sqlparser.SelectExpr{
				&sqlparser.AliasedExpr{
					Expr: expr,
				},
			},
		},
		From: tableExprs,
		Where: &sqlparser.Where{
			Type: sqlparser.WhereClause,
			Expr: whereExpr,
		},
		Limit: &sqlparser.Limit{
			Rowcount: sqlparser.NewIntLiteral("1"),
		},
	}

	return &sqlparser.Subquery{
		Select: sel,
	}
}

func (r *UpdateJoinRule) buildWhereInClause(targetAlias string, tableExprs sqlparser.TableExprs, where *sqlparser.Where, orderBy sqlparser.OrderBy, limit *sqlparser.Limit) *sqlparser.Subquery {
	sel := &sqlparser.Select{
		SelectExprs: &sqlparser.SelectExprs{
			Exprs: []sqlparser.SelectExpr{
				&sqlparser.AliasedExpr{
					Expr: &sqlparser.ColName{
						Qualifier: sqlparser.TableName{
							Name: sqlparser.NewIdentifierCS(targetAlias),
						},
						Name: sqlparser.NewIdentifierCI("rowid"),
					},
				},
			},
		},
		From:    tableExprs,
		Where:   where,
		OrderBy: orderBy,
		Limit:   limit,
	}

	return &sqlparser.Subquery{
		Select: sel,
	}
}
