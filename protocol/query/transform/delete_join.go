package transform

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

// DeleteJoinRule converts DELETE with JOIN to rowid-based subquery form.
//
// MySQL allows DELETE with JOIN:
//
//	DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.ref_id WHERE t2.status = 'old'
//
// SQLite requires:
//
//	DELETE FROM t1 WHERE rowid IN (
//	  SELECT t1.rowid FROM t1 JOIN t2 ON t1.id = t2.ref_id WHERE t2.status = 'old'
//	)
//
// Multi-table DELETE (DELETE a, b FROM ...) is transformed into separate DELETE statements:
//
//	DELETE FROM table WHERE rowid IN (SELECT a.rowid FROM ...)
//	DELETE FROM table WHERE rowid IN (SELECT b.rowid FROM ...)
type DeleteJoinRule struct{}

func (r *DeleteJoinRule) Name() string {
	return "DeleteJoin"
}

func (r *DeleteJoinRule) Priority() int {
	return 30
}

func (r *DeleteJoinRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	del, ok := stmt.(*sqlparser.Delete)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	// Handle multi-table DELETE (DELETE a, b FROM ...)
	if len(del.Targets) > 1 {
		return r.transformMultiTableDelete(del, params, serializer)
	}

	// Check for comma-separated tables without JOINs (DELETE FROM t1, t2 WHERE ... or DELETE t1 USING t1, t2 WHERE ...)
	// SQLite doesn't support this syntax
	if len(del.TableExprs) > 1 && !HasJoin(del.TableExprs) {
		return nil, fmt.Errorf("DELETE from multiple tables not supported: use separate DELETE statements")
	}

	if !HasJoin(del.TableExprs) && len(del.Targets) <= 1 {
		return nil, ErrRuleNotApplicable
	}

	// Single table DELETE with JOIN - MUTATE in place
	targetTable, targetAlias, err := r.extractTargetTable(del)
	if err != nil {
		return nil, fmt.Errorf("failed to extract target table: %w", err)
	}

	subquery := r.buildRowidSubquery(del.TableExprs, del.Where, del.OrderBy, del.Limit, targetAlias)

	// MUTATE del in place instead of creating new object
	del.TableExprs = sqlparser.TableExprs{
		&sqlparser.AliasedTableExpr{
			Expr: sqlparser.TableName{
				Name: sqlparser.NewIdentifierCS(targetTable),
			},
		},
	}
	del.Where = &sqlparser.Where{
		Type: sqlparser.WhereClause,
		Expr: &sqlparser.ComparisonExpr{
			Operator: sqlparser.InOp,
			Left:     &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("rowid")},
			Right:    subquery,
		},
	}
	del.Targets = nil
	del.OrderBy = nil
	del.Limit = nil

	// Serialize using provided serializer
	sql := serializer.Serialize(del)

	// Same params for the subquery WHERE clause
	return []TranspiledStatement{{SQL: sql, Params: params}}, nil
}

func (r *DeleteJoinRule) transformMultiTableDelete(del *sqlparser.Delete, params []interface{}, serializer Serializer) ([]TranspiledStatement, error) {
	var results []TranspiledStatement

	// Save original TableExprs and Where for reuse in subqueries
	originalTableExprs := del.TableExprs
	originalWhere := del.Where

	for _, target := range del.Targets {
		targetAlias := target.Name.String()
		targetTable, _, err := FindTableByAlias(originalTableExprs, targetAlias)
		if err != nil {
			return nil, fmt.Errorf("failed to find table for target %q: %w", targetAlias, err)
		}

		// Build subquery for this target
		subquery := r.buildRowidSubquery(originalTableExprs, originalWhere, del.OrderBy, del.Limit, targetAlias)

		// Create DELETE statement
		deleteStmt := &sqlparser.Delete{
			TableExprs: sqlparser.TableExprs{
				&sqlparser.AliasedTableExpr{
					Expr: sqlparser.TableName{Name: sqlparser.NewIdentifierCS(targetTable)},
				},
			},
			Where: &sqlparser.Where{
				Type: sqlparser.WhereClause,
				Expr: &sqlparser.ComparisonExpr{
					Operator: sqlparser.InOp,
					Left:     &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("rowid")},
					Right:    subquery,
				},
			},
		}

		sql := serializer.Serialize(deleteStmt)
		// All DELETE statements share the same params (for WHERE clause)
		results = append(results, TranspiledStatement{SQL: sql, Params: params})
	}

	return results, nil
}

func (r *DeleteJoinRule) extractTargetTable(del *sqlparser.Delete) (string, string, error) {
	if len(del.TableExprs) == 0 {
		return "", "", fmt.Errorf("DELETE has no table expressions")
	}

	var targetAlias string
	if len(del.Targets) == 1 {
		targetAlias = del.Targets[0].Name.String()
	}

	tableName, alias, err := FindTableByAlias(del.TableExprs, targetAlias)
	if err != nil {
		return "", "", err
	}

	if alias == "" {
		alias = tableName
	}

	return tableName, alias, nil
}

func (r *DeleteJoinRule) buildRowidSubquery(
	tableExprs sqlparser.TableExprs,
	where *sqlparser.Where,
	orderBy sqlparser.OrderBy,
	limit *sqlparser.Limit,
	targetAlias string,
) *sqlparser.Subquery {
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
