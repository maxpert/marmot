package rules

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// AutoIncrementZeroRule converts INSERT ... (id, ...) VALUES (0, ...) to use NULL
// for the id column, matching MySQL's auto-increment behavior where 0 means
// "use the next auto-increment value"
type AutoIncrementZeroRule struct{}

func (r *AutoIncrementZeroRule) Name() string  { return "AutoIncrementZero" }
func (r *AutoIncrementZeroRule) Priority() int { return 5 } // Run early

func (r *AutoIncrementZeroRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return stmt, false, nil
	}

	// Check if we have explicit columns
	if len(insert.Columns) == 0 {
		return stmt, false, nil
	}

	// Find the "id" column index (case-insensitive)
	idColIdx := -1
	for i, col := range insert.Columns {
		if strings.EqualFold(col.String(), "id") {
			idColIdx = i
			break
		}
	}

	if idColIdx == -1 {
		return stmt, false, nil
	}

	// Get the VALUES rows
	rows, ok := insert.Rows.(sqlparser.Values)
	if !ok {
		return stmt, false, nil
	}

	modified := false
	for _, row := range rows {
		if idColIdx >= len(row) {
			continue
		}

		// Check if the value is a literal 0
		if isZeroLiteral(row[idColIdx]) {
			// Replace with NULL
			row[idColIdx] = &sqlparser.NullVal{}
			modified = true
		}
	}

	if !modified {
		return stmt, false, nil
	}

	return insert, true, nil
}

func (r *AutoIncrementZeroRule) ApplyPattern(sql string) (string, bool, error) {
	return sql, false, nil
}

// isZeroLiteral checks if an expression is a literal 0
func isZeroLiteral(expr sqlparser.Expr) bool {
	lit, ok := expr.(*sqlparser.Literal)
	if !ok {
		return false
	}

	// Check for integer 0
	if lit.Type == sqlparser.IntVal && string(lit.Val) == "0" {
		return true
	}

	return false
}
