package protocol

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// VecSessionVarUpdate holds a single @@marmot_vec_* variable name/value pair
// extracted from a SET statement via the Vitess AST.
type VecSessionVarUpdate struct {
	Name  string // variable name without @@ prefix, e.g. "marmot_vec_nprobe"
	Value string // string representation of the literal value
}

// ExtractVecSessionVarUpdates parses sql via the Vitess AST and returns all
// @@marmot_vec_* session variable assignments it contains. Non-vec SET vars
// are silently ignored. Returns nil, nil for non-SET statements.
//
// Value extraction is strict: only integer, float, and string literals are
// accepted. Other expression types return an error so callers can surface a
// clear message rather than silently ignoring the assignment.
func ExtractVecSessionVarUpdates(sql string) ([]VecSessionVarUpdate, error) {
	parsed, err := vitessParser.Parse(sql)
	if err != nil {
		// Not parseable by Vitess — not a SET statement, nothing to extract.
		return nil, nil
	}

	setStmt, ok := parsed.(*sqlparser.Set)
	if !ok {
		return nil, nil
	}

	var updates []VecSessionVarUpdate
	for _, expr := range setStmt.Exprs {
		if expr.Var == nil {
			continue
		}
		name := strings.ToLower(expr.Var.Name.Lowered())
		if !strings.HasPrefix(name, "marmot_vec_") {
			continue
		}
		val, err := literalString(expr.Expr)
		if err != nil {
			return nil, err
		}
		updates = append(updates, VecSessionVarUpdate{Name: name, Value: val})
	}
	return updates, nil
}

// ParseVitessAST parses sql via the shared Vitess parser and returns the AST.
// Used by the coordinator's vector-query rewriter (P2-B) to detect and rewrite
// pgvector-style vec_match/vec_distance queries without re-instantiating the
// parser. Returns the underlying parser error on failure.
func ParseVitessAST(sql string) (sqlparser.Statement, error) {
	return vitessParser.Parse(sql)
}

// literalString extracts the string representation of a Vitess literal expression.
// Returns an error for non-literal expressions (function calls, subqueries, etc.)
// so callers get a clear failure instead of a silent no-op.
func literalString(expr sqlparser.Expr) (string, error) {
	lit, ok := expr.(*sqlparser.Literal)
	if !ok {
		return "", fmt.Errorf("@@marmot_vec_* value must be a literal (integer, float, or string), got %T", expr)
	}
	return lit.Val, nil
}
