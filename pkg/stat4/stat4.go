// Package stat4 provides selectivity estimation for cost-based query planning.
// It mirrors the cardinality-estimation rules in design §7.1, using Vitess AST
// exclusively — no string matching or regex on SQL text.
package stat4

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

// Histogram is a placeholder for SQLite stat4-derived histograms.
// For P2 it is an opaque struct with only the TotalRows field populated;
// a future phase wires real stat4 bucket loading. Zero value means "no stats".
type Histogram struct {
	TotalRows int64
}

// EstimateCardinality estimates how many rows satisfy the predicate expression
// against the total row count. Rules per design §7.1:
//   - Comparison col = literal          → TotalRows/10  (if Histogram has no buckets)
//   - col IN (v1..vn)                   → min(total, n * 0.1 * total)
//   - AND(all equality)                 → max(estimates)
//   - AND(mixed)                        → product(estimates) / total^(n-1)
//   - OR(a, b)                          → min(total, a+b)
//   - default                           → total/2
//
// A nil Histogram uses only TotalRows guidance; zero total returns 0.
func EstimateCardinality(expr sqlparser.Expr, total int64, h *Histogram) int64 {
	if total == 0 {
		return 0
	}
	if expr == nil {
		return total / 2
	}
	return estimate(expr, total)
}

func estimate(expr sqlparser.Expr, total int64) int64 {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		return estimateAnd(e, total)
	case *sqlparser.OrExpr:
		return estimateOr(e, total)
	case *sqlparser.ComparisonExpr:
		return estimateComparison(e, total)
	default:
		return total / 2
	}
}

// estimateComparison handles = and IN operators.
func estimateComparison(e *sqlparser.ComparisonExpr, total int64) int64 {
	switch e.Operator {
	case sqlparser.EqualOp:
		// col = literal → 10% selectivity (no bucket data).
		return atLeast1(total / 10)
	case sqlparser.InOp:
		// col IN (v1..vn) → n * 0.1 * total, capped at total.
		if vt, ok := e.Right.(sqlparser.ValTuple); ok {
			n := int64(len(vt))
			est := int64(float64(n) * 0.1 * float64(total))
			return clamp(est, 1, total)
		}
		return total / 2
	default:
		return total / 2
	}
}

// estimateAnd applies the AND rules from §7.1.
// If all children are equality predicates → max(estimates).
// Otherwise → product(estimates) / total^(n-1).
func estimateAnd(e *sqlparser.AndExpr, total int64) int64 {
	children := flattenAnd(e)
	estimates := make([]int64, 0, len(children))
	allEquality := true
	for _, child := range children {
		estimates = append(estimates, estimate(child, total))
		if !isEqualityComparison(child) {
			allEquality = false
		}
	}

	if allEquality {
		return maxSlice(estimates)
	}

	// Independence assumption: product / total^(n-1).
	product := int64(1)
	for _, est := range estimates {
		product = safeMul(product, est)
	}
	divisor := pow64(total, int64(len(estimates)-1))
	if divisor <= 0 {
		return atLeast1(total / 2)
	}
	return clamp(product/divisor, 1, total)
}

// estimateOr: min(total, a+b).
func estimateOr(e *sqlparser.OrExpr, total int64) int64 {
	left := estimate(e.Left, total)
	right := estimate(e.Right, total)
	sum := left + right
	if sum > total {
		return total
	}
	return sum
}

// flattenAnd recursively collects all leaf expressions from a tree of AndExprs.
func flattenAnd(e *sqlparser.AndExpr) []sqlparser.Expr {
	var out []sqlparser.Expr
	if left, ok := e.Left.(*sqlparser.AndExpr); ok {
		out = append(out, flattenAnd(left)...)
	} else {
		out = append(out, e.Left)
	}
	if right, ok := e.Right.(*sqlparser.AndExpr); ok {
		out = append(out, flattenAnd(right)...)
	} else {
		out = append(out, e.Right)
	}
	return out
}

// isEqualityComparison returns true when expr is a col = literal comparison.
func isEqualityComparison(expr sqlparser.Expr) bool {
	c, ok := expr.(*sqlparser.ComparisonExpr)
	return ok && c.Operator == sqlparser.EqualOp
}

func atLeast1(n int64) int64 {
	if n < 1 {
		return 1
	}
	return n
}

func clamp(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func maxSlice(s []int64) int64 {
	if len(s) == 0 {
		return 0
	}
	m := s[0]
	for _, v := range s[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

// safeMul multiplies a and b, clamping on overflow to avoid wrapping.
func safeMul(a, b int64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	result := a * b
	// Detect overflow: if result/a != b the multiplication wrapped.
	if result/a != b {
		// Return a large sentinel; callers divide this by total so the result
		// will be clamped to total anyway.
		return 1<<62 - 1
	}
	return result
}

// pow64 computes base^exp for non-negative exp.
func pow64(base, exp int64) int64 {
	if exp == 0 {
		return 1
	}
	result := int64(1)
	for i := int64(0); i < exp; i++ {
		result = safeMul(result, base)
	}
	return result
}
