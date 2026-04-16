package stat4_test

import (
	"testing"

	"github.com/maxpert/marmot/pkg/stat4"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

// parseWhere parses a SQL fragment like "col = 1" into a sqlparser.Expr.
// It wraps the expression in a SELECT to allow Vitess to parse it.
func parseWhere(t *testing.T, expr string) sqlparser.Expr {
	t.Helper()
	p, err := sqlparser.New(sqlparser.Options{})
	require.NoError(t, err)
	stmt, err := p.Parse("SELECT 1 FROM t WHERE " + expr)
	require.NoError(t, err)
	sel, ok := stmt.(*sqlparser.Select)
	require.True(t, ok)
	require.NotNil(t, sel.Where)
	return sel.Where.Expr
}

func TestEstimateCardinality_ZeroTotal(t *testing.T) {
	t.Parallel()
	expr := parseWhere(t, "a = 1")
	got := stat4.EstimateCardinality(expr, 0, nil)
	require.Equal(t, int64(0), got)
}

func TestEstimateCardinality_NilExpr(t *testing.T) {
	t.Parallel()
	got := stat4.EstimateCardinality(nil, 1000, nil)
	require.Equal(t, int64(500), got)
}

func TestEstimateCardinality_EqualOp(t *testing.T) {
	t.Parallel()
	cases := []struct {
		total int64
		want  int64
	}{
		{100, 10},
		{1000, 100},
		{7, 1},  // 7/10=0 → atLeast1 → 1
		{10, 1}, // 10/10=1
	}
	for _, tc := range cases {
		expr := parseWhere(t, "col = 42")
		got := stat4.EstimateCardinality(expr, tc.total, nil)
		require.Equal(t, tc.want, got, "total=%d", tc.total)
	}
}

func TestEstimateCardinality_InOp(t *testing.T) {
	t.Parallel()
	cases := []struct {
		exprSQL string
		total   int64
		want    int64
	}{
		// 3 values * 0.1 * 1000 = 300
		{"col IN (1,2,3)", 1000, 300},
		// 1 value * 0.1 * 100 = 10
		{"col IN (5)", 100, 10},
		// 20 values * 0.1 * 50 = 100 > total=50 → clamped to 50
		{"col IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)", 50, 50},
	}
	for _, tc := range cases {
		expr := parseWhere(t, tc.exprSQL)
		got := stat4.EstimateCardinality(expr, tc.total, nil)
		require.Equal(t, tc.want, got, "expr=%s total=%d", tc.exprSQL, tc.total)
	}
}

func TestEstimateCardinality_AndAllEquality(t *testing.T) {
	t.Parallel()
	// AND of equalities → max(estimates).
	// a=1 → 100, b=2 → 100 (total=1000, each gives 1000/10=100)
	expr := parseWhere(t, "a = 1 AND b = 2")
	got := stat4.EstimateCardinality(expr, 1000, nil)
	require.Equal(t, int64(100), got)
}

func TestEstimateCardinality_AndMixed(t *testing.T) {
	t.Parallel()
	// a = 1 → 100 (equality), b IN (1,2) → 200 (in)
	// mixed AND → product / total^(n-1) = (100 * 200) / 1000^1 = 20000/1000 = 20
	expr := parseWhere(t, "a = 1 AND b IN (1, 2)")
	got := stat4.EstimateCardinality(expr, 1000, nil)
	require.Equal(t, int64(20), got)
}

func TestEstimateCardinality_OrExpr(t *testing.T) {
	t.Parallel()
	// a=1 → 100, b=2 → 100; OR → min(1000, 200) = 200
	expr := parseWhere(t, "a = 1 OR b = 2")
	got := stat4.EstimateCardinality(expr, 1000, nil)
	require.Equal(t, int64(200), got)
}

func TestEstimateCardinality_OrCappedAtTotal(t *testing.T) {
	t.Parallel()
	// 10 IN values * 0.1 * 100 = 100 each side; OR → min(100, 200) = 100
	expr := parseWhere(t, "a IN (1,2,3,4,5,6,7,8,9,10) OR b IN (1,2,3,4,5,6,7,8,9,10)")
	got := stat4.EstimateCardinality(expr, 100, nil)
	require.Equal(t, int64(100), got)
}

func TestEstimateCardinality_Default(t *testing.T) {
	t.Parallel()
	// A NOT comparison falls through to default → total/2.
	expr := parseWhere(t, "a != 1")
	got := stat4.EstimateCardinality(expr, 1000, nil)
	require.Equal(t, int64(500), got)
}

func TestEstimateCardinality_NestedAnd(t *testing.T) {
	t.Parallel()
	// (a=1 AND b=2 AND c=3) — all equality → max(100,100,100) = 100
	expr := parseWhere(t, "a = 1 AND b = 2 AND c = 3")
	got := stat4.EstimateCardinality(expr, 1000, nil)
	require.Equal(t, int64(100), got)
}
