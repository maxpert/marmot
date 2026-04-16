package query_test

import (
	"testing"

	"github.com/maxpert/marmot/protocol/query"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

// intLiteral builds a Vitess integer literal AST node directly — no string parsing.
func intLiteral(val string) *sqlparser.Literal {
	return sqlparser.NewIntLiteral(val)
}

// strLiteral builds a Vitess string literal AST node directly.
func strLiteral(val string) *sqlparser.Literal {
	return sqlparser.NewStrLiteral(val)
}

func TestValidateMarmotVecSessionVar_IntValid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		val  string
		want int64
	}{
		{"marmot_vec_nprobe", "32", 32},
		{"marmot_vec_nprobe", "1", 1},
		{"marmot_vec_prefilter_cap", "10000", 10000},
		{"marmot_vec_prefilter_cap", "1", 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name+"="+tc.val, func(t *testing.T) {
			t.Parallel()
			got, err := query.ValidateMarmotVecSessionVar(tc.name, intLiteral(tc.val))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestValidateMarmotVecSessionVar_EnumValid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		val  string
	}{
		{"marmot_vec_force_plan", "auto"},
		{"marmot_vec_force_plan", "pre"},
		{"marmot_vec_force_plan", "post"},
		{"marmot_vec_fallback", "on"},
		{"marmot_vec_fallback", "off"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name+"="+tc.val, func(t *testing.T) {
			t.Parallel()
			got, err := query.ValidateMarmotVecSessionVar(tc.name, strLiteral(tc.val))
			require.NoError(t, err)
			require.Equal(t, tc.val, got)
		})
	}
}

func TestValidateMarmotVecSessionVar_InvalidEnum(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		val  string
	}{
		{"marmot_vec_force_plan", "maybe"},
		{"marmot_vec_fallback", "yes"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name+"="+tc.val, func(t *testing.T) {
			t.Parallel()
			_, err := query.ValidateMarmotVecSessionVar(tc.name, strLiteral(tc.val))
			require.Error(t, err)
			require.Contains(t, err.Error(), "MARMOT-VEC-012")
		})
	}
}

func TestValidateMarmotVecSessionVar_IntExpectedGotString(t *testing.T) {
	t.Parallel()
	_, err := query.ValidateMarmotVecSessionVar("marmot_vec_nprobe", strLiteral("32"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-012")
}

func TestValidateMarmotVecSessionVar_OutOfRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		val  string
	}{
		{"marmot_vec_nprobe", "0"},
		{"marmot_vec_prefilter_cap", "0"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name+"="+tc.val, func(t *testing.T) {
			t.Parallel()
			_, err := query.ValidateMarmotVecSessionVar(tc.name, intLiteral(tc.val))
			require.Error(t, err)
			require.Contains(t, err.Error(), "MARMOT-VEC-012")
		})
	}
}

func TestValidateMarmotVecSessionVar_UnknownVar(t *testing.T) {
	t.Parallel()
	_, err := query.ValidateMarmotVecSessionVar("marmot_vec_bogus", intLiteral("1"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-012")
}

func TestValidateMarmotVecSessionVar_NonLiteralExpr(t *testing.T) {
	t.Parallel()
	// Pass a ColName instead of a literal.
	colExpr := sqlparser.NewColName("something")
	_, err := query.ValidateMarmotVecSessionVar("marmot_vec_nprobe", colExpr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-012")
}
