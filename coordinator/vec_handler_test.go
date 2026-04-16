//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractVecMatchQueryArg_Placeholder(t *testing.T) {
	t.Parallel()
	sql := `SELECT d.title FROM docs d
		WHERE vec_match(d.embed, ?, 10) AND d.status = ?
		ORDER BY vec_distance(d.embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)

	qVec := []byte{0x01, 0x02, 0x03}
	params := []interface{}{qVec, "published", qVec}

	gotVec, idx, has, err := extractVecMatchQueryArg(stmt, params)
	require.NoError(t, err)
	require.True(t, has)
	require.Equal(t, 0, idx)
	require.Equal(t, qVec, gotVec)
}

func TestExtractVecMatchQueryArg_NoMatch(t *testing.T) {
	t.Parallel()
	stmt := parseSQL(t, `SELECT id FROM docs WHERE status = ? LIMIT 10`)
	_, _, has, err := extractVecMatchQueryArg(stmt, []interface{}{"x"})
	require.NoError(t, err)
	require.False(t, has)
}

func TestExtractVecMatchQueryArg_HexLiteral(t *testing.T) {
	t.Parallel()
	// vec_match with a hex-encoded literal blob. Vitess exposes X'...' as HexVal.
	sql := `SELECT id FROM docs WHERE vec_match(embed, X'010203', 10)
			ORDER BY vec_distance(embed, X'010203') LIMIT 10`
	stmt := parseSQL(t, sql)
	gotVec, idx, has, err := extractVecMatchQueryArg(stmt, nil)
	require.NoError(t, err)
	require.True(t, has)
	require.Equal(t, -1, idx)
	require.Equal(t, []byte{0x01, 0x02, 0x03}, gotVec)
}

func TestExtractVecMatchQueryArg_BadArgType(t *testing.T) {
	t.Parallel()
	// 2nd arg is a function call, not a literal/placeholder.
	sql := `SELECT id FROM docs WHERE vec_match(embed, foo(), 10)
			ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	_, _, has, err := extractVecMatchQueryArg(stmt, nil)
	require.True(t, has)
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-020")
}
