//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMergeExecParams_StaleParamOrderMisinterleavesResolvedArgs reproduces
// the coordinator/vec_handler.go regression: a rewritten statement (e.g. a
// vector-search fallback/primary query) copied from an original prepared
// statement that itself mixed wire params with pipeline-extracted literals.
// The rewrite's args are already fully resolved and positional for the NEW
// SQL, but the copy - built by hand as `fb := stmt; fb.SQL = ...;
// fb.ExtractedParams = args` - kept the ORIGINAL statement's ParamOrder,
// which describes the ORIGINAL SQL's placeholder layout, not the rewrite's.
// Passing args as both wireParams and ExtractedParams (as the caller was
// forced to do, having only one resolved list) then makes MergeExecParams
// interleave args against itself instead of passing it through untouched.
func TestMergeExecParams_StaleParamOrderMisinterleavesResolvedArgs(t *testing.T) {
	// Original prepared statement: one wire placeholder, one pipeline-extracted
	// literal, in that order (mirrors a vec_match(...) query with a bound
	// query vector alongside a literal WHERE clause the pipeline extracted).
	original := Statement{
		SQL:             "SELECT * FROM t WHERE vec_match(embedding, ?, 10) AND status = 'active'",
		ExtractedParams: []interface{}{"active"},
		ParamOrder:      []bool{true, false},
	}

	// The rewrite resolves everything (including the vector search result)
	// into a single positional args list for a completely different query.
	args := []interface{}{"resolved-a", "resolved-b"}

	// Pre-fix pattern: hand-copy the statement, replace SQL and
	// ExtractedParams, but leave the stale ParamOrder in place.
	stale := original
	stale.SQL = "SELECT * FROM t WHERE id IN (?, ?)"
	stale.ExtractedParams = args

	merged := stale.MergeExecParams(args)

	// BUG: [true, false] pulls args[0] for the wire slot, then
	// stale.ExtractedParams[0] (== args[0] again, not args[1]) for the
	// extracted slot - silently duplicating args[0] and dropping args[1]
	// instead of passing args through untouched.
	require.NotEqual(t, args, merged,
		"demonstrates the stale-ParamOrder bug this test guards against: "+
			"a rewritten statement must not reuse the original's ParamOrder")
	require.Equal(t, []interface{}{"resolved-a", "resolved-a"}, merged)
}

// TestStatement_WithResolvedParams_FixesStaleParamOrder is the fix: building
// the rewritten copy via WithResolvedParams clears ParamOrder, so
// MergeExecParams falls back to the single-source path and passes the
// rewrite's already-resolved args straight through.
func TestStatement_WithResolvedParams_FixesStaleParamOrder(t *testing.T) {
	original := Statement{
		SQL:             "SELECT * FROM t WHERE vec_match(embedding, ?, 10) AND status = 'active'",
		ExtractedParams: []interface{}{"active"},
		ParamOrder:      []bool{true, false},
	}
	args := []interface{}{"resolved-a", "resolved-b"}

	rewritten := original.WithResolvedParams("SELECT * FROM t WHERE id IN (?, ?)", args)

	require.Equal(t, "SELECT * FROM t WHERE id IN (?, ?)", rewritten.SQL)
	require.Nil(t, rewritten.ParamOrder, "rewrite must not carry the original statement's placeholder layout")
	require.Equal(t, args, rewritten.ExtractedParams)

	merged := rewritten.MergeExecParams(args)
	require.Equal(t, args, merged, "resolved args must pass through untouched, not interleave with themselves")
}

// TestStatement_WithResolvedParams_LeavesOriginalUntouched guards against a
// value-receiver mistake that mutates the caller's statement.
func TestStatement_WithResolvedParams_LeavesOriginalUntouched(t *testing.T) {
	original := Statement{
		SQL:             "SELECT 1",
		ExtractedParams: []interface{}{"active"},
		ParamOrder:      []bool{true, false},
	}

	_ = original.WithResolvedParams("SELECT 2", []interface{}{"x", "y"})

	require.Equal(t, "SELECT 1", original.SQL)
	require.Equal(t, []interface{}{"active"}, original.ExtractedParams)
	require.Equal(t, []bool{true, false}, original.ParamOrder)
}
