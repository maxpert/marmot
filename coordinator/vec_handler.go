package coordinator

import (
	"encoding/hex"
	"fmt"

	"github.com/maxpert/marmot/protocol"
	"vitess.io/vitess/go/vt/sqlparser"
)

// maybeRewriteVectorSelect inspects stmt.SQL for a vec_match() + vec_distance()
// pattern and, if found, produces an executable RewriteInfo along with the
// argument list to bind to PrimarySQL. Returns (nil, nil, nil) when stmt is
// not a vector query. Errors surface MARMOT-VEC-0xx codes from the rewriter.
//
// Design §7, §7.5. Detection uses Vitess AST only; no regex.
func (h *CoordinatorHandler) maybeRewriteVectorSelect(
	stmt protocol.Statement,
	params []interface{},
	session *protocol.ConnectionSession,
) (*RewriteInfo, []interface{}, error) {
	engine := h.loadVectorEngine()
	if engine == nil {
		return nil, nil, nil
	}
	mgr := h.dbManager.GetVectorIndexManager()
	if mgr == nil {
		return nil, nil, nil
	}

	// Prefer the AST threaded through by the upstream parser pipeline. The
	// rewriter mutates node fields (e.g., vec_distance → vec_distance_<metric>
	// in ORDER BY), so we hand it a Clone rather than the shared AST the
	// caller retains on stmt. When no pre-parsed AST is present (sqlite
	// dialect paths, direct callers) we fall back to a fresh Vitess parse.
	var ast sqlparser.Statement
	if stmt.ParsedAST != nil {
		ast = sqlparser.Clone(stmt.ParsedAST)
	} else {
		parsed, err := protocol.ParseVitessAST(stmt.SQL)
		if err != nil {
			return nil, nil, nil // let normal path handle / surface the parse error
		}
		ast = parsed
	}

	execParams := params
	if len(execParams) == 0 && len(stmt.ExtractedParams) > 0 {
		execParams = stmt.ExtractedParams
	}

	queryVec, queryArgIdx, hasMatch, err := extractVecMatchQueryArg(ast, execParams)
	if err != nil {
		return nil, nil, err
	}
	if !hasMatch {
		return nil, nil, nil
	}

	info, err := RewriteVectorQuery(ast, queryVec, NewQuerySession(session), engine, mgr)
	if err != nil {
		return nil, nil, err
	}
	if info == nil {
		return nil, nil, nil
	}

	// Drop the vec_match query-vector argument from the bound params if it was
	// supplied as a placeholder (design §7.4: vec_match is stripped from WHERE;
	// vec_distance keeps its own placeholder and param).
	rewrittenArgs := execParams
	if queryArgIdx >= 0 && queryArgIdx < len(execParams) {
		rewrittenArgs = make([]interface{}, 0, len(execParams)-1)
		rewrittenArgs = append(rewrittenArgs, execParams[:queryArgIdx]...)
		rewrittenArgs = append(rewrittenArgs, execParams[queryArgIdx+1:]...)
	}

	return info, rewrittenArgs, nil
}

// executeVectorPlan runs RewriteInfo.PrimarySQL and, when configured, the
// §7.5 short-result fallback. It returns the result set to the caller (which
// handles FoundRows accounting).
func (h *CoordinatorHandler) executeVectorPlan(
	stmt protocol.Statement,
	info *RewriteInfo,
	args []interface{},
	consistency protocol.ConsistencyLevel,
) (*protocol.ResultSet, error) {
	if info.GoRank != nil {
		return h.executeGoRankPlan(info.GoRank, args)
	}

	primary := stmt
	primary.SQL = info.PrimarySQL
	primary.ExtractedParams = args

	rs, err := h.handleRead(primary, args, consistency)
	if err != nil {
		return nil, err
	}

	// §7.5 short-result fallback: only post-filter with explicit FallbackOn.
	if info.FallbackOn && info.Plan == PlanPostFilter && rs != nil && len(rs.Rows) < info.K && info.FallbackSQL != "" {
		fb := stmt
		fb.SQL = info.FallbackSQL
		fb.ExtractedParams = args
		fbRS, fbErr := h.handleRead(fb, args, consistency)
		if fbErr != nil {
			return nil, fbErr
		}
		return fbRS, nil
	}
	return rs, nil
}

// extractVecMatchQueryArg walks stmt for the (single) vec_match() call and
// returns its second argument as []byte, along with the 0-based positional
// index of the argument in the bound-parameter list (or -1 when the argument
// is a literal).
//
// hasMatch is false when no vec_match() call is present — the caller should
// fall through to normal execution.
func extractVecMatchQueryArg(stmt sqlparser.Statement, params []interface{}) (queryVec []byte, argIdx int, hasMatch bool, err error) {
	var (
		match     *sqlparser.FuncExpr
		argCursor = -1
		argMap    = map[*sqlparser.Argument]int{}
	)

	// First pass: number all Argument nodes in document order so we can later
	// locate vec_match's 2nd argument index if it is a placeholder.
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if arg, ok := node.(*sqlparser.Argument); ok {
			argCursor++
			argMap[arg] = argCursor
		}
		if match == nil {
			if fn, ok := node.(*sqlparser.FuncExpr); ok && fn.Name.EqualString("vec_match") {
				match = fn
			}
		}
		return true, nil
	}, stmt)

	if match == nil {
		return nil, -1, false, nil
	}
	if len(match.Exprs) < 2 {
		return nil, -1, true, fmt.Errorf("MARMOT-VEC-020: vec_match requires exactly 3 arguments")
	}

	switch q := match.Exprs[1].(type) {
	case *sqlparser.Literal:
		b, decErr := decodeVecLiteral(q)
		if decErr != nil {
			return nil, -1, true, decErr
		}
		return b, -1, true, nil
	case *sqlparser.Argument:
		idx, ok := argMap[q]
		if !ok {
			return nil, -1, true, fmt.Errorf("MARMOT-VEC-020: vec_match query argument not found in parameter map")
		}
		if idx >= len(params) {
			return nil, -1, true, fmt.Errorf("MARMOT-VEC-020: vec_match query argument index %d out of range (have %d params)", idx, len(params))
		}
		b, convErr := paramToBytes(params[idx])
		if convErr != nil {
			return nil, -1, true, convErr
		}
		return b, idx, true, nil
	default:
		return nil, -1, true, fmt.Errorf("MARMOT-VEC-020: vec_match query argument must be a literal blob or bind parameter, got %T", q)
	}
}

// decodeVecLiteral decodes a Vitess literal holding a vector blob. Accepts
// hex-encoded strings (X'...') and raw string literals (caller guarantees
// well-formed bytes).
func decodeVecLiteral(lit *sqlparser.Literal) ([]byte, error) {
	switch lit.Type {
	case sqlparser.HexVal:
		b, err := hex.DecodeString(lit.Val)
		if err != nil {
			return nil, fmt.Errorf("MARMOT-VEC-020: invalid hex blob in vec_match: %w", err)
		}
		return b, nil
	case sqlparser.StrVal:
		return []byte(lit.Val), nil
	default:
		return nil, fmt.Errorf("MARMOT-VEC-020: vec_match query literal must be hex or string, got %v", lit.Type)
	}
}

// paramToBytes coerces a bound param value to []byte. The wire protocol
// delivers BLOB parameters as []byte; strings are accepted as a fallback for
// test paths that bind hex-encoded vectors.
func paramToBytes(v interface{}) ([]byte, error) {
	switch b := v.(type) {
	case []byte:
		return b, nil
	case string:
		return []byte(b), nil
	default:
		return nil, fmt.Errorf("MARMOT-VEC-020: vec_match query arg must be []byte or string, got %T", v)
	}
}
