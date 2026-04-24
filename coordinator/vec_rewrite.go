package coordinator

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/pkg/stat4"
	"vitess.io/vitess/go/vt/sqlparser"
)

// VectorIndexLookup is the minimal read interface the rewriter needs from
// VectorIndexManager. Defined here to avoid an import cycle with the db package.
type VectorIndexLookup interface {
	// GetIndexByColumn returns the index metadata for (database, table, column).
	// Second return is false when no index is defined on that column.
	// An empty database string performs a unique-match scan across all databases.
	GetIndexByColumn(database, table, column string) (*common.VectorIndexMeta, bool)

	// EstimatedRowCount returns an approximate row count for (database, table).
	// Implementations should cache the result; returns a default on any error.
	EstimatedRowCount(database, table string) int64
}

func useBudgetProbeForSession(meta *common.VectorIndexMeta, session QuerySession, effectiveNprobe int) bool {
	if meta == nil || session == nil || !meta.AutoTuneNprobe || effectiveNprobe != meta.Nprobe {
		return false
	}
	// Nprobe(0) exposes whether the session has an explicit override. An
	// explicit override equal to the stored auto default must still mean
	// fixed-probe execution, not row-budget probing.
	return session.Nprobe(0) == 0
}

// vecMatchInfo holds data extracted from a single vec_match(col, q, k) call.
type vecMatchInfo struct {
	colName  string         // unqualified column name
	tableRef string         // table alias or real name from col qualifier
	queryArg sqlparser.Expr // second argument (query vector expression)
	k        int            // third argument (number of results)
	node     *sqlparser.FuncExpr
}

// vecDistanceInfo holds data extracted from vec_distance(col, q) in ORDER BY.
type vecDistanceInfo struct {
	colName  string
	tableRef string
	queryArg sqlparser.Expr
	node     *sqlparser.FuncExpr
}

// RewriteVectorQuery inspects stmt for pgvector-style (vec_match + vec_distance)
// patterns and produces a cost-based rewrite. Returns (nil, nil) when stmt is
// not a vector search (handler must fall through to normal execution).
// Detection uses Vitess AST walks only; no regex or string matching.
func RewriteVectorQuery(
	stmt sqlparser.Statement,
	queryVec []byte,
	session QuerySession,
	engine vecindex.VectorUDFProvider,
	mgr VectorIndexLookup,
) (*RewriteInfo, error) {
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, nil
	}

	// --- Detection phase ---
	matches, err := findVecMatches(sel)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, nil // not a vector query
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("MARMOT-VEC-020: multiple vec_match not supported in a single SELECT")
	}

	vm := matches[0]

	// LIMIT is required.
	if sel.Limit == nil || sel.Limit.Rowcount == nil {
		return nil, fmt.Errorf("MARMOT-VEC-021: vec_match query must have LIMIT")
	}
	limitLit, ok := sel.Limit.Rowcount.(*sqlparser.Literal)
	if !ok {
		return nil, fmt.Errorf("MARMOT-VEC-021: vec_match query LIMIT must be a literal integer")
	}
	limitK, err := strconv.Atoi(limitLit.Val)
	if err != nil || limitK < 1 {
		return nil, fmt.Errorf("MARMOT-VEC-021: vec_match query LIMIT must be a positive integer")
	}

	// ORDER BY vec_distance(col, q) on the same column is required.
	vd, err := findVecDistance(sel, vm.colName, vm.tableRef)
	if err != nil {
		return nil, err
	}
	if vd == nil {
		return nil, fmt.Errorf("MARMOT-VEC-022: vec_match query must ORDER BY vec_distance(%s, ...) on the same column", vm.colName)
	}

	// --- Index lookup ---
	realTable := resolveRealTable(sel, vm.tableRef)
	meta, ok := mgr.GetIndexByColumn("", realTable, vm.colName)
	if !ok {
		return nil, fmt.Errorf("MARMOT-VEC-022: no vector index on %s.%s", realTable, vm.colName)
	}

	// --- Selectivity estimation ---
	userPred := stripVecMatch(sel.Where)
	total := mgr.EstimatedRowCount(meta.Database, realTable)
	estimatedF := stat4.EstimateCardinality(userPred, total)

	nprobe := session.Nprobe(meta.Nprobe)
	useBudgetProbe := useBudgetProbeForSession(meta, session, nprobe)
	const overfetch = 4
	estimatedI := int64(limitK) * int64(nprobe) * overfetch
	prefilterCap := session.PrefilterCap()
	forcePlan := session.ForcePlan()

	// --- Plan selection (design §7.2) ---
	var plan RewritePlan
	switch {
	case forcePlan == "pre":
		plan = PlanPreFilter
	case forcePlan == "post":
		plan = PlanPostFilter
	case userPred == nil:
		plan = PlanPostFilter
	case estimatedF <= min64(estimatedI, prefilterCap):
		plan = PlanPreFilter
	default:
		plan = PlanPostFilter
	}

	// --- Metric rewrite ---
	// Rename vec_distance → vec_distance_<metric> in the ORDER BY node in-place.
	metricSuffix := meta.Metric
	if metricSuffix == "" {
		metricSuffix = "l2"
	}
	vd.node.Name = sqlparser.NewIdentifierCI("vec_distance_" + metricSuffix)

	info := &RewriteInfo{
		Plan:         plan,
		IndexName:    meta.IndexName,
		Database:     meta.Database,
		TableName:    realTable,
		ColumnName:   vm.colName,
		Metric:       metricSuffix,
		K:            limitK,
		EstimatedF:   estimatedF,
		EstimatedI:   estimatedI,
		PrefilterCap: prefilterCap,
		ForcePlan:    forcePlan,
	}

	// --- Build primary statement ---
	switch plan {
	case PlanPreFilter:
		primary, err := buildPreFilter(sel, userPred)
		if err != nil {
			return nil, err
		}
		info.PrimaryStmt = primary
		info.PrimarySQL = sqlparser.String(primary)

	case PlanPostFilter:
		// Capture the probe epoch alongside cluster IDs so the cache path can
		// detect a post-REINDEX epoch mismatch (task #16 coherence). Engines
		// that do not expose the epoch-aware extension fall back to the
		// legacy API — cache path then stays gated by the zero-epoch guard.
		var (
			clusterIDs []int64
			probeEpoch uint64
			err        error
		)
		type epochProvider interface {
			TopNprobeClustersWithEpoch(indexName string, vec []byte, n int) ([]int64, uint64, error)
		}
		if ep, ok := engine.(epochProvider); ok {
			clusterIDs, probeEpoch, err = ep.TopNprobeClustersWithEpoch(meta.IndexName, queryVec, nprobe)
		} else {
			clusterIDs, err = engine.TopNprobeClusters(meta.IndexName, queryVec, nprobe)
		}
		if err != nil {
			if !errors.Is(err, vecindex.ErrNoCentroidsLoaded) {
				return nil, fmt.Errorf("MARMOT-VEC-023: top-n probe clusters failed: %w", err)
			}
			clusterIDs = nil
			probeEpoch = 0
		}
		info.ClusterIDs = clusterIDs

		vmIdx, vdIdx, totalArgs, walkErr := findArgIndices(sel, vm.node, vd.node)
		if walkErr != nil {
			return nil, walkErr
		}
		goRank, grErr := BuildGoRankPlan(
			sel,
			userPred,
			queryVec,
			vmIdx,
			vdIdx,
			totalArgs,
			meta,
			metricKindFromString(metricSuffix),
			clusterIDs,
			nprobe,
			useBudgetProbe,
			vm.tableRef,
			limitK,
		)
		if grErr != nil {
			return nil, grErr
		}
		goRank.Database = meta.Database
		goRank.ProbeEpoch = probeEpoch
		info.GoRank = goRank
	}

	// --- Fallback (design §7.5) ---
	if plan == PlanPostFilter && info.GoRank == nil && session.Fallback() == "on" {
		fallback, err := buildPreFilter(sel, userPred)
		if err != nil {
			return nil, err
		}
		info.FallbackStmt = fallback
		info.FallbackSQL = sqlparser.String(fallback)
		info.FallbackOn = true
	}

	return info, nil
}

// findVecMatches walks the WHERE clause and collects all vec_match(col, q, k) calls.
func findVecMatches(sel *sqlparser.Select) ([]*vecMatchInfo, error) {
	if sel.Where == nil {
		return nil, nil
	}
	var results []*vecMatchInfo
	var walkErr error
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		fn, ok := node.(*sqlparser.FuncExpr)
		if !ok {
			return true, nil
		}
		if !fn.Name.EqualString("vec_match") {
			return true, nil
		}
		if len(fn.Exprs) != 3 {
			walkErr = fmt.Errorf("MARMOT-VEC-020: vec_match requires exactly 3 arguments")
			return false, nil
		}
		col, ok := fn.Exprs[0].(*sqlparser.ColName)
		if !ok {
			walkErr = fmt.Errorf("MARMOT-VEC-020: vec_match first argument must be a column reference")
			return false, nil
		}
		queryArgExpr := fn.Exprs[1]
		kLit, ok := fn.Exprs[2].(*sqlparser.Literal)
		if !ok {
			walkErr = fmt.Errorf("MARMOT-VEC-020: vec_match third argument must be a literal integer")
			return false, nil
		}
		k, convErr := strconv.Atoi(kLit.Val)
		if convErr != nil || k < 1 {
			walkErr = fmt.Errorf("MARMOT-VEC-020: vec_match third argument must be a positive integer")
			return false, nil
		}
		tableRef := col.Qualifier.Name.String()
		results = append(results, &vecMatchInfo{
			colName:  col.Name.String(),
			tableRef: tableRef,
			queryArg: queryArgExpr,
			k:        k,
			node:     fn,
		})
		return true, nil
	}, sel.Where.Expr)
	if walkErr != nil {
		return nil, walkErr
	}
	return results, nil
}

// findVecDistance locates the vec_distance(col, q) node in ORDER BY that
// matches colName/tableRef. Returns nil when not present.
func findVecDistance(sel *sqlparser.Select, colName, tableRef string) (*vecDistanceInfo, error) {
	for _, order := range sel.OrderBy {
		fn, ok := order.Expr.(*sqlparser.FuncExpr)
		if !ok {
			continue
		}
		if !fn.Name.EqualString("vec_distance") {
			continue
		}
		if len(fn.Exprs) != 2 {
			continue
		}
		col, ok := fn.Exprs[0].(*sqlparser.ColName)
		if !ok {
			continue
		}
		if col.Name.String() != colName {
			continue
		}
		// tableRef match: either both empty, both equal, or one is empty.
		colTable := col.Qualifier.Name.String()
		if tableRef != "" && colTable != "" && tableRef != colTable {
			continue
		}
		return &vecDistanceInfo{
			colName:  col.Name.String(),
			tableRef: colTable,
			queryArg: fn.Exprs[1],
			node:     fn,
		}, nil
	}
	return nil, nil
}

// resolveRealTable maps a table alias back to the real table name using the
// FROM clause. Returns tableRef when no alias mapping is found.
func resolveRealTable(sel *sqlparser.Select, tableRef string) string {
	for _, tableExpr := range sel.From {
		aliased, ok := tableExpr.(*sqlparser.AliasedTableExpr)
		if !ok {
			continue
		}
		tn, ok := aliased.Expr.(sqlparser.TableName)
		if !ok {
			continue
		}
		realName := tn.Name.String()
		if tableRef == "" {
			return realName
		}
		alias := aliased.As.String()
		if alias == tableRef || realName == tableRef {
			return realName
		}
	}
	if tableRef != "" {
		return tableRef
	}
	return ""
}

// stripVecMatch returns the WHERE expression with all vec_match(...) nodes
// removed. Returns nil when vec_match is the only predicate.
func stripVecMatch(where *sqlparser.Where) sqlparser.Expr {
	if where == nil {
		return nil
	}
	return removeVecMatch(where.Expr)
}

// removeVecMatch recursively removes vec_match nodes from an expression tree.
func removeVecMatch(expr sqlparser.Expr) sqlparser.Expr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *sqlparser.FuncExpr:
		if e.Name.EqualString("vec_match") {
			return nil
		}
		return e
	case *sqlparser.AndExpr:
		left := removeVecMatch(e.Left)
		right := removeVecMatch(e.Right)
		if left == nil {
			return right
		}
		if right == nil {
			return left
		}
		return &sqlparser.AndExpr{Left: left, Right: right}
	default:
		return e
	}
}

// buildPreFilter constructs the pre-filter SELECT (design §7.3):
//   - WHERE = userPredicate (vec_match removed)
//   - ORDER BY vec_distance_<metric>(col, q) — already renamed on the node
//   - LIMIT unchanged
func buildPreFilter(sel *sqlparser.Select, userPred sqlparser.Expr) (*sqlparser.Select, error) {
	cloned := sqlparser.Clone(sel)
	if userPred == nil {
		cloned.Where = nil
	} else {
		cloned.Where = &sqlparser.Where{
			Type: sqlparser.WhereClause,
			Expr: sqlparser.Clone(userPred),
		}
	}
	return cloned, nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
