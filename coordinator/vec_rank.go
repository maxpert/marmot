package coordinator

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/protocol"
	"vitess.io/vitess/go/vt/sqlparser"
)

// GoRankPlan carries everything needed to execute the Go-side ranking path
// (§7.6): scan candidate rows from SQLite, compute distances in Go, then
// fetch the top-K via a final SELECT with literal rowid ordering.
//
// When the in-memory VectorCache is available and session.UseCache is on,
// executeGoRankPlan consults the cache directly (task #16) — IndexName +
// ClusterIDs drive cache lookup — and falls back to CandidateSQL otherwise.
type GoRankPlan struct {
	QueryVec   []float32
	MetricKind metric.Metric
	K          int
	Database   string
	BaseTable  string
	BaseAlias  string // empty → use BaseTable

	IndexName  string  // for cache lookup
	ClusterIDs []int64 // probed clusters; cache path iterates these + cluster 0
	ProbeEpoch uint64  // epoch of the probe set that produced ClusterIDs; guards cache coherence
	UseCache   bool    // session toggle: cache path enabled

	EmbedColumn string
	// CandidateSQL is built lazily: the cache-hit path (the hot path) never
	// invokes it and avoids the ~150 allocs + ~5KB string build from the
	// user-predicate render. The fallback path in executeGoRankPlan calls
	// this exactly once when the cache cannot serve the query.
	CandidateSQL func() string
	// CandidateArgFilter holds indices into rewrittenArgs that become the
	// positional parameters for CandidateSQL (in order). Computed once by
	// BuildGoRankPlan so executeGoRankPlan can resolve cheaply.
	CandidateArgFilter []int

	FinalSelectList string
	FinalFromClause string
}

// alias returns the effective alias for the base table.
func (p *GoRankPlan) alias() string {
	if p.BaseAlias != "" {
		return p.BaseAlias
	}
	return p.BaseTable
}

// BuildGoRankPlan constructs a GoRankPlan from an already-analysed SELECT.
// queryVec is the raw little-endian float32 bytes from vec_match's argument.
// queryVecArgIdx and vecDistArgIdx are 0-based indices into the original
// parameter list, or -1 when the argument was a literal.
func BuildGoRankPlan(
	sel *sqlparser.Select,
	userPred sqlparser.Expr,
	queryVec []byte,
	queryVecArgIdx int,
	vecDistArgIdx int,
	origArgCount int,
	meta *common.VectorIndexMeta,
	metricKind metric.Metric,
	clusterIDs []int64,
	tableAlias string,
	k int,
) (*GoRankPlan, error) {
	if len(queryVec)%4 != 0 {
		return nil, fmt.Errorf("MARMOT-VEC-030: queryVec length %d is not a multiple of 4", len(queryVec))
	}

	// Convert bytes → []float32 (allocate once, little-endian).
	dim := len(queryVec) / 4
	qf := make([]float32, dim)
	for i := range qf {
		bits := uint32(queryVec[i*4]) |
			uint32(queryVec[i*4+1])<<8 |
			uint32(queryVec[i*4+2])<<16 |
			uint32(queryVec[i*4+3])<<24
		qf[i] = math.Float32frombits(bits)
	}

	// Resolve alias.
	alias := tableAlias
	if alias == "" {
		alias = meta.TableName
	}

	membersTable := vecindex.MembersTable(meta.IndexName)

	// CandidateSQL closure: captured here, evaluated only on cache miss. Keeps
	// the cache-hit hot path free of the ~150 allocs the user-predicate render
	// costs per query. Capture userPred by pointer — it is read-only from here.
	//
	// SELECT `m`.`rowid`, `<alias>`.`<embed>` FROM `<members>` `m`
	//   JOIN `<base>` `<alias>` ON `<alias>`.`rowid` = `m`.`rowid`
	//   WHERE (`m`.`cluster_id` = 0 OR `m`.`cluster_id` IN (c1,...))
	//   [AND (<userPred>)]
	capturedUserPred := userPred
	capturedClusterIDs := clusterIDs
	capturedColumn := meta.ColumnName
	capturedTable := meta.TableName
	capturedMembers := membersTable
	capturedAlias := alias
	candidateSQL := func() string {
		var sb strings.Builder
		sb.WriteString("SELECT `m`.`rowid`, `")
		sb.WriteString(capturedAlias)
		sb.WriteString("`.`")
		sb.WriteString(capturedColumn)
		sb.WriteString("` FROM `")
		sb.WriteString(capturedMembers)
		sb.WriteString("` `m` JOIN `")
		sb.WriteString(capturedTable)
		sb.WriteString("` `")
		sb.WriteString(capturedAlias)
		sb.WriteString("` ON `")
		sb.WriteString(capturedAlias)
		sb.WriteString("`.`rowid` = `m`.`rowid` WHERE (`m`.`cluster_id` = 0")
		if len(capturedClusterIDs) > 0 {
			sb.WriteString(" OR `m`.`cluster_id` IN (")
			for i, id := range capturedClusterIDs {
				if i > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(strconv.FormatInt(id, 10))
			}
			sb.WriteByte(')')
		}
		sb.WriteByte(')')
		if capturedUserPred != nil {
			sb.WriteString(" AND (")
			sb.WriteString(sqlparser.String(capturedUserPred))
			sb.WriteString(")")
		}
		return sb.String()
	}

	// Compute CandidateArgFilter: the rewritten-args indices that correspond to
	// user-predicate placeholders. rewrittenArgs = origParams minus queryVecArgIdx.
	// vecDistArgIdx in rewrittenArgs = vecDistArgIdx - 1 if vecDistArgIdx > queryVecArgIdx, else vecDistArgIdx.
	// We collect all indices 0..origArgCount-1, map to rewritten-space, then exclude vecDist.
	candidateArgFilter := make([]int, 0, origArgCount)
	for origIdx := 0; origIdx < origArgCount; origIdx++ {
		if origIdx == queryVecArgIdx {
			continue // this was dropped when building rewrittenArgs
		}
		// Map to rewrittenArgs index.
		rewrittenIdx := origIdx
		if queryVecArgIdx >= 0 && origIdx > queryVecArgIdx {
			rewrittenIdx = origIdx - 1
		}
		// Map vecDistArgIdx to rewritten space.
		vecDistRewritten := vecDistArgIdx
		if queryVecArgIdx >= 0 && vecDistArgIdx > queryVecArgIdx {
			vecDistRewritten = vecDistArgIdx - 1
		}
		if rewrittenIdx == vecDistRewritten && vecDistArgIdx >= 0 {
			continue // exclude vec_distance placeholder
		}
		candidateArgFilter = append(candidateArgFilter, rewrittenIdx)
	}

	// FinalSelectList and FinalFromClause via AST rendering.
	finalSelectList := sqlparser.String(sel.SelectExprs)
	finalFromClause := sqlparser.String(sqlparser.TableExprs(sel.From))

	return &GoRankPlan{
		QueryVec:           qf,
		MetricKind:         metricKind,
		K:                  k,
		Database:           meta.Database,
		BaseTable:          meta.TableName,
		BaseAlias:          tableAlias,
		IndexName:          meta.IndexName,
		ClusterIDs:         clusterIDs,
		EmbedColumn:        meta.ColumnName,
		CandidateSQL:       candidateSQL,
		CandidateArgFilter: candidateArgFilter,
		FinalSelectList:    finalSelectList,
		FinalFromClause:    finalFromClause,
	}, nil
}

// --- min-heap of size K for top-K nearest neighbours ---

type rankItem struct {
	rowid int64
	dist  float32
}

// rankHeap is a max-heap on dist so we can efficiently evict the farthest
// candidate when the heap exceeds K.
type rankHeap []rankItem

func (h rankHeap) Len() int           { return len(h) }
func (h rankHeap) Less(i, j int) bool { return h[i].dist > h[j].dist } // max on top
func (h rankHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *rankHeap) Push(x interface{}) {
	*h = append(*h, x.(rankItem))
}
func (h *rankHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// executeGoRankPlan scans candidate rows, ranks them in Go, then fetches
// the final result set via a single SELECT with literal rowid ordering.
func (h *CoordinatorHandler) executeGoRankPlan(
	plan *GoRankPlan,
	rewrittenArgs []interface{},
) (*protocol.ResultSet, error) {
	conn, err := h.dbManager.GetDatabaseConnection(plan.Database)
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: get db for go-rank: %w", err)
	}

	// Cache-path fast lane (task #16): iterate the in-memory cache to rank
	// candidates entirely in Go — no SQLite cursor for the hot loop. Falls
	// through to the SQL candidate scan when the cache is unavailable, the
	// session disabled it, or the cache epoch lags the active probeState.
	if plan.UseCache {
		topK, ok := h.cacheRank(plan)
		if ok {
			return h.fetchProjectionByRowID(conn, plan, rewrittenArgs, topK)
		}
	}

	// Resolve candidate args from rewrittenArgs.
	candidateArgs := make([]interface{}, len(plan.CandidateArgFilter))
	for i, idx := range plan.CandidateArgFilter {
		if idx >= len(rewrittenArgs) {
			return nil, fmt.Errorf("MARMOT-VEC-030: candidate arg index %d out of range (have %d rewritten args)", idx, len(rewrittenArgs))
		}
		candidateArgs[i] = rewrittenArgs[idx]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := conn.QueryContext(ctx, plan.CandidateSQL(), candidateArgs...)
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: candidate query failed: %w", err)
	}
	defer rows.Close()

	// Stream rows into max-heap of size K.
	h2 := &rankHeap{}
	heap.Init(h2)

	for rows.Next() {
		var rowid int64
		var embedBytes []byte
		if err := rows.Scan(&rowid, &embedBytes); err != nil {
			return nil, fmt.Errorf("MARMOT-VEC-030: scan candidate row: %w", err)
		}
		if len(embedBytes) != len(plan.QueryVec)*4 {
			// Skip malformed rows — dimension mismatch.
			continue
		}
		dist := metric.DistanceFromBytes(plan.MetricKind, plan.QueryVec, embedBytes)
		item := rankItem{rowid: rowid, dist: dist}
		if h2.Len() < plan.K {
			heap.Push(h2, item)
		} else if dist < (*h2)[0].dist {
			heap.Pop(h2)
			heap.Push(h2, item)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: candidate scan error: %w", err)
	}

	if h2.Len() == 0 {
		return &protocol.ResultSet{}, nil
	}

	// Extract top-K in ascending-distance order.
	topK := make([]rankItem, h2.Len())
	for i := len(topK) - 1; i >= 0; i-- {
		topK[i] = heap.Pop(h2).(rankItem)
	}

	return h.fetchProjectionByRowID(conn, plan, nil, topK)
}

// cacheRank executes the Go-side ranking path against the in-memory
// VectorCache when the cache is usable. Returns (topK, true) on cache hit,
// (nil, false) when the caller should fall back to the SQL candidate scan.
//
// A cache is "usable" when: the engine exposes LookupCache (Engine does), a
// non-nil cache exists, and cache.Epoch matches the active probeState's
// epoch (guards against the reindex-swap gap between probe swap and cache
// installation). Delta-flush candidates live under cluster_id=0 and are
// always scanned in addition to plan.ClusterIDs to preserve recall.
func (h *CoordinatorHandler) cacheRank(plan *GoRankPlan) ([]rankItem, bool) {
	provider := h.loadVectorEngine()
	if provider == nil {
		return nil, false
	}
	cacheProvider, ok := provider.(interface {
		LookupCache(indexName string) *vecindex.VectorCache
	})
	if !ok {
		return nil, false
	}
	cache := cacheProvider.LookupCache(plan.IndexName)
	if cache == nil {
		return nil, false
	}
	// Coherence guard: plan.ClusterIDs were computed against a specific probe
	// epoch. A REINDEX that already installed a fresh cache under a new epoch
	// would silently return garbage if we indexed old-epoch cluster IDs into
	// the new cache. plan.ProbeEpoch==0 means the rewriter could not capture
	// an epoch (legacy provider); in that case we conservatively fall back.
	if plan.ProbeEpoch == 0 || cache.Epoch() != plan.ProbeEpoch {
		return nil, false
	}

	metricKind := plan.MetricKind
	qv := plan.QueryVec

	h2 := &rankHeap{}
	heap.Init(h2)
	push := func(entry vecindex.CachedVector) {
		if len(entry.Vec) != len(qv) {
			return
		}
		dist := metric.Distance(metricKind, qv, entry.Vec)
		item := rankItem{rowid: entry.RowID, dist: dist}
		if h2.Len() < plan.K {
			heap.Push(h2, item)
		} else if dist < (*h2)[0].dist {
			heap.Pop(h2)
			heap.Push(h2, item)
		}
	}

	// cluster_id=0 holds delta rows — always probed to preserve recall.
	for _, entry := range cache.Cluster(0) {
		push(entry)
	}
	for _, cid := range plan.ClusterIDs {
		for _, entry := range cache.Cluster(cid) {
			push(entry)
		}
	}

	topK := make([]rankItem, h2.Len())
	for i := len(topK) - 1; i >= 0; i-- {
		topK[i] = heap.Pop(h2).(rankItem)
	}
	return topK, true
}

// fetchProjectionByRowID issues the final SELECT that projects the user's
// columns in ascending-distance order for the top-K rowids. Shared by the
// SQL-candidate-scan path and the cache path. userArgs is appended for any
// parameterised user predicate baked into FinalFromClause — today always nil
// because the plan builder has stripped vec_match but retains user predicates
// only in CandidateSQL; final projection runs with rowid filter alone.
func (h *CoordinatorHandler) fetchProjectionByRowID(
	conn *sql.DB,
	plan *GoRankPlan,
	userArgs []interface{},
	topK []rankItem,
) (*protocol.ResultSet, error) {
	if len(topK) == 0 {
		return &protocol.ResultSet{}, nil
	}

	alias := plan.alias()
	var fsb strings.Builder
	fsb.WriteString("SELECT ")
	fsb.WriteString(plan.FinalSelectList)
	fsb.WriteString(" FROM ")
	fsb.WriteString(plan.FinalFromClause)
	fsb.WriteString(" WHERE `")
	fsb.WriteString(alias)
	fsb.WriteString("`.`rowid` IN (")
	for i, item := range topK {
		if i > 0 {
			fsb.WriteByte(',')
		}
		fsb.WriteString(strconv.FormatInt(item.rowid, 10))
	}
	fsb.WriteString(") ORDER BY CASE `")
	fsb.WriteString(alias)
	fsb.WriteString("`.`rowid`")
	for i, item := range topK {
		fsb.WriteString(" WHEN ")
		fsb.WriteString(strconv.FormatInt(item.rowid, 10))
		fsb.WriteString(" THEN ")
		fsb.WriteString(strconv.Itoa(i + 1))
	}
	fsb.WriteString(" END LIMIT ")
	fsb.WriteString(strconv.Itoa(plan.K))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	finalRows, err := conn.QueryContext(ctx, fsb.String(), userArgs...)
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: final ranked query failed: %w", err)
	}
	defer finalRows.Close()

	cols, err := finalRows.Columns()
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: final query columns: %w", err)
	}

	colDefs := make([]protocol.ColumnDef, len(cols))
	for i, name := range cols {
		colDefs[i] = protocol.ColumnDef{Name: name, Type: 0xFD}
	}

	var resultRows [][]interface{}
	for finalRows.Next() {
		scanDest := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range scanDest {
			ptrs[i] = &scanDest[i]
		}
		if err := finalRows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("MARMOT-VEC-030: scan final row: %w", err)
		}
		row := make([]interface{}, len(cols))
		copy(row, scanDest)
		resultRows = append(resultRows, row)
	}
	if err := finalRows.Err(); err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: final scan error: %w", err)
	}

	return &protocol.ResultSet{
		Columns: colDefs,
		Rows:    resultRows,
	}, nil
}

// findArgIndices walks sel once in document order, numbering all Argument
// nodes. It returns the 0-based index of vmNode's second argument and
// vdNode's second argument in original-param order, plus the total arg count.
// Returns -1 for each node whose second argument is not an *sqlparser.Argument.
func findArgIndices(
	sel *sqlparser.Select,
	vmNode *sqlparser.FuncExpr,
	vdNode *sqlparser.FuncExpr,
) (vecMatchIdx, vecDistIdx, totalArgs int, err error) {
	vecMatchIdx = -1
	vecDistIdx = -1

	argCursor := -1
	var vmArg, vdArg *sqlparser.Argument
	if len(vmNode.Exprs) >= 2 {
		vmArg, _ = vmNode.Exprs[1].(*sqlparser.Argument)
	}
	if len(vdNode.Exprs) >= 2 {
		vdArg, _ = vdNode.Exprs[1].(*sqlparser.Argument)
	}

	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		arg, ok := node.(*sqlparser.Argument)
		if !ok {
			return true, nil
		}
		argCursor++
		if vmArg != nil && arg == vmArg {
			vecMatchIdx = argCursor
		}
		if vdArg != nil && arg == vdArg {
			vecDistIdx = argCursor
		}
		return true, nil
	}, sel)

	totalArgs = argCursor + 1
	return vecMatchIdx, vecDistIdx, totalArgs, nil
}

// metricKindFromString maps a metric suffix string to a metric.Metric constant.
func metricKindFromString(s string) metric.Metric {
	switch s {
	case "cosine":
		return metric.MetricCosine
	case "dot":
		return metric.MetricDot
	default:
		return metric.MetricL2
	}
}
