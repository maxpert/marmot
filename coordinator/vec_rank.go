package coordinator

import (
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
// Candidate rows are streamed directly from the clustered sidecar members
// table; the base table is only consulted for the final top-K projection.
type GoRankPlan struct {
	RawQueryVec []byte
	QueryVec    []float32
	RankMetric  metric.Metric
	Nprobe      int
	K           int
	Database    string
	BaseTable   string
	BaseAlias   string // empty → use BaseTable

	IndexName  string
	ClusterIDs []int64 // probed clusters; execution adds cluster 0 for delta rows
	ProbeEpoch uint64  // epoch of the probe set that produced ClusterIDs; enables reprobe on reindex races

	EmbedColumn string
	// CandidateSQL is built lazily so the planner can carry the user predicate
	// without paying render cost until execution. The execute path renders it
	// exactly once per query, after any probe-epoch refresh.
	CandidateSQL func(clusterIDs []int64) string
	// CandidateArgFilter holds indices into rewrittenArgs that become the
	// positional parameters for CandidateSQL (in order). Computed once by
	// BuildGoRankPlan so executeGoRankPlan can resolve cheaply.
	CandidateArgFilter []int

	FinalSelectList  string
	FinalFromClause  string
	HasUserPredicate bool
	DirectPKColumn   string
	DirectPKLabel    string
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
	nprobe int,
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

	switch metricKind {
	case metric.MetricCosine:
		n := metric.Norm(qf)
		if n > 0 {
			inv := 1.0 / n
			for i := range qf {
				qf[i] *= inv
			}
		}
		// If n == 0, leave qf as-is; downstream CosineDistanceUnit will return 1.0
		// for every comparison, which is the correct "no direction" behaviour.
	case metric.MetricDot:
		qf = metric.AugmentQuery(qf, nil)
	}

	// Resolve alias.
	alias := tableAlias
	if alias == "" {
		alias = meta.TableName
	}

	membersTable := vecindex.MembersTable(meta.IndexName)
	finalSelectList := sqlparser.String(sel.SelectExprs)
	finalFromClause := sqlparser.String(sqlparser.TableExprs(sel.From))
	directPKColumn, directPKLabel := detectDirectPKProjection(sel, alias)

	rowidExpr := sqlparser.String(&sqlparser.ColName{
		Name:      sqlparser.NewIdentifierCI("rowid"),
		Qualifier: sqlparser.TableName{Name: sqlparser.NewIdentifierCS(alias)},
	})

	// CandidateSQL closure: build a sidecar-driven streaming scan over the
	// probed partitions. The base table is consulted only via EXISTS when a
	// user predicate is present; the final top-K projection fetches user columns
	// in a second query by rowid.
	capturedUserPred := userPred
	capturedMembers := membersTable
	capturedFromClause := finalFromClause
	candidateSQL := func(candidateClusterIDs []int64) string {
		var sb strings.Builder
		sb.WriteString("SELECT `m`.`rowid`, `m`.`vec` FROM `")
		sb.WriteString(capturedMembers)
		sb.WriteString("` `m` WHERE `m`.`cluster_id` IN (")
		for i, id := range clusterIDsWithDelta(candidateClusterIDs) {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(strconv.FormatInt(id, 10))
		}
		sb.WriteByte(')')
		if capturedUserPred != nil {
			sb.WriteString(" AND EXISTS (SELECT 1 FROM ")
			sb.WriteString(capturedFromClause)
			sb.WriteString(" WHERE ")
			sb.WriteString(rowidExpr)
			sb.WriteString(" = `m`.`rowid` AND (")
			sb.WriteString(sqlparser.String(capturedUserPred))
			sb.WriteString("))")
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

	return &GoRankPlan{
		RawQueryVec:        append([]byte(nil), queryVec...),
		QueryVec:           qf,
		RankMetric:         metricKindToRankMetric(metricKind),
		Nprobe:             nprobe,
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
		HasUserPredicate:   userPred != nil,
		DirectPKColumn:     directPKColumn,
		DirectPKLabel:      directPKLabel,
	}, nil
}

type rankItem struct {
	rowid int64
	dist  float32
}

// executeGoRankPlan scans candidate rows, ranks them in Go, then fetches
// the final result set via a single SELECT with literal rowid ordering.
func (h *CoordinatorHandler) executeGoRankPlan(
	plan *GoRankPlan,
	rewrittenArgs []interface{},
) (*protocol.ResultSet, error) {
	if items, ok, err := h.packedRank(plan); err != nil {
		return nil, err
	} else if ok {
		if len(items) == 0 {
			return &protocol.ResultSet{}, nil
		}
		if rs, ok, err := h.tryDirectPKResult(plan, items); err != nil {
			return nil, err
		} else if ok {
			return rs, nil
		}
		conn, err := h.dbManager.GetDatabaseReadConnection(plan.Database)
		if err != nil {
			return nil, fmt.Errorf("MARMOT-VEC-030: get db for go-rank: %w", err)
		}
		return h.fetchProjectionByRowID(conn, plan, nil, items)
	}

	if h.canUseSharedScan(plan) {
		if items, ok, err := h.sharedScanRank(plan); err != nil {
			return nil, err
		} else if ok {
			if len(items) == 0 {
				return &protocol.ResultSet{}, nil
			}
			if rs, ok, err := h.tryDirectPKResult(plan, items); err != nil {
				return nil, err
			} else if ok {
				return rs, nil
			}
			conn, err := h.dbManager.GetDatabaseReadConnection(plan.Database)
			if err != nil {
				return nil, fmt.Errorf("MARMOT-VEC-030: get db for go-rank: %w", err)
			}
			return h.fetchProjectionByRowID(conn, plan, nil, items)
		}
	}

	// Vector read path: use the read-only pool (multiple connections, WAL
	// concurrent readers). The write handle is single-conn + _txlock=immediate
	// and would serialise every ranked lookup against every other reader and
	// any inflight writer — a ~50,000× slowdown on 1M-row benches.
	conn, err := h.dbManager.GetDatabaseReadConnection(plan.Database)
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: get db for go-rank: %w", err)
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

	clusterIDs := h.refreshProbeClusterIDs(plan)
	rows, err := conn.QueryContext(ctx, plan.CandidateSQL(clusterIDs), candidateArgs...)
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: candidate query failed: %w", err)
	}
	defer rows.Close()

	// Stream rows into max-heap of size K.
	topK := newTopKHeap(plan.K)

	for rows.Next() {
		var rowid int64
		var vecBytes []byte
		if err := rows.Scan(&rowid, &vecBytes); err != nil {
			return nil, fmt.Errorf("MARMOT-VEC-030: scan candidate row: %w", err)
		}
		if len(vecBytes) != len(plan.QueryVec)*4 {
			continue
		}
		switch plan.RankMetric {
		case metric.MetricCosine:
			topK.Push(rowid, metric.CosineDistanceUnitFromBytes(plan.QueryVec, vecBytes))
		default:
			topK.Push(rowid, metric.DistanceFromBytes(plan.RankMetric, plan.QueryVec, vecBytes))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: candidate scan error: %w", err)
	}

	items := topK.Drain()
	if len(items) == 0 {
		return &protocol.ResultSet{}, nil
	}
	return h.fetchProjectionByRowID(conn, plan, nil, items)
}

func (h *CoordinatorHandler) canUseSharedScan(plan *GoRankPlan) bool {
	if plan == nil {
		return false
	}
	return !plan.HasUserPredicate && len(plan.CandidateArgFilter) == 0
}

func clusterIDsWithDelta(clusterIDs []int64) []int64 {
	out := make([]int64, 0, len(clusterIDs)+1)
	out = append(out, 0)
	for _, id := range clusterIDs {
		if id == 0 {
			continue
		}
		out = append(out, id)
	}
	return out
}

func metricKindToRankMetric(m metric.Metric) metric.Metric {
	if m == metric.MetricDot {
		return metric.MetricL2
	}
	return m
}

func (h *CoordinatorHandler) refreshProbeClusterIDs(plan *GoRankPlan) []int64 {
	if plan.ProbeEpoch == 0 || len(plan.RawQueryVec) == 0 || plan.Nprobe <= 0 {
		return plan.ClusterIDs
	}
	provider := h.loadVectorEngine()
	if provider == nil {
		return plan.ClusterIDs
	}
	type epochProvider interface {
		TopNprobeClustersWithEpoch(indexName string, vec []byte, n int) ([]int64, uint64, error)
	}
	ep, ok := provider.(epochProvider)
	if !ok {
		return plan.ClusterIDs
	}
	clusterIDs, epoch, err := ep.TopNprobeClustersWithEpoch(plan.IndexName, plan.RawQueryVec, plan.Nprobe)
	if err != nil || epoch == 0 || epoch == plan.ProbeEpoch {
		return plan.ClusterIDs
	}
	return clusterIDs
}

func (h *CoordinatorHandler) packedRank(plan *GoRankPlan) ([]rankItem, bool, error) {
	if plan == nil || plan.HasUserPredicate {
		return nil, false, nil
	}
	provider := h.loadVectorEngine()
	if provider == nil {
		return nil, false, nil
	}
	stateProvider, ok := provider.(interface {
		Lookup(indexName string) (*vecindex.IndexState, bool)
	})
	if !ok {
		return nil, false, nil
	}
	state, ok := stateProvider.Lookup(plan.IndexName)
	if !ok || state == nil {
		return nil, false, nil
	}
	store := state.LoadPackedStore()
	if store == nil {
		return nil, false, nil
	}

	var stableClusterIDs []int64
	var sqliteClusterIDs []int64
	for _, cid := range plan.ClusterIDs {
		if cid <= 0 || state.PackedClusterDirty(cid) {
			sqliteClusterIDs = append(sqliteClusterIDs, cid)
			continue
		}
		stableClusterIDs = append(stableClusterIDs, cid)
	}

	topK := newTopKHeap(plan.K)
	push := func(rowid int64, vecBytes []byte) {
		if len(vecBytes) != len(plan.QueryVec)*4 {
			return
		}
		switch plan.RankMetric {
		case metric.MetricCosine:
			topK.Push(rowid, metric.CosineDistanceUnitFromBytes(plan.QueryVec, vecBytes))
		default:
			topK.Push(rowid, metric.DistanceFromBytes(plan.RankMetric, plan.QueryVec, vecBytes))
		}
	}

	store.ScanClusters(stableClusterIDs, func(rowid int64, vecBytes []byte) bool {
		push(rowid, vecBytes)
		return true
	})

	if delta := state.LoadResidentDelta(); delta != nil {
		switch plan.RankMetric {
		case metric.MetricCosine:
			for _, entry := range delta.Snapshot() {
				if len(entry.Vec) == len(plan.QueryVec) {
					topK.Push(entry.RowID, metric.CosineDistanceUnit(plan.QueryVec, entry.Vec))
				}
			}
		default:
			for _, entry := range delta.Snapshot() {
				if len(entry.Vec) == len(plan.QueryVec) {
					topK.Push(entry.RowID, metric.Distance(plan.RankMetric, plan.QueryVec, entry.Vec))
				}
			}
		}
	}

	if len(sqliteClusterIDs) == 0 {
		return topK.Drain(), true, nil
	}

	conn, err := h.dbManager.GetDatabaseReadConnection(plan.Database)
	if err != nil {
		return nil, false, fmt.Errorf("MARMOT-VEC-030: get db for packed rank: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := scanVecSharedClusters(ctx, conn, plan.IndexName, sqliteClusterIDs, func(_cid, rowid int64, vecBytes []byte) error {
		push(rowid, vecBytes)
		return nil
	}); err != nil {
		return nil, false, fmt.Errorf("MARMOT-VEC-030: packed rank delta scan: %w", err)
	}

	return topK.Drain(), true, nil
}

func (h *CoordinatorHandler) tryDirectPKResult(
	plan *GoRankPlan,
	topK []rankItem,
) (*protocol.ResultSet, bool, error) {
	if len(topK) == 0 || plan.DirectPKColumn == "" {
		return nil, false, nil
	}
	pk, err := h.dbManager.GetAutoIncrementColumn(plan.Database, plan.BaseTable)
	if err != nil {
		return nil, false, nil
	}
	if pk != plan.DirectPKColumn {
		return nil, false, nil
	}
	rows := make([][]interface{}, len(topK))
	for i, item := range topK {
		rows[i] = []interface{}{item.rowid}
	}
	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{{Name: plan.DirectPKLabel, Type: 0xFD}},
		Rows:    rows,
	}, true, nil
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
	if rs, ok, err := h.tryDirectPKResult(plan, topK); err != nil {
		return nil, err
	} else if ok {
		return rs, nil
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

func detectDirectPKProjection(sel *sqlparser.Select, alias string) (column, label string) {
	if len(sel.From) != 1 || sel.SelectExprs == nil || len(sel.SelectExprs.Exprs) != 1 {
		return "", ""
	}
	expr, ok := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return "", ""
	}
	col, ok := expr.Expr.(*sqlparser.ColName)
	if !ok {
		return "", ""
	}
	if !col.Qualifier.IsEmpty() && col.Qualifier.Name.String() != alias {
		return "", ""
	}
	if !expr.As.IsEmpty() {
		return col.Name.String(), expr.As.String()
	}
	return col.Name.String(), col.Name.String()
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
