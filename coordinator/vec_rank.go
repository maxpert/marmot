package coordinator

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	vecmaterialize "github.com/maxpert/marmot/modules/vecindex/pkg/materialize"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/protocol"
	"vitess.io/vitess/go/vt/sqlparser"
)

// GoRankPlan carries everything needed to execute the Go-side ranking path:
// scan candidate rows from the local segment store plus overlay, compute
// distances in Go, then fetch the top-K projection from the base table.
type GoRankPlan struct {
	RawQueryVec    []byte
	QueryVec       []float32
	QueryNorm2     float32
	RankMetric     metric.Metric
	Nprobe         int
	UseBudgetProbe bool
	ScanBudgetRows int
	K              int
	Shortlist      int
	Database       string
	BaseTable      string
	BaseAlias      string // empty → use BaseTable

	IndexName           string
	IndexSpec           vecindex.IVFSpec
	ClusterIDs          []int64 // probed stable clusters; overlay merge handles fresh writes
	ProbeEpoch          uint64  // epoch of the probe set that produced ClusterIDs; enables reprobe on reindex races
	ProbeSet            *kmeans.CentroidSet
	TargetPartitionSize int

	EmbedColumn string
	// CandidateArgFilter holds indices into rewrittenArgs that correspond to
	// placeholders inside UserPredicateSQL. Computed once by BuildGoRankPlan so
	// executeGoRankPlan can resolve cheaply for the final projection fetch.
	CandidateArgFilter []int

	FinalSelectList  string
	FinalFromClause  string
	UserPredicateSQL string
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
	queryNorm2 := float32(0)
	if metricKindToRankMetric(metricKind) == metric.MetricL2 {
		queryNorm2 = metric.Norm2(qf)
	}

	// Resolve alias.
	alias := tableAlias
	if alias == "" {
		alias = meta.TableName
	}

	finalSelectList := sqlparser.String(sel.SelectExprs)
	finalFromClause := sqlparser.String(sqlparser.TableExprs(sel.From))
	directPKColumn, directPKLabel := detectDirectPKProjection(sel, alias)

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

	userPredicateSQL := ""
	if userPred != nil {
		userPredicateSQL = sqlparser.String(userPred)
	}

	return &GoRankPlan{
		RawQueryVec:    append([]byte(nil), queryVec...),
		QueryVec:       qf,
		QueryNorm2:     queryNorm2,
		RankMetric:     metricKindToRankMetric(metricKind),
		Nprobe:         nprobe,
		UseBudgetProbe: meta.AutoTuneNprobe && nprobe == meta.Nprobe,
		ScanBudgetRows: defaultProbeScanBudgetRows(meta.TargetPartitionSize),
		K:              k,
		Shortlist:      exactRerankShortlist(k),
		Database:       meta.Database,
		BaseTable:      meta.TableName,
		BaseAlias:      tableAlias,
		IndexName:      meta.IndexName,
		IndexSpec: vecindex.IVFSpec{
			ID:      meta.IndexName,
			Dim:     meta.Dim,
			Metric:  metricKind,
			Nlist:   meta.Nlist,
			Nprobe:  nprobe,
			MaxNorm: meta.MaxNorm,
		},
		ClusterIDs:          clusterIDs,
		TargetPartitionSize: meta.TargetPartitionSize,
		EmbedColumn:         meta.ColumnName,
		CandidateArgFilter:  candidateArgFilter,
		FinalSelectList:     finalSelectList,
		FinalFromClause:     finalFromClause,
		UserPredicateSQL:    userPredicateSQL,
		HasUserPredicate:    userPred != nil,
		DirectPKColumn:      directPKColumn,
		DirectPKLabel:       directPKLabel,
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
	items, ok, err := h.segmentRank(plan)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("MARMOT-VEC-030: local vector store unavailable for index %q", plan.IndexName)
	}
	if len(items) == 0 {
		return &protocol.ResultSet{}, nil
	}

	// Vector read path: use the read-only pool (multiple connections, WAL
	// concurrent readers). The write handle is single-conn + _txlock=immediate
	// and would serialise every ranked lookup against every other reader and
	// any inflight writer — a ~50,000× slowdown on 1M-row benches.
	readDB, err := h.dbManager.GetDatabaseReadConnection(plan.Database)
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: get db for go-rank: %w", err)
	}
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	readConn, err := readDB.Conn(queryCtx)
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: get read conn for go-rank: %w", err)
	}
	defer func() {
		_, _ = readConn.ExecContext(context.Background(), "ROLLBACK")
		_ = readConn.Close()
	}()
	if _, err := readConn.ExecContext(queryCtx, "BEGIN"); err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: begin go-rank read txn: %w", err)
	}

	userArgs, err := resolvePlanArgs(plan, rewrittenArgs)
	if err != nil {
		return nil, err
	}

	items, err = h.exactRerankCandidates(queryCtx, readConn, plan, userArgs, items)
	if err != nil {
		return nil, err
	}
	if rs, ok, err := h.tryDirectPKResult(plan, items); err != nil {
		return nil, err
	} else if ok {
		return rs, nil
	}
	return h.fetchProjectionByRowID(queryCtx, readConn, plan, userArgs, items)
}

func resolvePlanArgs(plan *GoRankPlan, rewrittenArgs []interface{}) ([]interface{}, error) {
	if plan == nil || len(plan.CandidateArgFilter) == 0 {
		return nil, nil
	}
	args := make([]interface{}, len(plan.CandidateArgFilter))
	for i, idx := range plan.CandidateArgFilter {
		if idx >= len(rewrittenArgs) {
			return nil, fmt.Errorf("MARMOT-VEC-030: candidate arg index %d out of range (have %d rewritten args)", idx, len(rewrittenArgs))
		}
		args[i] = rewrittenArgs[idx]
	}
	return args, nil
}

func metricKindToRankMetric(m metric.Metric) metric.Metric {
	if m == metric.MetricDot {
		return metric.MetricL2
	}
	return m
}

func defaultProbeScanBudgetRows(targetPartitionSize int) int {
	if targetPartitionSize <= 0 {
		targetPartitionSize = 512
	}
	budget := 8192
	if widened := 16 * targetPartitionSize; widened > budget {
		budget = widened
	}
	return budget
}

func pqProbeScanBudgetRows(targetPartitionSize int) int {
	if targetPartitionSize <= 0 {
		targetPartitionSize = 512
	}
	budget := defaultProbeScanBudgetRows(targetPartitionSize)
	if widened := 48 * targetPartitionSize; widened > budget {
		budget = widened
	}
	return budget
}

func loadLiveClusterRowCounts(state *vecindex.IndexState, segments *vecindex.SegmentGeneration) []uint64 {
	if state != nil {
		if maintenance := state.LoadMaintenanceState(); maintenance != nil {
			if counts := maintenance.LiveClusterRowCounts(); len(counts) > 0 {
				return counts
			}
		}
	}
	if segments != nil && len(segments.ClusterRowCounts) > 0 {
		return segments.ClusterRowCounts
	}
	if segments != nil && segments.Data != nil {
		return segments.Data.ClusterRowCounts()
	}
	return nil
}

func probeCountsUsable(counts []uint64, centroids *kmeans.CentroidSet) bool {
	if centroids == nil || len(counts) != centroids.Len()+1 {
		return false
	}
	for clusterID := 1; clusterID < len(counts); clusterID++ {
		if counts[clusterID] > 0 {
			return true
		}
	}
	return false
}

func selectProbeClusterIDs(
	plan *GoRankPlan,
	state *vecindex.IndexState,
	centroids *kmeans.CentroidSet,
	segments *vecindex.SegmentGeneration,
) []int64 {
	if plan == nil || state == nil || centroids == nil {
		if plan == nil {
			return nil
		}
		return plan.ClusterIDs
	}
	if !plan.UseBudgetProbe {
		return refreshFixedProbeClusterIDs(plan, state)
	}
	counts := loadLiveClusterRowCounts(state, segments)
	if !probeCountsUsable(counts, centroids) {
		return refreshFixedProbeClusterIDs(plan, state)
	}
	ids, _, err := centroids.AssignTopN(plan.QueryVec, centroids.Len(), plan.IndexSpec.InternalMetric())
	if err != nil || len(ids) == 0 {
		return refreshFixedProbeClusterIDs(plan, state)
	}
	budget := uint64(plan.ScanBudgetRows)
	if budget == 0 {
		budget = uint64(defaultProbeScanBudgetRows(plan.TargetPartitionSize))
	}
	if segments != nil && segments.Data != nil && segments.Data.Encoding() == vecindex.MemberEncodingResidualPQ8 {
		budget = uint64(pqProbeScanBudgetRows(plan.TargetPartitionSize))
	}
	selected := make([]int64, 0, len(ids))
	var cumulative uint64
	for _, id := range ids {
		clusterID := int64(id) + 1
		selected = append(selected, clusterID)
		if int(clusterID) < len(counts) {
			cumulative += counts[clusterID]
		}
		if cumulative >= budget {
			break
		}
	}
	if len(selected) == 0 {
		return refreshFixedProbeClusterIDs(plan, state)
	}
	plan.ProbeEpoch = centroids.Epoch()
	return selected
}

func refreshFixedProbeClusterIDs(plan *GoRankPlan, state *vecindex.IndexState) []int64 {
	if plan == nil || state == nil || len(plan.RawQueryVec) == 0 || plan.Nprobe <= 0 {
		if plan == nil {
			return nil
		}
		return plan.ClusterIDs
	}
	if state.ProbeVersion() == 0 || state.ProbeVersion() == plan.ProbeEpoch {
		return plan.ClusterIDs
	}
	clusterIDs, epoch, err := state.TopNprobeClustersWithEpoch(plan.RawQueryVec, plan.Nprobe)
	if err != nil || epoch == 0 || epoch == plan.ProbeEpoch {
		return plan.ClusterIDs
	}
	plan.ProbeEpoch = epoch
	return clusterIDs
}

func (h *CoordinatorHandler) refreshProbeClusterIDs(plan *GoRankPlan, state *vecindex.IndexState) []int64 {
	if plan == nil || state == nil {
		return refreshFixedProbeClusterIDs(plan, state)
	}
	cs := state.ProbeState()
	if cs == nil || cs.Epoch() == plan.ProbeEpoch {
		return plan.ClusterIDs
	}
	return selectProbeClusterIDs(plan, state, cs, state.LoadSegmentStore())
}

func exactRerankShortlist(k int) int {
	n := k * 8
	if n < 64 {
		n = 64
	}
	if n > 256 {
		n = 256
	}
	return n
}

func pqExactRerankShortlist(k int) int {
	n := k * 12
	if n < 128 {
		n = 128
	}
	if n > 512 {
		n = 512
	}
	return n
}

func (h *CoordinatorHandler) loadProbeCentroids(plan *GoRankPlan) (*kmeans.CentroidSet, error) {
	if plan != nil && plan.ProbeSet != nil {
		return plan.ProbeSet, nil
	}
	provider := h.loadVectorEngine()
	if provider == nil {
		return nil, nil
	}
	stateProvider, ok := provider.(interface {
		Lookup(indexName string) (*vecindex.IndexState, bool)
	})
	if !ok {
		return nil, nil
	}
	state, ok := stateProvider.Lookup(plan.IndexName)
	if !ok || state == nil {
		return nil, nil
	}
	cs := state.ProbeState()
	if plan != nil {
		plan.ProbeSet = cs
	}
	return cs, nil
}

func (h *CoordinatorHandler) segmentRank(plan *GoRankPlan) ([]rankItem, bool, error) {
	if plan == nil {
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

	// Read overlay before the stable generation. Maintenance publishes a new
	// generation before compacting overlay entries up to that watermark, so
	// this ordering prevents observing an old generation together with a
	// compacted overlay snapshot.
	overlay := state.LoadOverlay()
	segments := state.LoadSegmentStore()
	if (segments == nil || segments.Data == nil) && overlay == nil {
		return nil, false, nil
	}
	segmentEnc := int64(vecindex.MemberEncodingRawPreparedF32)
	var appliedOverlaySeq uint64
	if segments != nil && segments.Data != nil {
		segmentEnc = segments.Data.Encoding()
		appliedOverlaySeq = segments.AppliedOverlaySeq
	}

	topK := newTopKHeap(rankShortlistLimitForEncoding(plan, segmentEnc))
	overlaySnapshot := (*vecindex.OverlaySnapshot)(nil)
	if overlay != nil {
		overlaySnapshot = overlay.Snapshot()
	}

	var probeCentroids *kmeans.CentroidSet
	if state != nil {
		probeCentroids = state.ProbeState()
	}
	if probeCentroids == nil {
		var err error
		probeCentroids, err = h.loadProbeCentroids(plan)
		if err != nil {
			return nil, false, err
		}
	}
	plan.ProbeSet = probeCentroids
	plan.ClusterIDs = selectProbeClusterIDs(plan, state, probeCentroids, segments)

	if segments != nil && segments.Data != nil {
		stableCentroids := segments.StableCentroids
		if stableCentroids == nil {
			stableCentroids = probeCentroids
		}
		stableCodec := segments.StableCodec
		if stableCodec == nil {
			var err error
			stableCodec, err = vecindex.NewStableMemberCodec(plan.IndexSpec, stableCentroids, segmentEnc, nil)
			if err != nil {
				return nil, false, err
			}
		}
		scorerCache := make(map[int64]*vecindex.StableMemberScorer, len(plan.ClusterIDs))
		queryScorer, err := vecindex.NewStableMemberQueryScorerWithCodec(stableCodec, plan.QueryVec, plan.QueryNorm2)
		if err != nil {
			return nil, false, err
		}
		var encodedScanErr error
		if segmentEnc == vecindex.MemberEncodingResidualInt8 || segmentEnc == vecindex.MemberEncodingResidualPQ8 {
			distBuf := make([]float32, 0, 256)
			if err := segments.Data.ScanClustersFileOrderSpans(plan.ClusterIDs, func(clusterID int64, rows []byte, count uint64, entrySize int) bool {
				scorer, ok := scorerCache[clusterID]
				if !ok {
					var err error
					scorer, err = queryScorer.ClusterScorer(clusterID)
					if err != nil {
						encodedScanErr = err
						return false
					}
					scorerCache[clusterID] = scorer
				}
				n := int(count)
				if cap(distBuf) < n {
					distBuf = make([]float32, n)
				}
				dists := distBuf[:n]
				if err := scorer.ScoreSpan(rows, entrySize, dists); err != nil {
					encodedScanErr = err
					return false
				}
				cursor := 0
				for i := 0; i < n; i++ {
					rowid := int64(binary.LittleEndian.Uint64(rows[cursor : cursor+8]))
					if overlaySnapshot != nil {
						if _, ok := overlaySnapshot.RowClusterAfter(rowid, appliedOverlaySeq); ok {
							cursor += entrySize
							continue
						}
					}
					if overlaySnapshot != nil && overlaySnapshot.HasTombstoneAfter(rowid, appliedOverlaySeq) {
						cursor += entrySize
						continue
					}
					topK.Push(rowid, dists[i])
					cursor += entrySize
				}
				return true
			}); err != nil {
				return nil, false, fmt.Errorf("MARMOT-VEC-030: segment scan failed: %w", err)
			}
		} else if err := segments.Data.ScanClustersFileOrder(plan.ClusterIDs, func(clusterID, rowid int64, vecBytes []byte) bool {
			if overlaySnapshot != nil {
				if _, ok := overlaySnapshot.RowClusterAfter(rowid, appliedOverlaySeq); ok {
					return true
				}
			}
			if overlaySnapshot != nil && overlaySnapshot.HasTombstoneAfter(rowid, appliedOverlaySeq) {
				return true
			}
			scorer, ok := scorerCache[clusterID]
			if !ok {
				var err error
				scorer, err = queryScorer.ClusterScorer(clusterID)
				if err != nil {
					encodedScanErr = err
					return false
				}
				scorerCache[clusterID] = scorer
			}
			dist, err := scorer.Score(vecBytes)
			if err != nil {
				encodedScanErr = err
				return false
			}
			topK.Push(rowid, dist)
			return true
		}); err != nil {
			return nil, false, fmt.Errorf("MARMOT-VEC-030: segment scan failed: %w", err)
		}
		if encodedScanErr != nil {
			return nil, false, fmt.Errorf("MARMOT-VEC-030: encoded segment scoring failed: %w", encodedScanErr)
		}
		state.RecordClusterHits(plan.ClusterIDs)
	}

	if overlaySnapshot != nil {
		visitCluster := func(clusterID int64) {
			overlaySnapshot.VisitClusterAfter(clusterID, appliedOverlaySeq, func(rowID int64, vec []byte) bool {
				if len(vec) != len(plan.QueryVec)*4 {
					return true
				}
				switch plan.RankMetric {
				case metric.MetricCosine:
					topK.Push(rowID, metric.CosineDistanceUnitFromBytes(plan.QueryVec, vec))
				default:
					topK.Push(rowID, metric.DistanceFromBytes(plan.RankMetric, plan.QueryVec, vec))
				}
				return true
			})
		}
		if probeCentroids == nil || len(plan.ClusterIDs) == 0 {
			overlaySnapshot.VisitAllAfter(appliedOverlaySeq, func(_clusterID, rowID int64, vec []byte) bool {
				if len(vec) != len(plan.QueryVec)*4 {
					return true
				}
				switch plan.RankMetric {
				case metric.MetricCosine:
					topK.Push(rowID, metric.CosineDistanceUnitFromBytes(plan.QueryVec, vec))
				default:
					topK.Push(rowID, metric.DistanceFromBytes(plan.RankMetric, plan.QueryVec, vec))
				}
				return true
			})
		} else {
			visitCluster(0)
			for _, cid := range plan.ClusterIDs {
				visitCluster(cid)
			}
		}
	}
	return topK.Drain(), true, nil
}

func rankShortlistLimit(plan *GoRankPlan) int {
	if plan == nil {
		return 0
	}
	if plan.Shortlist > plan.K {
		return plan.Shortlist
	}
	return plan.K
}

func rankShortlistLimitForEncoding(plan *GoRankPlan, enc int64) int {
	limit := rankShortlistLimit(plan)
	if plan == nil || enc != vecindex.MemberEncodingResidualPQ8 {
		return limit
	}
	pqLimit := pqExactRerankShortlist(plan.K)
	if pqLimit > limit {
		return pqLimit
	}
	return limit
}

type sqlQueryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

func (h *CoordinatorHandler) exactRerankCandidates(
	ctx context.Context,
	conn sqlQueryer,
	plan *GoRankPlan,
	userArgs []interface{},
	candidates []rankItem,
) ([]rankItem, error) {
	if len(candidates) <= plan.K {
		return candidates, nil
	}

	var sb strings.Builder
	alias := plan.alias()
	sb.WriteString("SELECT `")
	sb.WriteString(alias)
	sb.WriteString("`.`rowid`, `")
	sb.WriteString(alias)
	sb.WriteString("`.`")
	sb.WriteString(plan.EmbedColumn)
	sb.WriteString("` FROM `")
	sb.WriteString(plan.BaseTable)
	sb.WriteString("` AS `")
	sb.WriteString(alias)
	sb.WriteString("` WHERE `")
	sb.WriteString(alias)
	sb.WriteString("`.`rowid` IN (")
	for i, item := range candidates {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatInt(item.rowid, 10))
	}
	sb.WriteByte(')')
	if plan.UserPredicateSQL != "" {
		sb.WriteString(" AND (")
		sb.WriteString(plan.UserPredicateSQL)
		sb.WriteByte(')')
	}

	rows, err := conn.QueryContext(ctx, sb.String(), userArgs...)
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: exact rerank query failed: %w", err)
	}
	defer rows.Close()

	topK := newTopKHeap(plan.K)
	for rows.Next() {
		var rowid int64
		var raw []byte
		if err := rows.Scan(&rowid, &raw); err != nil {
			return nil, fmt.Errorf("MARMOT-VEC-030: exact rerank scan failed: %w", err)
		}
		if raw == nil {
			continue
		}
		prepared, err := vecmaterialize.VectorBlob(raw, plan.IndexSpec.Metric, plan.IndexSpec.Dim, plan.IndexSpec.MaxNorm)
		if err != nil {
			return nil, fmt.Errorf("MARMOT-VEC-030: exact rerank materialize failed: %w", err)
		}
		if prepared == nil {
			continue
		}
		switch plan.RankMetric {
		case metric.MetricCosine:
			topK.Push(rowid, metric.CosineDistanceUnitFromBytes(plan.QueryVec, prepared))
		default:
			topK.Push(rowid, metric.DistanceFromBytes(plan.RankMetric, plan.QueryVec, prepared))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: exact rerank iter failed: %w", err)
	}
	return topK.Drain(), nil
}

func (h *CoordinatorHandler) tryDirectPKResult(
	plan *GoRankPlan,
	topK []rankItem,
) (*protocol.ResultSet, bool, error) {
	if len(topK) == 0 || plan.DirectPKColumn == "" || plan.HasUserPredicate {
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
// columns in ascending-distance order for the top-K rowids. userArgs are the
// positional values referenced by UserPredicateSQL when the original query had
// a post-filter predicate.
func (h *CoordinatorHandler) fetchProjectionByRowID(
	ctx context.Context,
	conn sqlQueryer,
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
	if plan.UserPredicateSQL != "" {
		fsb.WriteString(") AND (")
		fsb.WriteString(plan.UserPredicateSQL)
		fsb.WriteByte(')')
	} else {
		fsb.WriteByte(')')
	}
	fsb.WriteString(" ORDER BY CASE `")
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
