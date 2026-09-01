package coordinator

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
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
	// ScanBudgetRows is an optional test/legacy override. Production auto mode
	// leaves it unset so selectProbeClusterIDs can adapt to index/query shape.
	ScanBudgetRows int
	K              int // final output row count; kept as a compatibility alias for LimitK
	LimitK         int
	CandidateK     int
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
	useBudgetProbe bool,
	tableAlias string,
	limitK int,
	candidateK int,
) (*GoRankPlan, error) {
	if len(queryVec)%4 != 0 {
		return nil, fmt.Errorf("MARMOT-VEC-030: queryVec length %d is not a multiple of 4", len(queryVec))
	}
	if limitK < 1 {
		return nil, fmt.Errorf("MARMOT-VEC-030: LIMIT must be positive")
	}
	if candidateK < limitK {
		return nil, fmt.Errorf("MARMOT-VEC-030: vec_match candidate budget %d is smaller than LIMIT %d", candidateK, limitK)
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
		UseBudgetProbe: useBudgetProbe,
		K:              limitK,
		LimitK:         limitK,
		CandidateK:     candidateK,
		Shortlist:      exactRerankShortlist(limitK),
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

	var items []rankItem
	for {
		var ok bool
		items, ok, err = h.segmentRank(plan)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("MARMOT-VEC-030: local vector store unavailable for index %q", plan.IndexName)
		}
		if len(items) == 0 {
			return &protocol.ResultSet{}, nil
		}
		items, err = h.exactRerankCandidates(queryCtx, readConn, plan, userArgs, items)
		if err != nil {
			return nil, err
		}
		if !plan.HasUserPredicate || len(items) >= plan.finalLimit() || !plan.growRefillBudget() {
			break
		}
	}
	if rs, ok, err := h.tryDirectPKResult(plan, items); err != nil {
		return nil, err
	} else if ok {
		return rs, nil
	}
	return h.fetchProjectionByRowID(queryCtx, readConn, plan, userArgs, items)
}

const goRankRefillMaxCandidateK = 4096

func (p *GoRankPlan) growRefillBudget() bool {
	if p == nil {
		return false
	}
	current := p.candidateBudget()
	if current <= 0 {
		current = p.finalLimit()
	}
	maxBudget := goRankRefillMaxCandidateK
	if p.CandidateK > maxBudget {
		maxBudget = p.CandidateK
	}
	if current >= maxBudget {
		return false
	}
	next := current * 2
	if next < current+1 {
		next = current + 1
	}
	if next > maxBudget {
		next = maxBudget
	}
	p.CandidateK = next
	return true
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
		targetPartitionSize = common.DefaultVectorTargetPartitionSize
	}
	budget := 8192
	if widened := 16 * targetPartitionSize; widened > budget {
		budget = widened
	}
	return budget
}

type probeSelectionPolicy struct {
	budgetRows uint64
	minProbe   int
	maxProbe   int
}

func probeTargetPartitionSize(targetPartitionSize int) int {
	if targetPartitionSize <= 0 {
		return common.DefaultVectorTargetPartitionSize
	}
	return targetPartitionSize
}

func ceilDivInt(n, d int) int {
	if d <= 0 {
		return 0
	}
	return (n + d - 1) / d
}

func ceilSqrtInt(n int) int {
	if n <= 0 {
		return 0
	}
	return int(math.Ceil(math.Sqrt(float64(n))))
}

func scaleProbeCount(n, numerator, denominator int) int {
	if n <= 0 || numerator <= 0 || denominator <= 0 {
		return n
	}
	return ceilDivInt(n*numerator, denominator)
}

func autoProbePartitionCount(nlist, targetPartitionSize, k int, metricKind metric.Metric, encoding int64) int {
	if nlist <= 0 {
		return 1
	}
	target := probeTargetPartitionSize(targetPartitionSize)
	partitions := ceilDivInt(defaultProbeScanBudgetRows(target), target)
	if nlistFloor := ceilSqrtInt(nlist); nlistFloor > partitions {
		partitions = nlistFloor
	}
	if k > 0 {
		// Larger K needs a broader IVF prefix to keep enough candidates alive
		// for exact rerank after approximate stable scoring.
		if kFloor := ceilDivInt(k*64, target); kFloor > partitions {
			partitions = kFloor
		}
	}
	switch metricKind {
	case metric.MetricCosine:
		partitions = scaleProbeCount(partitions, 5, 4)
	case metric.MetricDot:
		partitions = scaleProbeCount(partitions, 3, 2)
	}
	if encoding == vecindex.MemberEncodingResidualPQ8 {
		partitions = scaleProbeCount(partitions, 6, 5)
	}
	if partitions < 1 {
		partitions = 1
	}
	if partitions > nlist {
		partitions = nlist
	}
	return partitions
}

func autoProbePolicy(plan *GoRankPlan, encoding int64, centroidCount int) probeSelectionPolicy {
	if centroidCount <= 0 {
		return probeSelectionPolicy{}
	}
	target := probeTargetPartitionSize(plan.TargetPartitionSize)
	nlist := plan.IndexSpec.Nlist
	if nlist <= 0 || nlist > centroidCount {
		nlist = centroidCount
	}
	minProbe := autoProbePartitionCount(nlist, target, plan.candidateBudget(), plan.IndexSpec.Metric, encoding)
	if plan.Nprobe > minProbe {
		minProbe = plan.Nprobe
	}
	if minProbe > centroidCount {
		minProbe = centroidCount
	}
	maxProbe := minProbe * 4
	if maxProbe < 32 {
		maxProbe = 32
	}
	if maxProbe > centroidCount {
		maxProbe = centroidCount
	}
	return probeSelectionPolicy{
		budgetRows: uint64(minProbe) * uint64(target),
		minProbe:   minProbe,
		maxProbe:   maxProbe,
	}
}

func rowBudgetProbePolicy(plan *GoRankPlan, centroidCount int) probeSelectionPolicy {
	if centroidCount <= 0 {
		return probeSelectionPolicy{}
	}
	target := probeTargetPartitionSize(plan.TargetPartitionSize)
	budget := uint64(plan.ScanBudgetRows)
	if budget == 0 {
		budget = uint64(defaultProbeScanBudgetRows(target))
	}
	minProbe := plan.Nprobe
	if minProbe <= 0 {
		minProbe = 1
	}
	if minProbe > centroidCount {
		minProbe = centroidCount
	}
	estimatedProbe := int((budget + uint64(target) - 1) / uint64(target))
	maxProbe := estimatedProbe * 4
	if maxProbe < minProbe {
		maxProbe = minProbe
	}
	if maxProbe < 32 {
		maxProbe = 32
	}
	if maxProbe > centroidCount {
		maxProbe = centroidCount
	}
	return probeSelectionPolicy{
		budgetRows: budget,
		minProbe:   minProbe,
		maxProbe:   maxProbe,
	}
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
	encoding := int64(vecindex.MemberEncodingResidualInt8)
	if segments != nil && segments.Data != nil {
		encoding = segments.Data.Encoding()
	}
	policy := autoProbePolicy(plan, encoding, centroids.Len())
	if plan.ScanBudgetRows > 0 {
		policy = rowBudgetProbePolicy(plan, centroids.Len())
	}
	counts := loadLiveClusterRowCounts(state, segments)
	if !probeCountsUsable(counts, centroids) {
		if plan.ScanBudgetRows == 0 {
			return refreshAutoProbeClusterIDs(plan, state, centroids, policy.minProbe)
		}
		return refreshFixedProbeClusterIDs(plan, state)
	}
	rowCounts := counts[1:]
	ids, _, err := centroids.AssignTopNUntilBudget(plan.QueryVec, rowCounts, policy.budgetRows, policy.minProbe, policy.maxProbe, plan.IndexSpec.InternalMetric())
	if err != nil || len(ids) == 0 {
		return refreshFixedProbeClusterIDs(plan, state)
	}
	var cumulative uint64
	for _, id := range ids {
		cumulative += rowCounts[id]
	}
	if cumulative < policy.budgetRows && policy.maxProbe < centroids.Len() {
		ids, _, err = centroids.AssignTopNUntilBudget(plan.QueryVec, rowCounts, policy.budgetRows, policy.minProbe, centroids.Len(), plan.IndexSpec.InternalMetric())
		if err != nil || len(ids) == 0 {
			return refreshFixedProbeClusterIDs(plan, state)
		}
	}
	selected := make([]int64, 0, len(ids))
	for _, id := range ids {
		clusterID := int64(id) + 1
		selected = append(selected, clusterID)
	}
	if len(selected) == 0 {
		return refreshFixedProbeClusterIDs(plan, state)
	}
	plan.ProbeEpoch = centroids.Epoch()
	return selected
}

func refreshAutoProbeClusterIDs(plan *GoRankPlan, state *vecindex.IndexState, centroids *kmeans.CentroidSet, nprobe int) []int64 {
	if plan == nil || state == nil || centroids == nil || nprobe <= 0 || len(plan.QueryVec) == 0 {
		return refreshFixedProbeClusterIDs(plan, state)
	}
	ids, _, err := centroids.AssignTopN(plan.QueryVec, nprobe, plan.IndexSpec.InternalMetric())
	if err != nil || len(ids) == 0 {
		return refreshFixedProbeClusterIDs(plan, state)
	}
	selected := make([]int64, 0, len(ids))
	for _, id := range ids {
		selected = append(selected, int64(id)+1)
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
	n := k * 14
	if n < 144 {
		n = 144
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
	var state *vecindex.IndexState
	var releaseState func()
	if refProvider, ok := provider.(interface {
		LookupRef(indexName string) (*vecindex.IndexState, func(), bool)
	}); ok {
		state, releaseState, ok = refProvider.LookupRef(plan.IndexName)
	} else {
		state, ok = stateProvider.Lookup(plan.IndexName)
	}
	if !ok || state == nil {
		return nil, false, nil
	}
	if releaseState != nil {
		defer releaseState()
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
	segmentEnc := int64(vecindex.MemberEncodingResidualInt8)
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
		queryScorer, err := vecindex.NewStableMemberQueryScorerWithCodec(stableCodec, plan.QueryVec, plan.QueryNorm2)
		if err != nil {
			return nil, false, err
		}
		if err := h.scanStableGeneration(plan, segments, queryScorer, segmentEnc, appliedOverlaySeq, overlaySnapshot, topK); err != nil {
			return nil, false, err
		}
		state.RecordClusterHits(plan.ClusterIDs)
	}

	if overlaySnapshot != nil {
		overlayScorers := make(map[int64]*vecindex.StableMemberScorer, len(plan.ClusterIDs)+1)
		var overlayScanErr error
		scoreOverlay := func(clusterID, rowID int64, encoding vecindex.OverlayVecEncoding, vec []byte) bool {
			switch encoding {
			case vecindex.OverlayPreparedF32:
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
			case vecindex.OverlayResidualInt8:
				if probeCentroids == nil || clusterID <= 0 {
					overlayScanErr = fmt.Errorf("missing probe centroid for overlay cluster %d", clusterID)
					return false
				}
				scorer, ok := overlayScorers[clusterID]
				if !ok {
					var err error
					scorer, err = vecindex.NewStableMemberScorer(plan.IndexSpec, probeCentroids, plan.QueryVec, plan.QueryNorm2, clusterID, vecindex.MemberEncodingResidualInt8)
					if err != nil {
						overlayScanErr = err
						return false
					}
					overlayScorers[clusterID] = scorer
				}
				dist, err := scorer.Score(vec)
				if err != nil {
					overlayScanErr = err
					return false
				}
				topK.Push(rowID, dist)
				return true
			default:
				overlayScanErr = fmt.Errorf("unsupported overlay vector encoding %d", encoding)
				return false
			}
		}
		visitCluster := func(clusterID int64) bool {
			overlaySnapshot.VisitClusterEncodedAfter(clusterID, appliedOverlaySeq, func(rowID int64, encoding vecindex.OverlayVecEncoding, vec []byte) bool {
				return scoreOverlay(clusterID, rowID, encoding, vec)
			})
			return overlayScanErr == nil
		}
		if probeCentroids == nil || len(plan.ClusterIDs) == 0 {
			overlaySnapshot.VisitAllEncodedAfter(appliedOverlaySeq, func(clusterID, rowID int64, encoding vecindex.OverlayVecEncoding, vec []byte) bool {
				return scoreOverlay(clusterID, rowID, encoding, vec)
			})
		} else {
			if !visitCluster(0) {
				return nil, false, fmt.Errorf("MARMOT-VEC-030: overlay scan failed: %w", overlayScanErr)
			}
			for _, cid := range plan.ClusterIDs {
				if !visitCluster(cid) {
					return nil, false, fmt.Errorf("MARMOT-VEC-030: overlay scan failed: %w", overlayScanErr)
				}
			}
		}
		if overlayScanErr != nil {
			return nil, false, fmt.Errorf("MARMOT-VEC-030: overlay scan failed: %w", overlayScanErr)
		}
	}
	return topK.Drain(), true, nil
}

type blockPruneMode int

const (
	// blockPruneSafe is safe only relative to Marmot's compressed approximate
	// candidate scan. It is not an exact-recall guarantee; exact rerank only
	// sees the approximate shortlist that survives this stage.
	blockPruneSafe blockPruneMode = iota
	blockPruneOff
	blockPruneShadow
)

const blockPruneMargin = 1e-5

type scoredBlockRecord struct {
	record     vecindex.SegmentBlockRecord
	lowerBound float32
	bounded    bool
}

type blockPruneCounters struct {
	considered uint64
	wouldSkip  uint64
	skipped    uint64
	scored     uint64
	rowsScored uint64
}

func currentBlockPruneMode() blockPruneMode {
	switch strings.ToLower(os.Getenv("MARMOT_VEC_BLOCK_PRUNE_MODE")) {
	case "safe", "approx-safe", "approx_safe":
		return blockPruneSafe
	case "shadow":
		return blockPruneShadow
	default:
		return blockPruneOff
	}
}

func (h *CoordinatorHandler) scanStableGeneration(
	plan *GoRankPlan,
	segments *vecindex.SegmentGeneration,
	queryScorer *vecindex.StableMemberQueryScorer,
	segmentEnc int64,
	appliedOverlaySeq uint64,
	overlaySnapshot *vecindex.OverlaySnapshot,
	topK *topKHeap,
) error {
	scanner := newStableGenerationScanner(plan, segments, queryScorer, appliedOverlaySeq, overlaySnapshot, topK)
	if scanner.shouldUseBlockPruning(segmentEnc) {
		used, err := scanner.scanBlockPrunedClusters()
		if err != nil {
			return err
		}
		if used {
			return nil
		}
	}
	if isCompressedStableEncoding(segmentEnc) {
		return scanner.scanEncodedClusters()
	}
	return scanner.scanPerRowClusters()
}

type stableGenerationScanner struct {
	plan              *GoRankPlan
	segments          *vecindex.SegmentGeneration
	queryScorer       *vecindex.StableMemberQueryScorer
	appliedOverlaySeq uint64
	overlaySnapshot   *vecindex.OverlaySnapshot
	topK              *topKHeap
	scorerCache       map[int64]*vecindex.StableMemberScorer
	distBuf           []float32
}

func newStableGenerationScanner(
	plan *GoRankPlan,
	segments *vecindex.SegmentGeneration,
	queryScorer *vecindex.StableMemberQueryScorer,
	appliedOverlaySeq uint64,
	overlaySnapshot *vecindex.OverlaySnapshot,
	topK *topKHeap,
) *stableGenerationScanner {
	return &stableGenerationScanner{
		plan:              plan,
		segments:          segments,
		queryScorer:       queryScorer,
		appliedOverlaySeq: appliedOverlaySeq,
		overlaySnapshot:   overlaySnapshot,
		topK:              topK,
		scorerCache:       make(map[int64]*vecindex.StableMemberScorer, len(plan.ClusterIDs)),
		distBuf:           make([]float32, 0, 256),
	}
}

func (s *stableGenerationScanner) shouldUseBlockPruning(segmentEnc int64) bool {
	return !s.plan.HasUserPredicate &&
		isCompressedStableEncoding(segmentEnc) &&
		s.segments.Blocks != nil &&
		currentBlockPruneMode() != blockPruneOff
}

func isCompressedStableEncoding(enc int64) bool {
	return enc == vecindex.MemberEncodingResidualInt8 || enc == vecindex.MemberEncodingResidualPQ8
}

func (s *stableGenerationScanner) clusterScorer(clusterID int64) (*vecindex.StableMemberScorer, error) {
	scorer, ok := s.scorerCache[clusterID]
	if ok {
		return scorer, nil
	}
	scorer, err := s.queryScorer.ClusterScorer(clusterID)
	if err != nil {
		return nil, err
	}
	s.scorerCache[clusterID] = scorer
	return scorer, nil
}

func (s *stableGenerationScanner) stableRowMasked(rowid int64) bool {
	if s.overlaySnapshot == nil {
		return false
	}
	if _, ok := s.overlaySnapshot.RowClusterAfter(rowid, s.appliedOverlaySeq); ok {
		return true
	}
	return s.overlaySnapshot.HasTombstoneAfter(rowid, s.appliedOverlaySeq)
}

func (s *stableGenerationScanner) scoreEncodedRows(clusterID int64, rows []byte, count uint64, entrySize int) error {
	scorer, err := s.clusterScorer(clusterID)
	if err != nil {
		return err
	}
	n := int(count)
	if cap(s.distBuf) < n {
		s.distBuf = make([]float32, n)
	}
	dists := s.distBuf[:n]
	if err := scorer.ScoreSpan(rows, entrySize, dists); err != nil {
		return err
	}
	cursor := 0
	for i := 0; i < n; i++ {
		rowid := int64(binary.LittleEndian.Uint64(rows[cursor : cursor+8]))
		if !s.stableRowMasked(rowid) {
			s.topK.Push(rowid, dists[i])
		}
		cursor += entrySize
	}
	return nil
}

func (s *stableGenerationScanner) scanEncodedClusters() error {
	var encodedScanErr error
	if err := s.segments.Data.ScanClustersFileOrderSpans(s.plan.ClusterIDs, func(clusterID int64, rows []byte, count uint64, entrySize int) bool {
		if err := s.scoreEncodedRows(clusterID, rows, count, entrySize); err != nil {
			encodedScanErr = err
			return false
		}
		return true
	}); err != nil {
		return fmt.Errorf("MARMOT-VEC-030: segment scan failed: %w", err)
	}
	if encodedScanErr != nil {
		return fmt.Errorf("MARMOT-VEC-030: encoded segment scoring failed: %w", encodedScanErr)
	}
	return nil
}

func (s *stableGenerationScanner) scanPerRowClusters() error {
	var encodedScanErr error
	if err := s.segments.Data.ScanClustersFileOrder(s.plan.ClusterIDs, func(clusterID, rowid int64, vecBytes []byte) bool {
		if s.stableRowMasked(rowid) {
			return true
		}
		scorer, err := s.clusterScorer(clusterID)
		if err != nil {
			encodedScanErr = err
			return false
		}
		dist, err := scorer.Score(vecBytes)
		if err != nil {
			encodedScanErr = err
			return false
		}
		s.topK.Push(rowid, dist)
		return true
	}); err != nil {
		return fmt.Errorf("MARMOT-VEC-030: segment scan failed: %w", err)
	}
	if encodedScanErr != nil {
		return fmt.Errorf("MARMOT-VEC-030: encoded segment scoring failed: %w", encodedScanErr)
	}
	return nil
}

func (s *stableGenerationScanner) scanBlockPrunedClusters() (bool, error) {
	scored, err := s.readScoredBlocks()
	if err != nil || len(scored) == 0 {
		return len(scored) > 0, err
	}
	sortScoredBlocks(scored)
	mode := currentBlockPruneMode()
	const waveSize = 64
	var counters blockPruneCounters
	defer func() {
		s.segments.Blocks.RecordQueryStats(counters.considered, counters.wouldSkip, counters.skipped, counters.scored, counters.rowsScored)
	}()
	for start := 0; start < len(scored); start += waveSize {
		end := start + waveSize
		if end > len(scored) {
			end = len(scored)
		}
		toScore := s.selectBlockWave(scored[start:end], mode, &counters)
		if len(toScore) == 0 {
			continue
		}
		if err := s.scanBlockWave(toScore); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (s *stableGenerationScanner) readScoredBlocks() ([]scoredBlockRecord, error) {
	blocks, err := s.segments.Blocks.ReadClusterBlocks(s.plan.ClusterIDs)
	if err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: block metadata scan failed: %w", err)
	}
	if len(blocks) == 0 {
		return nil, nil
	}
	scored := make([]scoredBlockRecord, 0, len(blocks))
	for _, block := range blocks {
		lb, ok := s.queryScorer.BlockLowerBound(block.ClusterID, block)
		block.Stats = nil
		scored = append(scored, scoredBlockRecord{record: block, lowerBound: lb, bounded: ok})
	}
	return scored, nil
}

func sortScoredBlocks(scored []scoredBlockRecord) {
	slices.SortFunc(scored, func(a, b scoredBlockRecord) int {
		if !a.bounded && b.bounded {
			return -1
		}
		if a.bounded && !b.bounded {
			return 1
		}
		switch {
		case a.lowerBound < b.lowerBound:
			return -1
		case a.lowerBound > b.lowerBound:
			return 1
		default:
			switch {
			case a.record.DataOffset < b.record.DataOffset:
				return -1
			case a.record.DataOffset > b.record.DataOffset:
				return 1
			default:
				return 0
			}
		}
	})
}

func (s *stableGenerationScanner) selectBlockWave(wave []scoredBlockRecord, mode blockPruneMode, counters *blockPruneCounters) []vecindex.SegmentBlockRecord {
	toScore := make([]vecindex.SegmentBlockRecord, 0, len(wave))
	for _, candidate := range wave {
		counters.considered++
		skip := false
		if candidate.bounded {
			if worst, ok := s.topK.WorstDistance(); ok && candidate.lowerBound >= worst+blockPruneMargin {
				skip = true
			}
		}
		if skip {
			counters.wouldSkip++
			if mode == blockPruneSafe {
				counters.skipped++
				continue
			}
		}
		toScore = append(toScore, candidate.record)
		counters.scored++
		counters.rowsScored += candidate.record.RowCount
	}
	slices.SortFunc(toScore, func(a, b vecindex.SegmentBlockRecord) int {
		switch {
		case a.DataOffset < b.DataOffset:
			return -1
		case a.DataOffset > b.DataOffset:
			return 1
		default:
			return 0
		}
	})
	return toScore
}

func (s *stableGenerationScanner) scanBlockWave(blocks []vecindex.SegmentBlockRecord) error {
	var scanErr error
	if err := s.segments.Data.ScanBlockRecordsFileOrder(blocks, func(clusterID int64, rows []byte, count uint64, entrySize int) bool {
		if err := s.scoreEncodedRows(clusterID, rows, count, entrySize); err != nil {
			scanErr = err
			return false
		}
		return true
	}); err != nil {
		return fmt.Errorf("MARMOT-VEC-030: block segment scan failed: %w", err)
	}
	if scanErr != nil {
		return fmt.Errorf("MARMOT-VEC-030: encoded block scoring failed: %w", scanErr)
	}
	return nil
}

func rankShortlistLimit(plan *GoRankPlan) int {
	if plan == nil {
		return 0
	}
	limit := plan.finalLimit()
	if plan.Shortlist > limit {
		return plan.Shortlist
	}
	return limit
}

func rankShortlistLimitForEncoding(plan *GoRankPlan, enc int64) int {
	limit := rankShortlistLimit(plan)
	if plan == nil {
		return limit
	}
	if candidateK := plan.candidateBudget(); candidateK > limit {
		limit = candidateK
	}
	if enc != vecindex.MemberEncodingResidualPQ8 {
		return limit
	}
	pqLimit := pqExactRerankShortlist(plan.finalLimit())
	if pqLimit > limit {
		return pqLimit
	}
	return limit
}

func (p *GoRankPlan) finalLimit() int {
	if p == nil {
		return 0
	}
	if p.LimitK > 0 {
		return p.LimitK
	}
	return p.K
}

func (p *GoRankPlan) candidateBudget() int {
	if p == nil {
		return 0
	}
	limit := p.finalLimit()
	if p.CandidateK > limit {
		return p.CandidateK
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
	if len(candidates) == 0 {
		return nil, nil
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

	topK := newTopKHeap(plan.finalLimit())
	for rows.Next() {
		var rowid int64
		var raw sql.RawBytes
		if err := rows.Scan(&rowid, &raw); err != nil {
			return nil, fmt.Errorf("MARMOT-VEC-030: exact rerank scan failed: %w", err)
		}
		dist, ok, err := exactDistanceFromRaw(plan, raw)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		topK.Push(rowid, dist)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: exact rerank iter failed: %w", err)
	}
	return topK.Drain(), nil
}

func exactDistanceFromRaw(plan *GoRankPlan, raw []byte) (float32, bool, error) {
	if plan == nil || len(raw) == 0 {
		return 0, false, nil
	}
	if len(raw)%4 != 0 || len(raw)/4 != plan.IndexSpec.Dim {
		return 0, false, fmt.Errorf("MARMOT-VEC-030: exact rerank vector dim mismatch: got bytes=%d dim=%d", len(raw), plan.IndexSpec.Dim)
	}
	switch plan.IndexSpec.Metric {
	case metric.MetricCosine:
		vec := metric.BytesToFloat32(raw)
		norm := metric.Norm(vec)
		if norm == 0 {
			return 0, false, nil
		}
		return 1 - metric.DotProduct(plan.QueryVec, vec)/norm, true, nil
	case metric.MetricDot:
		vec := metric.BytesToFloat32(raw)
		norm2 := metric.Norm2(vec)
		maxNorm2 := plan.IndexSpec.MaxNorm * plan.IndexSpec.MaxNorm
		if norm2 > maxNorm2 {
			return 0, false, fmt.Errorf("MARMOT-VEC-030: exact rerank vector norm exceeds MaxNorm")
		}
		dot := metric.DotProduct(plan.QueryVec[:plan.IndexSpec.Dim], vec)
		return plan.QueryNorm2 + maxNorm2 - 2*dot, true, nil
	case metric.MetricL2:
		return metric.DistanceFromBytes(metric.MetricL2, plan.QueryVec, raw), true, nil
	default:
		return 0, false, fmt.Errorf("MARMOT-VEC-030: exact rerank unknown metric %d", plan.IndexSpec.Metric)
	}
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
	pkCol := protocol.ColumnDef{Name: plan.DirectPKLabel}
	pkCol.Type = protocol.InferColumnTypes(rows, 1)[0]
	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{pkCol},
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
	fsb.WriteString(strconv.Itoa(plan.finalLimit()))

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
		colDefs[i] = protocol.ColumnDef{Name: name}
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

	// Types follow the projected values; see InferColumnTypes.
	for i, t := range protocol.InferColumnTypes(resultRows, len(colDefs)) {
		colDefs[i].Type = t
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
