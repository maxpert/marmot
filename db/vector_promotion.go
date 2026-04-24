package db

import (
	"context"
	"database/sql"
	"errors"
	"slices"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

var errIncrementalPromotionFallback = errors.New("incremental promotion requires full rebuild")

type incrementalPromotionPlan struct {
	state          *vecindex.IndexState
	spec           vecindex.IVFSpec
	nextSpec       vecindex.IVFSpec
	nextMeta       common.VectorIndexMeta
	currentEpoch   uint64
	baseGeneration uint64
	cutoff         uint64
	nextProbe      *kmeans.CentroidSet
	pending        *pendingSegmentGeneration
}

func (p *incrementalPromotionPlan) Close() {
	if p == nil || p.pending == nil {
		return
	}
	p.pending.Close()
	p.pending = nil
}

type promotionRow struct {
	rowID int64
	vec   []float32
}

func stepPromotionClusterCount(currentK, wantK int) int {
	if wantK <= currentK {
		return currentK
	}
	nextK := max(currentK+promotionStepFloor, int(float64(currentK)*promotionGrowthFactor))
	if nextK > wantK {
		nextK = wantK
	}
	if nextK <= currentK {
		nextK = wantK
	}
	return nextK
}

func prepareIncrementalPromotion(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	state *vecindex.IndexState,
	nextClusters int,
) (*incrementalPromotionPlan, error) {
	_ = ctx
	if state == nil || nextClusters <= spec.Nlist {
		return nil, nil
	}
	base := state.LoadSegmentStore()
	overlay := state.LoadOverlay()
	currentProbe := state.ProbeState()
	if base == nil || base.Data == nil || base.RowMap == nil || currentProbe == nil || base.StableCentroids == nil {
		return nil, nil
	}
	cutoff := base.AppliedOverlaySeq
	if overlay != nil {
		if snapshot := overlay.Snapshot(); snapshot != nil && snapshot.LastSequence() > cutoff {
			return nil, nil
		}
	}
	counts := append([]uint64(nil), base.ClusterRowCounts...)
	sources := selectPromotionSplitSources(counts, currentProbe.Len(), nextClusters, maintenanceTargetClusterSize(meta))
	if len(sources) == 0 {
		return nil, nil
	}
	var sourceRows uint64
	var totalRows uint64
	for clusterID := 1; clusterID < len(counts); clusterID++ {
		totalRows += counts[clusterID]
	}
	for _, source := range sources {
		sourceRows += source.count
	}
	if totalRows > 0 && sourceRows*4 > totalRows {
		return nil, errIncrementalPromotionFallback
	}

	pinnedBase, err := openPinnedSegmentGeneration(dbPath, meta, spec, currentProbe.Epoch(), base.Data.Generation())
	if err != nil || pinnedBase == nil {
		return nil, err
	}
	defer pinnedBase.Close()

	nextProbeSnapshot := currentProbe.Snapshot()
	nextStableSnapshot := pinnedBase.StableCentroids.Snapshot()
	for len(nextProbeSnapshot) < nextClusters {
		nextProbeSnapshot = append(nextProbeSnapshot, nil)
		nextStableSnapshot = append(nextStableSnapshot, nil)
	}
	nextCounts := append([]uint64(nil), counts...)
	if len(nextCounts) < nextClusters+1 {
		nextCounts = append(nextCounts, make([]uint64, nextClusters+1-len(nextCounts))...)
	}
	nextSums := cloneClusterVectorSums(base.ClusterVectorSums)
	if len(nextSums) < nextClusters+1 {
		nextSums = append(nextSums, make([][]float32, nextClusters+1-len(nextSums))...)
	}
	mutations := make([]vecindex.OverlayMutation, 0, sourceRows)
	nextClusterID := currentProbe.Len() + 1
	targetSize := maintenanceTargetClusterSize(meta)
	if targetSize <= 0 {
		targetSize = defaultTargetPartitionSize
	}
	opts := kmeans.MiniBatchBalancedOptions{
		BatchSize:         min(max(256, targetSize*2), 4096),
		MaxIter:           4,
		TargetClusterSize: targetSize,
	}
	for _, source := range sources {
		extra := source.splits
		if extra > nextClusters-nextClusterID+1 {
			extra = nextClusters - nextClusterID + 1
		}
		if extra <= 0 {
			break
		}
		rows, err := loadPromotionRows(ctx, conn, meta, spec, pinnedBase, source.clusterID)
		if err != nil {
			return nil, err
		}
		if len(rows) <= extra {
			return nil, errIncrementalPromotionFallback
		}
		initCentroids := make([][]float32, 0, extra+1)
		initCentroids = append(initCentroids, append([]float32(nil), currentProbe.Snapshot()[source.clusterID-1]...))
		seeds, err := clusterSplitSeedsFromRows(rows, spec, source.clusterID, initCentroids[0], extra)
		if err != nil {
			return nil, err
		}
		if len(seeds) != extra {
			return nil, errIncrementalPromotionFallback
		}
		initCentroids = append(initCentroids, seeds...)
		familyCentroids, assignments, familyCounts, familySums, err := splitPromotionFamily(rows, initCentroids, spec, opts, spec.Seed^uint64(source.clusterID))
		if err != nil {
			return nil, err
		}
		targets := make([]int64, len(familyCentroids))
		targets[0] = source.clusterID
		for i := 1; i < len(targets); i++ {
			targets[i] = int64(nextClusterID)
			nextClusterID++
		}
		for i, clusterID := range targets {
			nextProbeSnapshot[clusterID-1] = append([]float32(nil), familyCentroids[i]...)
			nextStableSnapshot[clusterID-1] = append([]float32(nil), familyCentroids[i]...)
			nextCounts[clusterID] = familyCounts[i]
			nextSums[clusterID] = familySums[i]
		}
		for i, row := range rows {
			clusterID := targets[assignments[i]]
			mutations = append(mutations, vecindex.OverlayMutation{
				Kind:      vecindex.OverlayMutationReplace,
				ClusterID: clusterID,
				RowID:     row.rowID,
				Vec:       vecindex.Float32ToBytes(row.vec),
			})
		}
	}
	nextEpoch := currentProbe.Epoch() + 1
	if nextEpoch == 0 {
		nextEpoch = 1
	}
	nextProbe, err := kmeans.NewCentroidSet(nextEpoch, nextProbeSnapshot[:nextClusters])
	if err != nil {
		return nil, err
	}
	nextStable, err := kmeans.NewCentroidSet(nextEpoch, nextStableSnapshot[:nextClusters])
	if err != nil {
		return nil, err
	}

	nextMeta := meta
	nextSpec := spec
	nextMeta.Nlist = nextClusters
	nextSpec.Nlist = nextClusters
	if nextMeta.AutoTuneNprobe {
		nextMeta.Nprobe = autoTuneNprobeForTarget(nextMeta.Nlist, nextMeta.TargetPartitionSize)
		nextSpec.Nprobe = nextMeta.Nprobe
	}
	pending, err := buildIncrementalSegmentGenerationFromMutations(
		ctx,
		conn,
		dbPath,
		nextMeta,
		nextSpec,
		nextProbe,
		nextStable,
		pinnedBase,
		mutations,
		cutoff,
		nextCounts,
		nextSums,
		state.HotClusterScores(segmentLayoutHotClusterLimit),
	)
	if err != nil || pending == nil {
		return nil, err
	}
	return &incrementalPromotionPlan{
		state:          state,
		spec:           spec,
		nextSpec:       nextSpec,
		nextMeta:       nextMeta,
		currentEpoch:   currentProbe.Epoch(),
		baseGeneration: base.Data.Generation(),
		cutoff:         cutoff,
		nextProbe:      nextProbe,
		pending:        pending,
	}, nil
}

func (h *EngineHook) publishIncrementalPromotion(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	plan *incrementalPromotionPlan,
) error {
	if plan == nil || plan.pending == nil || plan.nextProbe == nil || plan.state == nil {
		return nil
	}
	h.localChangeMu.Lock()
	defer h.localChangeMu.Unlock()

	current, ok := h.engine.Lookup(meta.IndexName)
	if !ok || current != plan.state {
		return nil
	}
	currentBase := current.LoadSegmentStore()
	currentOverlay := current.LoadOverlay()
	if currentBase == nil || currentBase.Data == nil || currentOverlay == nil {
		return nil
	}
	if current.ProbeVersion() != plan.currentEpoch || currentBase.Data.Generation() != plan.baseGeneration {
		return nil
	}
	currentSnapshot := currentOverlay.Snapshot()
	if currentSnapshot == nil || currentSnapshot.Epoch() != plan.currentEpoch || currentSnapshot.LastSequence() < plan.cutoff {
		return nil
	}
	if err := plan.pending.Publish(); err != nil {
		return err
	}
	tailMutations, err := reassignOverlayMutationsForProbe(currentSnapshot, plan.cutoff, plan.nextSpec, plan.nextProbe, plan.pending.generation)
	if err != nil {
		return err
	}
	nextOverlay, err := rewriteOverlayForEpoch(dbPath, meta.IndexName, plan.nextProbe.Epoch(), plan.cutoff, tailMutations)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes SET nlist=?, nprobe=?, status='ready' WHERE index_name=?`,
		plan.nextMeta.Nlist, plan.nextMeta.Nprobe, plan.nextMeta.IndexName,
	); err != nil {
		return err
	}
	newState := vecindex.NewIndexState(plan.nextSpec, plan.nextProbe)
	newState.StoreSegmentStore(plan.pending.generation)
	plan.pending.generation = nil
	newState.StoreOverlay(nextOverlay)
	if err := syncMaintenanceStateFromOverlay(newState); err != nil {
		newState.ClearOverlay()
		newState.ClearSegmentStore()
		return err
	}
	h.engine.Register(plan.nextMeta.IndexName, newState)
	h.retireState(plan.state)
	if h.indexMgr != nil {
		h.indexMgr.storeCachedIndexMeta(&plan.nextMeta)
	}
	return nil
}

func (h *EngineHook) runIncrementalPromotion(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	state *vecindex.IndexState,
	nextClusters int,
) error {
	plan, err := prepareIncrementalPromotion(ctx, conn, dbPath, meta, spec, state, nextClusters)
	if err != nil || plan == nil {
		return err
	}
	defer plan.Close()
	return h.publishIncrementalPromotion(ctx, conn, dbPath, meta, plan)
}

func loadPromotionRows(ctx context.Context, conn *sql.DB, meta common.VectorIndexMeta, spec vecindex.IVFSpec, base *vecindex.SegmentGeneration, clusterID int64) ([]promotionRow, error) {
	fetcher, err := newExactVectorFetcher(ctx, conn, meta, spec)
	if err != nil {
		return nil, err
	}
	if fetcher != nil {
		defer fetcher.Close()
	}
	rows := make([]promotionRow, 0, base.Data.ClusterCount(clusterID))
	var scanErr error
	if err := base.Data.ScanCluster(clusterID, func(rowID int64, vecBytes []byte) bool {
		var prepared []float32
		if fetcher != nil {
			preparedBlob, ok, err := fetcher.Prepared(ctx, rowID)
			if err != nil {
				scanErr = err
				return false
			}
			if !ok {
				return true
			}
			prepared = clonePreparedVector(preparedBlob)
		} else {
			var err error
			prepared, err = decodeStableMemberPrepared(spec, base.StableCodec, base.StableCentroids, clusterID, vecBytes)
			if err != nil {
				scanErr = err
				return false
			}
		}
		rows = append(rows, promotionRow{
			rowID: rowID,
			vec:   prepared,
		})
		return true
	}); err != nil {
		return nil, err
	}
	if scanErr != nil {
		return nil, scanErr
	}
	return rows, nil
}

func clusterSplitSeedsFromRows(
	rows []promotionRow,
	spec vecindex.IVFSpec,
	clusterID int64,
	baseCentroid []float32,
	extra int,
) ([][]float32, error) {
	if len(rows) <= extra || extra <= 0 {
		return nil, nil
	}
	vectors := make([][]float32, 0, len(rows))
	for _, row := range rows {
		if len(row.vec) == 0 {
			continue
		}
		vectors = append(vectors, row.vec)
	}
	if len(vectors) <= extra {
		return nil, nil
	}
	split, err := kmeans.KMeansPlusPlus(vectors, extra+1, spec.Seed^uint64(clusterID), 3)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(split, func(a, b []float32) int {
		da := metric.Distance(spec.InternalMetric(), a, baseCentroid)
		db := metric.Distance(spec.InternalMetric(), b, baseCentroid)
		switch {
		case da > db:
			return -1
		case da < db:
			return 1
		default:
			return 0
		}
	})
	if len(split) > extra {
		split = split[:extra]
	}
	out := make([][]float32, len(split))
	for i := range split {
		out[i] = append([]float32(nil), split[i]...)
	}
	return out, nil
}

func splitPromotionFamily(
	rows []promotionRow,
	initCentroids [][]float32,
	spec vecindex.IVFSpec,
	opts kmeans.MiniBatchBalancedOptions,
	seed uint64,
) ([][]float32, []int, []uint64, [][]float32, error) {
	if len(rows) == 0 || len(initCentroids) == 0 {
		return nil, nil, nil, nil, errIncrementalPromotionFallback
	}
	vectors := make([][]float32, len(rows))
	for i := range rows {
		vectors[i] = rows[i].vec
	}
	centroids, err := kmeans.MiniBatchBalancedFromInit(vectors, initCentroids, seed, opts)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	assignments, counts, sums := assignPromotionRows(rows, centroids, spec.InternalMetric())
	for _, count := range counts {
		if count == 0 {
			return nil, nil, nil, nil, errIncrementalPromotionFallback
		}
	}
	for i := range centroids {
		inv := 1 / float32(counts[i])
		next := make([]float32, len(sums[i]))
		for d, value := range sums[i] {
			next[d] = value * inv
		}
		centroids[i] = next
	}
	assignments, counts, sums = assignPromotionRows(rows, centroids, spec.InternalMetric())
	for _, count := range counts {
		if count == 0 {
			return nil, nil, nil, nil, errIncrementalPromotionFallback
		}
	}
	for i := range centroids {
		inv := 1 / float32(counts[i])
		next := make([]float32, len(sums[i]))
		for d, value := range sums[i] {
			next[d] = value * inv
		}
		centroids[i] = next
	}
	return centroids, assignments, counts, sums, nil
}

func assignPromotionRows(rows []promotionRow, centroids [][]float32, distMetric metric.Metric) ([]int, []uint64, [][]float32) {
	assignments := make([]int, len(rows))
	counts := make([]uint64, len(centroids))
	sums := make([][]float32, len(centroids))
	for i, row := range rows {
		best := nearestPromotionCentroid(row.vec, centroids, distMetric)
		assignments[i] = best
		counts[best]++
		if sums[best] == nil {
			sums[best] = make([]float32, len(row.vec))
		}
		for d, value := range row.vec {
			sums[best][d] += value
		}
	}
	return assignments, counts, sums
}

func nearestPromotionCentroid(vec []float32, centroids [][]float32, distMetric metric.Metric) int {
	best := 0
	bestDist := metric.Distance(distMetric, vec, centroids[0])
	for i := 1; i < len(centroids); i++ {
		dist := metric.Distance(distMetric, vec, centroids[i])
		if dist < bestDist {
			best = i
			bestDist = dist
		}
	}
	return best
}
