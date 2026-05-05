package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/rs/zerolog/log"
)

const (
	maintenancePollInterval  = time.Second
	mergeRowsThreshold       = 4096
	mergeBytesThreshold      = 32 << 20
	mergeAgeThreshold        = 30 * time.Second
	mergeMaxPrefixRows       = 256 * 1024
	mergeTargetMultiplier    = 128
	rebuildRowsFloor         = 50_000
	rebuildTargetClusterSize = 512
	rebuildClusterDriftPct   = 0.10
	rebuildClusterFactor     = 3
	repairClusterFactor      = 2
	repairP95Factor          = 1.5
	promotionStepFloor       = 32
	promotionGrowthFactor    = 1.25
	catchUpQuiesceDuration   = 2 * time.Second
)

func (h *EngineHook) startMaintenanceWatcher(meta common.VectorIndexMeta) {
	if h == nil || h.engine == nil || h.dbMgr == nil {
		return
	}
	h.maintenanceMu.Lock()
	if _, ok := h.maintenanceWatchers[meta.IndexName]; ok {
		h.maintenanceMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.maintenanceSeq++
	done := make(chan struct{})
	watch := maintenanceWatcher{cancel: cancel, done: done, seq: h.maintenanceSeq}
	h.maintenanceWatchers[meta.IndexName] = watch
	h.maintenanceMu.Unlock()
	go func() {
		defer close(done)
		h.maintenanceLoop(ctx, meta, watch.seq)
	}()
}

func (h *EngineHook) StartMaintenanceForIndex(meta common.VectorIndexMeta) {
	h.startMaintenanceWatcher(meta)
}

func (h *EngineHook) stopMaintenanceWatcher(indexName string) {
	if h == nil {
		return
	}
	h.maintenanceMu.Lock()
	watch, ok := h.maintenanceWatchers[indexName]
	delete(h.maintenanceWatchers, indexName)
	h.maintenanceMu.Unlock()
	if ok && watch.cancel != nil {
		watch.cancel()
	}
	if ok && watch.done != nil {
		<-watch.done
	}
}

func (h *EngineHook) clearMaintenanceWatcher(indexName string, seq uint64) {
	if h == nil {
		return
	}
	h.maintenanceMu.Lock()
	if current, ok := h.maintenanceWatchers[indexName]; ok && current.seq == seq {
		delete(h.maintenanceWatchers, indexName)
	}
	h.maintenanceMu.Unlock()
}

func (h *EngineHook) maintenanceLoop(ctx context.Context, meta common.VectorIndexMeta, seq uint64) {
	ticker := time.NewTicker(maintenancePollInterval)
	defer ticker.Stop()
	defer h.clearMaintenanceWatcher(meta.IndexName, seq)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		shouldStop := h.maintenanceOnce(ctx, meta)
		if shouldStop {
			return
		}
	}
}

func (h *EngineHook) maintenanceOnce(ctx context.Context, meta common.VectorIndexMeta) bool {
	conn, err := h.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: get db failed")
		return false
	}
	refreshedMeta, err := loadIndexMetaByName(ctx, conn, meta.IndexName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return true
		}
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: load metadata failed")
		return false
	}
	meta = *refreshedMeta

	state, spec, err := h.ensureIndexState(ctx, meta)
	if err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: ensure state failed")
		return false
	}
	if state == nil || state.ProbeVersion() == 0 {
		return false
	}
	base := state.LoadSegmentStore()
	overlay := state.LoadOverlay()
	if base == nil || base.Data == nil || overlay == nil {
		return false
	}
	overlaySnapshot := overlay.Snapshot()
	backlogRows, backlogBytes, oldestUnixNano := 0, int64(0), int64(0)
	if overlaySnapshot != nil && overlaySnapshot.LastSequence() > base.AppliedOverlaySeq {
		backlogRows, backlogBytes, oldestUnixNano = overlaySnapshot.BacklogStats(base.AppliedOverlaySeq)
	}
	maintenance := state.LoadMaintenanceState()
	clusterRowCounts := append([]uint64(nil), base.ClusterRowCounts...)
	driftBacklogRows := backlogRows
	if maintenance != nil {
		if live := maintenance.LiveClusterRowCounts(); len(live) > 0 {
			stableRows := totalTrackedRows(base.ClusterRowCounts, 0)
			liveRows := totalTrackedRows(live, 0)
			clusterRowCounts = live
			if liveRows >= stableRows+uint64(backlogRows) {
				driftBacklogRows = 0
			}
		}
	}
	currentClusters := currentClusterCount(meta, clusterRowCounts)
	stableRows := totalTrackedRows(clusterRowCounts, 0)
	wantClusters := desiredClusterCount(totalTrackedRows(clusterRowCounts, driftBacklogRows), maintenanceTargetClusterSize(meta))
	growForPromotion := meta.AutoTuneNlist &&
		wantClusters > currentClusters &&
		currentClusters > 0 &&
		float64(wantClusters-currentClusters)/float64(currentClusters) >= rebuildClusterDriftPct

	if growForPromotion && shouldCatchUpRebuild(stableRows, backlogRows, currentClusters, wantClusters) {
		if !catchUpOverlayQuiesced(overlaySnapshot, base.AppliedOverlaySeq) {
			return false
		}
		dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
		if err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: get db path failed")
			return false
		}
		if err := h.runCatchUpRebuild(ctx, conn, dbPath, meta, spec, state); err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: catch-up rebuild failed")
		} else {
			releaseVectorBuildResources(ctx, conn)
		}
		return false
	}

	if overlaySnapshot != nil && overlaySnapshot.LastSequence() > base.AppliedOverlaySeq && (shouldIncrementalMerge(backlogRows, backlogBytes, oldestUnixNano) || growForPromotion) {
		dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
		if err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: get db path failed")
			return false
		}
		if err := h.runIncrementalMerge(ctx, conn, dbPath, meta, spec, state); err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: incremental merge failed")
		} else {
			releaseVectorBuildResources(ctx, conn)
		}
		return false
	}
	if growForPromotion {
		dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
		if err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: get db path failed")
			return false
		}
		nextClusters := stepPromotionClusterCount(currentClusters, wantClusters)
		if err := h.runIncrementalPromotion(ctx, conn, dbPath, meta, spec, state, nextClusters); err != nil {
			if errors.Is(err, errIncrementalPromotionFallback) {
				log.Warn().Str("index", meta.IndexName).Msg("maintenance: incremental promotion exceeded bounded scope")
			} else {
				log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: incremental promotion failed")
			}
		} else {
			releaseVectorBuildResources(ctx, conn)
		}
		return false
	}

	if shouldIncrementalRepair(meta, clusterRowCounts, maintenance, countTargetClusterDrift(meta, clusterRowCounts, driftBacklogRows)) {
		dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
		if err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: get db path failed")
			return false
		}
		if err := h.runIncrementalRepair(ctx, conn, dbPath, meta, spec, state); err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: incremental repair failed")
		} else {
			releaseVectorBuildResources(ctx, conn)
		}
		return false
	}
	if overlaySnapshot == nil || overlaySnapshot.LastSequence() <= base.AppliedOverlaySeq {
		return false
	}
	if !shouldIncrementalMerge(backlogRows, backlogBytes, oldestUnixNano) {
		return false
	}

	dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
	if err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: get db path failed")
		return false
	}
	if err := h.runIncrementalMerge(ctx, conn, dbPath, meta, spec, state); err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: incremental merge failed")
	} else {
		releaseVectorBuildResources(ctx, conn)
	}
	return false
}

func shouldCatchUpRebuild(stableRows uint64, backlogRows int, currentClusters, wantClusters int) bool {
	if currentClusters <= 0 || wantClusters <= currentClusters || backlogRows <= 0 {
		return false
	}
	if wantClusters > currentClusters*2 {
		return true
	}
	if stableRows == 0 {
		return true
	}
	return uint64(backlogRows)*4 >= stableRows
}

func catchUpOverlayQuiesced(snapshot *vecindex.OverlaySnapshot, appliedSeq uint64) bool {
	if snapshot == nil {
		return false
	}
	newest := snapshot.NewestUnixNanoAfter(appliedSeq)
	if newest == 0 {
		return false
	}
	return time.Since(time.Unix(0, newest)) >= catchUpQuiesceDuration
}

func shouldIncrementalMerge(backlogRows int, backlogBytes int64, oldestUnixNano int64) bool {
	if backlogRows >= mergeRowsThreshold || backlogBytes >= mergeBytesThreshold {
		return true
	}
	if oldestUnixNano > 0 && time.Since(time.Unix(0, oldestUnixNano)) >= mergeAgeThreshold {
		return true
	}
	return false
}

func incrementalMergePrefixRows(meta common.VectorIndexMeta, spec vecindex.IVFSpec, backlogRows int, desiredK int) int {
	target := maintenanceTargetClusterSize(meta)
	if target <= 0 {
		target = defaultTargetPartitionSize
	}
	limit := target * mergeTargetMultiplier
	if limit < mergeRowsThreshold {
		limit = mergeRowsThreshold
	}
	if desiredK > 0 {
		catchUpLimit := desiredK * target / 2
		if catchUpLimit > limit {
			limit = catchUpLimit
		}
	}
	if rowBytes := spec.InternalDim() * 4; rowBytes > 0 {
		memoryCapRows := int((512 << 20) / rowBytes)
		if memoryCapRows < mergeRowsThreshold {
			memoryCapRows = mergeRowsThreshold
		}
		if limit > memoryCapRows {
			limit = memoryCapRows
		}
	}
	if limit > mergeMaxPrefixRows {
		limit = mergeMaxPrefixRows
	}
	if backlogRows > 0 && limit > backlogRows {
		limit = backlogRows
	}
	return limit
}

func overlayMutationCutoffSequence(snapshot *vecindex.OverlaySnapshot, minSequence uint64, maxMutations int) (uint64, int) {
	if snapshot == nil {
		return minSequence, 0
	}
	cutoff := minSequence
	count := 0
	stopped := false
	snapshot.VisitMutationHeadersAfter(minSequence, func(mutation vecindex.OverlayMutation) bool {
		count++
		cutoff = mutation.Sequence
		if maxMutations > 0 && count >= maxMutations {
			stopped = true
			return false
		}
		return true
	})
	if count == 0 {
		return minSequence, 0
	}
	if !stopped {
		return snapshot.LastSequence(), count
	}
	return cutoff, count
}

func shouldIncrementalRepair(meta common.VectorIndexMeta, clusterRowCounts []uint64, maintenance *vecindex.MaintenanceState, targetClusterDrift float64) bool {
	var rowsModifiedSinceRebuild uint64
	var lastRebuildRowCount uint64
	var skewCycles uint32
	if maintenance != nil {
		rowsModifiedSinceRebuild, lastRebuildRowCount, skewCycles = maintenance.Stats()
	}
	rebuildThreshold := uint64(rebuildRowsFloor)
	if lastRebuildRowCount > 0 {
		tenPct := lastRebuildRowCount / 10
		if tenPct > rebuildThreshold {
			rebuildThreshold = tenPct
		}
	}
	if rowsModifiedSinceRebuild >= rebuildThreshold {
		return true
	}
	targetClusterSize := maintenanceTargetClusterSize(meta)
	maxClusterRows, p95ClusterRows := clusterSkewMetrics(clusterRowCounts)
	if targetClusterSize > 0 && maxClusterRows > uint64(rebuildClusterFactor*targetClusterSize) {
		return true
	}
	if targetClusterSize > 0 && p95ClusterRows > uint64(float64(targetClusterSize)*repairP95Factor) {
		if skewCycles >= 2 {
			return true
		}
	}
	return meta.AutoTuneNlist && targetClusterDrift >= rebuildClusterDriftPct
}

func maintenanceTargetClusterSize(meta common.VectorIndexMeta) int {
	if meta.TargetPartitionSize > 0 {
		return meta.TargetPartitionSize
	}
	return rebuildTargetClusterSize
}

func currentClusterCount(meta common.VectorIndexMeta, clusterRowCounts []uint64) int {
	if len(clusterRowCounts) > 1 {
		return len(clusterRowCounts) - 1
	}
	return meta.Nlist
}

func totalTrackedRows(clusterRowCounts []uint64, backlogRows int) uint64 {
	var stableRows uint64
	for clusterID := 1; clusterID < len(clusterRowCounts); clusterID++ {
		stableRows += clusterRowCounts[clusterID]
	}
	if backlogRows > 0 {
		stableRows += uint64(backlogRows)
	}
	return stableRows
}

func desiredClusterCount(totalRows uint64, targetClusterSize int) int {
	if totalRows == 0 || targetClusterSize <= 0 {
		return 0
	}
	return int((totalRows + uint64(targetClusterSize) - 1) / uint64(targetClusterSize))
}

func countTargetClusterDrift(meta common.VectorIndexMeta, clusterRowCounts []uint64, backlogRows int) float64 {
	currentClusters := currentClusterCount(meta, clusterRowCounts)
	if !meta.AutoTuneNlist || currentClusters <= 0 {
		return 0
	}
	totalRows := totalTrackedRows(clusterRowCounts, backlogRows)
	if totalRows == 0 {
		return 0
	}
	tuned := desiredClusterCount(totalRows, maintenanceTargetClusterSize(meta))
	if tuned == currentClusters {
		return 0
	}
	diff := tuned - currentClusters
	if diff < 0 {
		diff = -diff
	}
	return float64(diff) / float64(currentClusters)
}

func clusterSkewMetrics(clusterRowCounts []uint64) (maxClusterRows uint64, p95ClusterRows uint64) {
	if len(clusterRowCounts) <= 1 {
		return 0, 0
	}
	nonzero := make([]uint64, 0, len(clusterRowCounts)-1)
	for clusterID := 1; clusterID < len(clusterRowCounts); clusterID++ {
		if clusterRowCounts[clusterID] == 0 {
			continue
		}
		if clusterRowCounts[clusterID] > maxClusterRows {
			maxClusterRows = clusterRowCounts[clusterID]
		}
		nonzero = append(nonzero, clusterRowCounts[clusterID])
	}
	if len(nonzero) == 0 {
		return maxClusterRows, 0
	}
	slices.Sort(nonzero)
	idx := int(math.Ceil(float64(len(nonzero))*0.95)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(nonzero) {
		idx = len(nonzero) - 1
	}
	return maxClusterRows, nonzero[idx]
}

func nextSkewCycleCount(clusterRowCounts []uint64, targetPartitionSize int, previous uint32) uint32 {
	if targetPartitionSize <= 0 {
		targetPartitionSize = rebuildTargetClusterSize
	}
	maxClusterRows, p95ClusterRows := clusterSkewMetrics(clusterRowCounts)
	if maxClusterRows > uint64(repairClusterFactor*targetPartitionSize) ||
		p95ClusterRows > uint64(float64(targetPartitionSize)*repairP95Factor) {
		return previous + 1
	}
	return 0
}

type incrementalMergePlan struct {
	state          *vecindex.IndexState
	spec           vecindex.IVFSpec
	currentEpoch   uint64
	baseGeneration uint64
	cutoff         uint64
	nextProbe      *kmeans.CentroidSet
	pending        *pendingSegmentGeneration
}

func (p *incrementalMergePlan) Close() {
	if p == nil || p.pending == nil {
		return
	}
	p.pending.Close()
	p.pending = nil
}

func openPinnedSegmentGeneration(
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	expectedEpoch uint64,
	expectedGeneration uint64,
) (*vecindex.SegmentGeneration, error) {
	dir := vecindex.SegmentStoreDir(dbPath, meta.IndexName)
	opened, err := openSegmentGeneration(dir, meta, spec, 0)
	if err != nil || opened == nil {
		return nil, err
	}
	if expectedEpoch != 0 && opened.Manifest.ProbeEpochValue() != expectedEpoch {
		_ = opened.Close()
		return nil, nil
	}
	if opened.Manifest.Generation != expectedGeneration {
		_ = opened.Close()
		return nil, nil
	}
	return segmentGenerationFromOpened(opened), nil
}

func (h *EngineHook) prepareIncrementalMerge(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	state *vecindex.IndexState,
) (*incrementalMergePlan, error) {
	h.localChangeMu.Lock()

	base := state.LoadSegmentStore()
	overlay := state.LoadOverlay()
	if base == nil || base.Data == nil || base.RowMap == nil || overlay == nil {
		h.localChangeMu.Unlock()
		return nil, nil
	}
	overlaySnapshot := overlay.Snapshot()
	if overlaySnapshot == nil || overlaySnapshot.LastSequence() <= base.AppliedOverlaySeq {
		h.localChangeMu.Unlock()
		return nil, nil
	}
	currentProbe := state.ProbeState()
	if currentProbe == nil {
		h.localChangeMu.Unlock()
		return nil, nil
	}
	backlogRows, _, _ := overlaySnapshot.BacklogStats(base.AppliedOverlaySeq)
	desiredK := desiredClusterCount(totalTrackedRows(base.ClusterRowCounts, backlogRows), maintenanceTargetClusterSize(meta))
	cutoff, prefixRows := overlayMutationCutoffSequence(overlaySnapshot, base.AppliedOverlaySeq, incrementalMergePrefixRows(meta, spec, backlogRows, desiredK))
	if prefixRows == 0 || cutoff <= base.AppliedOverlaySeq {
		h.localChangeMu.Unlock()
		return nil, nil
	}
	baseGeneration := base.Data.Generation()
	pinnedBase, err := openPinnedSegmentGeneration(dbPath, meta, spec, currentProbe.Epoch(), baseGeneration)
	hotClusterScores := state.HotClusterScores(segmentLayoutHotClusterLimit)
	h.localChangeMu.Unlock()
	if err != nil || pinnedBase == nil {
		return nil, err
	}
	defer pinnedBase.Close()

	exactFetcher, err := newExactVectorFetcher(ctx, conn, meta, spec)
	if err != nil {
		return nil, fmt.Errorf("incremental merge: exact vector fetcher: %w", err)
	}
	if exactFetcher != nil {
		defer exactFetcher.Close()
	}
	stats, err := buildCutoffClusterStats(ctx, spec, pinnedBase, overlaySnapshot, cutoff, state.LoadMaintenanceState(), exactFetcher)
	if err != nil {
		return nil, err
	}
	nextProbe := currentProbe
	nextStable := pinnedBase.StableCentroids
	if len(stats.Touched) > 0 {
		nextProbe, err = probeCentroidSetForTouched(currentProbe, stats.Counts, stats.Sums, stats.Touched, currentProbe.Epoch()+1)
		if err != nil {
			return nil, fmt.Errorf("incremental merge: refresh probe centroids: %w", err)
		}
		nextStable, err = stableCentroidSetForTouched(pinnedBase.StableCentroids, nextProbe, stats.Touched)
		if err != nil {
			return nil, fmt.Errorf("incremental merge: refresh stable centroids: %w", err)
		}
	}

	pending, err := BuildIncrementalSegmentGeneration(
		ctx,
		conn,
		dbPath,
		meta,
		spec,
		nextProbe,
		nextStable,
		pinnedBase,
		overlaySnapshot,
		cutoff,
		stats.Counts,
		stats.Sums,
		hotClusterScores,
	)
	if err != nil || pending == nil {
		return nil, err
	}
	return &incrementalMergePlan{
		state:          state,
		spec:           spec,
		currentEpoch:   currentProbe.Epoch(),
		baseGeneration: baseGeneration,
		cutoff:         cutoff,
		nextProbe:      nextProbe,
		pending:        pending,
	}, nil
}

func (h *EngineHook) publishIncrementalMerge(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	plan *incrementalMergePlan,
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
	skewCycles := nextSkewCycleCount(plan.pending.generation.ClusterRowCounts, meta.TargetPartitionSize, currentBase.ConsecutiveSkewCycles)
	plan.pending.manifest.ConsecutiveSkewCycles = skewCycles
	plan.pending.generation.ConsecutiveSkewCycles = skewCycles
	if err := plan.pending.Publish(); err != nil {
		return err
	}
	newState := vecindex.NewIndexState(plan.spec, plan.nextProbe)
	newState.StoreSegmentStore(plan.pending.generation)
	plan.pending.generation = nil
	nextOverlay, err := rewriteOverlayTailForProbe(ctx, conn, dbPath, meta, plan.nextProbe.Epoch(), plan.cutoff, currentSnapshot, plan.spec, plan.nextProbe, newState.LoadSegmentStore())
	if err != nil {
		newState.ClearSegmentStore()
		return err
	}
	newState.StoreOverlay(nextOverlay)
	h.engine.Register(meta.IndexName, newState)
	h.retireState(plan.state)
	return nil
}

func (h *EngineHook) runIncrementalMerge(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	state *vecindex.IndexState,
) error {
	h.maintenanceBuildMu.Lock()
	defer h.maintenanceBuildMu.Unlock()

	plan, err := h.prepareIncrementalMerge(ctx, conn, dbPath, meta, spec, state)
	if err != nil || plan == nil {
		return err
	}
	defer plan.Close()
	return h.publishIncrementalMerge(ctx, conn, dbPath, meta, plan)
}

func rewriteOverlayTailForProbe(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	epoch uint64,
	minSequence uint64,
	snapshot *vecindex.OverlaySnapshot,
	spec vecindex.IVFSpec,
	probe *kmeans.CentroidSet,
	stable *vecindex.SegmentGeneration,
) (*vecindex.JournaledOverlay, error) {
	dir := vecindex.SegmentStoreDir(dbPath, meta.IndexName)
	overlay, err := vecindex.OpenJournaledOverlayForRewrite(vecindex.OverlayJournalPath(dir))
	if err != nil {
		return nil, err
	}
	if err := overlay.Rewrite(epoch, minSequence, nil); err != nil {
		_ = overlay.Close()
		return nil, err
	}
	if snapshot == nil {
		return overlay, nil
	}
	exactFetcher, err := newExactVectorFetcher(ctx, conn, meta, spec)
	if err != nil {
		_ = overlay.Close()
		return nil, err
	}
	if exactFetcher != nil {
		defer exactFetcher.Close()
	}
	batch := make([]vecindex.OverlayMutation, 0, 1024)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := overlay.ApplyCommittedBatch(batch); err != nil {
			return err
		}
		for i := range batch {
			batch[i].Vec = nil
		}
		batch = batch[:0]
		return nil
	}
	var rewriteErr error
	snapshot.VisitMutationsAfter(minSequence, func(mutation vecindex.OverlayMutation) bool {
		next, err := reassignOverlayMutationForProbe(ctx, exactFetcher, mutation, spec, probe, stable)
		if err != nil {
			rewriteErr = err
			return false
		}
		batch = append(batch, next)
		if len(batch) < cap(batch) {
			return true
		}
		if err := flush(); err != nil {
			rewriteErr = err
			return false
		}
		return true
	})
	if rewriteErr != nil {
		_ = overlay.Close()
		return nil, rewriteErr
	}
	if err := flush(); err != nil {
		_ = overlay.Close()
		return nil, err
	}
	return overlay, nil
}
