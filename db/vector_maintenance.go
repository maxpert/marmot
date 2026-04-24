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
	rebuildRowsFloor         = 50_000
	rebuildTargetClusterSize = 512
	rebuildClusterDriftPct   = 0.10
	rebuildClusterFactor     = 3
	repairClusterFactor      = 2
	repairP95Factor          = 1.5
	promotionStepFloor       = 32
	promotionGrowthFactor    = 1.25
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
	watch := maintenanceWatcher{cancel: cancel, seq: h.maintenanceSeq}
	h.maintenanceWatchers[meta.IndexName] = watch
	h.maintenanceMu.Unlock()
	go h.maintenanceLoop(ctx, meta, watch.seq)
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
		if ctx.Err() != nil {
			return
		}
		shouldStop := h.maintenanceOnce(ctx, meta)
		if shouldStop {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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
			clusterRowCounts = live
			driftBacklogRows = 0
		}
	}
	currentClusters := currentClusterCount(meta, clusterRowCounts)
	wantClusters := desiredClusterCount(totalTrackedRows(clusterRowCounts, driftBacklogRows), maintenanceTargetClusterSize(meta))
	growForPromotion := meta.AutoTuneNlist &&
		wantClusters > currentClusters &&
		currentClusters > 0 &&
		float64(wantClusters-currentClusters)/float64(currentClusters) >= rebuildClusterDriftPct

	if overlaySnapshot != nil && overlaySnapshot.LastSequence() > base.AppliedOverlaySeq && (shouldIncrementalMerge(backlogRows, backlogBytes, oldestUnixNano) || growForPromotion) {
		dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
		if err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: get db path failed")
			return false
		}
		if err := h.runIncrementalMerge(ctx, conn, dbPath, meta, spec, state); err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: incremental merge failed")
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
				if err := h.runAutomaticRebuild(ctx, conn, meta); err != nil {
					log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: promotion fallback rebuild failed")
				}
			} else {
				log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: incremental promotion failed")
			}
		}
		return false
	}

	if shouldAutoRebuild(meta, clusterRowCounts, maintenance, countTargetClusterDrift(meta, clusterRowCounts, driftBacklogRows)) {
		if err := h.runAutomaticRebuild(ctx, conn, meta); err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Msg("maintenance: automatic rebuild failed")
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
	}
	return false
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

func shouldAutoRebuild(meta common.VectorIndexMeta, clusterRowCounts []uint64, maintenance *vecindex.MaintenanceState, targetClusterDrift float64) bool {
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
	opened, err := openSegmentGeneration(dir, meta, spec, expectedEpoch)
	if err != nil || opened == nil {
		return nil, err
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
	cutoff := overlaySnapshot.LastSequence()
	baseGeneration := base.Data.Generation()
	pinnedBase, err := openPinnedSegmentGeneration(dbPath, meta, spec, currentProbe.Epoch(), baseGeneration)
	hotClusterScores := state.HotClusterScores(segmentLayoutHotClusterLimit)
	h.localChangeMu.Unlock()
	if err != nil || pinnedBase == nil {
		return nil, err
	}
	defer pinnedBase.Close()

	stats, err := buildCutoffClusterStats(spec, pinnedBase, overlaySnapshot, cutoff, state.LoadMaintenanceState())
	if err != nil {
		return nil, err
	}
	nextEpoch := currentProbe.Epoch() + 1
	if nextEpoch == 0 {
		nextEpoch = 1
	}
	nextProbe, err := probeCentroidSetForTouched(currentProbe, stats.Counts, stats.Sums, stats.Touched, nextEpoch)
	if err != nil {
		return nil, err
	}
	nextStable, err := stableCentroidSetForTouched(pinnedBase.StableCentroids, nextProbe, stats.Touched)
	if err != nil {
		return nil, err
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
	tailMutations, err := reassignOverlayMutationsForProbe(currentSnapshot, plan.cutoff, plan.spec, plan.nextProbe, plan.pending.generation)
	if err != nil {
		return err
	}
	nextOverlay, err := rewriteOverlayForEpoch(dbPath, meta.IndexName, plan.nextProbe.Epoch(), plan.cutoff, tailMutations)
	if err != nil {
		return err
	}
	newState := vecindex.NewIndexState(plan.spec, plan.nextProbe)
	newState.StoreSegmentStore(plan.pending.generation)
	plan.pending.generation = nil
	newState.StoreOverlay(nextOverlay)
	if err := syncMaintenanceStateFromOverlay(newState); err != nil {
		newState.ClearOverlay()
		newState.ClearSegmentStore()
		return err
	}
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
	plan, err := h.prepareIncrementalMerge(ctx, conn, dbPath, meta, spec, state)
	if err != nil || plan == nil {
		return err
	}
	defer plan.Close()
	return h.publishIncrementalMerge(dbPath, meta, plan)
}

func (h *EngineHook) runAutomaticRebuild(ctx context.Context, conn *sql.DB, meta common.VectorIndexMeta) error {
	h.localChangeMu.Lock()
	oldState, ok := h.engine.Lookup(meta.IndexName)
	if !ok {
		h.localChangeMu.Unlock()
		return fmt.Errorf("automatic rebuild: index %q not registered", meta.IndexName)
	}
	liveCounts := oldState.LoadMaintenanceState().LiveClusterRowCounts()
	wantClusters := desiredClusterCount(totalTrackedRows(liveCounts, 0), maintenanceTargetClusterSize(meta))
	nextClusters := meta.Nlist
	if meta.AutoTuneNlist && wantClusters > 0 {
		switch {
		case wantClusters > nextClusters:
			stepped := max(nextClusters+promotionStepFloor, int(math.Ceil(float64(nextClusters)*promotionGrowthFactor)))
			if stepped < wantClusters {
				nextClusters = stepped
			} else {
				nextClusters = wantClusters
			}
		case wantClusters < nextClusters:
			stepped := min(nextClusters-promotionStepFloor, int(math.Floor(float64(nextClusters)/promotionGrowthFactor)))
			if stepped < wantClusters {
				nextClusters = wantClusters
			} else {
				nextClusters = stepped
			}
			if nextClusters < 1 {
				nextClusters = 1
			}
		}
	}
	dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
	h.localChangeMu.Unlock()
	if err != nil {
		return err
	}
	h.localChangeMu.Lock()
	defer h.localChangeMu.Unlock()

	updatedMeta, newState, err := Reindex(ctx, conn, h.engine, meta, nextClusters, time.Now().UnixNano())
	if err != nil {
		return err
	}
	if newState.ProbeVersion() != 0 {
		if err := buildAndStoreSegmentGeneration(ctx, conn, dbPath, newState, updatedMeta, newState.Spec()); err != nil {
			return err
		}
		if err := openAndStoreOverlay(dbPath, updatedMeta.IndexName, newState, newState.ProbeVersion()); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes SET nlist=?, nprobe=?, status='ready' WHERE index_name=?`,
		updatedMeta.Nlist, updatedMeta.Nprobe, updatedMeta.IndexName,
	); err != nil {
		return err
	}
	h.engine.Register(updatedMeta.IndexName, newState)
	h.retireState(oldState)
	if newState.ProbeVersion() == 0 {
		h.startBootstrapWatcher(updatedMeta, newState.Spec())
	}
	if h.indexMgr != nil {
		h.indexMgr.storeCachedIndexMeta(&updatedMeta)
	}
	return nil
}

func rewriteOverlayForEpoch(dbPath, indexName string, epoch uint64, minSequence uint64, mutations []vecindex.OverlayMutation) (*vecindex.JournaledOverlay, error) {
	dir := vecindex.SegmentStoreDir(dbPath, indexName)
	overlay, err := vecindex.OpenJournaledOverlayForRewrite(vecindex.OverlayJournalPath(dir))
	if err != nil {
		return nil, err
	}
	if err := overlay.Rewrite(epoch, minSequence, mutations); err != nil {
		_ = overlay.Close()
		return nil, err
	}
	return overlay, nil
}
