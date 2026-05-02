package db

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

const (
	catchUpMaxOpenSpools              = 128
	catchUpOverlayTailCapRows         = 64 * 1024
	catchUpDefaultParentGuardFamilies = 3
)

type catchUpRebuildPlan struct {
	state          *vecindex.IndexState
	nextSpec       vecindex.IVFSpec
	nextMeta       common.VectorIndexMeta
	currentEpoch   uint64
	baseGeneration uint64
	cutoff         uint64
	nextProbe      *kmeans.CentroidSet
	pending        *pendingSegmentGeneration
}

func (p *catchUpRebuildPlan) Close() {
	if p == nil || p.pending == nil {
		return
	}
	p.pending.Close()
	p.pending = nil
}

func (h *EngineHook) runCatchUpRebuild(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	state *vecindex.IndexState,
) error {
	h.maintenanceBuildMu.Lock()
	defer h.maintenanceBuildMu.Unlock()

	plan, err := h.prepareCatchUpRebuild(ctx, conn, dbPath, meta, spec, state)
	if err != nil || plan == nil {
		return err
	}
	defer plan.Close()
	return h.publishCatchUpRebuild(ctx, conn, dbPath, meta, plan)
}

func (h *EngineHook) prepareCatchUpRebuild(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	state *vecindex.IndexState,
) (*catchUpRebuildPlan, error) {
	h.localChangeMu.Lock()
	base := state.LoadSegmentStore()
	overlay := state.LoadOverlay()
	currentProbe := state.ProbeState()
	if base == nil || base.Data == nil || base.RowMap == nil || overlay == nil || currentProbe == nil {
		h.localChangeMu.Unlock()
		return nil, nil
	}
	overlaySnapshot := overlay.Snapshot()
	if overlaySnapshot == nil || overlaySnapshot.LastSequence() <= base.AppliedOverlaySeq {
		h.localChangeMu.Unlock()
		return nil, nil
	}
	cutoff := overlaySnapshot.LastSequence()
	baseGeneration := base.Data.Generation()
	baseAppliedSeq := base.AppliedOverlaySeq
	currentEpoch := currentProbe.Epoch()
	pinnedBase, err := openPinnedSegmentGeneration(dbPath, meta, spec, currentEpoch, baseGeneration)
	h.localChangeMu.Unlock()
	if err != nil || pinnedBase == nil {
		return nil, err
	}
	defer pinnedBase.Close()

	totalRows, err := countCatchUpRows(pinnedBase, overlaySnapshot, baseAppliedSeq, cutoff)
	if err != nil {
		return nil, err
	}
	if totalRows == 0 {
		return nil, nil
	}
	nextMeta, nextSpec := retuneReindexMeta(meta, spec, int64(totalRows))
	if nextSpec.Nlist <= spec.Nlist {
		return nil, nil
	}
	nextProbe, pending, err := BuildHierarchicalCatchUpSegmentGeneration(
		ctx,
		conn,
		dbPath,
		nextMeta,
		nextSpec,
		currentProbe,
		pinnedBase,
		overlaySnapshot,
		baseAppliedSeq,
		cutoff,
		currentEpoch+1,
		state.HotClusterScores(segmentLayoutHotClusterLimit),
	)
	if err != nil || pending == nil {
		return nil, err
	}
	if nextProbe == nil || nextProbe.Len() <= spec.Nlist {
		pending.Close()
		return nil, nil
	}
	return &catchUpRebuildPlan{
		state:          state,
		nextSpec:       nextSpec,
		nextMeta:       nextMeta,
		currentEpoch:   currentEpoch,
		baseGeneration: baseGeneration,
		cutoff:         cutoff,
		nextProbe:      nextProbe,
		pending:        pending,
	}, nil
}

func (h *EngineHook) publishCatchUpRebuild(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	plan *catchUpRebuildPlan,
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
	if tailRows, _, _ := currentSnapshot.BacklogStats(plan.cutoff); tailRows > catchUpOverlayTailCapRows {
		return fmt.Errorf("catch-up publish: overlay tail rows %d exceed cap %d", tailRows, catchUpOverlayTailCapRows)
	}
	if err := plan.pending.Publish(); err != nil {
		return err
	}
	nextOverlay, err := rewriteOverlayTailForProbe(ctx, conn, dbPath, meta, plan.nextProbe.Epoch(), plan.cutoff, currentSnapshot, plan.nextSpec, plan.nextProbe, plan.pending.generation)
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
	h.engine.Register(plan.nextMeta.IndexName, newState)
	h.retireState(plan.state)
	if h.indexMgr != nil {
		h.indexMgr.storeCachedIndexMeta(&plan.nextMeta)
	}
	return nil
}

func countCatchUpRows(base *vecindex.SegmentGeneration, snapshot *vecindex.OverlaySnapshot, minSequence, cutoff uint64) (uint64, error) {
	if base == nil || base.RowMap == nil {
		return 0, nil
	}
	shadow := catchUpShadowRowIDs(base, snapshot, minSequence, cutoff)
	var total uint64
	if err := base.RowMap.Scan(func(loc vecindex.SegmentRowLocation) bool {
		if _, ok := shadow[loc.RowID]; ok {
			return true
		}
		total++
		return true
	}); err != nil {
		return 0, err
	}
	if snapshot != nil {
		snapshot.VisitMutationHeadersAfterUnordered(minSequence, func(mutation vecindex.OverlayMutation) bool {
			if cutoff > 0 && mutation.Sequence > cutoff {
				return true
			}
			if mutation.Kind != vecindex.OverlayMutationDelete {
				total++
			}
			return true
		})
	}
	return total, nil
}

func catchUpShadowRowIDs(base *vecindex.SegmentGeneration, snapshot *vecindex.OverlaySnapshot, minSequence, cutoff uint64) map[int64]struct{} {
	shadow := make(map[int64]struct{})
	if base == nil || base.RowMap == nil || snapshot == nil {
		return shadow
	}
	snapshot.VisitMutationHeadersAfterUnordered(minSequence, func(mutation vecindex.OverlayMutation) bool {
		if cutoff > 0 && mutation.Sequence > cutoff {
			return true
		}
		if _, ok, err := base.RowMap.Lookup(mutation.RowID); err == nil && ok {
			shadow[mutation.RowID] = struct{}{}
		}
		return true
	})
	return shadow
}

func visitCatchUpPreparedVectors(
	ctx context.Context,
	conn *sql.DB,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	base *vecindex.SegmentGeneration,
	snapshot *vecindex.OverlaySnapshot,
	minSequence uint64,
	cutoff uint64,
	visit func(rowID int64, prepared []byte) error,
) error {
	if visit == nil {
		return nil
	}
	shadow := catchUpShadowRowIDs(base, snapshot, minSequence, cutoff)
	fetcher, err := newExactVectorFetcher(ctx, conn, meta, spec)
	if err != nil {
		return err
	}
	if fetcher != nil {
		defer fetcher.Close()
	}
	if base != nil && base.RowMap != nil {
		var scanErr error
		if err := base.RowMap.Scan(func(loc vecindex.SegmentRowLocation) bool {
			if _, ok := shadow[loc.RowID]; ok {
				return true
			}
			prepared, ok, err := fetcher.Prepared(ctx, loc.RowID)
			if err != nil {
				scanErr = err
				return false
			}
			if !ok {
				return true
			}
			if err := visit(loc.RowID, prepared); err != nil {
				scanErr = err
				return false
			}
			return true
		}); err != nil {
			return err
		}
		if scanErr != nil {
			return scanErr
		}
	}
	if snapshot == nil {
		return nil
	}
	var visitErr error
	snapshot.VisitMutationsAfter(minSequence, func(mutation vecindex.OverlayMutation) bool {
		if cutoff > 0 && mutation.Sequence > cutoff {
			return false
		}
		if mutation.Kind == vecindex.OverlayMutationDelete {
			return true
		}
		prepared, ok, err := overlayMutationPrepared(ctx, fetcher, mutation)
		if err != nil {
			visitErr = err
			return false
		}
		if !ok {
			return true
		}
		if err := visit(mutation.RowID, prepared); err != nil {
			visitErr = err
			return false
		}
		return true
	})
	return visitErr
}

func BuildHierarchicalCatchUpSegmentGeneration(
	ctx context.Context,
	conn *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	parentProbe *kmeans.CentroidSet,
	base *vecindex.SegmentGeneration,
	overlaySnapshot *vecindex.OverlaySnapshot,
	minSequence uint64,
	cutoffSequence uint64,
	epoch uint64,
	hotClusterScores map[int64]uint64,
) (*kmeans.CentroidSet, *pendingSegmentGeneration, error) {
	if parentProbe == nil || parentProbe.Len() == 0 || base == nil || base.RowMap == nil || overlaySnapshot == nil {
		return nil, nil, nil
	}
	currentK := parentProbe.Len()
	desiredK := spec.Nlist
	if desiredK <= currentK {
		return nil, nil, nil
	}
	if epoch == 0 {
		epoch = 1
	}
	targetSize := maintenanceTargetClusterSize(meta)
	if targetSize <= 0 {
		targetSize = defaultTargetPartitionSize
	}
	shadow := catchUpShadowRowIDs(base, overlaySnapshot, minSequence, cutoffSequence)
	parentCounts, err := catchUpParentCounts(base, overlaySnapshot, minSequence, cutoffSequence, currentK, shadow)
	if err != nil {
		return nil, nil, err
	}
	childCounts := allocateCatchUpChildCounts(parentCounts, currentK, desiredK, targetSize)
	childIDsByParent := catchUpChildIDLayout(childCounts, currentK, desiredK)
	parentGuards := catchUpParentGuardFamilies(parentProbe, spec, catchUpDefaultParentGuardFamilies)
	parentSnapshot := parentProbe.Snapshot()

	trainedCentroids := make([][]float32, desiredK)
	opts := kmeans.MiniBatchBalancedOptions{
		BatchSize:         min(max(256, targetSize*2), 4096),
		MaxIter:           4,
		TargetClusterSize: targetSize,
	}
	for parentID := 1; parentID <= currentK; parentID++ {
		childIDs := childIDsByParent[parentID]
		if len(childIDs) == 0 {
			continue
		}
		rows, err := loadCatchUpParentRows(ctx, conn, meta, spec, base, overlaySnapshot, minSequence, cutoffSequence, int64(parentID), shadow)
		if err != nil {
			return nil, nil, err
		}
		centroids, err := trainCatchUpFamily(rows, parentSnapshot[parentID-1], int64(parentID), len(childIDs), spec, opts)
		if err != nil {
			return nil, nil, err
		}
		for i, childID := range childIDs {
			trainedCentroids[childID-1] = append([]float32(nil), centroids[i]...)
		}
	}
	for i := range trainedCentroids {
		if len(trainedCentroids[i]) == 0 {
			parentID := i + 1
			if parentID > currentK {
				parentID = currentK
			}
			trainedCentroids[i] = append([]float32(nil), parentSnapshot[parentID-1]...)
		}
	}

	dir := vecindex.SegmentStoreDir(dbPath, meta.IndexName)
	generation, err := nextSegmentGeneration(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("hierarchical catch-up: next generation: %w", err)
	}
	stagingDir, dataPath, rowMapPath, blockPath, err := createSegmentGenerationStaging(dir, generation)
	if err != nil {
		return nil, nil, fmt.Errorf("hierarchical catch-up: create staging: %w", err)
	}
	keepStaging := false
	defer func() {
		if !keepStaging {
			_ = os.RemoveAll(stagingDir)
		}
	}()

	clusterRowCounts := make([]uint64, desiredK+1)
	clusterVectorSums := make([][]float32, desiredK+1)
	codecReservoir, err := newStableCodecReservoir(spec.Seed^epoch, spec.InternalDim())
	if err != nil {
		return nil, nil, fmt.Errorf("hierarchical catch-up: stable codec reservoir: %w", err)
	}
	defer codecReservoir.Close()
	spools := newCatchUpSpoolSet(dir, desiredK)
	defer spools.Cleanup()

	var rowCount uint64
	for parentID := 1; parentID <= currentK; parentID++ {
		rows, err := loadCatchUpParentRows(ctx, conn, meta, spec, base, overlaySnapshot, minSequence, cutoffSequence, int64(parentID), shadow)
		if err != nil {
			return nil, nil, err
		}
		if len(rows) == 0 {
			continue
		}
		candidateIDs := catchUpCandidateChildIDs(parentID, parentGuards, childIDsByParent)
		if len(candidateIDs) == 0 {
			candidateIDs = childIDsByParent[parentID]
		}
		if err := assignCatchUpRowsToSpools(rows, candidateIDs, trainedCentroids, spec, targetSize, spools, codecReservoir, clusterRowCounts, clusterVectorSums); err != nil {
			return nil, nil, err
		}
		rowCount += uint64(len(rows))
	}
	if rowCount == 0 {
		return nil, nil, nil
	}
	if err := spools.CloseAll(); err != nil {
		return nil, nil, fmt.Errorf("hierarchical catch-up: close cluster spools: %w", err)
	}
	finalCentroids := make([][]float32, desiredK)
	for clusterID := 1; clusterID <= desiredK; clusterID++ {
		if clusterRowCounts[clusterID] == 0 || len(clusterVectorSums[clusterID]) == 0 {
			finalCentroids[clusterID-1] = append([]float32(nil), trainedCentroids[clusterID-1]...)
			continue
		}
		centroid := make([]float32, len(clusterVectorSums[clusterID]))
		inv := 1 / float32(clusterRowCounts[clusterID])
		for d, value := range clusterVectorSums[clusterID] {
			centroid[d] = value * inv
		}
		finalCentroids[clusterID-1] = centroid
	}
	probeCS, err := kmeans.NewCentroidSet(epoch, finalCentroids)
	if err != nil {
		return nil, nil, err
	}
	stableCodec, stableCodecBlob, err := buildStableMemberCodec(spec, probeCS, codecReservoir)
	if err != nil {
		return nil, nil, fmt.Errorf("hierarchical catch-up: build stable codec: %w", err)
	}
	dataWriter, err := vecindex.CreateSegmentDataWriter(
		dataPath,
		spec.InternalMetric(),
		stableCodec.Encoding(),
		spec.Dim,
		spec.InternalDim(),
		stableCodec.EncodedSize(),
		desiredK,
		probeCS.Epoch(),
		generation,
	)
	if err != nil {
		return nil, nil, err
	}
	defer dataWriter.Abort()
	blockWriter, err := vecindex.CreateSegmentBlockMetaWriter(
		blockPath,
		spec,
		stableCodec,
		vecindex.DefaultSegmentBlockRows(stableCodec.Encoding()),
		desiredK,
		probeCS.Epoch(),
		generation,
	)
	if err != nil {
		return nil, nil, err
	}
	defer blockWriter.Abort()

	preparedEntrySize := 8 + spec.InternalDim()*4
	buf := make([]byte, preparedEntrySize*256)
	rowLocs := make([]rowLoc, 0, rowCount)
	for _, clusterID := range segmentClusterWriteOrder(spec, probeCS, hotClusterScores) {
		spool := spools.spools[clusterID]
		if spool == nil || spool.path == "" {
			continue
		}
		file, err := os.Open(spool.path)
		if err != nil {
			return nil, nil, fmt.Errorf("hierarchical catch-up: open cluster spool: %w", err)
		}
		for {
			n, readErr := io.ReadFull(file, buf)
			if readErr == io.EOF {
				break
			}
			if readErr == io.ErrUnexpectedEOF {
				if n == 0 {
					break
				}
				if n%preparedEntrySize != 0 {
					_ = file.Close()
					return nil, nil, fmt.Errorf("hierarchical catch-up: truncated cluster spool")
				}
			} else if readErr != nil {
				_ = file.Close()
				return nil, nil, fmt.Errorf("hierarchical catch-up: read cluster spool: %w", readErr)
			}
			for cursor := 0; cursor < n; cursor += preparedEntrySize {
				rowID := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+8]))
				prepared := buf[cursor+8 : cursor+preparedEntrySize]
				enc, encoded, err := stableCodec.Encode(clusterID, prepared)
				if err != nil {
					_ = file.Close()
					return nil, nil, fmt.Errorf("hierarchical catch-up: encode rowid %d: %w", rowID, err)
				}
				if enc != stableCodec.Encoding() {
					_ = file.Close()
					return nil, nil, fmt.Errorf("hierarchical catch-up: unexpected stable encoding %d for rowid %d", enc, rowID)
				}
				offset := dataWriter.NextOffset()
				if err := dataWriter.Append(clusterID, rowID, encoded); err != nil {
					_ = file.Close()
					return nil, nil, fmt.Errorf("hierarchical catch-up: append rowid %d: %w", rowID, err)
				}
				if err := blockWriter.Append(clusterID, rowID, offset, dataWriter.EntrySize(), encoded); err != nil {
					_ = file.Close()
					return nil, nil, fmt.Errorf("hierarchical catch-up: append block rowid %d: %w", rowID, err)
				}
				rowLocs = append(rowLocs, rowLoc{rowID: rowID, clusterID: clusterID, offset: offset})
			}
			if readErr == io.ErrUnexpectedEOF {
				break
			}
		}
		if err := file.Close(); err != nil {
			return nil, nil, fmt.Errorf("hierarchical catch-up: close cluster spool: %w", err)
		}
	}
	dataStore, err := dataWriter.Close()
	if err != nil {
		return nil, nil, fmt.Errorf("hierarchical catch-up: close data writer: %w", err)
	}
	dataWriter = nil
	blockStore, err := blockWriter.Close()
	if err != nil {
		_ = dataStore.Close()
		return nil, nil, fmt.Errorf("hierarchical catch-up: close block writer: %w", err)
	}
	blockWriter = nil

	slices.SortFunc(rowLocs, func(a, b rowLoc) int {
		switch {
		case a.rowID < b.rowID:
			return -1
		case a.rowID > b.rowID:
			return 1
		default:
			return 0
		}
	})
	rowMapWriter, err := vecindex.CreateSegmentRowMapWriter(rowMapPath, epoch, generation)
	if err != nil {
		_ = dataStore.Close()
		_ = blockStore.Close()
		return nil, nil, err
	}
	defer rowMapWriter.Abort()
	for _, loc := range rowLocs {
		if err := rowMapWriter.Append(loc.rowID, loc.clusterID, loc.offset); err != nil {
			_ = dataStore.Close()
			_ = blockStore.Close()
			return nil, nil, fmt.Errorf("hierarchical catch-up: append rowmap rowid %d: %w", loc.rowID, err)
		}
	}
	rowMapStore, err := rowMapWriter.Close()
	if err != nil {
		_ = dataStore.Close()
		_ = blockStore.Close()
		return nil, nil, fmt.Errorf("hierarchical catch-up: close rowmap writer: %w", err)
	}
	rowMapWriter = nil

	manifest := vecindex.SegmentManifest{
		Version:                  vecindex.SegmentStoreVersion,
		Database:                 meta.Database,
		IndexName:                meta.IndexName,
		IndexCreatedAt:           meta.CreatedAt,
		Metric:                   meta.Metric,
		Dim:                      uint32(meta.Dim),
		InternalDim:              uint32(spec.InternalDim()),
		ProbeCentroidEpoch:       probeCS.Epoch(),
		ProbeCentroidBlob:        mustCentroidBlob(probeCS),
		StableCentroidEpoch:      probeCS.Epoch(),
		StableCentroidBlob:       mustCentroidBlob(probeCS),
		StableMemberCodecBlob:    stableCodecBlob,
		AppliedOverlaySeq:        cutoffSequence,
		Generation:               generation,
		MaxCluster:               uint32(desiredK),
		RowCount:                 rowCount,
		ClusterRowCounts:         clusterRowCounts,
		ClusterVectorSums:        cloneClusterVectorSums(clusterVectorSums),
		RowsModifiedSinceRebuild: 0,
		LastRebuildRowCount:      rowCount,
		ConsecutiveSkewCycles:    nextSkewCycleCount(clusterRowCounts, meta.TargetPartitionSize, 0),
		LayoutHotClusters:        uint32Slice(orderedHotClusterIDs(hotClusterScores, segmentLayoutHotClusterLimit)),
		BlockRows:                uint32(blockStore.BlockRows()),
		CreatedAtUnixNano:        time.Now().UnixNano(),
	}
	keepStaging = true
	pending := &pendingSegmentGeneration{
		meta:       meta,
		spec:       spec,
		dir:        dir,
		stagingDir: stagingDir,
		manifest:   manifest,
		dataPath:   dataStore.Path(),
		rowMapPath: rowMapStore.Path(),
		blockPath:  blockStore.Path(),
		generation: &vecindex.SegmentGeneration{
			Data:                     dataStore,
			RowMap:                   rowMapStore,
			Blocks:                   blockStore,
			ProbeCentroids:           probeCS,
			StableCentroids:          probeCS,
			StableCodec:              stableCodec,
			AppliedOverlaySeq:        cutoffSequence,
			ClusterRowCounts:         append([]uint64(nil), clusterRowCounts...),
			ClusterVectorSums:        cloneClusterVectorSums(clusterVectorSums),
			RowsModifiedSinceRebuild: 0,
			LastRebuildRowCount:      rowCount,
			ConsecutiveSkewCycles:    manifest.ConsecutiveSkewCycles,
			LayoutHotClusters:        int64Slice(manifest.LayoutHotClusters),
		},
	}
	return probeCS, pending, nil
}

func catchUpParentCounts(
	base *vecindex.SegmentGeneration,
	snapshot *vecindex.OverlaySnapshot,
	minSequence uint64,
	cutoff uint64,
	currentK int,
	shadow map[int64]struct{},
) ([]uint64, error) {
	counts := make([]uint64, currentK+1)
	if base != nil && base.RowMap != nil {
		if err := base.RowMap.Scan(func(loc vecindex.SegmentRowLocation) bool {
			if loc.ClusterID <= 0 || int(loc.ClusterID) > currentK {
				return true
			}
			if _, ok := shadow[loc.RowID]; ok {
				return true
			}
			counts[loc.ClusterID]++
			return true
		}); err != nil {
			return nil, err
		}
	}
	if snapshot != nil {
		snapshot.VisitMutationHeadersAfterUnordered(minSequence, func(mutation vecindex.OverlayMutation) bool {
			if cutoff > 0 && mutation.Sequence > cutoff {
				return true
			}
			if mutation.Kind == vecindex.OverlayMutationDelete || mutation.ClusterID <= 0 || int(mutation.ClusterID) > currentK {
				return true
			}
			counts[mutation.ClusterID]++
			return true
		})
	}
	return counts, nil
}

func allocateCatchUpChildCounts(parentCounts []uint64, currentK int, desiredK int, targetSize int) []int {
	if currentK <= 0 || desiredK <= 0 {
		return nil
	}
	if targetSize <= 0 {
		targetSize = defaultTargetPartitionSize
	}
	childCounts := make([]int, currentK+1)
	base := currentK
	if desiredK < base {
		base = desiredK
	}
	for parentID := 1; parentID <= base; parentID++ {
		childCounts[parentID] = 1
	}
	extras := desiredK - base
	if extras <= 0 {
		return childCounts
	}
	type candidate struct {
		parentID  int
		remainder float64
		count     uint64
	}
	candidates := make([]candidate, 0, currentK)
	for parentID := 1; parentID <= currentK; parentID++ {
		var count uint64
		if parentID < len(parentCounts) {
			count = parentCounts[parentID]
		}
		if count == 0 {
			continue
		}
		ideal := float64(count) / float64(targetSize)
		want := int(math.Floor(ideal))
		if want < 1 {
			want = 1
		}
		maxChildren := int(count)
		if maxChildren < 1 {
			maxChildren = 1
		}
		if want > maxChildren {
			want = maxChildren
		}
		add := want - childCounts[parentID]
		if add > extras {
			add = extras
		}
		if add > 0 {
			childCounts[parentID] += add
			extras -= add
		}
		candidates = append(candidates, candidate{
			parentID:  parentID,
			remainder: ideal - math.Floor(ideal),
			count:     count,
		})
	}
	slices.SortFunc(candidates, func(a, b candidate) int {
		switch {
		case a.remainder > b.remainder:
			return -1
		case a.remainder < b.remainder:
			return 1
		case a.count > b.count:
			return -1
		case a.count < b.count:
			return 1
		case a.parentID < b.parentID:
			return -1
		default:
			return 1
		}
	})
	for extras > 0 && len(candidates) > 0 {
		progress := false
		for _, candidate := range candidates {
			if extras <= 0 {
				break
			}
			if candidate.count > 0 && childCounts[candidate.parentID] >= int(candidate.count) {
				continue
			}
			childCounts[candidate.parentID]++
			extras--
			progress = true
		}
		if !progress {
			break
		}
	}
	for extras > 0 {
		for parentID := 1; parentID <= currentK && extras > 0; parentID++ {
			childCounts[parentID]++
			extras--
		}
	}
	return childCounts
}

func catchUpChildIDLayout(childCounts []int, currentK int, desiredK int) [][]int64 {
	children := make([][]int64, currentK+1)
	nextID := currentK + 1
	for parentID := 1; parentID <= currentK; parentID++ {
		count := 0
		if parentID < len(childCounts) {
			count = childCounts[parentID]
		}
		if count <= 0 {
			continue
		}
		children[parentID] = append(children[parentID], int64(parentID))
		for i := 1; i < count && nextID <= desiredK; i++ {
			children[parentID] = append(children[parentID], int64(nextID))
			nextID++
		}
	}
	return children
}

func catchUpParentGuardFamilies(parentProbe *kmeans.CentroidSet, spec vecindex.IVFSpec, guardFamilies int) [][]int {
	if parentProbe == nil || parentProbe.Len() == 0 {
		return nil
	}
	if guardFamilies < 1 {
		guardFamilies = 1
	}
	if guardFamilies > parentProbe.Len() {
		guardFamilies = parentProbe.Len()
	}
	snapshot := parentProbe.Snapshot()
	guards := make([][]int, parentProbe.Len()+1)
	for parentID := 1; parentID <= parentProbe.Len(); parentID++ {
		type candidate struct {
			id   int
			dist float32
		}
		candidates := make([]candidate, 0, parentProbe.Len())
		for otherID := 1; otherID <= parentProbe.Len(); otherID++ {
			dist := metric.Distance(spec.InternalMetric(), snapshot[parentID-1], snapshot[otherID-1])
			candidates = append(candidates, candidate{id: otherID, dist: dist})
		}
		slices.SortFunc(candidates, func(a, b candidate) int {
			switch {
			case a.dist < b.dist:
				return -1
			case a.dist > b.dist:
				return 1
			case a.id < b.id:
				return -1
			default:
				return 1
			}
		})
		for i := 0; i < guardFamilies && i < len(candidates); i++ {
			guards[parentID] = append(guards[parentID], candidates[i].id)
		}
	}
	return guards
}

func catchUpCandidateChildIDs(parentID int, guards [][]int, childIDsByParent [][]int64) []int64 {
	seen := make(map[int64]struct{})
	candidates := make([]int64, 0)
	if parentID < len(guards) {
		for _, guardedParent := range guards[parentID] {
			if guardedParent <= 0 || guardedParent >= len(childIDsByParent) {
				continue
			}
			for _, childID := range childIDsByParent[guardedParent] {
				if _, ok := seen[childID]; ok {
					continue
				}
				seen[childID] = struct{}{}
				candidates = append(candidates, childID)
			}
		}
	}
	return candidates
}

func loadCatchUpParentRows(
	ctx context.Context,
	conn *sql.DB,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	base *vecindex.SegmentGeneration,
	snapshot *vecindex.OverlaySnapshot,
	minSequence uint64,
	cutoff uint64,
	parentID int64,
	shadow map[int64]struct{},
) ([]promotionRow, error) {
	var rows []promotionRow
	fetcher, err := newExactVectorFetcher(ctx, conn, meta, spec)
	if err != nil {
		return nil, err
	}
	if fetcher != nil {
		defer fetcher.Close()
	}
	if base != nil && base.Data != nil {
		rows = make([]promotionRow, 0, base.Data.ClusterCount(parentID))
		var scanErr error
		if err := base.Data.ScanCluster(parentID, func(rowID int64, _ []byte) bool {
			if _, ok := shadow[rowID]; ok {
				return true
			}
			preparedBlob, ok, err := fetcher.Prepared(ctx, rowID)
			if err != nil {
				scanErr = err
				return false
			}
			if !ok {
				return true
			}
			rows = append(rows, promotionRow{
				rowID: rowID,
				vec:   metric.BytesToFloat32(preparedBlob),
				blob:  preparedBlob,
			})
			return true
		}); err != nil {
			return nil, err
		}
		if scanErr != nil {
			return nil, scanErr
		}
	}
	if snapshot != nil {
		var visitErr error
		snapshot.VisitMutationsAfter(minSequence, func(mutation vecindex.OverlayMutation) bool {
			if cutoff > 0 && mutation.Sequence > cutoff {
				return false
			}
			if mutation.Kind == vecindex.OverlayMutationDelete || mutation.ClusterID != parentID {
				return true
			}
			blob, ok, err := overlayMutationPrepared(ctx, fetcher, mutation)
			if err != nil {
				visitErr = err
				return false
			}
			if !ok {
				return true
			}
			blob = append([]byte(nil), blob...)
			rows = append(rows, promotionRow{
				rowID: mutation.RowID,
				vec:   metric.BytesToFloat32(blob),
				blob:  blob,
			})
			return true
		})
		if visitErr != nil {
			return nil, visitErr
		}
	}
	return rows, nil
}

func trainCatchUpFamily(
	rows []promotionRow,
	parentCentroid []float32,
	parentID int64,
	childCount int,
	spec vecindex.IVFSpec,
	opts kmeans.MiniBatchBalancedOptions,
) ([][]float32, error) {
	if childCount <= 0 {
		return nil, nil
	}
	if len(rows) == 0 {
		out := make([][]float32, childCount)
		for i := range out {
			out[i] = append([]float32(nil), parentCentroid...)
		}
		return out, nil
	}
	if childCount == 1 {
		return [][]float32{meanPromotionRows(rows, len(parentCentroid))}, nil
	}
	if len(rows) < childCount {
		return fallbackCatchUpFamilyCentroids(rows, parentCentroid, childCount, spec.InternalMetric()), nil
	}
	initCentroids := make([][]float32, 0, childCount)
	initCentroids = append(initCentroids, append([]float32(nil), parentCentroid...))
	seeds, err := clusterSplitSeedsFromRows(rows, spec, parentID, parentCentroid, childCount-1)
	if err == nil && len(seeds) == childCount-1 {
		initCentroids = append(initCentroids, seeds...)
		centroids, _, _, _, err := splitPromotionFamily(rows, initCentroids, spec, opts, spec.Seed^uint64(parentID))
		if err == nil && len(centroids) == childCount {
			return centroids, nil
		}
	}
	return fallbackCatchUpFamilyCentroids(rows, parentCentroid, childCount, spec.InternalMetric()), nil
}

func meanPromotionRows(rows []promotionRow, dim int) []float32 {
	centroid := make([]float32, dim)
	if len(rows) == 0 {
		return centroid
	}
	for _, row := range rows {
		for d, value := range row.vec {
			centroid[d] += value
		}
	}
	inv := 1 / float32(len(rows))
	for d := range centroid {
		centroid[d] *= inv
	}
	return centroid
}

func fallbackCatchUpFamilyCentroids(rows []promotionRow, parentCentroid []float32, childCount int, distMetric metric.Metric) [][]float32 {
	out := make([][]float32, childCount)
	if len(rows) == 0 {
		for i := range out {
			out[i] = append([]float32(nil), parentCentroid...)
		}
		return out
	}
	for i := range out {
		idx := (i * len(rows)) / childCount
		if idx >= len(rows) {
			idx = len(rows) - 1
		}
		out[i] = append([]float32(nil), rows[idx].vec...)
	}
	assignments, counts, sums := assignPromotionRowsBalanced(rows, out, distMetric, 0)
	_ = assignments
	for i := range out {
		if counts[i] == 0 || len(sums[i]) == 0 {
			continue
		}
		inv := 1 / float32(counts[i])
		next := make([]float32, len(sums[i]))
		for d, value := range sums[i] {
			next[d] = value * inv
		}
		out[i] = next
	}
	return out
}

func assignCatchUpRowsToSpools(
	rows []promotionRow,
	candidateIDs []int64,
	centroids [][]float32,
	spec vecindex.IVFSpec,
	targetSize int,
	spools *catchUpSpoolSet,
	codecReservoir *stableCodecReservoir,
	clusterRowCounts []uint64,
	clusterVectorSums [][]float32,
) error {
	if len(rows) == 0 || len(candidateIDs) == 0 {
		return nil
	}
	hardLimit := uint64(0)
	if targetSize > 0 {
		hardLimit = uint64(targetSize * repairClusterFactor)
	}
	for _, row := range rows {
		bestClusterID := candidateIDs[0]
		bestDist := float32(0)
		bestSet := false
		overflowClusterID := candidateIDs[0]
		overflowDist := float32(0)
		overflowSet := false
		for _, clusterID := range candidateIDs {
			if clusterID <= 0 || int(clusterID) > len(centroids) || len(centroids[clusterID-1]) == 0 {
				continue
			}
			dist := metric.Distance(spec.InternalMetric(), row.vec, centroids[clusterID-1])
			if !overflowSet || dist < overflowDist || (dist == overflowDist && clusterID < overflowClusterID) {
				overflowSet = true
				overflowClusterID = clusterID
				overflowDist = dist
			}
			if hardLimit > 0 && clusterRowCounts[clusterID] >= hardLimit {
				continue
			}
			if !bestSet || dist < bestDist || (dist == bestDist && clusterID < bestClusterID) {
				bestSet = true
				bestDist = dist
				bestClusterID = clusterID
			}
		}
		if !bestSet {
			bestClusterID = overflowClusterID
		}
		prepared := row.preparedBlob()
		if clusterVectorSums[bestClusterID] == nil {
			clusterVectorSums[bestClusterID] = make([]float32, spec.InternalDim())
		}
		for i, value := range row.vec {
			clusterVectorSums[bestClusterID][i] += value
		}
		codecReservoir.Add(bestClusterID, prepared)
		if err := spools.Write(bestClusterID, row.rowID, prepared); err != nil {
			return err
		}
		clusterRowCounts[bestClusterID]++
	}
	return nil
}

type catchUpClusterSpool struct {
	path    string
	file    *os.File
	lastUse uint64
}

type catchUpSpoolSet struct {
	dir       string
	spools    map[int64]*catchUpClusterSpool
	openCount int
	useClock  uint64
}

func newCatchUpSpoolSet(dir string, maxCluster int) *catchUpSpoolSet {
	return &catchUpSpoolSet{
		dir:    dir,
		spools: make(map[int64]*catchUpClusterSpool, maxCluster),
	}
}

func (s *catchUpSpoolSet) Write(clusterID int64, rowID int64, prepared []byte) error {
	spool, err := s.openForAppend(clusterID)
	if err != nil {
		return err
	}
	var rowidBuf [8]byte
	binary.LittleEndian.PutUint64(rowidBuf[:], uint64(rowID))
	if _, err := spool.file.Write(rowidBuf[:]); err != nil {
		return err
	}
	_, err = spool.file.Write(prepared)
	return err
}

func (s *catchUpSpoolSet) openForAppend(clusterID int64) (*catchUpClusterSpool, error) {
	spool := s.spools[clusterID]
	if spool == nil {
		tmp, err := os.CreateTemp(s.dir, fmt.Sprintf("catchup-cluster-%06d-*.segrows", clusterID))
		if err != nil {
			return nil, err
		}
		spool = &catchUpClusterSpool{path: tmp.Name(), file: tmp}
		s.spools[clusterID] = spool
		s.openCount++
	}
	if spool.file == nil {
		file, err := os.OpenFile(spool.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		spool.file = file
		s.openCount++
	}
	s.useClock++
	spool.lastUse = s.useClock
	return spool, s.evictOpenSpools()
}

func (s *catchUpSpoolSet) evictOpenSpools() error {
	for s.openCount > catchUpMaxOpenSpools {
		var victim *catchUpClusterSpool
		for _, spool := range s.spools {
			if spool == nil || spool.file == nil {
				continue
			}
			if victim == nil || spool.lastUse < victim.lastUse {
				victim = spool
			}
		}
		if victim == nil {
			return nil
		}
		if err := victim.file.Close(); err != nil {
			return err
		}
		victim.file = nil
		s.openCount--
	}
	return nil
}

func (s *catchUpSpoolSet) CloseAll() error {
	var firstErr error
	for _, spool := range s.spools {
		if spool == nil || spool.file == nil {
			continue
		}
		if err := spool.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		spool.file = nil
	}
	s.openCount = 0
	return firstErr
}

func (s *catchUpSpoolSet) Cleanup() {
	if s == nil {
		return
	}
	_ = s.CloseAll()
	for _, spool := range s.spools {
		if spool != nil && spool.path != "" {
			_ = os.Remove(spool.path)
		}
	}
}
