package db

import (
	"fmt"
	"math/rand"
	"slices"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

func computeCentroidsFromOverlaySnapshot(
	snapshot *vecindex.OverlaySnapshot,
	spec vecindex.IVFSpec,
	targetClusterSize int,
	cutoff uint64,
	epoch uint64,
) (*kmeans.CentroidSet, error) {
	nTotal := countOverlayPreparedVectors(snapshot, cutoff)
	if nTotal == 0 {
		return nil, nil
	}
	actualK := spec.Nlist
	if actualK > nTotal {
		actualK = nTotal
	}
	if actualK <= 0 {
		return nil, nil
	}
	if targetClusterSize <= 0 {
		targetClusterSize = max(1, (nTotal+actualK-1)/actualK)
	}
	opts := kmeans.MiniBatchBalancedOptions{
		BatchSize:         min(max(4096, targetClusterSize*4), 16384),
		MaxIter:           kmeans.DefaultMiniBatchMaxIter,
		TargetClusterSize: targetClusterSize,
		Metric:            spec.InternalMetric(),
	}
	initSize := max(actualK*32, opts.BatchSize*kmeans.DefaultMiniBatchInitFactor)
	if initSize < actualK {
		initSize = actualK
	}
	if initSize > nTotal {
		initSize = nTotal
	}
	samples, err := collectOverlayReservoirSample(snapshot, cutoff, initSize, spec.Seed, spec.InternalDim())
	if err != nil {
		return nil, fmt.Errorf("overlay bootstrap sample: %w", err)
	}
	if len(samples) == 0 {
		return nil, nil
	}
	initCentroids, err := kmeans.KMeansPlusPlusWithMetric(samples, actualK, spec.Seed, 1, spec.InternalMetric())
	if err != nil {
		return nil, fmt.Errorf("overlay bootstrap init centroids: %w", err)
	}
	initCentroids, err = kmeans.RebalanceInitialCentroids(samples, initCentroids, opts, spec.Seed)
	if err != nil {
		return nil, fmt.Errorf("overlay bootstrap rebalance: %w", err)
	}
	trainer, err := kmeans.NewMiniBatchBalancedTrainer(initCentroids, opts)
	if err != nil {
		return nil, fmt.Errorf("overlay bootstrap trainer: %w", err)
	}
	stablePasses := 0
	var lastResult kmeans.MiniBatchPassResult
	var bestAssignedCentroids [][]float32
	bestAssignedShift := float32(1 << 30)
	var bestCentroids [][]float32
	bestShift := float32(1 << 30)
	for iter := 0; iter < opts.MaxIter; iter++ {
		result, err := runMiniBatchOverlayTrainerPass(snapshot, cutoff, opts.BatchSize, spec.InternalDim(), trainer, spec.Seed^uint64(iter+1))
		if err != nil {
			return nil, fmt.Errorf("overlay bootstrap trainer pass %d: %w", iter+1, err)
		}
		lastResult = result
		shapeOK := trainerClusterShapeAcceptable(trainer.Counts(), targetClusterSize)
		if !result.Repaired && result.MaxShift < bestAssignedShift {
			bestAssignedCentroids = trainer.Centroids()
			bestAssignedShift = result.MaxShift
		}
		if !result.Repaired && shapeOK && result.MaxShift < bestShift {
			bestCentroids = trainer.Centroids()
			bestShift = result.MaxShift
		}
		if result.Repaired || !result.Converged || !shapeOK {
			stablePasses = 0
			continue
		}
		stablePasses++
		if stablePasses >= 2 {
			break
		}
	}
	for extra := 0; extra < 4 && (lastResult.Repaired || !trainerClusterShapeAcceptable(trainer.Counts(), targetClusterSize)); extra++ {
		result, err := runMiniBatchOverlayTrainerPass(snapshot, cutoff, opts.BatchSize, spec.InternalDim(), trainer, spec.Seed^uint64(opts.MaxIter+extra+1))
		if err != nil {
			return nil, fmt.Errorf("overlay bootstrap repair pass %d: %w", extra+1, err)
		}
		lastResult = result
		if !result.Repaired && result.MaxShift < bestAssignedShift {
			bestAssignedCentroids = trainer.Centroids()
			bestAssignedShift = result.MaxShift
		}
		if !result.Repaired && trainerClusterShapeAcceptable(trainer.Counts(), targetClusterSize) && result.MaxShift < bestShift {
			bestCentroids = trainer.Centroids()
			bestShift = result.MaxShift
		}
	}
	var centroids [][]float32
	switch {
	case !lastResult.Repaired && trainerClusterShapeAcceptable(trainer.Counts(), targetClusterSize):
		centroids = trainer.Centroids()
	case len(bestCentroids) > 0:
		centroids = bestCentroids
	case len(bestAssignedCentroids) > 0:
		centroids = bestAssignedCentroids
	default:
		return nil, fmt.Errorf("overlay bootstrap centroids: trainer failed to converge to an assigned layout")
	}
	if epoch == 0 {
		epoch = 1
	}
	return kmeans.NewCentroidSet(epoch, centroids)
}

func BuildBootstrapSegmentGenerationFromOverlay(
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	probeCS *kmeans.CentroidSet,
	overlaySnapshot *vecindex.OverlaySnapshot,
	cutoffSequence uint64,
	hotClusterScores map[int64]uint64,
) (*pendingSegmentGeneration, error) {
	if probeCS == nil || overlaySnapshot == nil {
		return nil, nil
	}
	if countOverlayPreparedVectors(overlaySnapshot, cutoffSequence) == 0 {
		return nil, nil
	}
	maxCluster := probeCS.Len()
	if maxCluster == 0 {
		return nil, nil
	}
	dir := vecindex.SegmentStoreDir(dbPath, meta.IndexName)
	generation, err := nextSegmentGeneration(dir)
	if err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: next generation: %w", err)
	}
	staging, err := createSegmentGenerationStaging(dir, generation)
	if err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: create staging: %w", err)
	}
	keepStaging := false
	defer func() {
		if !keepStaging {
			staging.cleanup()
		}
	}()
	rowMapWriter, err := vecindex.CreateSegmentRowMapWriter(staging.artifacts.rowMapPath, probeCS.Epoch(), generation)
	if err != nil {
		return nil, err
	}
	defer rowMapWriter.Abort()

	clusterRowCounts := make([]uint64, maxCluster+1)
	clusterVectorSums := make([][]float32, maxCluster+1)
	var codecReservoir *stableCodecReservoir
	if spec.InternalDim() >= vecindex.StablePQMinInternalDim {
		codecReservoir, err = newStableCodecReservoir(spec.Seed^probeCS.Epoch(), spec.InternalDim())
		if err != nil {
			return nil, fmt.Errorf("bootstrap segment generation: stable codec reservoir: %w", err)
		}
		defer codecReservoir.Close()
	}
	spools := newCatchUpSpoolSet(dir, maxCluster)
	defer spools.Cleanup()
	var visitErr error
	var scratch []byte
	scratch, err = overlaySnapshot.VisitMutationsAfterBuffered(0, scratch, func(mutation vecindex.OverlayMutation) bool {
		if cutoffSequence > 0 && mutation.Sequence > cutoffSequence {
			return false
		}
		if mutation.Kind == vecindex.OverlayMutationDelete || len(mutation.Vec) == 0 {
			return true
		}
		clusterID, err := assignPreparedAgainstSet(mutation.Vec, spec, probeCS)
		if err != nil {
			visitErr = fmt.Errorf("bootstrap segment generation: assign rowid %d: %w", mutation.RowID, err)
			return false
		}
		if codecReservoir != nil {
			codecReservoir.Add(clusterID, mutation.Vec)
		}
		if err := spools.Write(clusterID, mutation.RowID, mutation.Vec); err != nil {
			visitErr = fmt.Errorf("bootstrap segment generation: spool rowid %d: %w", mutation.RowID, err)
			return false
		}
		clusterRowCounts[clusterID]++
		if clusterVectorSums[clusterID] == nil {
			clusterVectorSums[clusterID] = make([]float32, spec.InternalDim())
		}
		sum := clusterVectorSums[clusterID]
		for i, value := range metric.BytesToFloat32(mutation.Vec) {
			sum[i] += value
		}
		return true
	})
	_ = scratch
	if err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: read overlay: %w", err)
	}
	if visitErr != nil {
		return nil, visitErr
	}
	if err := spools.CloseAll(); err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: close spools: %w", err)
	}
	stableCodec, stableCodecBlob, err := buildStableMemberCodec(spec, probeCS, codecReservoir)
	if err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: build stable codec: %w", err)
	}
	dataWriter, err := vecindex.CreateSegmentDataWriter(
		staging.artifacts.dataPath,
		spec.InternalMetric(),
		stableCodec.Encoding(),
		spec.Dim,
		spec.InternalDim(),
		stableCodec.EncodedSize(),
		maxCluster,
		probeCS.Epoch(),
		generation,
	)
	if err != nil {
		return nil, err
	}
	defer dataWriter.Abort()
	blockWriter, err := vecindex.CreateSegmentBlockMetaWriter(
		staging.artifacts.blockPath,
		spec,
		stableCodec,
		vecindex.DefaultSegmentBlockRows(stableCodec.Encoding()),
		maxCluster,
		probeCS.Epoch(),
		generation,
	)
	if err != nil {
		return nil, err
	}
	defer blockWriter.Abort()

	rowLocs := make([]rowLoc, 0)
	var spoolReadBuf []byte
	for _, clusterID := range segmentClusterWriteOrder(spec, probeCS, hotClusterScores) {
		spool := spools.spools[clusterID]
		if spool == nil {
			continue
		}
		var readErr error
		spoolReadBuf, readErr = visitCatchUpSpoolRowsBuffered(spool, spec, spoolReadBuf, func(rowID int64, prepared []byte) error {
			enc, encoded, err := stableCodec.Encode(clusterID, prepared)
			if err != nil {
				return fmt.Errorf("bootstrap segment generation: encode rowid %d: %w", rowID, err)
			}
			if enc != stableCodec.Encoding() {
				return fmt.Errorf("bootstrap segment generation: unexpected stable encoding %d for rowid %d", enc, rowID)
			}
			offset := dataWriter.NextOffset()
			if err := dataWriter.Append(clusterID, rowID, encoded); err != nil {
				return fmt.Errorf("bootstrap segment generation: append rowid %d: %w", rowID, err)
			}
			if err := blockWriter.Append(clusterID, rowID, offset, dataWriter.EntrySize(), encoded); err != nil {
				return fmt.Errorf("bootstrap segment generation: append block rowid %d: %w", rowID, err)
			}
			rowLocs = append(rowLocs, rowLoc{rowID: rowID, clusterID: clusterID, offset: offset})
			return nil
		})
		if readErr != nil {
			return nil, readErr
		}
	}
	dataStore, err := dataWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: close data writer: %w", err)
	}
	dataWriter = nil
	blockStore, err := blockWriter.Close()
	if err != nil {
		_ = dataStore.Close()
		return nil, fmt.Errorf("bootstrap segment generation: close block writer: %w", err)
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
	for _, loc := range rowLocs {
		if err := rowMapWriter.Append(loc.rowID, loc.clusterID, loc.offset); err != nil {
			_ = dataStore.Close()
			_ = blockStore.Close()
			return nil, fmt.Errorf("bootstrap segment generation: append rowmap rowid %d: %w", loc.rowID, err)
		}
	}
	rowMapStore, err := rowMapWriter.Close()
	if err != nil {
		_ = dataStore.Close()
		_ = blockStore.Close()
		return nil, fmt.Errorf("bootstrap segment generation: close rowmap writer: %w", err)
	}
	rowMapWriter = nil

	var rowCount uint64
	for clusterID := 1; clusterID <= maxCluster; clusterID++ {
		rowCount += clusterRowCounts[clusterID]
	}
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
		MaxCluster:               uint32(maxCluster),
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
	return &pendingSegmentGeneration{
		meta:     meta,
		spec:     spec,
		dir:      dir,
		staging:  staging,
		manifest: manifest,
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
	}, nil
}

func countOverlayPreparedVectors(snapshot *vecindex.OverlaySnapshot, cutoff uint64) int {
	if snapshot == nil {
		return 0
	}
	count := 0
	snapshot.VisitMutationHeadersAfterUnordered(0, func(mutation vecindex.OverlayMutation) bool {
		if cutoff > 0 && mutation.Sequence > cutoff {
			return true
		}
		if mutation.Kind != vecindex.OverlayMutationDelete {
			count++
		}
		return true
	})
	return count
}

func overlayPreparedVectorCutoffSequence(snapshot *vecindex.OverlaySnapshot, maxRows int) (uint64, int) {
	if snapshot == nil || maxRows <= 0 {
		return 0, 0
	}
	count := 0
	cutoff := uint64(0)
	stopped := false
	snapshot.VisitMutationHeadersAfter(0, func(mutation vecindex.OverlayMutation) bool {
		if mutation.Kind == vecindex.OverlayMutationDelete {
			return true
		}
		count++
		cutoff = mutation.Sequence
		if count >= maxRows {
			stopped = true
			return false
		}
		return true
	})
	if count == 0 {
		return 0, 0
	}
	if !stopped {
		return snapshot.LastSequence(), count
	}
	return cutoff, count
}

func collectOverlayReservoirSample(snapshot *vecindex.OverlaySnapshot, cutoff uint64, want int, seed uint64, dim int) ([][]float32, error) {
	if snapshot == nil || want <= 0 || dim <= 0 {
		return nil, nil
	}
	sample := make([][]float32, 0, want)
	backing := make([]float32, want*dim)
	var seen int
	rng := rand.New(rand.NewSource(int64(seed ^ 0x6a09e667f3bcc909)))
	var visitErr error
	var scratch []byte
	scratch, err := snapshot.VisitMutationsAfterBuffered(0, scratch, func(mutation vecindex.OverlayMutation) bool {
		if cutoff > 0 && mutation.Sequence > cutoff {
			return false
		}
		if mutation.Kind == vecindex.OverlayMutationDelete || len(mutation.Vec) == 0 {
			return true
		}
		vec := metric.BytesToFloat32(mutation.Vec)
		if len(vec) != dim {
			visitErr = fmt.Errorf("overlay bootstrap sample: vector dim=%d want=%d", len(vec), dim)
			return false
		}
		if len(sample) < want {
			slot := len(sample)
			dst := backing[slot*dim : (slot+1)*dim]
			copy(dst, vec)
			sample = append(sample, dst)
		} else {
			j := int(rng.Uint64() % uint64(seen+1))
			if j < want {
				copy(sample[j], vec)
			}
		}
		seen++
		return true
	})
	_ = scratch
	if err != nil {
		return nil, err
	}
	if visitErr != nil {
		return nil, visitErr
	}
	return sample, nil
}

func runMiniBatchOverlayTrainerPass(
	snapshot *vecindex.OverlaySnapshot,
	cutoff uint64,
	batchSize int,
	dim int,
	trainer *kmeans.MiniBatchBalancedTrainer,
	seed uint64,
) (kmeans.MiniBatchPassResult, error) {
	if err := trainer.BeginPass(); err != nil {
		return kmeans.MiniBatchPassResult{}, err
	}
	batch := make([][]float32, 0, batchSize)
	batchBacking := make([]float32, batchSize*dim)
	var observeErr error
	var scratch []byte
	scratch, err := snapshot.VisitMutationsAfterBuffered(0, scratch, func(mutation vecindex.OverlayMutation) bool {
		if cutoff > 0 && mutation.Sequence > cutoff {
			return false
		}
		if mutation.Kind == vecindex.OverlayMutationDelete || len(mutation.Vec) == 0 {
			return true
		}
		vec := metric.BytesToFloat32(mutation.Vec)
		if len(vec) != dim {
			observeErr = fmt.Errorf("overlay bootstrap trainer: vector dim=%d want=%d", len(vec), dim)
			return false
		}
		slot := len(batch)
		dst := batchBacking[slot*dim : (slot+1)*dim]
		copy(dst, vec)
		batch = append(batch, dst)
		if len(batch) < batchSize {
			return true
		}
		if err := trainer.ObserveBatch(batch); err != nil {
			observeErr = err
			return false
		}
		batch = batch[:0]
		return true
	})
	_ = scratch
	if err != nil {
		return kmeans.MiniBatchPassResult{}, err
	}
	if observeErr != nil {
		return kmeans.MiniBatchPassResult{}, observeErr
	}
	if len(batch) > 0 {
		if err := trainer.ObserveBatch(batch); err != nil {
			return kmeans.MiniBatchPassResult{}, err
		}
	}
	return trainer.EndPass(seed)
}
