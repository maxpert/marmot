package db

import (
	"fmt"
	"math/rand"
	"os"
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
	}
	initSize := max(actualK*32, opts.BatchSize*kmeans.DefaultMiniBatchInitFactor)
	if initSize < actualK {
		initSize = actualK
	}
	if initSize > nTotal {
		initSize = nTotal
	}
	samples, err := collectOverlayReservoirSample(snapshot, cutoff, initSize, spec.Seed)
	if err != nil {
		return nil, fmt.Errorf("overlay bootstrap sample: %w", err)
	}
	if len(samples) == 0 {
		return nil, nil
	}
	initCentroids, err := kmeans.KMeansPlusPlus(samples, actualK, spec.Seed, 1)
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
		result, err := runMiniBatchOverlayTrainerPass(snapshot, cutoff, opts.BatchSize, trainer, spec.Seed^uint64(iter+1))
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
		result, err := runMiniBatchOverlayTrainerPass(snapshot, cutoff, opts.BatchSize, trainer, spec.Seed^uint64(opts.MaxIter+extra+1))
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
	stagingDir, dataPath, rowMapPath, err := createSegmentGenerationStaging(dir, generation)
	if err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: create staging: %w", err)
	}
	keepStaging := false
	defer func() {
		if !keepStaging {
			_ = os.RemoveAll(stagingDir)
		}
	}()
	rowMapWriter, err := vecindex.CreateSegmentRowMapWriter(rowMapPath, probeCS.Epoch(), generation)
	if err != nil {
		return nil, err
	}
	defer rowMapWriter.Abort()

	clusterEntries := make([][]incrementalClusterEntry, maxCluster+1)
	clusterRowCounts := make([]uint64, maxCluster+1)
	clusterVectorSums := make([][]float32, maxCluster+1)
	codecReservoir, err := newStableCodecReservoir(spec.Seed^probeCS.Epoch(), spec.InternalDim())
	if err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: stable codec reservoir: %w", err)
	}
	defer codecReservoir.Close()
	var visitErr error
	overlaySnapshot.VisitMutationsAfter(0, func(mutation vecindex.OverlayMutation) bool {
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
		codecReservoir.Add(clusterID, mutation.Vec)
		clusterEntries[clusterID] = append(clusterEntries[clusterID], incrementalClusterEntry{
			rowID: mutation.RowID,
			vec:   append([]byte(nil), mutation.Vec...),
		})
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
	if visitErr != nil {
		return nil, visitErr
	}
	stableCodec, stableCodecBlob, err := buildStableMemberCodec(spec, probeCS, codecReservoir)
	if err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: build stable codec: %w", err)
	}
	dataWriter, err := vecindex.CreateSegmentDataWriter(
		dataPath,
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

	rowLocs := make([]rowLoc, 0)
	for _, clusterID := range segmentClusterWriteOrder(spec, probeCS, hotClusterScores) {
		entries := clusterEntries[clusterID]
		if len(entries) == 0 {
			continue
		}
		slices.SortFunc(entries, func(a, b incrementalClusterEntry) int {
			switch {
			case a.rowID < b.rowID:
				return -1
			case a.rowID > b.rowID:
				return 1
			default:
				return 0
			}
		})
		for _, entry := range entries {
			enc, encoded, err := stableCodec.Encode(clusterID, entry.vec)
			if err != nil {
				return nil, fmt.Errorf("bootstrap segment generation: encode rowid %d: %w", entry.rowID, err)
			}
			if enc != stableCodec.Encoding() {
				return nil, fmt.Errorf("bootstrap segment generation: unexpected stable encoding %d for rowid %d", enc, entry.rowID)
			}
			offset := dataWriter.NextOffset()
			if err := dataWriter.Append(clusterID, entry.rowID, encoded); err != nil {
				return nil, fmt.Errorf("bootstrap segment generation: append rowid %d: %w", entry.rowID, err)
			}
			rowLocs = append(rowLocs, rowLoc{rowID: entry.rowID, clusterID: clusterID, offset: offset})
		}
	}
	dataStore, err := dataWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("bootstrap segment generation: close data writer: %w", err)
	}
	dataWriter = nil

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
			return nil, fmt.Errorf("bootstrap segment generation: append rowmap rowid %d: %w", loc.rowID, err)
		}
	}
	rowMapStore, err := rowMapWriter.Close()
	if err != nil {
		_ = dataStore.Close()
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
		CreatedAtUnixNano:        time.Now().UnixNano(),
	}
	keepStaging = true
	return &pendingSegmentGeneration{
		meta:       meta,
		spec:       spec,
		dir:        dir,
		stagingDir: stagingDir,
		manifest:   manifest,
		dataPath:   dataStore.Path(),
		rowMapPath: rowMapStore.Path(),
		generation: &vecindex.SegmentGeneration{
			Data:                     dataStore,
			RowMap:                   rowMapStore,
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
	snapshot.VisitMutationsAfter(0, func(mutation vecindex.OverlayMutation) bool {
		if cutoff > 0 && mutation.Sequence > cutoff {
			return false
		}
		if mutation.Kind != vecindex.OverlayMutationDelete && len(mutation.Vec) > 0 {
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
	snapshot.VisitMutationsAfter(0, func(mutation vecindex.OverlayMutation) bool {
		if mutation.Kind == vecindex.OverlayMutationDelete || len(mutation.Vec) == 0 {
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

func collectOverlayReservoirSample(snapshot *vecindex.OverlaySnapshot, cutoff uint64, want int, seed uint64) ([][]float32, error) {
	if snapshot == nil || want <= 0 {
		return nil, nil
	}
	sample := make([][]float32, 0, want)
	var seen int
	rng := rand.New(rand.NewSource(int64(seed ^ 0x6a09e667f3bcc909)))
	snapshot.VisitMutationsAfter(0, func(mutation vecindex.OverlayMutation) bool {
		if cutoff > 0 && mutation.Sequence > cutoff {
			return false
		}
		if mutation.Kind == vecindex.OverlayMutationDelete || len(mutation.Vec) == 0 {
			return true
		}
		vec := metric.BytesToFloat32(mutation.Vec)
		if len(sample) < want {
			sample = append(sample, append([]float32(nil), vec...))
		} else {
			j := int(rng.Uint64() % uint64(seen+1))
			if j < want {
				sample[j] = append(sample[j][:0], vec...)
			}
		}
		seen++
		return true
	})
	return sample, nil
}

func runMiniBatchOverlayTrainerPass(
	snapshot *vecindex.OverlaySnapshot,
	cutoff uint64,
	batchSize int,
	trainer *kmeans.MiniBatchBalancedTrainer,
	seed uint64,
) (kmeans.MiniBatchPassResult, error) {
	if err := trainer.BeginPass(); err != nil {
		return kmeans.MiniBatchPassResult{}, err
	}
	batch := make([][]float32, 0, batchSize)
	var observeErr error
	snapshot.VisitMutationsAfter(0, func(mutation vecindex.OverlayMutation) bool {
		if cutoff > 0 && mutation.Sequence > cutoff {
			return false
		}
		if mutation.Kind == vecindex.OverlayMutationDelete || len(mutation.Vec) == 0 {
			return true
		}
		batch = append(batch, metric.BytesToFloat32(mutation.Vec))
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
