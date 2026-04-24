package db

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/quantize"
)

type incrementalClusterEntry struct {
	rowID    int64
	vec      []byte
	prepared []float32
}

type rowLoc struct {
	rowID     int64
	clusterID int64
	offset    uint64
}

type pendingMutation struct {
	kind      vecindex.OverlayMutationKind
	clusterID int64
	rowID     int64
	vec       []byte
}

type pendingSegmentGeneration struct {
	dir        string
	manifest   vecindex.SegmentManifest
	dataPath   string
	rowMapPath string
	generation *vecindex.SegmentGeneration
}

func (p *pendingSegmentGeneration) Publish() error {
	if p == nil {
		return nil
	}
	return publishSegmentGeneration(p.dir, p.manifest, p.dataPath, p.rowMapPath)
}

func (p *pendingSegmentGeneration) Close() {
	if p == nil || p.generation == nil {
		return
	}
	_ = p.generation.Close()
}

func BuildIncrementalSegmentGeneration(
	ctx context.Context,
	db *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	probeCS *kmeans.CentroidSet,
	stableCS *kmeans.CentroidSet,
	base *vecindex.SegmentGeneration,
	overlaySnapshot *vecindex.OverlaySnapshot,
	cutoffSequence uint64,
	clusterRowCounts []uint64,
	clusterVectorSums [][]float32,
	hotClusterScores map[int64]uint64,
) (*pendingSegmentGeneration, error) {
	if probeCS == nil || stableCS == nil || base == nil || base.Data == nil || base.RowMap == nil {
		return nil, fmt.Errorf("incremental segment generation: stable base generation is required")
	}
	if overlaySnapshot == nil {
		return nil, nil
	}
	mutations := overlaySnapshot.MutationsAfter(base.AppliedOverlaySeq)
	if cutoffSequence > 0 && len(mutations) > 0 {
		filtered := mutations[:0]
		for _, mutation := range mutations {
			if mutation.Sequence > cutoffSequence {
				break
			}
			filtered = append(filtered, mutation)
		}
		mutations = filtered
	}
	return buildIncrementalSegmentGenerationFromMutations(
		ctx,
		db,
		dbPath,
		meta,
		spec,
		probeCS,
		stableCS,
		base,
		mutations,
		cutoffSequence,
		clusterRowCounts,
		clusterVectorSums,
		hotClusterScores,
	)
}

func buildIncrementalSegmentGenerationFromMutations(
	ctx context.Context,
	db *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	probeCS *kmeans.CentroidSet,
	stableCS *kmeans.CentroidSet,
	base *vecindex.SegmentGeneration,
	mutations []vecindex.OverlayMutation,
	cutoffSequence uint64,
	clusterRowCounts []uint64,
	clusterVectorSums [][]float32,
	hotClusterScores map[int64]uint64,
) (*pendingSegmentGeneration, error) {
	if len(mutations) == 0 {
		return nil, nil
	}

	maxCluster := stableCS.Len()
	if maxCluster == 0 {
		return nil, nil
	}
	dir := vecindex.SegmentStoreDir(dbPath, meta.IndexName)
	generation, err := nextSegmentGeneration(dir)
	if err != nil {
		return nil, fmt.Errorf("incremental segment generation: next generation: %w", err)
	}

	pendingByRow := make(map[int64]pendingMutation, len(mutations))
	touchedClusters := make(map[int64]struct{}, len(mutations)*2)
	for _, mutation := range mutations {
		pendingByRow[mutation.RowID] = pendingMutation{
			kind:      mutation.Kind,
			clusterID: mutation.ClusterID,
			rowID:     mutation.RowID,
			vec:       append([]byte(nil), mutation.Vec...),
		}
		if loc, ok, err := base.RowMap.Lookup(mutation.RowID); err != nil {
			return nil, fmt.Errorf("incremental segment generation: lookup rowmap rowid %d: %w", mutation.RowID, err)
		} else if ok {
			touchedClusters[loc.ClusterID] = struct{}{}
		}
		if mutation.Kind != vecindex.OverlayMutationDelete && mutation.ClusterID > 0 {
			touchedClusters[mutation.ClusterID] = struct{}{}
		}
	}
	if len(touchedClusters) == 0 {
		return nil, nil
	}
	exactFetcher, err := newExactVectorFetcher(ctx, db, meta, spec)
	if err != nil {
		return nil, fmt.Errorf("incremental segment generation: exact vector fetcher: %w", err)
	}
	if exactFetcher != nil {
		defer exactFetcher.Close()
	}

	baseCodec := base.StableCodec
	if baseCodec == nil {
		baseCodec, err = vecindex.DecodeStableMemberCodecBlob(spec, base.StableCentroids, base.Data.Encoding(), nil)
		if err != nil {
			return nil, fmt.Errorf("incremental segment generation: load base stable codec: %w", err)
		}
	}
	stableCodec, err := baseCodec.WithCentroids(stableCS)
	if err != nil {
		return nil, fmt.Errorf("incremental segment generation: derive stable codec: %w", err)
	}
	stableCodecBlob, err := vecindex.EncodeStableMemberCodecBlob(stableCodec)
	if err != nil {
		return nil, fmt.Errorf("incremental segment generation: encode stable codec metadata: %w", err)
	}
	dataPath := vecindex.SegmentDataPath(dir, generation)
	rowMapPath := vecindex.SegmentRowMapPath(dir, generation)
	dataWriter, err := vecindex.CreateSegmentDataWriter(
		dataPath,
		spec.InternalMetric(),
		stableCodec.Encoding(),
		spec.Dim,
		spec.InternalDim(),
		stableCodec.EncodedSize(),
		maxCluster,
		stableCS.Epoch(),
		generation,
	)
	if err != nil {
		return nil, err
	}
	defer dataWriter.Abort()

	rowMapWriter, err := vecindex.CreateSegmentRowMapWriter(rowMapPath, stableCS.Epoch(), generation)
	if err != nil {
		return nil, err
	}
	defer rowMapWriter.Abort()

	oldDataFile, err := os.Open(base.Data.Path())
	if err != nil {
		return nil, err
	}
	defer oldDataFile.Close()

	order := incrementalClusterWriteOrder(base.Data, maxCluster)
	clusterRowCounts = append([]uint64(nil), clusterRowCounts...)
	if len(clusterRowCounts) < maxCluster+1 {
		next := make([]uint64, maxCluster+1)
		copy(next, clusterRowCounts)
		clusterRowCounts = next
	}
	clusterVectorSums = cloneClusterVectorSums(clusterVectorSums)
	if len(clusterVectorSums) < maxCluster+1 {
		next := make([][]float32, maxCluster+1)
		copy(next, clusterVectorSums)
		clusterVectorSums = next
	}
	oldOffsets := make([]uint64, maxCluster+1)
	newOffsets := make([]uint64, maxCluster+1)
	rowLocs := make([]rowLoc, 0, base.RowMap.EntryCount()+uint64(len(mutations)))

	for _, clusterID := range order {
		if clusterID <= 0 || int(clusterID) > maxCluster {
			continue
		}
		newOffsets[clusterID] = dataWriter.NextOffset()
		if offset, _, _, ok := base.Data.ClusterSpan(clusterID); ok {
			oldOffsets[clusterID] = uint64(offset)
		}

		if _, touched := touchedClusters[clusterID]; !touched {
			count := base.Data.ClusterCount(clusterID)
			if clusterID >= int64(len(clusterRowCounts)) {
				return nil, fmt.Errorf("incremental segment generation: cluster metadata missing for cluster %d", clusterID)
			}
			clusterRowCounts[clusterID] = count
			if count == 0 {
				continue
			}
			offset, bytes, _, ok := base.Data.ClusterSpan(clusterID)
			if !ok {
				return nil, fmt.Errorf("incremental segment generation: missing span for untouched cluster %d", clusterID)
			}
			if err := dataWriter.AppendRawCluster(clusterID, count, io.NewSectionReader(oldDataFile, offset, bytes)); err != nil {
				return nil, fmt.Errorf("incremental segment generation: copy untouched cluster %d: %w", clusterID, err)
			}
			continue
		}

		entries, err := rebuildTouchedClusterEntries(ctx, baseCodec, stableCodec, exactFetcher, base, clusterID, pendingByRow)
		if err != nil {
			return nil, err
		}
		clusterRowCounts[clusterID] = uint64(len(entries))
		clusterVectorSums[clusterID] = make([]float32, spec.InternalDim())
		for _, entry := range entries {
			offset := dataWriter.NextOffset()
			if err := dataWriter.Append(clusterID, entry.rowID, entry.vec); err != nil {
				return nil, fmt.Errorf("incremental segment generation: append touched cluster %d rowid %d: %w", clusterID, entry.rowID, err)
			}
			prepared := entry.prepared
			if len(prepared) == 0 {
				var err error
				prepared, err = stableCodec.DecodePrepared(clusterID, entry.vec)
				if err != nil {
					return nil, fmt.Errorf("incremental segment generation: decode prepared rowid %d: %w", entry.rowID, err)
				}
			}
			for i, value := range prepared {
				clusterVectorSums[clusterID][i] += value
			}
			rowLocs = append(rowLocs, rowLoc{rowID: entry.rowID, clusterID: clusterID, offset: offset})
		}
	}

	dataStore, err := dataWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("incremental segment generation: close data writer: %w", err)
	}
	dataWriter = nil

	err = base.RowMap.Scan(func(loc vecindex.SegmentRowLocation) bool {
		if _, touched := touchedClusters[loc.ClusterID]; touched {
			return true
		}
		rowLocs = append(rowLocs, rowLoc{
			rowID:     loc.RowID,
			clusterID: loc.ClusterID,
			offset:    newOffsets[loc.ClusterID] + (loc.Offset - oldOffsets[loc.ClusterID]),
		})
		return true
	})
	if err != nil {
		_ = dataStore.Close()
		return nil, fmt.Errorf("incremental segment generation: scan existing rowmap: %w", err)
	}

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
			return nil, fmt.Errorf("incremental segment generation: append rowmap rowid %d: %w", loc.rowID, err)
		}
	}

	rowMapStore, err := rowMapWriter.Close()
	if err != nil {
		_ = dataStore.Close()
		return nil, fmt.Errorf("incremental segment generation: close rowmap writer: %w", err)
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
		StableCentroidEpoch:      stableCS.Epoch(),
		StableCentroidBlob:       mustCentroidBlob(stableCS),
		StableMemberCodecBlob:    stableCodecBlob,
		AppliedOverlaySeq:        cutoffSequence,
		Generation:               generation,
		MaxCluster:               uint32(maxCluster),
		RowCount:                 rowCount,
		ClusterRowCounts:         clusterRowCounts,
		ClusterVectorSums:        cloneClusterVectorSums(clusterVectorSums),
		RowsModifiedSinceRebuild: 0,
		LastRebuildRowCount:      rowCount,
		ConsecutiveSkewCycles:    nextSkewCycleCount(clusterRowCounts, meta.TargetPartitionSize, base.ConsecutiveSkewCycles),
		LayoutHotClusters:        uint32Slice(orderedHotClusterIDs(hotClusterScores, segmentLayoutHotClusterLimit)),
		CreatedAtUnixNano:        time.Now().UnixNano(),
	}
	return &pendingSegmentGeneration{
		dir:        dir,
		manifest:   manifest,
		dataPath:   dataStore.Path(),
		rowMapPath: rowMapStore.Path(),
		generation: &vecindex.SegmentGeneration{
			Data:                     dataStore,
			RowMap:                   rowMapStore,
			ProbeCentroids:           probeCS,
			StableCentroids:          stableCS,
			StableCodec:              stableCodec,
			AppliedOverlaySeq:        cutoffSequence,
			ClusterRowCounts:         append([]uint64(nil), clusterRowCounts...),
			ClusterVectorSums:        cloneClusterVectorSums(clusterVectorSums),
			RowsModifiedSinceRebuild: 0,
			LastRebuildRowCount:      rowCount,
			ConsecutiveSkewCycles:    nextSkewCycleCount(clusterRowCounts, meta.TargetPartitionSize, base.ConsecutiveSkewCycles),
			LayoutHotClusters:        int64Slice(manifest.LayoutHotClusters),
		},
	}, nil
}

func rebuildTouchedClusterEntries(
	ctx context.Context,
	baseCodec *vecindex.StableMemberCodec,
	stableCodec *vecindex.StableMemberCodec,
	exactFetcher *exactVectorFetcher,
	base *vecindex.SegmentGeneration,
	clusterID int64,
	pendingByRow map[int64]pendingMutation,
) ([]incrementalClusterEntry, error) {
	entries := make([]incrementalClusterEntry, 0, base.Data.ClusterCount(clusterID))
	var scanErr error
	if err := base.Data.ScanCluster(clusterID, func(rowID int64, vecBytes []byte) bool {
		if _, changed := pendingByRow[rowID]; changed {
			return true
		}
		var preparedBlob []byte
		var prepared []float32
		if exactFetcher != nil {
			var ok bool
			var err error
			preparedBlob, ok, err = exactFetcher.Prepared(ctx, rowID)
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
			prepared, err = baseCodec.DecodePrepared(clusterID, vecBytes)
			if err != nil {
				scanErr = err
				return false
			}
			preparedBlob = vecindex.Float32ToBytes(prepared)
		}
		enc, encoded, err := stableCodec.Encode(clusterID, preparedBlob)
		if err != nil {
			scanErr = err
			return false
		}
		if enc != stableCodec.Encoding() {
			scanErr = fmt.Errorf("unexpected stable encoding %d for rowid %d", enc, rowID)
			return false
		}
		entries = append(entries, incrementalClusterEntry{
			rowID:    rowID,
			vec:      encoded,
			prepared: prepared,
		})
		return true
	}); err != nil {
		return nil, fmt.Errorf("incremental segment generation: scan touched cluster %d: %w", clusterID, err)
	}
	if scanErr != nil {
		return nil, fmt.Errorf("incremental segment generation: rebuild touched cluster %d: %w", clusterID, scanErr)
	}
	for _, mutation := range pendingByRow {
		if mutation.kind == vecindex.OverlayMutationDelete || mutation.clusterID != clusterID {
			continue
		}
		enc, encoded, err := stableCodec.Encode(clusterID, mutation.vec)
		if err != nil {
			return nil, fmt.Errorf("incremental segment generation: encode rowid %d: %w", mutation.rowID, err)
		}
		if enc != stableCodec.Encoding() {
			return nil, fmt.Errorf("incremental segment generation: unexpected stable encoding %d for rowid %d", enc, mutation.rowID)
		}
		entries = append(entries, incrementalClusterEntry{
			rowID:    mutation.rowID,
			vec:      encoded,
			prepared: clonePreparedVector(mutation.vec),
		})
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
	return entries, nil
}

func decodeStableMemberPrepared(spec vecindex.IVFSpec, codec *vecindex.StableMemberCodec, cs *kmeans.CentroidSet, clusterID int64, vecBytes []byte) ([]float32, error) {
	if codec != nil {
		return codec.DecodePrepared(clusterID, vecBytes)
	}
	enc, _ := vecindex.StableMemberEncodingSpec(spec)
	switch enc {
	case vecindex.MemberEncodingRawPreparedF32:
		return append([]float32(nil), metric.BytesToFloat32(vecBytes)...), nil
	case vecindex.MemberEncodingResidualInt8:
		if cs == nil || clusterID <= 0 || int(clusterID) > cs.Len() {
			return nil, fmt.Errorf("missing centroid for cluster %d", clusterID)
		}
		decoded, _, err := quantize.DecodeResidualInt8(spec.InternalMetric(), cs.Snapshot()[clusterID-1], vecBytes, vecindex.MemberResidualBlockSize, nil)
		if err != nil {
			return nil, err
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported stable encoding %d", enc)
	}
}

func incrementalClusterWriteOrder(store *vecindex.SegmentDataStore, maxCluster int) []int64 {
	ordered := make([]int64, 0, maxCluster)
	seen := make(map[int64]struct{}, maxCluster)
	if store != nil {
		for _, clusterID := range store.FileOrderedClusters() {
			if clusterID <= 0 {
				continue
			}
			ordered = append(ordered, clusterID)
			seen[clusterID] = struct{}{}
		}
	}
	for clusterID := 1; clusterID <= maxCluster; clusterID++ {
		if _, ok := seen[int64(clusterID)]; ok {
			continue
		}
		ordered = append(ordered, int64(clusterID))
	}
	return ordered
}
