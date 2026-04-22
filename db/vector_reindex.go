package db

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/rs/zerolog/log"
)

func retuneReindexMeta(meta common.VectorIndexMeta, spec vecindex.IVFSpec, rows int64) (common.VectorIndexMeta, vecindex.IVFSpec) {
	if meta.AutoTuneNlist {
		meta.Nlist = autoTuneNlistForTarget(rows, meta.TargetPartitionSize)
		spec.Nlist = meta.Nlist
	}
	if meta.AutoTuneNprobe {
		meta.Nprobe = autoTuneNprobeForTarget(meta.Nlist, meta.TargetPartitionSize)
		spec.Nprobe = meta.Nprobe
	}
	if meta.AutoTuneNlist || meta.AutoTuneNprobe {
		spec.Seed = StableIndexSeed(meta)
	}
	return meta, spec
}

func Reindex(
	ctx context.Context,
	db *sql.DB,
	engine *vecindex.Engine,
	meta common.VectorIndexMeta,
	overrideNlist int,
	_ int64,
) (common.VectorIndexMeta, *vecindex.IndexState, error) {
	state, ok := engine.Lookup(meta.IndexName)
	if !ok {
		return meta, nil, fmt.Errorf("MARMOT-VEC-013: vector index %q not registered in engine", meta.IndexName)
	}

	spec := state.Spec()
	currentN, err := countIndexableRows(ctx, db, meta.TableName, meta.ColumnName, spec)
	if err != nil {
		return meta, nil, fmt.Errorf("reindex: count rows for tuning: %w", err)
	}
	meta, spec = retuneReindexMeta(meta, spec, currentN)
	if overrideNlist > 0 {
		meta.Nlist = overrideNlist
		spec.Nlist = overrideNlist
		if meta.AutoTuneNprobe {
			meta.Nprobe = autoTuneNprobeForTarget(meta.Nlist, meta.TargetPartitionSize)
			spec.Nprobe = meta.Nprobe
		}
	}

	oldEpoch := state.ProbeVersion()
	var initCentroids [][]float32
	if maintenance := state.LoadMaintenanceState(); maintenance != nil {
		if current := state.ProbeState(); current != nil {
			initCentroids = maintenance.LiveCentroids(current.Snapshot())
		}
	}
	if len(initCentroids) == 0 {
		if current := state.ProbeState(); current != nil {
			initCentroids = current.Snapshot()
		}
	}
	if spec.Nlist > len(initCentroids) {
		splitInit, err := promotionWarmStartCentroids(state, spec, initCentroids, meta.TargetPartitionSize)
		if err != nil {
			return meta, nil, fmt.Errorf("reindex: promotion warm start: %w", err)
		}
		if len(splitInit) > len(initCentroids) {
			initCentroids = splitInit
		}
	}
	cs, err := computeCentroids(ctx, db, meta.TableName, meta.ColumnName, spec, meta.TargetPartitionSize, initCentroids)
	if err != nil {
		return meta, nil, fmt.Errorf("reindex: compute centroids: %w", err)
	}
	if cs != nil {
		nextEpoch := oldEpoch + 1
		if nextEpoch == 0 {
			nextEpoch = 1
		}
		cs, err = kmeans.NewCentroidSet(nextEpoch, cs.Snapshot())
		if err != nil {
			return meta, nil, fmt.Errorf("reindex: re-epoch centroids: %w", err)
		}
	}

	newState := vecindex.NewIndexState(spec, cs)

	log.Info().
		Str("index", meta.IndexName).
		Uint64("old_epoch", oldEpoch).
		Uint64("new_epoch", newState.ProbeVersion()).
		Int64("rows", currentN).
		Msg("Reindex: prepared local generation")
	return meta, newState, nil
}

type promotionSplitSource struct {
	clusterID int64
	count     uint64
	splits    int
}

func promotionWarmStartCentroids(
	state *vecindex.IndexState,
	spec vecindex.IVFSpec,
	base [][]float32,
	targetPartitionSize int,
) ([][]float32, error) {
	if state == nil || len(base) == 0 || spec.Nlist <= len(base) {
		return base, nil
	}
	segments := state.LoadSegmentStore()
	if segments == nil || segments.Data == nil || segments.Centroids == nil {
		return base, nil
	}
	counts := segments.ClusterRowCounts
	if maintenance := state.LoadMaintenanceState(); maintenance != nil {
		if live := maintenance.LiveClusterRowCounts(); len(live) == len(base)+1 {
			counts = live
		}
	}
	sources := selectPromotionSplitSources(counts, len(base), spec.Nlist, targetPartitionSize)
	if len(sources) == 0 {
		return base, nil
	}
	seeded := make([][]float32, len(base), spec.Nlist)
	for i := range base {
		seeded[i] = append([]float32(nil), base[i]...)
	}
	for _, source := range sources {
		if len(seeded) >= spec.Nlist {
			break
		}
		extra := source.splits
		remaining := spec.Nlist - len(seeded)
		if extra > remaining {
			extra = remaining
		}
		splitSeeds, err := clusterSplitSeeds(segments, spec, source.clusterID, seeded[source.clusterID-1], extra)
		if err != nil {
			return nil, err
		}
		for _, seed := range splitSeeds {
			if len(seeded) >= spec.Nlist {
				break
			}
			seeded = append(seeded, seed)
		}
	}
	return seeded, nil
}

func selectPromotionSplitSources(counts []uint64, currentK, wantK, targetPartitionSize int) []promotionSplitSource {
	if len(counts) <= 1 || wantK <= currentK {
		return nil
	}
	if targetPartitionSize <= 0 {
		targetPartitionSize = defaultTargetPartitionSize
	}
	extraNeeded := wantK - currentK
	sources := make([]promotionSplitSource, 0, len(counts)-1)
	for clusterID := 1; clusterID < len(counts); clusterID++ {
		count := counts[clusterID]
		if count <= uint64(targetPartitionSize) {
			continue
		}
		splits := int((count + uint64(targetPartitionSize) - 1) / uint64(targetPartitionSize))
		splits--
		if splits < 1 {
			continue
		}
		sources = append(sources, promotionSplitSource{
			clusterID: int64(clusterID),
			count:     count,
			splits:    splits,
		})
	}
	slices.SortFunc(sources, func(a, b promotionSplitSource) int {
		switch {
		case a.count > b.count:
			return -1
		case a.count < b.count:
			return 1
		case a.clusterID < b.clusterID:
			return -1
		default:
			return 1
		}
	})
	if len(sources) == 0 {
		return nil
	}
	selected := make([]promotionSplitSource, 0, len(sources))
	for _, source := range sources {
		if extraNeeded <= 0 {
			break
		}
		if source.splits > extraNeeded {
			source.splits = extraNeeded
		}
		selected = append(selected, source)
		extraNeeded -= source.splits
	}
	return selected
}

func clusterSplitSeeds(
	segments *vecindex.SegmentGeneration,
	spec vecindex.IVFSpec,
	clusterID int64,
	baseCentroid []float32,
	extra int,
) ([][]float32, error) {
	if segments == nil || segments.Data == nil || segments.Centroids == nil || extra <= 0 {
		return nil, nil
	}
	clusterCount := int(segments.Data.ClusterCount(clusterID))
	if clusterCount <= extra {
		return nil, nil
	}
	vectors := make([][]float32, 0, clusterCount)
	if err := segments.Data.ScanCluster(clusterID, func(_ int64, vecBytes []byte) bool {
		prepared, err := decodeStableMemberPrepared(spec, segments.Centroids, clusterID, vecBytes)
		if err != nil {
			vectors = nil
			return false
		}
		vectors = append(vectors, prepared)
		return true
	}); err != nil {
		return nil, err
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

func countIndexableRows(
	ctx context.Context,
	db *sql.DB,
	tableName, columnName string,
	spec vecindex.IVFSpec,
) (int64, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY rowid",
			quoteIdent(columnName), quoteIdent(tableName), quoteIdent(columnName)),
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return 0, err
		}
		mv, err := materializeVectorBlob(blob, spec.Metric, spec.Dim, spec.MaxNorm)
		if err != nil {
			return 0, err
		}
		if mv != nil {
			n++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return n, nil
}
