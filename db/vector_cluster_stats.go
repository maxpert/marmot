package db

import (
	"fmt"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

type cutoffClusterStats struct {
	Counts          []uint64
	Sums            [][]float32
	Touched         map[int64]struct{}
	TotalRows       uint64
	PrefixMutations []vecindex.OverlayMutation
}

func overlayMutationsUpTo(snapshot *vecindex.OverlaySnapshot, minSequence, cutoff uint64) []vecindex.OverlayMutation {
	if snapshot == nil {
		return nil
	}
	mutations := make([]vecindex.OverlayMutation, 0)
	snapshot.VisitMutationsAfter(minSequence, func(mutation vecindex.OverlayMutation) bool {
		if cutoff > 0 && mutation.Sequence > cutoff {
			return false
		}
		mutations = append(mutations, mutation)
		return true
	})
	return mutations
}

func buildCutoffClusterStats(
	spec vecindex.IVFSpec,
	base *vecindex.SegmentGeneration,
	overlaySnapshot *vecindex.OverlaySnapshot,
	cutoff uint64,
	maintenance *vecindex.MaintenanceState,
) (*cutoffClusterStats, error) {
	if base == nil || base.Data == nil || base.RowMap == nil {
		return nil, fmt.Errorf("cutoff cluster stats: base generation is required")
	}
	counts := append([]uint64(nil), base.ClusterRowCounts...)
	sums := cloneClusterVectorSums(base.ClusterVectorSums)
	useMaintenanceLiveStats := maintenance != nil &&
		overlaySnapshot != nil &&
		(cutoff == 0 || cutoff == overlaySnapshot.LastSequence())
	if useMaintenanceLiveStats {
		if liveCounts := maintenance.LiveClusterRowCounts(); len(liveCounts) > 0 {
			counts = liveCounts
		}
		if liveSums := maintenance.LiveClusterVectorSums(); len(liveSums) > 0 {
			sums = liveSums
		}
	}
	if len(counts) == 0 {
		counts = make([]uint64, base.Data.MaxCluster()+1)
		for clusterID := 1; clusterID <= base.Data.MaxCluster(); clusterID++ {
			counts[clusterID] = base.Data.ClusterCount(int64(clusterID))
		}
	}
	prefix := overlayMutationsUpTo(overlaySnapshot, base.AppliedOverlaySeq, cutoff)
	touched := make(map[int64]struct{}, len(prefix)*2)
	for _, mutation := range prefix {
		if loc, ok, err := base.RowMap.Lookup(mutation.RowID); err != nil {
			return nil, fmt.Errorf("cutoff cluster stats: lookup rowid %d: %w", mutation.RowID, err)
		} else if ok {
			touched[loc.ClusterID] = struct{}{}
			if !useMaintenanceLiveStats && (mutation.Kind == vecindex.OverlayMutationReplace || mutation.Kind == vecindex.OverlayMutationDelete) {
				if err := applyStableDelta(counts, sums, base, spec, loc.ClusterID, mutation.RowID, -1); err != nil {
					return nil, err
				}
			}
		}
		if mutation.Kind == vecindex.OverlayMutationDelete || mutation.ClusterID <= 0 {
			continue
		}
		touched[mutation.ClusterID] = struct{}{}
		if useMaintenanceLiveStats {
			continue
		}
		ensureClusterStatsCapacity(&counts, &sums, int(mutation.ClusterID), len(metric.BytesToFloat32(mutation.Vec)))
		counts[mutation.ClusterID]++
		sum := sums[mutation.ClusterID]
		for i, value := range metric.BytesToFloat32(mutation.Vec) {
			sum[i] += value
		}
	}
	var totalRows uint64
	for clusterID := 1; clusterID < len(counts); clusterID++ {
		totalRows += counts[clusterID]
	}
	return &cutoffClusterStats{
		Counts:          counts,
		Sums:            sums,
		Touched:         touched,
		TotalRows:       totalRows,
		PrefixMutations: prefix,
	}, nil
}

func applyStableDelta(
	counts []uint64,
	sums [][]float32,
	base *vecindex.SegmentGeneration,
	spec vecindex.IVFSpec,
	clusterID int64,
	rowID int64,
	sign int,
) error {
	if clusterID <= 0 || rowID == 0 || base == nil {
		return nil
	}
	preparedClusterID, prepared, err := loadStablePreparedForMaintenance(base, spec, rowID)
	if err != nil {
		return fmt.Errorf("cutoff cluster stats: load stable rowid %d: %w", rowID, err)
	}
	if preparedClusterID == 0 || len(prepared) == 0 {
		return nil
	}
	if preparedClusterID != clusterID {
		clusterID = preparedClusterID
	}
	if int(clusterID) >= len(counts) || int(clusterID) >= len(sums) {
		return nil
	}
	if sign < 0 {
		if counts[clusterID] > 0 {
			counts[clusterID]--
		}
	} else {
		counts[clusterID]++
	}
	if sums[clusterID] == nil {
		sums[clusterID] = make([]float32, len(prepared))
	}
	scale := float32(sign)
	for i, value := range prepared {
		sums[clusterID][i] += scale * value
	}
	return nil
}

func ensureClusterStatsCapacity(counts *[]uint64, sums *[][]float32, clusterID int, dim int) {
	if clusterID < 0 {
		return
	}
	if len(*counts) <= clusterID {
		next := make([]uint64, clusterID+1)
		copy(next, *counts)
		*counts = next
	}
	if len(*sums) <= clusterID {
		next := make([][]float32, clusterID+1)
		copy(next, *sums)
		*sums = next
	}
	if dim > 0 && (*sums)[clusterID] == nil {
		(*sums)[clusterID] = make([]float32, dim)
	}
}

func centroidSetFromCountsAndSums(base *kmeans.CentroidSet, counts []uint64, sums [][]float32, epoch uint64) (*kmeans.CentroidSet, error) {
	if base == nil {
		return nil, nil
	}
	centroids := base.Snapshot()
	for clusterID := 1; clusterID <= len(centroids); clusterID++ {
		if clusterID >= len(counts) || counts[clusterID] == 0 {
			continue
		}
		if clusterID >= len(sums) || len(sums[clusterID]) == 0 {
			continue
		}
		next := make([]float32, len(sums[clusterID]))
		inv := 1 / float32(counts[clusterID])
		for i, value := range sums[clusterID] {
			next[i] = value * inv
		}
		centroids[clusterID-1] = next
	}
	return kmeans.NewCentroidSet(epoch, centroids)
}

func probeCentroidSetForTouched(base *kmeans.CentroidSet, counts []uint64, sums [][]float32, touched map[int64]struct{}, epoch uint64) (*kmeans.CentroidSet, error) {
	if base == nil {
		return nil, nil
	}
	centroids := base.Snapshot()
	for clusterID := range touched {
		if clusterID <= 0 || int(clusterID) > len(centroids) {
			continue
		}
		if int(clusterID) >= len(counts) || counts[clusterID] == 0 {
			continue
		}
		if int(clusterID) >= len(sums) || len(sums[clusterID]) == 0 {
			continue
		}
		next := make([]float32, len(sums[clusterID]))
		inv := 1 / float32(counts[clusterID])
		for i, value := range sums[clusterID] {
			next[i] = value * inv
		}
		centroids[clusterID-1] = next
	}
	return kmeans.NewCentroidSet(epoch, centroids)
}

func stableCentroidSetForTouched(baseStable *kmeans.CentroidSet, nextProbe *kmeans.CentroidSet, touched map[int64]struct{}) (*kmeans.CentroidSet, error) {
	if baseStable == nil || nextProbe == nil {
		return nil, nil
	}
	centroids := baseStable.Snapshot()
	nextSnapshot := nextProbe.Snapshot()
	for clusterID := range touched {
		if clusterID <= 0 || int(clusterID) > len(centroids) || int(clusterID) > len(nextSnapshot) {
			continue
		}
		centroids[clusterID-1] = append([]float32(nil), nextSnapshot[clusterID-1]...)
	}
	return kmeans.NewCentroidSet(nextProbe.Epoch(), centroids)
}

func reassignOverlayMutationsForProbe(
	snapshot *vecindex.OverlaySnapshot,
	minSequence uint64,
	spec vecindex.IVFSpec,
	probe *kmeans.CentroidSet,
	stable *vecindex.SegmentGeneration,
) ([]vecindex.OverlayMutation, error) {
	if snapshot == nil {
		return nil, nil
	}
	rewritten := make([]vecindex.OverlayMutation, 0)
	var reassignErr error
	snapshot.VisitMutationsAfter(minSequence, func(mutation vecindex.OverlayMutation) bool {
		next, err := reassignOverlayMutationForProbe(mutation, spec, probe, stable)
		if err != nil {
			reassignErr = err
			return false
		}
		rewritten = append(rewritten, next)
		return true
	})
	if reassignErr != nil {
		return nil, reassignErr
	}
	return rewritten, nil
}

func reassignOverlayMutationForProbe(
	mutation vecindex.OverlayMutation,
	spec vecindex.IVFSpec,
	probe *kmeans.CentroidSet,
	stable *vecindex.SegmentGeneration,
) (vecindex.OverlayMutation, error) {
	next := mutation
	next.Epoch = probe.Epoch()
	if mutation.Kind == vecindex.OverlayMutationDelete {
		next.ClusterID = 0
		return next, nil
	}
	next.Kind = vecindex.OverlayMutationUpsert
	if stable != nil && stable.RowMap != nil {
		if _, ok, err := stable.RowMap.Lookup(mutation.RowID); err != nil {
			return vecindex.OverlayMutation{}, fmt.Errorf("reassign overlay mutation rowid %d: stable lookup: %w", mutation.RowID, err)
		} else if ok {
			next.Kind = vecindex.OverlayMutationReplace
		}
	}
	clusterID, err := assignPreparedAgainstSet(mutation.Vec, spec, probe)
	if err != nil {
		return vecindex.OverlayMutation{}, fmt.Errorf("reassign overlay mutation rowid %d: %w", mutation.RowID, err)
	}
	next.ClusterID = clusterID
	return next, nil
}
