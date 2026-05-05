package db

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
)

func uniformClusterRows(nlist int, rowsPerCluster uint64) []uint64 {
	clusterRows := make([]uint64, nlist+1)
	for clusterID := 1; clusterID <= nlist; clusterID++ {
		clusterRows[clusterID] = rowsPerCluster
	}
	return clusterRows
}

func TestShouldIncrementalMerge(t *testing.T) {
	t.Parallel()

	now := time.Now()
	tests := []struct {
		name         string
		rows         int
		bytes        int64
		oldest       int64
		wantDecision bool
	}{
		{name: "rows threshold", rows: mergeRowsThreshold, wantDecision: true},
		{name: "bytes threshold", bytes: mergeBytesThreshold, wantDecision: true},
		{name: "age threshold", oldest: now.Add(-mergeAgeThreshold - time.Second).UnixNano(), wantDecision: true},
		{name: "below thresholds", rows: mergeRowsThreshold - 1, bytes: mergeBytesThreshold - 1, oldest: now.Add(-mergeAgeThreshold + time.Second).UnixNano(), wantDecision: false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := shouldIncrementalMerge(tc.rows, tc.bytes, tc.oldest)
			if got != tc.wantDecision {
				t.Fatalf("shouldIncrementalMerge() = %v, want %v", got, tc.wantDecision)
			}
		})
	}
}

func TestShouldIncrementalRepair(t *testing.T) {
	t.Parallel()

	meta := common.VectorIndexMeta{
		Nlist:               4,
		AutoTuneNlist:       true,
		TargetPartitionSize: rebuildTargetClusterSize,
	}
	tests := []struct {
		name         string
		clusterRows  []uint64
		maintenance  *vecindex.MaintenanceState
		clusterDrift float64
		wantDecision bool
	}{
		{
			name:         "modified rows over floor",
			clusterRows:  uniformClusterRows(meta.Nlist, rebuildTargetClusterSize),
			maintenance:  &vecindex.MaintenanceState{RowsModifiedSinceRebuild: rebuildRowsFloor, LastRebuildRowCount: 10_000},
			wantDecision: true,
		},
		{
			name:         "oversized cluster",
			clusterRows:  []uint64{0, uint64(rebuildClusterFactor*meta.TargetPartitionSize + 1), rebuildTargetClusterSize, rebuildTargetClusterSize, rebuildTargetClusterSize},
			maintenance:  &vecindex.MaintenanceState{RowsModifiedSinceRebuild: 1, LastRebuildRowCount: 10_000},
			wantDecision: true,
		},
		{
			name:         "target cluster drift",
			clusterRows:  uniformClusterRows(meta.Nlist, 640),
			maintenance:  &vecindex.MaintenanceState{LastRebuildRowCount: 10_000},
			clusterDrift: rebuildClusterDriftPct,
			wantDecision: true,
		},
		{
			name:         "steady state",
			clusterRows:  uniformClusterRows(meta.Nlist, rebuildTargetClusterSize),
			maintenance:  &vecindex.MaintenanceState{RowsModifiedSinceRebuild: 10, LastRebuildRowCount: 10_000},
			clusterDrift: rebuildClusterDriftPct / 2,
			wantDecision: false,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := shouldIncrementalRepair(meta, tc.clusterRows, tc.maintenance, tc.clusterDrift)
			if got != tc.wantDecision {
				t.Fatalf("shouldIncrementalRepair() = %v, want %v", got, tc.wantDecision)
			}
		})
	}
}

func TestCountTargetClusterDrift(t *testing.T) {
	t.Parallel()

	meta := common.VectorIndexMeta{
		Nlist:         4,
		AutoTuneNlist: true,
	}
	clusterRows := uniformClusterRows(meta.Nlist, rebuildTargetClusterSize)
	drift := countTargetClusterDrift(meta, clusterRows, 0)
	if drift != 0 {
		t.Fatalf("countTargetClusterDrift() = %f, want 0", drift)
	}
}

func TestTouchedCentroidRefreshUpdatesOnlyTouchedClusters(t *testing.T) {
	t.Parallel()

	baseProbe, err := kmeans.NewCentroidSet(7, [][]float32{
		{1, 0},
		{0, 1},
		{-1, 0},
	})
	if err != nil {
		t.Fatalf("new base probe: %v", err)
	}
	baseStable, err := kmeans.NewCentroidSet(7, [][]float32{
		{10, 0},
		{0, 10},
		{-10, 0},
	})
	if err != nil {
		t.Fatalf("new base stable: %v", err)
	}

	counts := []uint64{0, 2, 4, 8}
	sums := [][]float32{
		nil,
		{4, 2},
		{8, 4},
		{-16, 8},
	}
	touched := map[int64]struct{}{1: {}, 3: {}}
	nextProbe, err := probeCentroidSetForTouched(baseProbe, counts, sums, touched, 8)
	if err != nil {
		t.Fatalf("probeCentroidSetForTouched: %v", err)
	}
	if got := nextProbe.Epoch(); got != 8 {
		t.Fatalf("probe epoch = %d, want 8", got)
	}
	snap := nextProbe.Snapshot()
	if snap[0][0] != 2 || snap[0][1] != 1 {
		t.Fatalf("cluster 1 centroid = %v, want [2 1]", snap[0])
	}
	if snap[1][0] != 0 || snap[1][1] != 1 {
		t.Fatalf("cluster 2 centroid changed: %v", snap[1])
	}
	if snap[2][0] != -2 || snap[2][1] != 1 {
		t.Fatalf("cluster 3 centroid = %v, want [-2 1]", snap[2])
	}

	nextStable, err := stableCentroidSetForTouched(baseStable, nextProbe, touched)
	if err != nil {
		t.Fatalf("stableCentroidSetForTouched: %v", err)
	}
	stableSnap := nextStable.Snapshot()
	if stableSnap[0][0] != snap[0][0] || stableSnap[0][1] != snap[0][1] {
		t.Fatalf("stable touched cluster 1 = %v, want %v", stableSnap[0], snap[0])
	}
	if stableSnap[1][0] != 0 || stableSnap[1][1] != 10 {
		t.Fatalf("stable untouched cluster changed: %v", stableSnap[1])
	}
	if stableSnap[2][0] != snap[2][0] || stableSnap[2][1] != snap[2][1] {
		t.Fatalf("stable touched cluster 3 = %v, want %v", stableSnap[2], snap[2])
	}
}

func TestAllocateCatchUpChildCountsSumsToDesiredK(t *testing.T) {
	t.Parallel()

	parentCounts := []uint64{0, 16_000, 15_000, 1_000, 0}
	got := allocateCatchUpChildCounts(parentCounts, 4, 64, 512)
	var sum int
	for parentID := 1; parentID < len(got); parentID++ {
		sum += got[parentID]
		if got[parentID] < 1 {
			t.Fatalf("parent %d child count = %d, want at least 1", parentID, got[parentID])
		}
	}
	if sum != 64 {
		t.Fatalf("child sum = %d, want 64 (%v)", sum, got)
	}
	if got[1] <= got[3] {
		t.Fatalf("heavy parent child count = %d, light parent = %d; want heavy > light", got[1], got[3])
	}
}

func TestCatchUpChildIDLayoutPreservesParentIDs(t *testing.T) {
	t.Parallel()

	children := catchUpChildIDLayout([]int{0, 3, 1, 2}, 3, 6)
	want := [][]int64{
		nil,
		{1, 4, 5},
		{2},
		{3, 6},
	}
	for parentID := 1; parentID < len(want); parentID++ {
		if len(children[parentID]) != len(want[parentID]) {
			t.Fatalf("parent %d children = %v, want %v", parentID, children[parentID], want[parentID])
		}
		for i := range want[parentID] {
			if children[parentID][i] != want[parentID][i] {
				t.Fatalf("parent %d children = %v, want %v", parentID, children[parentID], want[parentID])
			}
		}
	}
}
