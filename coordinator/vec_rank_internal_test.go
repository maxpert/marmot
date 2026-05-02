package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
)

func TestRefreshProbeClusterIDs_ReprobesOnEpochChange(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(3, [][]float32{{0}, {10}, {20}})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	state := vecindex.NewIndexState(vecindex.IVFSpec{
		ID:     "idx",
		Dim:    1,
		Metric: vecindex.MetricL2,
		Nlist:  3,
		Nprobe: 2,
	}, cs)

	plan := &GoRankPlan{
		RawQueryVec: []byte{0, 0, 16, 65}, // 9.0
		Nprobe:      2,
		ProbeEpoch:  1,
		ClusterIDs:  []int64{1, 3},
	}

	got := (&CoordinatorHandler{}).refreshProbeClusterIDs(plan, state)
	want := []int64{2, 1}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("refreshProbeClusterIDs() = %v, want %v", got, want)
	}
	if plan.ProbeEpoch != 3 {
		t.Fatalf("ProbeEpoch = %d, want 3", plan.ProbeEpoch)
	}
}

func TestRefreshProbeClusterIDs_LeavesPlanOnSameEpoch(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(1, [][]float32{{0}, {10}, {20}})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	state := vecindex.NewIndexState(vecindex.IVFSpec{
		ID:     "idx",
		Dim:    1,
		Metric: vecindex.MetricL2,
		Nlist:  3,
		Nprobe: 2,
	}, cs)

	plan := &GoRankPlan{
		RawQueryVec: []byte{0, 0, 16, 65}, // 9.0
		Nprobe:      2,
		ProbeEpoch:  1,
		ClusterIDs:  []int64{1, 3},
	}

	got := (&CoordinatorHandler{}).refreshProbeClusterIDs(plan, state)
	want := []int64{1, 3}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("refreshProbeClusterIDs() = %v, want %v", got, want)
	}
	if plan.ProbeEpoch != 1 {
		t.Fatalf("ProbeEpoch = %d, want 1", plan.ProbeEpoch)
	}
}

func TestSelectProbeClusterIDs_UsesRowBudget(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(7, [][]float32{{0}, {10}, {20}, {30}})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	spec := vecindex.IVFSpec{
		ID:     "idx",
		Dim:    1,
		Metric: vecindex.MetricL2,
		Nlist:  4,
		Nprobe: 4,
	}
	state := vecindex.NewIndexState(spec, cs)
	state.StoreMaintenanceState(&vecindex.MaintenanceState{
		ClusterRowCounts: []uint64{0, 7000, 900, 500, 6000},
	})
	plan := &GoRankPlan{
		QueryVec:            []float32{9},
		IndexSpec:           spec,
		UseBudgetProbe:      true,
		ScanBudgetRows:      8192,
		TargetPartitionSize: 512,
		ClusterIDs:          []int64{4},
		ProbeEpoch:          1,
	}

	got := selectProbeClusterIDs(plan, state, cs, &vecindex.SegmentGeneration{
		ClusterRowCounts: []uint64{0, 7000, 900, 500, 6000},
	})
	want := []int64{2, 1, 3}
	if len(got) != len(want) {
		t.Fatalf("selectProbeClusterIDs() len=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selectProbeClusterIDs()[%d] = %d, want %d (full=%v)", i, got[i], want[i], got)
		}
	}
	if plan.ProbeEpoch != 7 {
		t.Fatalf("ProbeEpoch = %d, want 7", plan.ProbeEpoch)
	}
}

func TestSelectProbeClusterIDs_UsesLiveMaintenanceCounts(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(9, [][]float32{{0}, {10}, {20}})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	spec := vecindex.IVFSpec{
		ID:     "idx",
		Dim:    1,
		Metric: vecindex.MetricL2,
		Nlist:  3,
		Nprobe: 3,
	}
	state := vecindex.NewIndexState(spec, cs)
	state.StoreMaintenanceState(&vecindex.MaintenanceState{
		ClusterRowCounts:       []uint64{0, 7000, 0, 0},
		PendingClusterRowDelta: []int64{0, 0, 1500, 0},
	})
	plan := &GoRankPlan{
		QueryVec:            []float32{9},
		IndexSpec:           spec,
		UseBudgetProbe:      true,
		ScanBudgetRows:      8192,
		TargetPartitionSize: 512,
		ClusterIDs:          []int64{3},
		ProbeEpoch:          1,
	}

	got := selectProbeClusterIDs(plan, state, cs, &vecindex.SegmentGeneration{
		ClusterRowCounts: []uint64{0, 7000, 0, 0},
	})
	want := []int64{2, 1}
	if len(got) != len(want) {
		t.Fatalf("selectProbeClusterIDs() len=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selectProbeClusterIDs()[%d] = %d, want %d (full=%v)", i, got[i], want[i], got)
		}
	}
}

func TestCurrentBlockPruneModeDefaultsOff(t *testing.T) {
	t.Setenv("MARMOT_VEC_BLOCK_PRUNE_MODE", "")
	if got := currentBlockPruneMode(); got != blockPruneOff {
		t.Fatalf("currentBlockPruneMode() = %v, want off", got)
	}

	t.Setenv("MARMOT_VEC_BLOCK_PRUNE_MODE", "safe")
	if got := currentBlockPruneMode(); got != blockPruneSafe {
		t.Fatalf("currentBlockPruneMode() = %v, want safe", got)
	}

	t.Setenv("MARMOT_VEC_BLOCK_PRUNE_MODE", "shadow")
	if got := currentBlockPruneMode(); got != blockPruneShadow {
		t.Fatalf("currentBlockPruneMode() = %v, want shadow", got)
	}
}
