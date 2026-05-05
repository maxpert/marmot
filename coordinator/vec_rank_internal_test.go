package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
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

func TestSelectProbeClusterIDs_AutoPolicyWidensLargeNlist(t *testing.T) {
	t.Parallel()

	const (
		nlist  = 977
		target = 512
	)
	centroids := make([][]float32, nlist)
	counts := make([]uint64, nlist+1)
	for i := range centroids {
		centroids[i] = []float32{float32(i)}
		counts[i+1] = target
	}
	cs, err := kmeans.NewCentroidSet(11, centroids)
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	spec := vecindex.IVFSpec{
		ID:     "idx",
		Dim:    1,
		Metric: vecindex.MetricL2,
		Nlist:  nlist,
		Nprobe: 16,
	}
	state := vecindex.NewIndexState(spec, cs)
	state.StoreMaintenanceState(&vecindex.MaintenanceState{
		ClusterRowCounts: counts,
	})
	plan := &GoRankPlan{
		QueryVec:            []float32{0},
		IndexSpec:           spec,
		Nprobe:              16,
		UseBudgetProbe:      true,
		TargetPartitionSize: target,
		K:                   10,
		ClusterIDs:          []int64{1},
		ProbeEpoch:          1,
	}

	got := selectProbeClusterIDs(plan, state, cs, &vecindex.SegmentGeneration{
		ClusterRowCounts: counts,
	})
	if len(got) <= 16 {
		t.Fatalf("selectProbeClusterIDs() selected %d probes, want wider than 16", len(got))
	}
	if len(got) != 32 {
		t.Fatalf("selectProbeClusterIDs() selected %d probes, want 32", len(got))
	}
}

func TestAutoProbePartitionCount_CosinePQUsesMeasuredBudget(t *testing.T) {
	t.Parallel()

	if got := autoProbePartitionCount(196, 512, 10, metric.MetricCosine, vecindex.MemberEncodingResidualPQ8); got != 24 {
		t.Fatalf("autoProbePartitionCount(196, cosine, pq8) = %d, want 24", got)
	}
	if got := autoProbePartitionCount(977, 512, 10, metric.MetricCosine, vecindex.MemberEncodingResidualPQ8); got != 48 {
		t.Fatalf("autoProbePartitionCount(977, cosine, pq8) = %d, want 48", got)
	}
}

func TestSelectProbeClusterIDs_ExplicitNprobeStaysFixed(t *testing.T) {
	t.Parallel()

	const (
		nlist  = 977
		target = 512
		nprobe = 16
	)
	centroids := make([][]float32, nlist)
	counts := make([]uint64, nlist+1)
	for i := range centroids {
		centroids[i] = []float32{float32(i)}
		counts[i+1] = target
	}
	cs, err := kmeans.NewCentroidSet(13, centroids)
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	spec := vecindex.IVFSpec{
		ID:     "idx",
		Dim:    1,
		Metric: vecindex.MetricL2,
		Nlist:  nlist,
		Nprobe: nprobe,
	}
	state := vecindex.NewIndexState(spec, cs)
	state.StoreMaintenanceState(&vecindex.MaintenanceState{
		ClusterRowCounts: counts,
	})
	plan := &GoRankPlan{
		RawQueryVec:         []byte{0, 0, 0, 0},
		QueryVec:            []float32{0},
		IndexSpec:           spec,
		Nprobe:              nprobe,
		UseBudgetProbe:      false,
		TargetPartitionSize: target,
		K:                   10,
		ClusterIDs:          []int64{1},
		ProbeEpoch:          1,
	}

	got := selectProbeClusterIDs(plan, state, cs, &vecindex.SegmentGeneration{
		ClusterRowCounts: counts,
	})
	if len(got) != nprobe {
		t.Fatalf("selectProbeClusterIDs() selected %d probes, want explicit nprobe %d", len(got), nprobe)
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

	t.Setenv("MARMOT_VEC_BLOCK_PRUNE_MODE", "approx_safe")
	if got := currentBlockPruneMode(); got != blockPruneSafe {
		t.Fatalf("currentBlockPruneMode() = %v, want approx-safe", got)
	}

	t.Setenv("MARMOT_VEC_BLOCK_PRUNE_MODE", "shadow")
	if got := currentBlockPruneMode(); got != blockPruneShadow {
		t.Fatalf("currentBlockPruneMode() = %v, want shadow", got)
	}
}

func TestRankShortlistUsesCandidateBudget(t *testing.T) {
	t.Parallel()

	plan := &GoRankPlan{K: 10, LimitK: 10, CandidateK: 100, Shortlist: exactRerankShortlist(10)}
	if got := rankShortlistLimitForEncoding(plan, vecindex.MemberEncodingResidualInt8); got != 100 {
		t.Fatalf("int8 shortlist = %d, want explicit candidate budget 100", got)
	}
	if got := rankShortlistLimitForEncoding(plan, vecindex.MemberEncodingResidualPQ8); got != 144 {
		t.Fatalf("pq shortlist = %d, want max(default pq, candidate) 144", got)
	}

	plan.CandidateK = 800
	if got := rankShortlistLimitForEncoding(plan, vecindex.MemberEncodingResidualPQ8); got != 800 {
		t.Fatalf("pq shortlist = %d, want explicit candidate budget 800", got)
	}
}

func TestGrowRefillBudgetWidensToInternalCap(t *testing.T) {
	t.Parallel()

	plan := &GoRankPlan{K: 10, LimitK: 10, CandidateK: 100}
	if !plan.growRefillBudget() || plan.CandidateK != 200 {
		t.Fatalf("first refill CandidateK=%d, want 200", plan.CandidateK)
	}
	for plan.growRefillBudget() {
	}
	if plan.CandidateK != goRankRefillMaxCandidateK {
		t.Fatalf("final refill CandidateK=%d, want cap %d", plan.CandidateK, goRankRefillMaxCandidateK)
	}
}
