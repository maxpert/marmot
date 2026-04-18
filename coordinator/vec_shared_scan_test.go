package coordinator

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

func TestMakeVecSharedScanKey(t *testing.T) {
	t.Parallel()

	planA := &GoRankPlan{
		Database:   "dbpedia",
		IndexName:  "docs_embed",
		ProbeEpoch: 7,
		QueryVec:   []float32{1, 0},
		RankMetric: metric.MetricCosine,
		K:          10,
	}
	planB := &GoRankPlan{
		Database:   "dbpedia",
		IndexName:  "docs_embed",
		ProbeEpoch: 7,
		QueryVec:   []float32{0, 1},
		RankMetric: metric.MetricCosine,
		K:          3,
	}

	keyA, ok := makeVecSharedScanKey(planA)
	if !ok {
		t.Fatal("expected planA to be eligible")
	}
	keyB, ok := makeVecSharedScanKey(planB)
	if !ok {
		t.Fatal("expected planB to be eligible")
	}
	if keyA != keyB {
		t.Fatalf("expected batch key to exclude k and query contents, got %+v vs %+v", keyA, keyB)
	}

	planC := *planA
	planC.RankMetric = metric.MetricL2
	keyC, ok := makeVecSharedScanKey(&planC)
	if !ok {
		t.Fatal("expected planC to be eligible")
	}
	if keyC == keyA {
		t.Fatalf("expected metric to participate in the batch key, got %+v", keyC)
	}

	planF := *planA
	planF.ProbeEpoch = 8
	keyF, ok := makeVecSharedScanKey(&planF)
	if !ok {
		t.Fatal("expected planF to be eligible")
	}
	if keyF == keyA {
		t.Fatalf("expected probe epoch to participate in the batch key, got %+v", keyF)
	}

	planD := *planA
	planD.AllowCache = true
	if _, ok := makeVecSharedScanKey(&planD); ok {
		t.Fatal("cacheable plan must not enter shared scan batching")
	}

	planE := *planA
	planE.CandidateArgFilter = []int{0}
	if _, ok := makeVecSharedScanKey(&planE); ok {
		t.Fatal("filtered plan must not enter shared scan batching")
	}

	planG := *planA
	planG.HasUserPredicate = true
	if _, ok := makeVecSharedScanKey(&planG); ok {
		t.Fatal("user-predicate plan must not produce a shared scan key")
	}
	if (&CoordinatorHandler{}).canUseSharedScan(&planG) {
		t.Fatal("user-predicate plan must not enter shared scan batching")
	}
}

func TestVecSharedScanJoinAndSeal(t *testing.T) {
	t.Parallel()

	var (
		mu        sync.Mutex
		scanCalls []int64
	)
	scanner := vecSharedScanClusterScannerFunc(func(_ context.Context, _ vecSharedScanKey, clusterID int64, yield func(int64, []byte) error) error {
		mu.Lock()
		scanCalls = append(scanCalls, clusterID)
		mu.Unlock()
		return yield(clusterID, float32sToVecBytes(1, 0))
	})
	coord := newVecSharedScanCoordinator(scanner, vecSharedScanOptions{
		MicrobatchWindow: 25 * time.Millisecond,
		MaxRequests:      2,
		MaxUnionClusters: 4,
	})

	type result struct {
		items []rankItem
		err   error
	}
	run := func(plan *GoRankPlan) <-chan result {
		ch := make(chan result, 1)
		go func() {
			items, ok, err := coord.Execute(context.Background(), plan)
			if !ok {
				ch <- result{err: errors.New("shared scan did not engage")}
				return
			}
			ch <- result{items: items, err: err}
		}()
		return ch
	}

	plan1 := &GoRankPlan{
		Database:   "dbpedia",
		IndexName:  "docs_embed",
		ProbeEpoch: 1,
		QueryVec:   []float32{1, 0},
		RankMetric: metric.MetricCosine,
		K:          1,
	}
	plan2 := &GoRankPlan{
		Database:   "dbpedia",
		IndexName:  "docs_embed",
		ProbeEpoch: 1,
		QueryVec:   []float32{0, 1},
		RankMetric: metric.MetricCosine,
		K:          5,
	}
	plan3 := &GoRankPlan{
		Database:   "dbpedia",
		IndexName:  "docs_embed",
		ProbeEpoch: 1,
		QueryVec:   []float32{1, 0},
		RankMetric: metric.MetricCosine,
		K:          2,
	}

	res1Ch := run(plan1)
	res2Ch := run(plan2)
	res1 := <-res1Ch
	res2 := <-res2Ch
	if res1.err != nil {
		t.Fatalf("plan1 failed: %v", res1.err)
	}
	if res2.err != nil {
		t.Fatalf("plan2 failed: %v", res2.err)
	}
	if len(res1.items) != 1 || len(res2.items) != 1 {
		t.Fatalf("unexpected result sizes: %d %d", len(res1.items), len(res2.items))
	}

	if _, ok, err := coord.Execute(context.Background(), plan3); err != nil {
		t.Fatalf("plan3 failed: %v", err)
	} else if ok {
		t.Fatal("expected singleton plan3 to fall back instead of using shared scan")
	}

	mu.Lock()
	gotCalls := append([]int64(nil), scanCalls...)
	mu.Unlock()

	if !reflect.DeepEqual(gotCalls, []int64{0}) {
		t.Fatalf("expected only the shared batch to scan the delta cluster, got %v", gotCalls)
	}

	stats := coord.SnapshotStats()
	if stats.ExecuteCalls != 3 {
		t.Fatalf("execute call count mismatch: got %d want 3", stats.ExecuteCalls)
	}
	if stats.SharedBatches != 1 {
		t.Fatalf("shared batch count mismatch: got %d want 1", stats.SharedBatches)
	}
	if stats.SharedRequests != 2 {
		t.Fatalf("shared request count mismatch: got %d want 2", stats.SharedRequests)
	}
	if stats.SingletonFallbacks != 1 {
		t.Fatalf("singleton fallback count mismatch: got %d want 1", stats.SingletonFallbacks)
	}
	if stats.ScanClusters != 1 || stats.ScanRows != 1 {
		t.Fatalf("scan counters mismatch: clusters=%d rows=%d want 1/1", stats.ScanClusters, stats.ScanRows)
	}
	if stats.SealByMaxRequests != 1 {
		t.Fatalf("expected max-requests seal count 1, got %d", stats.SealByMaxRequests)
	}
}

func TestVecSharedScanPerClusterRouting(t *testing.T) {
	t.Parallel()

	rowsByCluster := map[int64][]struct {
		rowid int64
		vec   []byte
	}{
		1: {
			{rowid: 11, vec: float32sToVecBytes(1, 0)},
			{rowid: 12, vec: float32sToVecBytes(0.8, 0.6)},
		},
		2: {
			{rowid: 21, vec: float32sToVecBytes(0, 1)},
			{rowid: 22, vec: float32sToVecBytes(0.6, 0.8)},
		},
	}
	var (
		mu        sync.Mutex
		scanOrder []int64
	)
	scanner := vecSharedScanClusterScannerFunc(func(_ context.Context, _ vecSharedScanKey, clusterID int64, yield func(int64, []byte) error) error {
		mu.Lock()
		scanOrder = append(scanOrder, clusterID)
		mu.Unlock()
		for _, row := range rowsByCluster[clusterID] {
			if err := yield(row.rowid, row.vec); err != nil {
				return err
			}
		}
		return nil
	})
	coord := newVecSharedScanCoordinator(scanner, vecSharedScanOptions{
		MicrobatchWindow: 25 * time.Millisecond,
		MaxRequests:      4,
		MaxUnionClusters: 8,
	})

	type result struct {
		items []rankItem
		err   error
	}
	run := func(plan *GoRankPlan) <-chan result {
		ch := make(chan result, 1)
		go func() {
			items, ok, err := coord.Execute(context.Background(), plan)
			if !ok {
				ch <- result{err: errors.New("shared scan did not engage")}
				return
			}
			ch <- result{items: items, err: err}
		}()
		return ch
	}

	reqACh := run(&GoRankPlan{
		Database:   "dbpedia",
		IndexName:  "docs_embed",
		QueryVec:   []float32{1, 0},
		RankMetric: metric.MetricCosine,
		ClusterIDs: []int64{1},
		K:          2,
	})
	reqBCh := run(&GoRankPlan{
		Database:   "dbpedia",
		IndexName:  "docs_embed",
		QueryVec:   []float32{0, 1},
		RankMetric: metric.MetricCosine,
		ClusterIDs: []int64{2},
		K:          2,
	})
	reqCCh := run(&GoRankPlan{
		Database:   "dbpedia",
		IndexName:  "docs_embed",
		QueryVec:   []float32{1, 0},
		RankMetric: metric.MetricCosine,
		ClusterIDs: []int64{1, 2},
		K:          1,
	})

	reqA := <-reqACh
	reqB := <-reqBCh
	reqC := <-reqCCh

	for _, res := range []result{reqA, reqB, reqC} {
		if res.err != nil {
			t.Fatalf("shared scan request failed: %v", res.err)
		}
	}

	if got := rankRowIDs(reqA.items); !reflect.DeepEqual(got, []int64{11, 12}) {
		t.Fatalf("cluster 1 routing mismatch: got %v want [11 12]", got)
	}
	if got := rankRowIDs(reqB.items); !reflect.DeepEqual(got, []int64{21, 22}) {
		t.Fatalf("cluster 2 routing mismatch: got %v want [21 22]", got)
	}
	if got := rankRowIDs(reqC.items); !reflect.DeepEqual(got, []int64{11}) {
		t.Fatalf("union routing mismatch: got %v want [11]", got)
	}

	mu.Lock()
	gotOrder := append([]int64(nil), scanOrder...)
	mu.Unlock()
	if len(gotOrder) != 3 || gotOrder[0] != 0 {
		t.Fatalf("unexpected cluster scan order: got %v want delta first plus two probed clusters", gotOrder)
	}
	if tail := gotOrder[1:]; !(reflect.DeepEqual(tail, []int64{1, 2}) || reflect.DeepEqual(tail, []int64{2, 1})) {
		t.Fatalf("unexpected probed cluster coverage: got %v want [1 2] in any order", gotOrder)
	}
}

func TestVecSharedScanSkipsOversizedRequest(t *testing.T) {
	t.Parallel()

	coord := newVecSharedScanCoordinator(
		vecSharedScanClusterScannerFunc(func(_ context.Context, _ vecSharedScanKey, _ int64, _ func(int64, []byte) error) error {
			return nil
		}),
		vecSharedScanOptions{MaxUnionClusters: 2},
	)
	plan := &GoRankPlan{
		Database:   "dbpedia",
		IndexName:  "docs_embed",
		QueryVec:   []float32{1, 0},
		RankMetric: metric.MetricCosine,
		ClusterIDs: []int64{1, 2},
		K:          10,
	}
	if _, ok, err := coord.Execute(context.Background(), plan); err != nil {
		t.Fatalf("oversized request returned error: %v", err)
	} else if ok {
		t.Fatal("oversized request should fall back instead of using shared scan")
	}
	stats := coord.SnapshotStats()
	if stats.OversizedRequestFallbacks != 1 {
		t.Fatalf("oversized fallback count mismatch: got %d want 1", stats.OversizedRequestFallbacks)
	}
}

func TestSharedScanRankFallsBackOnProbeRefreshMismatch(t *testing.T) {
	t.Parallel()

	h := &CoordinatorHandler{}
	h.SetVectorEngine(probeRefreshStub{})
	plan := &GoRankPlan{
		Database:    "dbpedia",
		IndexName:   "docs_embed",
		RawQueryVec: []byte{1, 0, 0, 0},
		QueryVec:    []float32{1},
		RankMetric:  metric.MetricCosine,
		Nprobe:      2,
		ClusterIDs:  []int64{1, 2},
		ProbeEpoch:  5,
		K:           10,
	}
	if _, ok, err := h.sharedScanRank(plan); err != nil {
		t.Fatalf("sharedScanRank returned error: %v", err)
	} else if ok {
		t.Fatal("expected probe refresh mismatch to fall back")
	}
}

func float32sToVecBytes(v ...float32) []byte {
	buf := make([]byte, len(v)*4)
	for i, x := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(x))
	}
	return buf
}

type probeRefreshStub struct{}

func (probeRefreshStub) AssignNearest(string, []byte) (int64, error) { return 0, nil }
func (probeRefreshStub) NotifyCentroidChange(string, int64) error    { return nil }
func (probeRefreshStub) TopNprobeClusters(string, []byte, int) ([]int64, error) {
	return []int64{1, 2}, nil
}
func (probeRefreshStub) TopNprobeClustersWithEpoch(string, []byte, int) ([]int64, uint64, error) {
	return []int64{9, 10}, 6, nil
}

var _ vecindex.VectorUDFProvider = probeRefreshStub{}

func rankRowIDs(items []rankItem) []int64 {
	out := make([]int64, len(items))
	for i, item := range items {
		out[i] = item.rowid
	}
	return out
}
