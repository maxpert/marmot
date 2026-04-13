package hdindex

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"testing"

	"github.com/maxpert/marmot/modules/hdindex/pkg/metric"
)

// randomVectors generates n random float32 vectors of dimension dim using the given seed.
func randomVectors(n, dim int, seed uint64) [][]float32 {
	rng := rand.New(rand.NewPCG(seed, 0))
	vecs := make([][]float32, n)
	for i := range n {
		v := make([]float32, dim)
		for d := range dim {
			v[d] = rng.Float32()*2 - 1
		}
		vecs[i] = v
	}
	return vecs
}

// makeVectorEntries converts raw vectors into VectorEntry slices.
func makeVectorEntries(vecs [][]float32) []VectorEntry {
	entries := make([]VectorEntry, len(vecs))
	for i, v := range vecs {
		entries[i] = VectorEntry{
			ExternalID: []byte(fmt.Sprintf("vec-%d", i)),
			Vector:     v,
		}
	}
	return entries
}

func TestCreateAndSearch(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vecs := randomVectors(500, 32, 42)
	spec := DefaultSpec("test-euclidean", 32, MetricEuclidean)
	idx, err := eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	query := vecs[0]
	result, err := idx.Search(ctx, SearchRequest{VectorFP32: query, TopK: 10})
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Hits) == 0 {
		t.Fatal("expected hits, got none")
	}
	if len(result.Hits) > 10 {
		t.Fatalf("expected at most 10 hits, got %d", len(result.Hits))
	}

	for i, h := range result.Hits {
		if h.Distance < 0 {
			t.Errorf("hit %d: distance %f should be non-negative", i, h.Distance)
		}
	}

	// Results must be sorted ascending by distance.
	for i := 1; i < len(result.Hits); i++ {
		if result.Hits[i].Distance < result.Hits[i-1].Distance {
			t.Errorf("results not sorted: hit[%d].Distance=%f > hit[%d].Distance=%f",
				i-1, result.Hits[i-1].Distance, i, result.Hits[i].Distance)
		}
	}
}

func TestCreateAndSearch_Cosine(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vecs := randomVectors(500, 32, 43)
	spec := DefaultSpec("test-cosine", 32, MetricCosine)
	idx, err := eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	query := vecs[10]
	result, err := idx.Search(ctx, SearchRequest{VectorFP32: query, TopK: 10})
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Hits) == 0 {
		t.Fatal("expected hits, got none")
	}

	// Cosine distance = 1 - CosineSimilarity. Range is [0, 2].
	for i, h := range result.Hits {
		if h.Distance < 0 || h.Distance > 2.0001 {
			t.Errorf("hit %d: cosine distance %f outside [0, 2]", i, h.Distance)
		}
	}

	for i := 1; i < len(result.Hits); i++ {
		if result.Hits[i].Distance < result.Hits[i-1].Distance {
			t.Errorf("results not sorted: hit[%d].Distance=%f > hit[%d].Distance=%f",
				i-1, result.Hits[i-1].Distance, i, result.Hits[i].Distance)
		}
	}
}

func TestCreateAndSearch_Dot(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vecs := randomVectors(500, 32, 44)
	spec := DefaultSpec("test-dot", 32, MetricDot)
	idx, err := eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	query := vecs[5]
	result, err := idx.Search(ctx, SearchRequest{VectorFP32: query, TopK: 10})
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Hits) == 0 {
		t.Fatal("expected hits, got none")
	}

	// Results sorted ascending (lower = higher dot product).
	for i := 1; i < len(result.Hits); i++ {
		if result.Hits[i].Distance < result.Hits[i-1].Distance {
			t.Errorf("results not sorted: hit[%d].Distance=%f > hit[%d].Distance=%f",
				i-1, result.Hits[i-1].Distance, i, result.Hits[i].Distance)
		}
	}
}

func TestUpsert_Insert(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vecs := randomVectors(100, 32, 45)
	spec := DefaultSpec("test-insert", 32, MetricEuclidean)
	idx, err := eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	// Create a new distinct vector to insert.
	newVec := make([]float32, 32)
	for i := range 32 {
		newVec[i] = float32(i) * 0.1
	}
	newExtID := []byte("new-vector")

	err = idx.Upsert(ctx, Mutation{
		TxnID:      1,
		SeqID:      1,
		ExternalID: newExtID,
		VectorFP32: newVec,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Stats should show 101 vectors.
	stats := idx.Stats()
	if stats.VectorCount != 101 {
		t.Errorf("expected 101 vectors after insert, got %d", stats.VectorCount)
	}

	// Search for the new vector; it should appear.
	result, err := idx.Search(ctx, SearchRequest{VectorFP32: newVec, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, h := range result.Hits {
		if string(h.ExternalID) == string(newExtID) {
			found = true
			break
		}
	}
	if !found {
		t.Error("newly inserted vector not found in search results")
	}
}

func TestUpsert_Update(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vecs := randomVectors(100, 32, 46)
	spec := DefaultSpec("test-update", 32, MetricEuclidean)
	entries := makeVectorEntries(vecs)
	idx, err := eng.CreateIndex(ctx, spec, entries)
	if err != nil {
		t.Fatal(err)
	}

	// Update the first vector to a very distinct location.
	updatedVec := make([]float32, 32)
	for i := range 32 {
		updatedVec[i] = 100.0 + float32(i)
	}
	targetExtID := entries[0].ExternalID

	err = idx.Upsert(ctx, Mutation{
		TxnID:      2,
		SeqID:      1,
		ExternalID: targetExtID,
		VectorFP32: updatedVec,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Count should remain at 100 (update, not insert).
	stats := idx.Stats()
	if stats.VectorCount != 100 {
		t.Errorf("expected 100 vectors after update, got %d", stats.VectorCount)
	}

	// Search near the updated vector.
	result, err := idx.Search(ctx, SearchRequest{VectorFP32: updatedVec, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, h := range result.Hits {
		if string(h.ExternalID) == string(targetExtID) {
			found = true
			break
		}
	}
	if !found {
		t.Error("updated vector not found near its new position")
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vecs := randomVectors(100, 32, 47)
	spec := DefaultSpec("test-delete", 32, MetricEuclidean)
	entries := makeVectorEntries(vecs)
	idx, err := eng.CreateIndex(ctx, spec, entries)
	if err != nil {
		t.Fatal(err)
	}

	targetEntry := entries[0]

	err = idx.Delete(ctx, DeleteMutation{
		TxnID:      3,
		SeqID:      1,
		ExternalID: targetEntry.ExternalID,
	})
	if err != nil {
		t.Fatal(err)
	}

	stats := idx.Stats()
	if stats.VectorCount != 99 {
		t.Errorf("expected 99 vectors after delete, got %d", stats.VectorCount)
	}

	// Search near the deleted vector; it should not appear.
	result, err := idx.Search(ctx, SearchRequest{VectorFP32: targetEntry.Vector, TopK: 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range result.Hits {
		if string(h.ExternalID) == string(targetEntry.ExternalID) {
			t.Error("deleted vector still appears in search results")
		}
	}
}

func TestOpenIndex(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	eng, err := NewEngine(dir, EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}

	vecs := randomVectors(200, 32, 48)
	spec := DefaultSpec("test-open", 32, MetricEuclidean)
	idx, err := eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	query := vecs[0]
	resultBefore, err := idx.Search(ctx, SearchRequest{VectorFP32: query, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}

	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen via new engine.
	eng2, err := NewEngine(dir, EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()

	idx2, err := eng2.OpenIndex(ctx, "test-open")
	if err != nil {
		t.Fatal(err)
	}

	resultAfter, err := idx2.Search(ctx, SearchRequest{VectorFP32: query, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}

	if len(resultAfter.Hits) == 0 {
		t.Fatal("no results after reopening index")
	}

	// Both runs should return results (approximation may differ slightly, check non-empty).
	if len(resultBefore.Hits) == 0 || len(resultAfter.Hits) == 0 {
		t.Error("expected hits in both before and after results")
	}
}

func TestDropIndex(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	eng, err := NewEngine(dir, EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vecs := randomVectors(50, 32, 49)
	spec := DefaultSpec("test-drop", 32, MetricEuclidean)
	_, err = eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	idxDir := fmt.Sprintf("%s/test-drop", dir)
	if _, err := os.Stat(idxDir); os.IsNotExist(err) {
		t.Fatal("index directory should exist before drop")
	}

	if err := eng.DropIndex(ctx, "test-drop"); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(idxDir); !os.IsNotExist(err) {
		t.Error("index directory should not exist after drop")
	}
}

func TestSearchRecall(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	const (
		n      = 1000
		dim    = 64
		topK   = 10
		nQuery = 50
	)

	vecs := randomVectors(n, dim, 100)
	spec := DefaultSpec("test-recall", dim, MetricEuclidean)
	// Use a larger alpha for better recall.
	spec.Alpha = 8192
	entries := makeVectorEntries(vecs)
	idx, err := eng.CreateIndex(ctx, spec, entries)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewPCG(200, 0))

	var totalRecall float64

	for q := range nQuery {
		_ = q
		queryIdx := rng.IntN(n)
		query := vecs[queryIdx]

		// Brute force exact top-k.
		type distIdx struct {
			idx  int
			dist float32
		}
		bruteForce := make([]distIdx, n)
		for i, v := range vecs {
			bruteForce[i] = distIdx{
				idx:  i,
				dist: float32(math.Sqrt(float64(metric.L2Squared(query, v)))),
			}
		}
		sort.Slice(bruteForce, func(a, b int) bool {
			return bruteForce[a].dist < bruteForce[b].dist
		})
		trueTopK := make(map[string]struct{}, topK)
		for i := range topK {
			trueTopK[fmt.Sprintf("vec-%d", bruteForce[i].idx)] = struct{}{}
		}

		// Approximate search.
		result, err := idx.Search(ctx, SearchRequest{VectorFP32: query, TopK: topK})
		if err != nil {
			t.Fatal(err)
		}

		// Count overlap.
		var found int
		for _, h := range result.Hits {
			if _, ok := trueTopK[string(h.ExternalID)]; ok {
				found++
			}
		}
		totalRecall += float64(found) / float64(topK)
	}

	recall := totalRecall / float64(nQuery)
	t.Logf("recall@%d = %.3f", topK, recall)
	if recall < 0.7 {
		t.Errorf("recall@%d = %.3f < 0.7 threshold", topK, recall)
	}
}

// TestUpsert_DimensionMismatch verifies that Upsert returns an error when
// the vector dimension does not match the index dimension.
func TestUpsert_DimensionMismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	vecs := randomVectors(50, 32, 60)
	spec := DefaultSpec("test-dim-mismatch", 32, MetricEuclidean)
	idx, err := eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	wrongDimVec := make([]float32, 16) // wrong: index expects 32
	err = idx.Upsert(ctx, Mutation{
		TxnID:      1,
		SeqID:      1,
		ExternalID: []byte("bad-vec"),
		VectorFP32: wrongDimVec,
	})
	if err == nil {
		t.Error("expected error for dimension mismatch in Upsert, got nil")
	}
}

// TestDotMetric_MIPSOrdering verifies that searching with MetricDot returns
// vectors ordered by descending dot product (highest dot product = lowest
// distance = first result). We compare against a brute-force ranking.
func TestDotMetric_MIPSOrdering(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	const (
		n    = 200
		dim  = 32
		topK = 5
	)

	vecs := randomVectors(n, dim, 70)
	spec := DefaultSpec("test-mips-order", dim, MetricDot)
	spec.Alpha = 8192
	idx, err := eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	query := vecs[0]

	// Brute-force: rank all vectors by descending dot product.
	type dotIdx struct {
		i   int
		dot float32
	}
	dots := make([]dotIdx, n)
	for i, v := range vecs {
		dots[i] = dotIdx{i: i, dot: metric.DotProduct(query, v)}
	}
	sort.Slice(dots, func(a, b int) bool {
		return dots[a].dot > dots[b].dot // descending
	})
	trueTopK := make(map[string]struct{}, topK)
	for i := range topK {
		trueTopK[fmt.Sprintf("vec-%d", dots[i].i)] = struct{}{}
	}

	result, err := idx.Search(ctx, SearchRequest{VectorFP32: query, TopK: topK})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) == 0 {
		t.Fatal("expected hits, got none")
	}

	// Distances must be sorted ascending (lowest distance = highest dot product).
	for i := 1; i < len(result.Hits); i++ {
		if result.Hits[i].Distance < result.Hits[i-1].Distance {
			t.Errorf("dot results not sorted ascending by distance: hit[%d]=%f > hit[%d]=%f",
				i-1, result.Hits[i-1].Distance, i, result.Hits[i].Distance)
		}
	}

	// All returned distances must be negative or zero (distance = -dot, dot >= 0 not guaranteed,
	// but the top results for a non-trivial query should have positive dot product).
	// The best result must have the highest dot product among all returned hits.
	if len(result.Hits) > 1 {
		bestDot := -result.Hits[0].Distance
		worstDot := -result.Hits[len(result.Hits)-1].Distance
		if bestDot < worstDot {
			t.Errorf("first result dot=%f < last result dot=%f: ordering incorrect", bestDot, worstDot)
		}
	}

	// Verify approximate recall: at least one of our top-k matches brute-force top-k.
	var found int
	for _, h := range result.Hits {
		if _, ok := trueTopK[string(h.ExternalID)]; ok {
			found++
		}
	}
	if found == 0 {
		t.Errorf("dot metric search found none of brute-force top-%d results", topK)
	}
}

func TestStats(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(t.TempDir(), EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	const n = 150
	vecs := randomVectors(n, 32, 55)
	spec := DefaultSpec("test-stats", 32, MetricEuclidean)
	idx, err := eng.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	stats := idx.Stats()
	if stats.VectorCount != n {
		t.Errorf("expected VectorCount=%d, got %d", n, stats.VectorCount)
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	ctx := context.Background()
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	eng1, err := NewEngine(dir1, EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng1.Close()

	vecs := randomVectors(200, 32, 99)
	spec := DefaultSpec("snap-test", 32, MetricEuclidean)
	idx1, err := eng1.CreateIndex(ctx, spec, makeVectorEntries(vecs))
	if err != nil {
		t.Fatal(err)
	}

	query := vecs[0]
	resBefore, err := idx1.Search(ctx, SearchRequest{VectorFP32: query, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := eng1.SnapshotIndex(ctx, "snap-test", &buf); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Fatal("snapshot should produce data")
	}

	eng2, err := NewEngine(dir2, EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()

	if err := eng2.RestoreIndex(ctx, "snap-test", &buf); err != nil {
		t.Fatal(err)
	}

	idx2, err := eng2.OpenIndex(ctx, "snap-test")
	if err != nil {
		t.Fatal(err)
	}

	resAfter, err := idx2.Search(ctx, SearchRequest{VectorFP32: query, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}

	if len(resBefore.Hits) != len(resAfter.Hits) {
		t.Fatalf("hit count mismatch: before=%d after=%d", len(resBefore.Hits), len(resAfter.Hits))
	}
	for i := range resBefore.Hits {
		if string(resBefore.Hits[i].ExternalID) != string(resAfter.Hits[i].ExternalID) {
			t.Errorf("hit[%d] ExternalID mismatch: before=%s after=%s",
				i, resBefore.Hits[i].ExternalID, resAfter.Hits[i].ExternalID)
		}
	}
}
