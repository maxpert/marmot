package vecindex

import (
	"context"
	"math/rand"
	"sort"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

// makeRandomVectors generates n random float32 vectors of the given dimension
// seeded deterministically so tests are reproducible.
func makeRandomVectors(n, dim int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		vecs[i] = v
	}
	return vecs
}

// bruteForceTopK returns the sorted indices of the true k nearest neighbours
// of query in vecs according to metric m (ascending distance).
func bruteForceTopK(query []float32, vecs [][]float32, k int, m metric.Metric) []int {
	type entry struct {
		idx  int
		dist float32
	}
	entries := make([]entry, len(vecs))
	for i, v := range vecs {
		entries[i] = entry{i, metric.Distance(m, query, v)}
	}
	sort.Slice(entries, func(a, b int) bool {
		return entries[a].dist < entries[b].dist
	})
	if k > len(entries) {
		k = len(entries)
	}
	result := make([]int, k)
	for i := range result {
		result[i] = entries[i].idx
	}
	return result
}

// computeRecall returns the fraction of truth indices that appear in got.
func computeRecall(got []SearchHit, truth []int, k int) float32 {
	truthSet := make(map[uint64]struct{}, len(truth))
	for i, idx := range truth {
		if i >= k {
			break
		}
		truthSet[uint64(idx)] = struct{}{}
	}
	hit := 0
	for _, h := range got {
		if _, ok := truthSet[h.DocID]; ok {
			hit++
		}
	}
	if len(truth) == 0 {
		return 1.0
	}
	return float32(hit) / float32(len(truth))
}

// newTempEngine creates an Engine backed by t.TempDir().
func newTempEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := NewEngine(t.TempDir(), newTestLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// buildTestIndex creates an index on e with id, inserts n random dim-dimensional
// vectors and returns the open *Index.
func buildTestIndex(t *testing.T, e *Engine, id string, dim, n int, m Metric) *Index {
	t.Helper()
	ctx := context.Background()
	vecs := makeRandomVectors(n, dim, 42)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{
			ExternalID: []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)},
			Vector:     v,
		}
	}
	spec := DefaultSpec(id, dim, m)
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	return idx
}
