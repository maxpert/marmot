package query

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestTopK(t *testing.T) {
	candidates := map[string][]float32{
		"a": {1, 0},
		"b": {0.5, 0.5},
		"c": {0, 1},
	}
	out := TopK(api.MetricDot, []float32{1, 0}, candidates, 2)
	require.Len(t, out, 2)
	require.Equal(t, []byte("a"), out[0].ExternalID)
}

func TestTopKEuclidean(t *testing.T) {
	candidates := map[string][]float32{
		"a": {1, 1},
		"b": {2, 2},
		"c": {4, 4},
	}
	out := TopK(api.MetricEuclidean, []float32{1, 1}, candidates, 2)
	require.Len(t, out, 2)
	require.Equal(t, []byte("a"), out[0].ExternalID)
	require.Equal(t, []byte("b"), out[1].ExternalID)
	require.Equal(t, float32(0), DistanceFromScore(api.MetricEuclidean, out[0].Score))
}

func TestTopKWithWorkersMatchesSequential(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	candidates := make(map[string][]float32, 5000)
	for i := 0; i < 5000; i++ {
		v := make([]float32, 16)
		for j := range v {
			v[j] = rng.Float32()
		}
		candidates[strconv.Itoa(i)] = v
	}
	query := make([]float32, 16)
	for i := range query {
		query[i] = rng.Float32()
	}

	seq := TopKWithWorkers(api.MetricCosine, query, candidates, 25, 1)
	par := TopKWithWorkers(api.MetricCosine, query, candidates, 25, 8)
	require.Equal(t, len(seq), len(par))
	for i := range seq {
		require.Equal(t, string(seq[i].ExternalID), string(par[i].ExternalID))
		require.InDelta(t, seq[i].Score, par[i].Score, 1e-6)
	}
}

func TestTopKDocIDsWithWorkersMatchesSequential(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	candidates := make(map[uint64][]float32, 5000)
	for i := 0; i < 5000; i++ {
		v := make([]float32, 16)
		for j := range v {
			v[j] = rng.Float32()
		}
		candidates[uint64(i)] = v
	}
	query := make([]float32, 16)
	for i := range query {
		query[i] = rng.Float32()
	}

	seq := TopKDocIDsWithWorkers(api.MetricCosine, query, candidates, 25, 1)
	par := TopKDocIDsWithWorkers(api.MetricCosine, query, candidates, 25, 8)
	require.Equal(t, len(seq), len(par))
	for i := range seq {
		require.Equal(t, seq[i].DocID, par[i].DocID)
		require.InDelta(t, seq[i].Score, par[i].Score, 1e-6)
	}
}
