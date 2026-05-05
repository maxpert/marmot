package kmeans_test

import (
	"math"
	"math/rand"
	"runtime"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

func partitionCounts(vectors [][]float32, centroids [][]float32) []int {
	counts := make([]int, len(centroids))
	for _, vec := range vectors {
		clusterID, _, err := kmeans.Assign(vec, centroids, metric.MetricL2)
		if err != nil {
			panic(err)
		}
		counts[clusterID]++
	}
	return counts
}

func countSkew(counts []int) int {
	if len(counts) == 0 {
		return 0
	}
	minCount, maxCount := counts[0], counts[0]
	for _, count := range counts[1:] {
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}
	return maxCount - minCount
}

func maxCount64(counts []int64) int64 {
	var maxCount int64
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}
	return maxCount
}

func TestMiniBatchBalanced_DeterministicSeed(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(42))
	vecs := make([][]float32, 200)
	for i := range vecs {
		vecs[i] = []float32{float32(rng.Float64()), float32(rng.Float64())}
	}
	opts := kmeans.MiniBatchBalancedOptions{BatchSize: 64, MaxIter: 5, TargetClusterSize: 100}

	c1, err := kmeans.MiniBatchBalanced(vecs, 4, 7, opts)
	require.NoError(t, err)
	c2, err := kmeans.MiniBatchBalanced(vecs, 4, 7, opts)
	require.NoError(t, err)
	require.Equal(t, c1, c2)
}

func TestMiniBatchBalancedFromInit_DeterministicAcrossGOMAXPROCS(t *testing.T) {
	rng := rand.New(rand.NewSource(20260421))
	vecs := make([][]float32, 384)
	for i := range vecs {
		base := float32(i % 8)
		vecs[i] = []float32{
			base + float32(rng.NormFloat64())*0.15,
			base*0.5 + float32(rng.NormFloat64())*0.15,
			float32((i/8)%4) + float32(rng.NormFloat64())*0.1,
			float32(rng.NormFloat64()) * 0.05,
		}
	}
	initCentroids := [][]float32{
		{0, 0, 0, 0},
		{1, 0.5, 0, 0},
		{2, 1, 1, 0},
		{3, 1.5, 1, 0},
		{4, 2, 2, 0},
		{5, 2.5, 2, 0},
		{6, 3, 3, 0},
		{7, 3.5, 3, 0},
	}
	opts := kmeans.MiniBatchBalancedOptions{
		BatchSize:         96,
		MaxIter:           6,
		TargetClusterSize: len(vecs) / len(initCentroids),
		BalancePenalty:    0.75,
	}

	prev := runtime.GOMAXPROCS(1)
	gotSingle, err := kmeans.MiniBatchBalancedFromInit(vecs, initCentroids, 123, opts)
	require.NoError(t, err)
	runtime.GOMAXPROCS(4)
	gotMulti, err := kmeans.MiniBatchBalancedFromInit(vecs, initCentroids, 123, opts)
	runtime.GOMAXPROCS(prev)
	require.NoError(t, err)
	require.Equal(t, gotSingle, gotMulti)
}

func TestMiniBatchBalancedFromInit_PreservesWarmStartShape(t *testing.T) {
	t.Parallel()

	vecs := [][]float32{
		{0, 0}, {0.1, 0}, {0, 0.1},
		{10, 10}, {10.1, 10}, {10, 10.1},
	}
	initCentroids := [][]float32{{0, 0}, {10, 10}}

	centroids, err := kmeans.MiniBatchBalancedFromInit(vecs, initCentroids, 9, kmeans.MiniBatchBalancedOptions{
		BatchSize:         2,
		MaxIter:           4,
		TargetClusterSize: 3,
	})
	require.NoError(t, err)
	require.Len(t, centroids, 2)
	require.Len(t, centroids[0], 2)
	require.Len(t, centroids[1], 2)
}

func TestMiniBatchBalancedTrainer_UsesCosineAssignmentObjective(t *testing.T) {
	t.Parallel()

	vecs := [][]float32{
		{1, 0}, {1, 0}, {1, 0}, {1, 0},
		{0, 1}, {0, 1}, {0, 1}, {0, 1},
	}
	trainer, err := kmeans.NewMiniBatchBalancedTrainer([][]float32{
		{100, 0},
		{0, 1},
	}, kmeans.MiniBatchBalancedOptions{
		BatchSize:         len(vecs),
		MaxIter:           1,
		TargetClusterSize: len(vecs),
		Metric:            metric.MetricCosine,
	})
	require.NoError(t, err)
	require.NoError(t, trainer.BeginPass())
	require.NoError(t, trainer.ObserveBatch(vecs))
	result, err := trainer.EndPass(1)
	require.NoError(t, err)
	require.False(t, result.Repaired)

	counts := trainer.Counts()
	require.Equal(t, []int64{4, 4}, counts)
	centroids := trainer.Centroids()
	require.InDelta(t, 1, centroids[0][0], 1e-6)
	require.InDelta(t, 0, centroids[0][1], 1e-6)
	require.InDelta(t, 0, centroids[1][0], 1e-6)
	require.InDelta(t, 1, centroids[1][1], 1e-6)
}

func TestMiniBatchBalanced_ReducesPartitionSkew(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(77))
	var vecs [][]float32
	for i := 0; i < 400; i++ {
		vecs = append(vecs, []float32{
			float32(rng.NormFloat64()) * 0.05,
			float32(rng.NormFloat64()) * 0.05,
		})
	}
	for i := 0; i < 40; i++ {
		vecs = append(vecs, []float32{
			10 + float32(rng.NormFloat64())*0.05,
			10 + float32(rng.NormFloat64())*0.05,
		})
	}
	for i := 0; i < 40; i++ {
		vecs = append(vecs, []float32{
			-10 + float32(rng.NormFloat64())*0.05,
			10 + float32(rng.NormFloat64())*0.05,
		})
	}

	unbalanced, err := kmeans.MiniBatchBalanced(vecs, 3, 13, kmeans.MiniBatchBalancedOptions{
		BatchSize:         64,
		MaxIter:           6,
		TargetClusterSize: len(vecs) / 3,
		BalancePenalty:    0,
	})
	require.NoError(t, err)

	balanced, err := kmeans.MiniBatchBalanced(vecs, 3, 13, kmeans.MiniBatchBalancedOptions{
		BatchSize:         64,
		MaxIter:           6,
		TargetClusterSize: len(vecs) / 3,
		BalancePenalty:    2.0,
	})
	require.NoError(t, err)

	unbalancedSkew := countSkew(partitionCounts(vecs, unbalanced))
	balancedCounts := partitionCounts(vecs, balanced)
	balancedSkew := countSkew(balancedCounts)
	require.LessOrEqual(t, balancedSkew, unbalancedSkew)
	require.LessOrEqual(t, maxCount64([]int64{int64(balancedCounts[0]), int64(balancedCounts[1]), int64(balancedCounts[2])}), int64((len(vecs)/3)*2))
}

func TestMiniBatchBalancedTrainer_RepairsIdleWarmStart(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(8080))
	vecs := make([][]float32, 0, 384)
	for _, mean := range [][]float32{{0, 0}, {20, 20}, {-20, 15}} {
		vecs = append(vecs, gaussianBlob(rng, 128, 2, mean, 0.7)...)
	}
	trainer, err := kmeans.NewMiniBatchBalancedTrainer([][]float32{
		{0, 0},
		{0, 0},
		{0, 0},
	}, kmeans.MiniBatchBalancedOptions{
		BatchSize:         64,
		MaxIter:           6,
		TargetClusterSize: len(vecs) / 3,
		BalancePenalty:    1.0,
	})
	require.NoError(t, err)

	for iter := 0; iter < 6; iter++ {
		require.NoError(t, trainer.BeginPass())
		for start := 0; start < len(vecs); start += 64 {
			end := start + 64
			if end > len(vecs) {
				end = len(vecs)
			}
			require.NoError(t, trainer.ObserveBatch(vecs[start:end]))
		}
		result, err := trainer.EndPass(uint64(iter + 1))
		require.NoError(t, err)
		_ = result
	}
	counts := trainer.Counts()
	for _, count := range counts {
		require.Greater(t, count, int64(0))
	}
	require.LessOrEqual(t, maxCount64(counts), int64((len(vecs)/3)*2))
}

func TestMiniBatchBalanced_ConvergesOnClusteredData(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1234))
	means := [][]float32{{0, 0}, {100, 100}, {-100, 50}}
	var vecs [][]float32
	for _, mean := range means {
		vecs = append(vecs, gaussianBlob(rng, 128, 2, mean, 1.0)...)
	}

	centroids, err := kmeans.MiniBatchBalanced(vecs, 3, 99, kmeans.MiniBatchBalancedOptions{
		BatchSize:         64,
		MaxIter:           8,
		TargetClusterSize: len(vecs) / 3,
	})
	require.NoError(t, err)
	require.Len(t, centroids, 3)

	const tolerance = 6.0
	matched := make([]bool, len(means))
	for _, centroid := range centroids {
		for i, mean := range means {
			var d float64
			for j := range centroid {
				diff := float64(centroid[j] - mean[j])
				d += diff * diff
			}
			if math.Sqrt(d) <= tolerance {
				matched[i] = true
			}
		}
	}
	for i := range matched {
		require.True(t, matched[i], "no centroid found near mean %d", i)
	}
}
