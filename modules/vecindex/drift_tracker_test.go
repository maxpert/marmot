package vecindex

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDriftTracker_NewFromCentroids(t *testing.T) {
	t.Parallel()
	centroids := [][]float32{{1, 2, 3}, {4, 5, 6}}
	dt := NewDriftTracker(centroids)

	require.Equal(t, 2, dt.Len())
	require.Equal(t, int64(1), dt.ClusterCount(0))
	require.Equal(t, int64(1), dt.ClusterCount(1))

	// Centroids should match originals (sum/1 == original).
	got := dt.Centroids()
	require.InDeltaSlice(t, []float64{1, 2, 3}, toFloat64(got[0]), 1e-6)
	require.InDeltaSlice(t, []float64{4, 5, 6}, toFloat64(got[1]), 1e-6)
}

func TestDriftTracker_Empty(t *testing.T) {
	t.Parallel()
	dt := NewDriftTracker(nil)
	require.Equal(t, 0, dt.Len())
	require.Empty(t, dt.Centroids())
}

func TestDriftTracker_Update_ManualVerification(t *testing.T) {
	t.Parallel()
	centroids := [][]float32{{2, 0}, {0, 2}}
	dt := NewDriftTracker(centroids)

	// Apply 100 updates to cluster 0: vec = {1, 1}
	for i := 0; i < 100; i++ {
		dt = dt.Update(0, []float32{1, 1})
	}
	// Apply 50 updates to cluster 1: vec = {3, 3}
	for i := 0; i < 50; i++ {
		dt = dt.Update(1, []float32{3, 3})
	}

	require.Equal(t, int64(101), dt.ClusterCount(0)) // 1 initial + 100 updates
	require.Equal(t, int64(51), dt.ClusterCount(1))  // 1 initial + 50 updates

	// Verify sums manually:
	// Cluster 0: sum = (2, 0) + 100*(1, 1) = (102, 100). centroid = (102/101, 100/101)
	// Cluster 1: sum = (0, 2) + 50*(3, 3) = (150, 152). centroid = (150/51, 152/51)
	got := dt.Centroids()
	require.InDelta(t, 102.0/101.0, got[0][0], 1e-5)
	require.InDelta(t, 100.0/101.0, got[0][1], 1e-5)
	require.InDelta(t, 150.0/51.0, got[1][0], 1e-5)
	require.InDelta(t, 152.0/51.0, got[1][1], 1e-5)
}

func TestDriftTracker_Update_OutOfRange(t *testing.T) {
	t.Parallel()
	dt := NewDriftTracker([][]float32{{1, 0}})

	// Out of range should return same tracker.
	same := dt.Update(-1, []float32{1, 1})
	require.Equal(t, dt, same)
	same = dt.Update(1, []float32{1, 1})
	require.Equal(t, dt, same)
}

func TestDriftTracker_ClusterCount_OutOfRange(t *testing.T) {
	t.Parallel()
	dt := NewDriftTracker([][]float32{{1, 0}})
	require.Equal(t, int64(0), dt.ClusterCount(-1))
	require.Equal(t, int64(0), dt.ClusterCount(5))
}

func TestDriftTracker_COW_Immutability(t *testing.T) {
	t.Parallel()
	dt := NewDriftTracker([][]float32{{1, 0}, {0, 1}})
	updated := dt.Update(0, []float32{3, 0})

	// Original should be unchanged.
	require.Equal(t, int64(1), dt.ClusterCount(0))
	// Updated should reflect the change.
	require.Equal(t, int64(2), updated.ClusterCount(0))
}

func TestDriftTracker_ConcurrentUpdateSafety(t *testing.T) {
	t.Parallel()
	centroids := [][]float32{{0, 0}, {1, 1}}
	dt := NewDriftTracker(centroids)

	// Concurrent COW updates from multiple goroutines — validates no
	// data race under -race. Each goroutine produces independent copies.
	const workers = 16
	const updates = 100
	var wg sync.WaitGroup
	results := make([]*DriftTracker, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := dt
			for i := 0; i < updates; i++ {
				local = local.Update(w%2, []float32{float32(w), float32(i)})
			}
			results[w] = local
		}(w)
	}
	wg.Wait()

	// Each worker applied 100 updates to one cluster; verify consistency.
	for w, r := range results {
		cluster := w % 2
		require.Equal(t, int64(1+updates), r.ClusterCount(cluster))
	}
}

func toFloat64(v []float32) []float64 {
	out := make([]float64, len(v))
	for i, f := range v {
		out[i] = float64(f)
	}
	return out
}
