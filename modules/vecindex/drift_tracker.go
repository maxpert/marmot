package vecindex

// DriftTracker holds MacQueen online centroid drift statistics.
// Immutable after creation — updates produce a new copy (COW) so it can
// be stored behind an atomic.Pointer and read lock-free.
//
// Each cluster tracks a running sum (float64 for precision) and a count
// of vectors assigned since the last REINDEX. Centroids() computes
// sum[i]/counts[i] to yield the MacQueen-drifted centroid positions.
type DriftTracker struct {
	sum    [][]float64
	counts []int64
	dim    int
}

// NewDriftTracker creates a DriftTracker initialized from the given centroids.
// Each cluster starts with sum = centroid (as float64) and count = 1,
// representing the centroid itself as one observation. Subsequent DriftUpdate
// calls accumulate new vectors on top.
func NewDriftTracker(centroids [][]float32) *DriftTracker {
	k := len(centroids)
	if k == 0 {
		return &DriftTracker{}
	}
	dim := len(centroids[0])
	sum := make([][]float64, k)
	counts := make([]int64, k)
	for i, c := range centroids {
		s := make([]float64, dim)
		for d, v := range c {
			s[d] = float64(v)
		}
		sum[i] = s
		counts[i] = 1
	}
	return &DriftTracker{sum: sum, counts: counts, dim: dim}
}

// Update returns a new DriftTracker with vec accumulated into the specified
// cluster's running statistics. clusterID is 0-based. Returns the original
// tracker unchanged if clusterID is out of range.
func (t *DriftTracker) Update(clusterID int, vec []float32) *DriftTracker {
	if clusterID < 0 || clusterID >= len(t.sum) {
		return t
	}
	k := len(t.sum)
	newSum := make([][]float64, k)
	newCounts := make([]int64, k)
	for i := range t.sum {
		if i == clusterID {
			s := make([]float64, t.dim)
			copy(s, t.sum[i])
			for d, v := range vec {
				s[d] += float64(v)
			}
			newSum[i] = s
			newCounts[i] = t.counts[i] + 1
		} else {
			newSum[i] = t.sum[i] // immutable; safe to share
			newCounts[i] = t.counts[i]
		}
	}
	return &DriftTracker{sum: newSum, counts: newCounts, dim: t.dim}
}

// Centroids computes the current drifted centroids as sum[i] / counts[i].
func (t *DriftTracker) Centroids() [][]float32 {
	k := len(t.sum)
	result := make([][]float32, k)
	for i := range t.sum {
		c := make([]float32, t.dim)
		if t.counts[i] > 0 {
			inv := 1.0 / float64(t.counts[i])
			for d := range c {
				c[d] = float32(t.sum[i][d] * inv)
			}
		}
		result[i] = c
	}
	return result
}

// Len returns the number of clusters in the tracker.
func (t *DriftTracker) Len() int {
	return len(t.sum)
}

// ClusterCount returns the running count for the 0-based clusterID.
// Returns 0 if clusterID is out of range.
func (t *DriftTracker) ClusterCount(clusterID int) int64 {
	if clusterID < 0 || clusterID >= len(t.counts) {
		return 0
	}
	return t.counts[clusterID]
}

// TotalCount returns the sum of all cluster counts.
func (t *DriftTracker) TotalCount() int64 {
	var total int64
	for _, c := range t.counts {
		total += c
	}
	return total
}
