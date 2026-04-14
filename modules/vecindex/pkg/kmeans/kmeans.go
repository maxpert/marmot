package kmeans

import (
	"errors"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// KMeansPlusPlus runs k-means++ initialisation followed by Lloyd's algorithm.
// Returns k centroids or an error if the inputs are invalid.
// The algorithm is deterministic: same (vectors, k, seed) always produces the
// same byte-identical centroid output. maxIter must be >= 1.
func KMeansPlusPlus(vectors [][]float32, k int, seed uint64, maxIter int) ([][]float32, error) {
	return nil, errors.New("not implemented: KMeansPlusPlus")
}

// Assign returns the index and distance of the nearest centroid for vec.
// Returns an error if centroids is empty or dimensions are mismatched.
func Assign(vec []float32, centroids [][]float32, m metric.Metric) (clusterID uint32, dist float32, err error) {
	return 0, 0, errors.New("not implemented: Assign")
}

// AssignTopN returns the n nearest centroids sorted by ascending distance.
// If n >= len(centroids), all centroids are returned sorted.
// If n == 0, an empty result is returned.
// Returns an error if centroids is empty or dimensions are mismatched.
func AssignTopN(vec []float32, centroids [][]float32, n int, m metric.Metric) (ids []uint32, dists []float32, err error) {
	return nil, nil, errors.New("not implemented: AssignTopN")
}
