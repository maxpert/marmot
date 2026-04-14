package kmeans

import (
	"errors"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// KMeansPlusPlus runs k-means++ initialisation followed by Lloyd's algorithm.
// Returns k centroids or an error if the inputs are invalid.
func KMeansPlusPlus(vectors [][]float32, k int, seed uint64, maxIter int) ([][]float32, error) {
	return nil, errors.New("not implemented: KMeansPlusPlus")
}

// Assign returns the index and distance of the nearest centroid for vec.
func Assign(vec []float32, centroids [][]float32, m metric.Metric) (clusterID uint32, dist float32) {
	panic("not implemented: Assign")
}

// AssignTopN returns the n nearest centroids sorted by ascending distance.
func AssignTopN(vec []float32, centroids [][]float32, n int, m metric.Metric) (ids []uint32, dists []float32) {
	panic("not implemented: AssignTopN")
}
