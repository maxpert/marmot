// Package kmeans provides k-means++ clustering and centroid management for IVF indexes.
package kmeans

// CentroidSet holds a snapshot of cluster centroids at a given epoch.
// Index uses atomic.Pointer[CentroidSet] to allow lock-free reads.
type CentroidSet struct {
	// Centroids is the ordered list of cluster centroid vectors.
	Centroids [][]float32
	// Epoch is the generation counter; incremented on each retrain.
	Epoch uint64
}

// Len returns the number of centroids in the set.
func (cs *CentroidSet) Len() int {
	return len(cs.Centroids)
}
