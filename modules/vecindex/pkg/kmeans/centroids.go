// Package kmeans provides k-means++ clustering and centroid management for IVF indexes.
package kmeans

import "errors"

// CentroidSet holds a snapshot of cluster centroids at a given epoch.
// Index uses atomic.Pointer[CentroidSet] to allow lock-free reads.
// CentroidSet is immutable after creation via NewCentroidSet.
type CentroidSet struct {
	epoch     uint64
	centroids [][]float32
}

// NewCentroidSet creates an immutable CentroidSet from the given epoch and centroid vectors.
// The centroids slice is deep-copied so the caller may reuse it.
func NewCentroidSet(epoch uint64, centroids [][]float32) (*CentroidSet, error) {
	return nil, errors.New("not implemented: NewCentroidSet")
}

// Len returns the number of centroids in the set.
func (cs *CentroidSet) Len() int {
	return len(cs.centroids)
}

// Epoch returns the generation counter for this centroid set.
func (cs *CentroidSet) Epoch() uint64 {
	return cs.epoch
}

// Get returns a copy of the centroid vector for the given cluster ID.
// Returns an error if clusterID is out of range.
func (cs *CentroidSet) Get(clusterID uint32) ([]float32, error) {
	return nil, errors.New("not implemented: Get")
}

// Encode serialises the CentroidSet to msgpack bytes.
func (cs *CentroidSet) Encode() ([]byte, error) {
	return nil, errors.New("not implemented: Encode")
}

// DecodeCentroidSet deserialises a CentroidSet from msgpack bytes produced by Encode.
func DecodeCentroidSet(data []byte) (*CentroidSet, error) {
	return nil, errors.New("not implemented: DecodeCentroidSet")
}
