// Package kmeans provides k-means++ clustering and centroid management for IVF indexes.
package kmeans

import (
	"errors"
	"fmt"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/vmihailenco/msgpack/v5"
)

// CentroidSet holds a snapshot of cluster centroids at a given epoch.
// Index uses atomic.Pointer[CentroidSet] to allow lock-free reads.
// CentroidSet is immutable after creation via NewCentroidSet.
type CentroidSet struct {
	epoch     uint64
	centroids [][]float32
}

// centroidSetMsg is the msgpack wire format for CentroidSet.
type centroidSetMsg struct {
	Epoch     uint64      `msgpack:"epoch"`
	Centroids [][]float32 `msgpack:"centroids"`
}

// NewCentroidSet creates an immutable CentroidSet from the given epoch and centroid vectors.
// The centroids slice is deep-copied so the caller may reuse it.
func NewCentroidSet(epoch uint64, centroids [][]float32) (*CentroidSet, error) {
	copied := make([][]float32, len(centroids))
	for i, c := range centroids {
		cp := make([]float32, len(c))
		copy(cp, c)
		copied[i] = cp
	}
	return &CentroidSet{epoch: epoch, centroids: copied}, nil
}

// Len returns the number of centroids in the set.
func (cs *CentroidSet) Len() int {
	return len(cs.centroids)
}

// Epoch returns the generation counter for this centroid set.
func (cs *CentroidSet) Epoch() uint64 {
	return cs.epoch
}

// Get returns a mutable copy of the centroid vector for the given cluster ID.
// Use this when the caller needs to modify the returned slice.
// Returns an error if clusterID is out of range.
func (cs *CentroidSet) Get(clusterID uint32) ([]float32, error) {
	if int(clusterID) >= len(cs.centroids) {
		return nil, fmt.Errorf("kmeans: cluster ID %d out of range (len=%d)", clusterID, len(cs.centroids))
	}
	src := cs.centroids[clusterID]
	cp := make([]float32, len(src))
	copy(cp, src)
	return cp, nil
}

// GetReadOnly returns the internal centroid slice for read-only access.
// Callers MUST NOT mutate the returned slice — it aliases the CentroidSet's
// internal state and is shared across concurrent readers. For a mutable copy
// use Get instead.
// Returns an error if clusterID is out of range.
func (cs *CentroidSet) GetReadOnly(clusterID uint32) ([]float32, error) {
	if int(clusterID) >= len(cs.centroids) {
		return nil, fmt.Errorf("kmeans: cluster ID %d out of range (len=%d)", clusterID, len(cs.centroids))
	}
	return cs.centroids[clusterID], nil
}

// Encode serialises the CentroidSet to msgpack bytes.
func (cs *CentroidSet) Encode() ([]byte, error) {
	return msgpack.Marshal(&centroidSetMsg{
		Epoch:     cs.epoch,
		Centroids: cs.centroids,
	})
}

// AssignNearest returns the 0-based cluster ID and distance for the nearest
// centroid in the set. Delegates to Assign using the caller-supplied metric.
// Returns an error if vec length mismatches centroid dimensionality.
func (cs *CentroidSet) AssignNearest(vec []float32, m metric.Metric) (uint32, float32, error) {
	return Assign(vec, cs.centroids, m)
}

// AssignTopN returns the n nearest 0-based cluster IDs sorted by ascending
// distance. Delegates to AssignTopN using the caller-supplied metric. n is
// clamped to [0, cs.Len()] by the package-level implementation.
// Returns an error if vec length mismatches centroid dimensionality.
func (cs *CentroidSet) AssignTopN(vec []float32, n int, m metric.Metric) ([]uint32, []float32, error) {
	return AssignTopN(vec, cs.centroids, n, m)
}

// Snapshot returns a deep copy of all centroid vectors. Use this when the
// caller needs an independent mutable copy, e.g. as a warm start for a
// subsequent k-means run.
func (cs *CentroidSet) Snapshot() [][]float32 {
	out := make([][]float32, len(cs.centroids))
	for i, c := range cs.centroids {
		cp := make([]float32, len(c))
		copy(cp, c)
		out[i] = cp
	}
	return out
}

// DecodeCentroidSet deserialises a CentroidSet from msgpack bytes produced by Encode.
func DecodeCentroidSet(data []byte) (*CentroidSet, error) {
	if len(data) == 0 {
		return nil, errors.New("kmeans: cannot decode empty data")
	}
	var msg centroidSetMsg
	if err := msgpack.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("kmeans: decode centroid set: %w", err)
	}
	return &CentroidSet{epoch: msg.Epoch, centroids: msg.Centroids}, nil
}
