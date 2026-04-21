// Package vecindex implements IVF (Inverted File Index) vector similarity
// search using local segment files, overlay journals, and shared metric/kmeans
// primitives.
package vecindex

import "github.com/maxpert/marmot/modules/vecindex/pkg/metric"

// Metric identifies the distance function used for vector comparisons.
// It is an alias for metric.Metric so callers can use either import path.
type Metric = metric.Metric

const (
	// MetricL2 is squared Euclidean distance.
	MetricL2 = metric.MetricL2
	// MetricDot is negative inner product (higher dot = closer).
	MetricDot = metric.MetricDot
	// MetricCosine is cosine distance (1 - cosine similarity).
	MetricCosine = metric.MetricCosine
)

// MaxNlist is the maximum number of IVF clusters allowed.
const MaxNlist = 16384

// IVFSpec describes the configuration for a single IVF vector index.
type IVFSpec struct {
	// ID is the unique index identifier.
	ID string
	// Dim is the vector dimensionality of caller-supplied vectors.
	Dim int
	// Metric is the distance function to use.
	Metric Metric
	// Nlist is the number of IVF centroids (clusters).
	Nlist int
	// Nprobe is the number of clusters searched at query time.
	Nprobe int
	// Seed is the RNG seed used for k-means initialisation.
	Seed uint64
	// Epoch tracks the centroid generation; incremented on retrain.
	Epoch uint64
	// MaxNorm is the fixed upper bound on vector L2 norms used for MIPS→L2
	// augmentation when Metric == MetricDot.
	MaxNorm float32
}

// InternalDim returns the dimensionality of vectors as stored in the index.
// For MetricDot this is Dim+1 (one augmented dimension for MIPS→L2 reduction).
// For all other metrics it equals Dim.
func (s IVFSpec) InternalDim() int {
	if s.Metric == MetricDot {
		return s.Dim + 1
	}
	return s.Dim
}

// InternalMetric returns the distance metric used for centroid assignment.
// For MetricDot the MIPS→L2 reduction stores augmented vectors, so centroid
// assignment uses MetricL2. For all other metrics this equals Metric.
func (s IVFSpec) InternalMetric() Metric {
	if s.Metric == MetricDot {
		return MetricL2
	}
	return s.Metric
}

// BulkEntry is a single vector supplied during index creation.
type BulkEntry struct {
	// ExternalID is the caller-supplied identifier for the vector.
	ExternalID []byte
	// Vector holds the raw float32 values.
	Vector []float32
}
