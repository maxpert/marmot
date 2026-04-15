// Package vecindex implements an IVF (Inverted File Index) vector similarity
// search engine backed by Pebble for persistent storage.
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

const (
	// MaxNlist is the maximum number of IVF clusters allowed.
	MaxNlist = 16384
	// BloomBitsPerKey is the bits-per-key for Bloom filters in Pebble.
	BloomBitsPerKey = 10
	// PostingBlockSize is the Pebble SST block size for the posting-list column family.
	// IVF posting entries are 6144 B each (1536-dim float32); default 4 KB blocks force
	// multiple pread() calls per vector. 64 KB amortises that to ~10 reads/vector on
	// average while staying within the standard Pebble block-cache unit size.
	PostingBlockSize = 64 << 10 // 64 KB
)

// defaultMaxNorm is the fixed upper bound on vector L2 norms for MetricDot.
// Vectors whose L2 norm exceeds this value are rejected at Upsert time.
// 1000.0 comfortably covers unnormalized embeddings from common models.
const defaultMaxNorm = float32(1000.0)

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
	// augmentation when Metric == MetricDot. Any vector with ||v|| > MaxNorm
	// is rejected at Upsert time. Defaults to 1000.0 via DefaultSpec.
	// Ignored for MetricL2 and MetricCosine.
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

// DefaultSpec returns a sensible IVFSpec for a new empty index.
// Lifecycle management may adjust Nlist/Nprobe as the index grows.
func DefaultSpec(id string, dim int, metric Metric) IVFSpec {
	return IVFSpec{
		ID:      id,
		Dim:     dim,
		Metric:  metric,
		Nlist:   256,
		Nprobe:  16,
		MaxNorm: defaultMaxNorm,
	}
}

// SearchRequest encapsulates parameters for an approximate nearest-neighbour query.
type SearchRequest struct {
	// Vector is the query vector.
	Vector []float32
	// K is the number of nearest neighbours to return.
	K int
	// NprobeOverride overrides the spec's Nprobe when > 0.
	NprobeOverride int
}

// SearchHit is a single result returned by a Search call.
type SearchHit struct {
	// DocID is the internal numeric identifier for the vector.
	DocID uint64
	// ExternalID is the caller-supplied identifier for the vector.
	ExternalID []byte
	// Distance is the computed distance to the query vector.
	// For MetricDot: Distance = -⟨q,v⟩ (lower = more similar).
	Distance float32
}

// Stats captures point-in-time metrics for an open index.
type Stats struct {
	// VectorCount is the total number of vectors stored in the index.
	VectorCount uint64
	// CentroidCount is the current number of IVF clusters.
	CentroidCount uint64
	// Epoch is the current centroid generation.
	Epoch uint64
	// WatermarkTxnID is the highest committed transaction ID seen.
	WatermarkTxnID uint64
	// WatermarkSeqID is the highest sequence ID within WatermarkTxnID.
	WatermarkSeqID uint64
	// LastQueryNprobe is the effective nprobe used in the most recent Search call.
	// Used to verify adaptive multi-probe behaviour in tests.
	LastQueryNprobe uint64
}

// BulkEntry is a single vector supplied during index creation.
type BulkEntry struct {
	// ExternalID is the caller-supplied identifier for the vector.
	ExternalID []byte
	// Vector holds the raw float32 values.
	Vector []float32
}

// RetrainCluster is the CDC event that triggers a deterministic centroid retrain
// across all nodes. Nodes apply the same seed to reproduce identical centroids.
type RetrainCluster struct {
	// IndexID identifies the target index.
	IndexID string
	// Epoch is the new centroid generation after retrain.
	Epoch uint64
	// Seed is the RNG seed used for k-means initialisation.
	Seed uint64
}
