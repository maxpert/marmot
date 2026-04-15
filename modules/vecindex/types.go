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
)

// Quantization selects the posting-list vector encoding format.
type Quantization uint8

const (
	// QuantNone stores posting vectors as raw float32 (legacy path).
	QuantNone Quantization = iota
	// QuantSQ8 stores posting vectors as scalar int8 quantization:
	// [scale float32][sqNorm2 float32][codes int8[dim]] = 8+dim bytes.
	// At dim=1536 this is 1544 bytes vs 6144 for float32 — ~4× smaller.
	QuantSQ8
)

// IVFSpec describes the configuration for a single IVF vector index.
type IVFSpec struct {
	// ID is the unique index identifier.
	ID string
	// Dim is the vector dimensionality.
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
	// Quantization selects the posting-list encoding. QuantSQ8 is the default.
	Quantization Quantization
}

// DefaultSpec returns a sensible IVFSpec for a new empty index.
// Lifecycle management may adjust Nlist/Nprobe as the index grows.
// Quantization defaults to QuantSQ8 for optimal I/O performance.
func DefaultSpec(id string, dim int, metric Metric) IVFSpec {
	return IVFSpec{
		ID:           id,
		Dim:          dim,
		Metric:       metric,
		Nlist:        256,
		Nprobe:       16,
		Quantization: QuantSQ8,
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
