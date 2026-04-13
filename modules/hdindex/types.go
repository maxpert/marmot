package hdindex

// Metric defines the distance metric for the index.
type Metric int

const (
	MetricEuclidean Metric = iota
	MetricCosine
	MetricDot
)

func (m Metric) String() string {
	switch m {
	case MetricEuclidean:
		return "euclidean"
	case MetricCosine:
		return "cosine"
	case MetricDot:
		return "dot"
	default:
		return "unknown"
	}
}

func ParseMetric(s string) (Metric, bool) {
	switch s {
	case "euclidean", "l2":
		return MetricEuclidean, true
	case "cosine":
		return MetricCosine, true
	case "dot", "ip", "inner_product":
		return MetricDot, true
	default:
		return 0, false
	}
}

// HDIndexSpec defines the configuration for an HD-Index instance.
type HDIndexSpec struct {
	ID          string    `msgpack:"id"`
	Dim         int       `msgpack:"dim"`
	Metric      Metric    `msgpack:"metric"`
	InternalDim int       `msgpack:"internal_dim"`
	Tau         int       `msgpack:"tau"`
	Omega       int       `msgpack:"omega"`
	Eta         int       `msgpack:"eta"`
	RefCount    int       `msgpack:"ref_count"`
	Alpha       int       `msgpack:"alpha"`
	Gamma       int       `msgpack:"gamma"`
	Seed        int64     `msgpack:"seed"`
	NormMax     float64   `msgpack:"norm_max"`
	DomainMin   []float32 `msgpack:"domain_min"`
	DomainMax   []float32 `msgpack:"domain_max"`
}

// DefaultSpec returns an HDIndexSpec with sensible defaults for the given dimension and metric.
func DefaultSpec(id string, dim int, metric Metric) HDIndexSpec {
	tau := 8
	internalDim := dim
	if metric == MetricDot {
		internalDim = dim + 1
	}
	// Adjust tau if it doesn't divide InternalDim evenly
	for tau > 1 && internalDim%tau != 0 {
		tau--
	}
	omega := 8
	if dim > 384 {
		omega = 16
	}
	return HDIndexSpec{
		ID:          id,
		Dim:         dim,
		Metric:      metric,
		InternalDim: internalDim,
		Tau:         tau,
		Omega:       omega,
		Eta:         internalDim / tau,
		RefCount:    10,
		Alpha:       4096,
		Gamma:       1024,
	}
}

// Mutation represents a vector upsert operation.
type Mutation struct {
	TxnID      uint64
	SeqID      uint64
	ExternalID []byte
	VectorFP32 []float32
}

// DeleteMutation represents a vector delete operation.
type DeleteMutation struct {
	TxnID      uint64
	SeqID      uint64
	ExternalID []byte
}

// SearchRequest defines parameters for a kNN search.
type SearchRequest struct {
	VectorFP32 []float32
	TopK       int
	Alpha      int // override per-query, 0 = use index default
	Gamma      int // override per-query, 0 = use index default
}

// SearchHit represents a single search result.
type SearchHit struct {
	ExternalID []byte
	Distance   float32
	Score      float32
}

// SearchResult holds the output of a search operation.
type SearchResult struct {
	Hits  []SearchHit
	Stats SearchStats
}

// SearchStats provides diagnostics about a search.
type SearchStats struct {
	CandidatesScanned        int
	CandidatesAfterTriangle  int
	CandidatesAfterPtolemaic int
	CandidatesExactScored    int
	PartitionsSearched       int
}

// IndexStats provides statistics about the index.
type IndexStats struct {
	VectorCount    uint64
	WatermarkTxnID uint64
	WatermarkSeqID uint64
}

// ApplyToken identifies a mutation for idempotency tracking.
type ApplyToken struct {
	TxnID uint64
	SeqID uint64
}

// EngineConfig holds options for creating an HD-Index engine.
type EngineConfig struct {
	PebbleCacheMB int // Block cache size in MB (0 = Pebble default)
}
