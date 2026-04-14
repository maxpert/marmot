package hdindex

import "math"

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

// DefaultSpec returns an HDIndexSpec with defaults matching the HD-Index paper
// (Arora et al., VLDB 2018, Sections 5.2.1–5.2.6):
//   - m=10 reference objects (§5.2.3: "quality saturates at m=10")
//   - τ ≈ sqrt(dim) (§5.2.4: Enron 1369-dim uses τ=37, Glove 100-dim uses τ=10)
//   - α=4096, γ=1024, α/γ=4 (§5.2.6: recommended values)
//   - Triangle inequality only, no Ptolemaic (§5.2.5: "more prudent")
func DefaultSpec(id string, dim int, metric Metric) HDIndexSpec {
	internalDim := dim
	if metric == MetricDot {
		internalDim = dim + 1
	}

	// Paper §5.2.4: τ ≈ sqrt(dim). For Enron(1369d) they used τ=37,η=37.
	// For Glove(100d) they used τ=10,η=10. Minimum τ=8 for small dims.
	tau, adjustedDim := chooseTauAndDim(internalDim)

	// Ensure η = adjustedDim/tau ≥ 2; a degenerate η=1 collapses the Hilbert
	// curve to a single segment and destroys partitioning quality.
	if adjustedDim/tau < 2 {
		tau = adjustedDim / 2
		if tau < 1 {
			tau = 1
		}
		// Re-align adjustedDim so it remains divisible by tau.
		if adjustedDim%tau != 0 {
			adjustedDim = ((adjustedDim + tau - 1) / tau) * tau
		}
	}

	omega := 8
	if dim > 384 {
		omega = 16
	}

	return HDIndexSpec{
		ID:          id,
		Dim:         dim,
		Metric:      metric,
		InternalDim: adjustedDim,
		Tau:         tau,
		Omega:       omega,
		Eta:         adjustedDim / tau,
		RefCount:    10, // Paper §5.2.3: saturates at m=10
		Alpha:       4096,
		Gamma:       1024,
	}
}

// chooseTauAndDim selects τ ≈ sqrt(internalDim) by finding the closest
// divisor, with minimum τ=8. If internalDim has no suitable divisor
// (e.g., prime from dot-product's dim+1), pads up to a balanced factorization.
// Paper §5.2.4: Enron(1369d) uses τ=37,η=37; Glove(100d) uses τ=10,η=10.
func chooseTauAndDim(internalDim int) (tau, adjustedDim int) {
	target := max(8, int(math.Round(math.Sqrt(float64(internalDim)))))

	// Find the divisor of internalDim closest to target (≥8).
	if d := closestDivisor(internalDim, target); d > 0 && absDiff(d, target) <= target/2 {
		return d, internalDim
	}

	// No good divisor (prime or poor factorization). Pad to target × ceil.
	padded := ((internalDim + target - 1) / target) * target
	return target, padded
}

// closestDivisor finds the divisor of n closest to target that is ≥ 8.
// Returns 0 if no non-trivial divisor ≥ 8 exists.
func closestDivisor(n, target int) int {
	const minTau = 8
	best, bestDiff := 0, n
	for d := 2; d*d <= n; d++ {
		if n%d != 0 {
			continue
		}
		for _, f := range [2]int{d, n / d} {
			if f < minTau {
				continue
			}
			if diff := absDiff(f, target); diff < bestDiff {
				best, bestDiff = f, diff
			}
		}
	}
	return best
}

func absDiff(a, b int) int {
	if a > b {
		return a - b
	}
	return b - a
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
	CandidatesScanned       int
	CandidatesAfterTriangle int
	CandidatesExactScored   int
	PartitionsSearched      int
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
