package materialize

import (
	"fmt"
	"math"

	vecmetric "github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// VectorBlob converts a raw user-space embedding blob into the canonical
// internal search-space representation used by the vector index.
//
// Returns nil, nil for vectors that should not be indexed (currently only
// zero-norm cosine vectors). Malformed blobs still return an error.
func VectorBlob(raw []byte, m vecmetric.Metric, dim int, maxNorm float32) ([]byte, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("MARMOT-VEC-014: empty vector blob")
	}
	if len(raw)%4 != 0 {
		return nil, fmt.Errorf("MARMOT-VEC-014: vector blob length %d is not a multiple of 4", len(raw))
	}
	if got := len(raw) / 4; got != dim {
		return nil, fmt.Errorf("MARMOT-VEC-014: vector dimension mismatch: got %d, want %d", got, dim)
	}

	src := vecmetric.BytesToFloat32(raw)
	switch m {
	case vecmetric.MetricL2:
		return append([]byte(nil), raw...), nil
	case vecmetric.MetricCosine:
		norm := vecmetric.Norm(src)
		if norm == 0 {
			return nil, nil
		}
		inv := 1.0 / norm
		out := make([]byte, len(raw))
		for i, value := range src {
			putFloat32(out[i*4:], value*inv)
		}
		return out, nil
	case vecmetric.MetricDot:
		aug, err := vecmetric.AugmentData(src, maxNorm, nil)
		if err != nil {
			return nil, err
		}
		return float32sToBlob(aug), nil
	default:
		return nil, fmt.Errorf("MARMOT-VEC-016: unknown metric code %d", m)
	}
}

func float32sToBlob(v []float32) []byte {
	out := make([]byte, len(v)*4)
	for i, f := range v {
		putFloat32(out[i*4:], f)
	}
	return out
}

func putFloat32(dst []byte, f float32) {
	bits := math.Float32bits(f)
	dst[0] = byte(bits)
	dst[1] = byte(bits >> 8)
	dst[2] = byte(bits >> 16)
	dst[3] = byte(bits >> 24)
}
