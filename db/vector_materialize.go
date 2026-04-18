package db

import (
	"fmt"
	"math"

	vecmetric "github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// materializeVectorBlob converts a raw user-space embedding blob into the
// canonical internal sidecar representation. The transform is pure and depends
// only on (metric, dim, maxNorm, rawBlob); it does not require engine state.
//
// Returns nil, nil for vectors that should not be indexed (currently only
// zero-norm cosine vectors). Malformed blobs still return an error so the
// caller can reject invalid writes.
func materializeVectorBlob(raw []byte, m vecmetric.Metric, dim int, maxNorm float32) ([]byte, error) {
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
		vec := append([]float32(nil), src...)
		norm := vecmetric.Norm(vec)
		if norm == 0 {
			return nil, nil
		}
		inv := 1.0 / norm
		for i := range vec {
			vec[i] *= inv
		}
		return float32sToBlob(vec), nil
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
		bits := math.Float32bits(f)
		out[i*4] = byte(bits)
		out[i*4+1] = byte(bits >> 8)
		out[i*4+2] = byte(bits >> 16)
		out[i*4+3] = byte(bits >> 24)
	}
	return out
}
