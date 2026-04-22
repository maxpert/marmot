package db

import (
	"math"

	vecmaterialize "github.com/maxpert/marmot/modules/vecindex/pkg/materialize"
	vecmetric "github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// MaterializeVectorBlob converts a raw user-space embedding blob into the
// canonical internal search-space representation. The transform is pure and
// depends only on (metric, dim, maxNorm, rawBlob); it does not require engine
// state.
//
// Returns nil, nil for vectors that should not be indexed (currently only
// zero-norm cosine vectors). Malformed blobs still return an error so the
// caller can reject invalid writes.
func MaterializeVectorBlob(raw []byte, m vecmetric.Metric, dim int, maxNorm float32) ([]byte, error) {
	return vecmaterialize.VectorBlob(raw, m, dim, maxNorm)
}

func materializeVectorBlob(raw []byte, m vecmetric.Metric, dim int, maxNorm float32) ([]byte, error) {
	return MaterializeVectorBlob(raw, m, dim, maxNorm)
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
