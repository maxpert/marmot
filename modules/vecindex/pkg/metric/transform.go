package metric

import (
	"errors"
	"math"
)

// Normalize normalizes a vector to unit length in-place.
// Returns the original norm. If norm is 0, the vector is unchanged.
func Normalize(v []float32) float32 {
	n := Norm(v)
	if n == 0 {
		return 0
	}
	for i := range v {
		v[i] /= n
	}
	return n
}

// NormalizeCopy returns a unit-length copy of v.
// If norm is 0, returns a zero vector of the same length.
func NormalizeCopy(v []float32) []float32 {
	out := make([]float32, len(v))
	copy(out, v)
	Normalize(out)
	return out
}

// AugmentForMIPS appends sqrt(M² - ||v||²) to the vector for MIPS-to-L2 reduction.
// Returns the augmented vector (length dim+1).
// Returns error if ||v|| > M.
func AugmentForMIPS(v []float32, normMax float64) ([]float32, error) {
	n := float64(Norm(v))
	if n > normMax {
		return nil, errors.New("metric: vector norm exceeds normMax")
	}
	extra := float32(math.Sqrt(normMax*normMax - n*n))
	out := make([]float32, len(v)+1)
	copy(out, v)
	out[len(v)] = extra
	return out, nil
}

// AugmentQueryForMIPS appends 0 to the query vector for MIPS-to-L2 reduction.
// Returns the augmented vector (length dim+1).
func AugmentQueryForMIPS(q []float32) []float32 {
	out := make([]float32, len(q)+1)
	copy(out, q)
	out[len(q)] = 0
	return out
}

// QuantizeToBins quantizes a float32 value from [min, max] range into [0, 2^bits - 1].
// Values outside range are clamped.
func QuantizeToBins(val, min, max float32, bits int) uint32 {
	bins := uint32((1 << bits) - 1)
	if val <= min {
		return 0
	}
	if val >= max {
		return bins
	}
	ratio := (val - min) / (max - min)
	return uint32(ratio * float32(bins))
}

// QuantizeDims quantizes a slice of dimensions to integers for Hilbert encoding.
// Each dimension is quantized independently using its min/max range.
// Panics if len(dims), len(domainMin), and len(domainMax) are not equal.
func QuantizeDims(dims []float32, domainMin, domainMax []float32, bits int) []uint32 {
	if len(dims) != len(domainMin) || len(dims) != len(domainMax) {
		panic("metric: dims, domainMin and domainMax must have equal length")
	}
	out := make([]uint32, len(dims))
	for i, v := range dims {
		out[i] = QuantizeToBins(v, domainMin[i], domainMax[i], bits)
	}
	return out
}
