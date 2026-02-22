package quant

import (
	"errors"
	"fmt"
	"math/rand"

	"gonum.org/v1/gonum/floats"
)

type PQModel struct {
	Dim           int
	SubQuantizers int
	CodebookSize  int
	SubDim        int
	Centroids     [][][]float32 // [subq][k][subdim]
}

func TrainPQ(vectors [][]float32, subQuantizers, bits int, seed int64, iterations int) (*PQModel, error) {
	if len(vectors) == 0 {
		return nil, errors.New("no vectors")
	}
	dim := len(vectors[0])
	if dim == 0 {
		return nil, errors.New("zero dimension")
	}
	for _, v := range vectors {
		if len(v) != dim {
			return nil, errors.New("inconsistent dimensions")
		}
	}
	if subQuantizers <= 0 {
		return nil, errors.New("subQuantizers must be > 0")
	}
	if dim%subQuantizers != 0 {
		return nil, fmt.Errorf("dim %d must be divisible by subQuantizers %d", dim, subQuantizers)
	}
	if bits <= 0 || bits > 8 {
		return nil, errors.New("bits must be in [1,8]")
	}
	k := 1 << bits
	subDim := dim / subQuantizers
	if iterations <= 0 {
		iterations = 8
	}

	rng := rand.New(rand.NewSource(seed))
	model := &PQModel{
		Dim:           dim,
		SubQuantizers: subQuantizers,
		CodebookSize:  k,
		SubDim:        subDim,
		Centroids:     make([][][]float32, subQuantizers),
	}

	for sq := 0; sq < subQuantizers; sq++ {
		centroids := make([][]float32, k)
		for i := 0; i < k; i++ {
			sample := vectors[rng.Intn(len(vectors))]
			centroids[i] = append([]float32(nil), sample[sq*subDim:(sq+1)*subDim]...)
		}
		assign := make([]int, len(vectors))
		for iter := 0; iter < iterations; iter++ {
			for i, v := range vectors {
				sub := v[sq*subDim : (sq+1)*subDim]
				best := 0
				bestDist := l2sq(sub, centroids[0])
				for c := 1; c < k; c++ {
					d := l2sq(sub, centroids[c])
					if d < bestDist {
						bestDist = d
						best = c
					}
				}
				assign[i] = best
			}
			sums := make([][]float64, k)
			counts := make([]int, k)
			for c := range sums {
				sums[c] = make([]float64, subDim)
			}
			for i, v := range vectors {
				c := assign[i]
				sub := v[sq*subDim : (sq+1)*subDim]
				for d := 0; d < subDim; d++ {
					sums[c][d] += float64(sub[d])
				}
				counts[c]++
			}
			for c := 0; c < k; c++ {
				if counts[c] == 0 {
					sample := vectors[rng.Intn(len(vectors))]
					copy(centroids[c], sample[sq*subDim:(sq+1)*subDim])
					continue
				}
				for d := 0; d < subDim; d++ {
					centroids[c][d] = float32(sums[c][d] / float64(counts[c]))
				}
			}
		}
		model.Centroids[sq] = centroids
	}
	return model, nil
}

func (m *PQModel) Encode(vec []float32) ([]byte, error) {
	if len(vec) != m.Dim {
		return nil, fmt.Errorf("dimension mismatch expected=%d actual=%d", m.Dim, len(vec))
	}
	code := make([]byte, m.SubQuantizers)
	for sq := 0; sq < m.SubQuantizers; sq++ {
		sub := vec[sq*m.SubDim : (sq+1)*m.SubDim]
		best := 0
		bestDist := l2sq(sub, m.Centroids[sq][0])
		for c := 1; c < m.CodebookSize; c++ {
			d := l2sq(sub, m.Centroids[sq][c])
			if d < bestDist {
				bestDist = d
				best = c
			}
		}
		code[sq] = byte(best)
	}
	return code, nil
}

func (m *PQModel) Decode(code []byte) ([]float32, error) {
	if len(code) != m.SubQuantizers {
		return nil, fmt.Errorf("code length mismatch expected=%d actual=%d", m.SubQuantizers, len(code))
	}
	out := make([]float32, m.Dim)
	for sq := 0; sq < m.SubQuantizers; sq++ {
		idx := int(code[sq])
		if idx >= m.CodebookSize {
			return nil, fmt.Errorf("invalid code index %d for sub-quantizer %d", idx, sq)
		}
		copy(out[sq*m.SubDim:(sq+1)*m.SubDim], m.Centroids[sq][idx])
	}
	return out, nil
}

func l2sq(a, b []float32) float64 {
	bufA := make([]float64, len(a))
	bufB := make([]float64, len(b))
	for i := range a {
		bufA[i] = float64(a[i])
		bufB[i] = float64(b[i])
	}
	diff := make([]float64, len(a))
	copy(diff, bufA)
	floats.Sub(diff, bufB)
	return floats.Dot(diff, diff)
}
