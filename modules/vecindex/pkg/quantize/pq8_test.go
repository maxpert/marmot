package quantize

import (
	"math"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

func TestPQ8CodecDeterministicTraining(t *testing.T) {
	t.Parallel()

	residuals := pq8TestResiduals(300, 17)
	a, err := TrainPQ8(residuals, 17, PQ8Options{M: 5, MaxIter: 3, Seed: 42})
	if err != nil {
		t.Fatalf("TrainPQ8 a: %v", err)
	}
	b, err := TrainPQ8(residuals, 17, PQ8Options{M: 5, MaxIter: 3, Seed: 42})
	if err != nil {
		t.Fatalf("TrainPQ8 b: %v", err)
	}
	if a.Dim != b.Dim || a.M != b.M || len(a.Offsets) != len(b.Offsets) || len(a.Codebooks) != len(b.Codebooks) {
		t.Fatalf("codec shape mismatch")
	}
	for i := range a.Offsets {
		if a.Offsets[i] != b.Offsets[i] {
			t.Fatalf("offset %d = %d, want %d", i, a.Offsets[i], b.Offsets[i])
		}
	}
	for i := range a.Codebooks {
		if a.Codebooks[i] != b.Codebooks[i] {
			t.Fatalf("codebook[%d] = %v, want %v", i, a.Codebooks[i], b.Codebooks[i])
		}
	}
}

func TestPQ8ScorerScalarSpanParity(t *testing.T) {
	t.Parallel()

	const dim = 16
	residuals := pq8TestResiduals(300, dim)
	codec, err := TrainPQ8(residuals, dim, PQ8Options{M: 4, MaxIter: 3, Seed: 7})
	if err != nil {
		t.Fatalf("TrainPQ8: %v", err)
	}
	centroid := make([]float32, dim)
	query := make([]float32, dim)
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		centroid[i] = float32(i%5) * 0.01
		query[i] = float32((i*3)%11) * 0.03
		vec[i] = centroid[i] + residuals[17][i]
	}
	queryNorm2 := metric.Norm2(query)
	blob, err := codec.EncodeResidual(metric.MetricL2, vec, centroid)
	if err != nil {
		t.Fatalf("EncodeResidual: %v", err)
	}
	if got, want := len(blob), codec.EncodedSize(metric.MetricL2); got != want {
		t.Fatalf("encoded size = %d, want %d", got, want)
	}
	scorer, err := NewPQ8Scorer(metric.MetricL2, query, queryNorm2, centroid, codec)
	if err != nil {
		t.Fatalf("NewPQ8Scorer: %v", err)
	}
	scalar, err := scorer.Distance(blob)
	if err != nil {
		t.Fatalf("Distance: %v", err)
	}
	row := make([]byte, 8+len(blob))
	copy(row[8:], blob)
	out := make([]float32, 1)
	if err := scorer.ScoreSpan(row, len(row), out); err != nil {
		t.Fatalf("ScoreSpan: %v", err)
	}
	if math.Abs(float64(out[0]-scalar)) > 1e-6 {
		t.Fatalf("span score = %v, want %v", out[0], scalar)
	}
}

func TestPQ8ValidationRejectsMalformedCodec(t *testing.T) {
	t.Parallel()

	cases := []*PQ8Codec{
		nil,
		{Dim: 0, M: 1, Offsets: []int{0, 1}, Codebooks: make([]float32, pq8CodebookSize)},
		{Dim: 4, M: 5, Offsets: []int{0, 1, 2, 3, 4, 5}, Codebooks: make([]float32, pq8CodebookSize*5)},
		{Dim: 4, M: 2, Offsets: []int{0, 3, 4}, Codebooks: make([]float32, 1)},
	}
	for i, codec := range cases {
		if err := codec.Validate(); err == nil {
			t.Fatalf("case %d Validate succeeded, want error", i)
		}
	}
}

func pq8TestResiduals(n, dim int) [][]float32 {
	out := make([][]float32, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dim)
		for d := 0; d < dim; d++ {
			x := float64((i+1)*(d+3)%97) / 97
			vec[d] = float32(math.Sin(x*math.Pi*2) * 0.25)
		}
		out[i] = vec
	}
	return out
}
