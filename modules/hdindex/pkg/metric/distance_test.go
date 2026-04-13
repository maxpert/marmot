package metric

import (
	"math"
	"testing"
)

func TestL2Squared(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b []float32
		want float32
	}{
		{"orthogonal unit", []float32{1, 0, 0}, []float32{0, 1, 0}, 2.0},
		{"identical", []float32{1, 2, 3}, []float32{1, 2, 3}, 0.0},
		{"known pair", []float32{0, 0}, []float32{3, 4}, 25.0},
		{"negative components", []float32{-1, 0}, []float32{1, 0}, 4.0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := L2Squared(tc.a, tc.b)
			if math.Abs(float64(got-tc.want)) > 1e-6 {
				t.Errorf("L2Squared(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestL2Squared_MismatchedLengths(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for mismatched lengths")
		}
	}()
	L2Squared([]float32{1, 2}, []float32{1})
}

func TestL2(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b []float32
		want float32
	}{
		{"3-4-5 triangle", []float32{0, 0}, []float32{3, 4}, 5.0},
		{"identical", []float32{1, 2}, []float32{1, 2}, 0.0},
		{"unit orthogonal", []float32{1, 0}, []float32{0, 1}, float32(math.Sqrt(2))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := L2(tc.a, tc.b)
			if math.Abs(float64(got-tc.want)) > 1e-6 {
				t.Errorf("L2(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestDotProduct(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b []float32
		want float32
	}{
		{"orthogonal", []float32{1, 0}, []float32{0, 1}, 0.0},
		{"parallel", []float32{2, 0}, []float32{3, 0}, 6.0},
		{"known pair", []float32{1, 2, 3}, []float32{4, 5, 6}, 32.0},
		{"negative", []float32{1, -1}, []float32{1, 1}, 0.0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := DotProduct(tc.a, tc.b)
			if math.Abs(float64(got-tc.want)) > 1e-6 {
				t.Errorf("DotProduct(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestDotProduct_MismatchedLengths(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for mismatched lengths")
		}
	}()
	DotProduct([]float32{1, 2}, []float32{1})
}

func TestCosineSimilarity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b []float32
		want float32
		eps  float64
	}{
		{"identical", []float32{1, 0, 0}, []float32{1, 0, 0}, 1.0, 1e-6},
		{"orthogonal", []float32{1, 0}, []float32{0, 1}, 0.0, 1e-6},
		{"opposite", []float32{1, 0}, []float32{-1, 0}, -1.0, 1e-6},
		{"zero vector a", []float32{0, 0}, []float32{1, 0}, 0.0, 1e-6},
		{"zero vector b", []float32{1, 0}, []float32{0, 0}, 0.0, 1e-6},
		{"both zero", []float32{0, 0}, []float32{0, 0}, 0.0, 1e-6},
		{"scaled same direction", []float32{2, 0}, []float32{5, 0}, 1.0, 1e-6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CosineSimilarity(tc.a, tc.b)
			if math.Abs(float64(got-tc.want)) > tc.eps {
				t.Errorf("CosineSimilarity(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// TestCosineSimilarity_NormProductUnderflow guards against a float32 precision edge case
// where each norm is individually non-zero but their product underflows to zero, which
// would otherwise produce a NaN/Inf result from division. Values around 1e-24 trigger
// this: Norm returns a non-zero subnormal, but the product of two such values is zero.
func TestCosineSimilarity_NormProductUnderflow(t *testing.T) {
	t.Parallel()
	// ~1e-24: norm is non-zero float32 subnormal, but norm*norm == 0.
	tiny := float32(1e-24)
	a := []float32{tiny, 0, 0}
	b := []float32{tiny, 0, 0}
	got := CosineSimilarity(a, b)
	if math.IsNaN(float64(got)) || math.IsInf(float64(got), 0) {
		t.Errorf("CosineSimilarity with subnormal norms = %v, want finite value (0)", got)
	}
	// Both vectors point in the same direction; return 0 (degenerate, not 1)
	// because we cannot reliably compute similarity at this scale.
	if got != 0 {
		t.Errorf("CosineSimilarity with underflowing norm product = %v, want 0", got)
	}
}

func TestNorm(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		v    []float32
		want float32
	}{
		{"unit x", []float32{1, 0, 0}, 1.0},
		{"zero vector", []float32{0, 0, 0}, 0.0},
		{"3-4-5", []float32{3, 4}, 5.0},
		{"negative", []float32{-3, -4}, 5.0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Norm(tc.v)
			if math.Abs(float64(got-tc.want)) > 1e-6 {
				t.Errorf("Norm(%v) = %v, want %v", tc.v, got, tc.want)
			}
		})
	}
}
