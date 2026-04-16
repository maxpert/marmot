package metric

import (
	"encoding/binary"
	"math"
	"testing"
)

// encodeVec serialises a []float32 as raw little-endian bytes for testing.
func encodeVec(v []float32) []byte {
	buf := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

// TestL2SquaredSIMD_MatchesGoFallback pins the invariant that the SIMD
// squared-distance implementation stays within a tight relative tolerance of
// the pure-Go 4-wide reference across representative dims. Absolute mismatch
// is expected from the sqrt-then-square roundtrip; the contract is relative
// agreement for downstream k-means weighting.
func TestL2SquaredSIMD_MatchesGoFallback(t *testing.T) {
	t.Parallel()
	dims := []int{8, 16, 32, 64, 128, 1536}
	for _, dim := range dims {
		dim := dim
		t.Run("dim="+itoaTest(dim), func(t *testing.T) {
			t.Parallel()
			// Per-subtest rng seeded by dim so parallel subtests do not
			// share mutable state (the race detector flags a shared rng).
			rng := randSource(int64(424242 + dim))
			for trial := 0; trial < 16; trial++ {
				a := randomVec(rng, dim)
				b := randomVec(rng, dim)
				got := L2SquaredSIMD(a, b)
				want := L2SquaredGo(a, b)
				// Use relative tolerance; sqrt-then-square roundtrip can lose ~1 ULP
				// of float32 precision which becomes absolute error proportional to
				// the magnitude. Guard against zero to avoid div-by-zero.
				denom := float64(want)
				if denom < 1e-6 {
					denom = 1e-6
				}
				rel := math.Abs(float64(got-want)) / denom
				if rel > 1e-5 {
					t.Fatalf("dim=%d trial=%d SIMD=%v Go=%v relErr=%.3e", dim, trial, got, want, rel)
				}
			}
		})
	}
}

// TestL2Squared_DispatchesToGo pins the L2Squared default to the pure-Go
// reference implementation. This locks the choice made after benchmarking:
// SIMD L2Squared is not ≥ 2× faster on the k-means dims, and the Go path
// preserves bit-exact determinism across platforms.
func TestL2Squared_DispatchesToGo(t *testing.T) {
	t.Parallel()
	rng := randSource(1)
	for _, dim := range []int{8, 32, 128, 1536} {
		a := randomVec(rng, dim)
		b := randomVec(rng, dim)
		if L2Squared(a, b) != L2SquaredGo(a, b) {
			t.Fatalf("dim=%d: L2Squared must delegate to L2SquaredGo", dim)
		}
	}
}

func BenchmarkL2SquaredGo(b *testing.B) {
	for _, dim := range []int{32, 128, 1536} {
		dim := dim
		b.Run("dim="+itoaTest(dim), func(b *testing.B) {
			rng := randSource(7)
			x := randomVec(rng, dim)
			y := randomVec(rng, dim)
			b.ResetTimer()
			var sink float32
			for i := 0; i < b.N; i++ {
				sink += L2SquaredGo(x, y)
			}
			benchSink = sink
		})
	}
}

func BenchmarkL2SquaredSIMD(b *testing.B) {
	for _, dim := range []int{32, 128, 1536} {
		dim := dim
		b.Run("dim="+itoaTest(dim), func(b *testing.B) {
			rng := randSource(7)
			x := randomVec(rng, dim)
			y := randomVec(rng, dim)
			b.ResetTimer()
			var sink float32
			for i := 0; i < b.N; i++ {
				sink += L2SquaredSIMD(x, y)
			}
			benchSink = sink
		})
	}
}

var benchSink float32

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

// TestFromBytes_BitIdenticalToFloat32 verifies that the FromBytes variants produce
// exactly the same results as their []float32 counterparts for L2, Dot, and Cosine.
func TestFromBytes_BitIdenticalToFloat32(t *testing.T) {
	t.Parallel()
	a := []float32{1.0, -2.5, 3.14, 0.0, -0.001, 100.0}
	b := []float32{0.5, 1.0, -3.14, 1.0, 200.0, -1.0}
	bBytes := encodeVec(b)

	t.Run("L2Squared", func(t *testing.T) {
		t.Parallel()
		want := L2Squared(a, b)
		got := L2SquaredFromBytes(a, bBytes)
		if want != got {
			t.Errorf("L2SquaredFromBytes = %v, want %v", got, want)
		}
	})

	t.Run("Dot", func(t *testing.T) {
		t.Parallel()
		want := -DotProduct(a, b)
		got := DotFromBytes(a, bBytes)
		if want != got {
			t.Errorf("DotFromBytes = %v, want %v", got, want)
		}
	})

	t.Run("Cosine", func(t *testing.T) {
		t.Parallel()
		want := 1 - CosineSimilarity(a, b)
		got := CosineFromBytes(a, bBytes)
		if math.Abs(float64(got-want)) > 1e-6 {
			t.Errorf("CosineFromBytes = %v, want %v", got, want)
		}
	})
}

func TestFromBytes_LengthMismatchPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for mismatched lengths")
		}
	}()
	L2SquaredFromBytes([]float32{1, 2}, []byte{0, 0, 0, 0}) // 1 float vs 2-dim query
}
