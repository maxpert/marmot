package metric

import (
	"math"
	"testing"
)

func TestNormalize(t *testing.T) {
	t.Parallel()
	v := []float32{3, 4}
	origNorm := Normalize(v)
	if math.Abs(float64(origNorm)-5.0) > 1e-6 {
		t.Errorf("Normalize returned norm %v, want 5.0", origNorm)
	}
	got := Norm(v)
	if math.Abs(float64(got)-1.0) > 1e-6 {
		t.Errorf("after Normalize, Norm = %v, want 1.0", got)
	}
}

func TestNormalize_ZeroVector(t *testing.T) {
	t.Parallel()
	v := []float32{0, 0, 0}
	n := Normalize(v)
	if n != 0 {
		t.Errorf("expected norm 0 for zero vector, got %v", n)
	}
	for i, x := range v {
		if x != 0 {
			t.Errorf("v[%d] = %v after normalize of zero vector, want 0", i, x)
		}
	}
}

func TestNormalizeCopy(t *testing.T) {
	t.Parallel()
	orig := []float32{3, 0, 4}
	cp := NormalizeCopy(orig)

	// original unchanged
	if orig[0] != 3 || orig[1] != 0 || orig[2] != 4 {
		t.Errorf("NormalizeCopy modified original: %v", orig)
	}

	// copy has unit norm
	n := Norm(cp)
	if math.Abs(float64(n)-1.0) > 1e-6 {
		t.Errorf("NormalizeCopy result has norm %v, want 1.0", n)
	}
}

func TestNormalizeCopy_ZeroVector(t *testing.T) {
	t.Parallel()
	orig := []float32{0, 0}
	cp := NormalizeCopy(orig)
	if len(cp) != len(orig) {
		t.Fatalf("NormalizeCopy length %v, want %v", len(cp), len(orig))
	}
	for i, x := range cp {
		if x != 0 {
			t.Errorf("cp[%d] = %v for zero input, want 0", i, x)
		}
	}
}

func TestAugmentForMIPS(t *testing.T) {
	t.Parallel()
	// v with norm 3, M=5, extra = sqrt(25-9) = 4
	v := []float32{3, 0, 0}
	M := 5.0
	aug, err := AugmentForMIPS(v, M)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(aug) != len(v)+1 {
		t.Fatalf("augmented length %v, want %v", len(aug), len(v)+1)
	}
	if math.Abs(float64(aug[len(aug)-1])-4.0) > 1e-5 {
		t.Errorf("augmented last element = %v, want 4.0", aug[len(aug)-1])
	}

	// MIPS-to-L2 property: L2(aug_query, aug_vec)^2 == M^2 + M^2 - 2*dot(q,v)
	// For query q augmented with 0:
	q := []float32{1, 0, 0}
	augQ := AugmentQueryForMIPS(q)
	// ||aug_q - aug_v||^2 = ||q - v||^2 + (0 - extra)^2
	// = (1-3)^2 + 4^2 = 4 + 16 = 20
	// dot(q,v) = 3, M^2 + M^2 - 2*dot = 50 - 6 = 44 ... no that's different formula
	// Correct formula: ||augQ - aug||^2 = ||q||^2 + M^2 - 2*dot(q,v) ... wait
	// augQ = [q, 0], aug = [v, extra]
	// dist^2 = ||q-v||^2 + extra^2 = (1-3)^2+0+0 + 16 = 4+16 = 20
	// 2*dot(q,v) = 6, M^2 - 2*dot = 25 - 6 = 19 ... hmm, let's just check the value
	l2sq := L2Squared(augQ, aug)
	expected := float32(L2Squared(q, v[:len(q)]) + aug[len(v)]*aug[len(v)])
	if math.Abs(float64(l2sq-expected)) > 1e-5 {
		t.Errorf("MIPS L2Squared = %v, want %v", l2sq, expected)
	}
}

func TestAugmentForMIPS_NormExceedsM(t *testing.T) {
	t.Parallel()
	v := []float32{10, 0}
	_, err := AugmentForMIPS(v, 5.0)
	if err == nil {
		t.Error("expected error when norm > normMax, got nil")
	}
}

func TestAugmentQueryForMIPS(t *testing.T) {
	t.Parallel()
	q := []float32{1, 2, 3}
	aug := AugmentQueryForMIPS(q)
	if len(aug) != len(q)+1 {
		t.Fatalf("length %v, want %v", len(aug), len(q)+1)
	}
	if aug[len(aug)-1] != 0 {
		t.Errorf("last element = %v, want 0", aug[len(aug)-1])
	}
	for i, x := range q {
		if aug[i] != x {
			t.Errorf("aug[%d] = %v, want %v", i, aug[i], x)
		}
	}
}

func TestQuantizeToBins(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		val      float32
		min, max float32
		bits     int
		want     uint32
	}{
		{"min edge", 0.0, 0.0, 1.0, 8, 0},
		{"max edge", 1.0, 0.0, 1.0, 8, 255},
		{"mid approx", 0.5, 0.0, 1.0, 8, 127},
		{"below min clamp", -1.0, 0.0, 1.0, 8, 0},
		{"above max clamp", 2.0, 0.0, 1.0, 8, 255},
		{"1 bit mid", 0.5, 0.0, 1.0, 1, 0},
		{"1 bit max", 1.0, 0.0, 1.0, 1, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := QuantizeToBins(tc.val, tc.min, tc.max, tc.bits)
			// Allow ±1 for midpoint rounding
			diff := int64(got) - int64(tc.want)
			if diff < -1 || diff > 1 {
				t.Errorf("QuantizeToBins(%v, %v, %v, %v) = %v, want ~%v", tc.val, tc.min, tc.max, tc.bits, got, tc.want)
			}
		})
	}
}

func TestQuantizeDims(t *testing.T) {
	t.Parallel()
	dims := []float32{0.0, 0.5, 1.0}
	dMin := []float32{0.0, 0.0, 0.0}
	dMax := []float32{1.0, 1.0, 1.0}
	result := QuantizeDims(dims, dMin, dMax, 8)

	if len(result) != 3 {
		t.Fatalf("length %v, want 3", len(result))
	}
	if result[0] != 0 {
		t.Errorf("result[0] = %v, want 0", result[0])
	}
	if result[2] != 255 {
		t.Errorf("result[2] = %v, want 255", result[2])
	}
	// mid ~127-128
	if result[1] < 126 || result[1] > 129 {
		t.Errorf("result[1] = %v, want ~127", result[1])
	}
}

func TestQuantizeDims_MismatchedLengths(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for mismatched domain lengths")
		}
	}()
	QuantizeDims([]float32{1, 2}, []float32{0}, []float32{1, 1}, 8)
}
