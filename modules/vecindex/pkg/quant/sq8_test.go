package quant

import (
	"math"
	"math/rand"
	"testing"
)

func randomVec(rng *rand.Rand, dim int, scale float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = (rng.Float32()*2 - 1) * scale
	}
	return v
}

func normalizeVec(v []float32) []float32 {
	var sq float32
	for _, x := range v {
		sq += x * x
	}
	n := float32(math.Sqrt(float64(sq)))
	if n == 0 {
		return v
	}
	out := make([]float32, len(v))
	inv := 1.0 / n
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}

func l2Squared(a, b []float32) float32 {
	var s float32
	for i := range a {
		d := a[i] - b[i]
		s += d * d
	}
	return s
}

func dotProduct(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// TestSQ8_EncodeDecodeRoundtrip verifies mean quantization error < 1%.
func TestSQ8_EncodeDecodeRoundtrip(t *testing.T) {
	t.Parallel()
	const dim = 1536
	rng := rand.New(rand.NewSource(42))

	var totalRelErr float64
	const nVecs = 100
	for range nVecs {
		v := randomVec(rng, dim, 1.0)
		q := Encode(v)
		rec := Decode(q)

		// Compute relative error as ||v - rec|| / ||v||
		var errSq, normSq float64
		for i := range v {
			d := float64(v[i] - rec[i])
			errSq += d * d
			normSq += float64(v[i]) * float64(v[i])
		}
		if normSq > 0 {
			totalRelErr += math.Sqrt(errSq/normSq)
		}
	}
	meanRelErr := totalRelErr / nVecs
	if meanRelErr > 0.01 {
		t.Errorf("mean relative quantization error %.4f > 1%%", meanRelErr)
	}
}

// TestSQ8_DotProductAccuracy verifies int8-derived dot vs float32 dot < 0.02 absolute error
// on random 1536-D normalized vectors. Dot products of unit vectors lie in [-1,1],
// so an absolute tolerance of 0.02 corresponds to ≤2% of the full range.
func TestSQ8_DotProductAccuracy(t *testing.T) {
	t.Parallel()
	const dim = 1536
	rng := rand.New(rand.NewSource(7))

	const nPairs = 200
	var maxAbsErr float64
	buf := make([]byte, EncodedSize(dim))

	for range nPairs {
		a := normalizeVec(randomVec(rng, dim, 1.0))
		b := normalizeVec(randomVec(rng, dim, 1.0))

		qa := Encode(a)
		qb := Encode(b)
		encoded := MarshalBinary(qb, buf)

		_, _, off := UnmarshalHeader(encoded)
		dot := DotInt8Bytes(qa.Codes, encoded[off:])
		gotDot := qa.Scale * qb.Scale * float32(dot)

		wantDot := dotProduct(a, b)

		absErr := math.Abs(float64(gotDot - wantDot))
		if absErr > maxAbsErr {
			maxAbsErr = absErr
		}
	}
	// Dot products of unit vectors in [-1,1]; 0.02 absolute tolerance = 2% range.
	if maxAbsErr > 0.02 {
		t.Errorf("max absolute dot product error %.4f > 0.02", maxAbsErr)
	}
}

// TestSQ8_MarshalUnmarshal verifies round-trip through MarshalBinary / UnmarshalHeader.
func TestSQ8_MarshalUnmarshal(t *testing.T) {
	t.Parallel()
	const dim = 128
	rng := rand.New(rand.NewSource(99))
	v := randomVec(rng, dim, 2.5)
	q := Encode(v)

	buf := MarshalBinary(q, nil)
	if len(buf) != EncodedSize(dim) {
		t.Fatalf("unexpected encoded size: got %d, want %d", len(buf), EncodedSize(dim))
	}

	scale, sqNorm2, off := UnmarshalHeader(buf)
	if math.Abs(float64(scale-q.Scale)) > 1e-6 {
		t.Errorf("scale mismatch: got %v, want %v", scale, q.Scale)
	}
	if math.Abs(float64(sqNorm2-q.SqNorm2)) > 1e-3 {
		t.Errorf("sqNorm2 mismatch: got %v, want %v", sqNorm2, q.SqNorm2)
	}
	if off != headerSize {
		t.Errorf("codesOffset: got %d, want %d", off, headerSize)
	}
	for i, c := range q.Codes {
		if int8(buf[off+i]) != c {
			t.Errorf("code[%d] mismatch", i)
		}
	}
}

// TestSQ8_L2SquaredAccuracy verifies L2SquaredFromSQ8 vs exact float32 L2 < 2% relative error.
func TestSQ8_L2SquaredAccuracy(t *testing.T) {
	t.Parallel()
	const dim = 1536
	rng := rand.New(rand.NewSource(13))
	buf := make([]byte, EncodedSize(dim))

	const nPairs = 200
	var maxRelErr float64

	for range nPairs {
		a := randomVec(rng, dim, 1.0)
		b := randomVec(rng, dim, 1.0)

		qa := Encode(a)
		qb := Encode(b)
		encoded := MarshalBinary(qb, buf)

		got := L2SquaredFromSQ8(qa, encoded)
		want := l2Squared(a, b)

		if want > 1e-6 {
			relErr := math.Abs(float64(got-want)) / float64(want)
			if relErr > maxRelErr {
				maxRelErr = relErr
			}
		}
	}
	if maxRelErr > 0.02 {
		t.Errorf("max relative L2 error %.4f > 2%%", maxRelErr)
	}
}

// TestSQ8_CosineAccuracy verifies CosineFromSQ8 vs exact cosine distance < 2% absolute error.
func TestSQ8_CosineAccuracy(t *testing.T) {
	t.Parallel()
	const dim = 1536
	rng := rand.New(rand.NewSource(17))
	buf := make([]byte, EncodedSize(dim))

	const nPairs = 200
	var maxAbsErr float64

	for range nPairs {
		a := normalizeVec(randomVec(rng, dim, 1.0))
		b := normalizeVec(randomVec(rng, dim, 1.0))

		qa := Encode(a)
		qb := Encode(b)
		encoded := MarshalBinary(qb, buf)

		got := CosineFromSQ8(qa, encoded)
		want := float32(1) - dotProduct(a, b) // exact cosine dist for normalized vecs

		absErr := math.Abs(float64(got - want))
		if absErr > maxAbsErr {
			maxAbsErr = absErr
		}
	}
	if maxAbsErr > 0.02 {
		t.Errorf("max absolute cosine error %.4f > 0.02", maxAbsErr)
	}
}

// TestSQ8_EncodeInto_BufferReuse verifies EncodeInto reuses the Codes slice.
func TestSQ8_EncodeInto_BufferReuse(t *testing.T) {
	t.Parallel()
	const dim = 64
	rng := rand.New(rand.NewSource(5))
	v := randomVec(rng, dim, 1.0)

	var dst Vector
	dst.Codes = make([]int8, dim)
	origPtr := &dst.Codes[0]

	EncodeInto(v, &dst)

	if &dst.Codes[0] != origPtr {
		t.Error("EncodeInto allocated a new slice despite sufficient capacity")
	}
	if len(dst.Codes) != dim {
		t.Errorf("Codes length: got %d, want %d", len(dst.Codes), dim)
	}
}

// TestSQ8_ZeroVector handles the degenerate all-zero vector.
func TestSQ8_ZeroVector(t *testing.T) {
	t.Parallel()
	const dim = 16
	v := make([]float32, dim)
	q := Encode(v)
	if q.Scale != 1.0 {
		t.Errorf("zero vector scale: got %v, want 1.0", q.Scale)
	}
	for i, c := range q.Codes {
		if c != 0 {
			t.Errorf("code[%d] = %d, want 0", i, c)
		}
	}
}

// BenchmarkEncode measures encoding throughput for dim=1536.
func BenchmarkEncode(b *testing.B) {
	const dim = 1536
	rng := rand.New(rand.NewSource(1))
	v := randomVec(rng, dim, 1.0)
	b.SetBytes(int64(dim * 4))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = Encode(v)
	}
}

// BenchmarkEncodeInto measures encoding throughput with buffer reuse.
func BenchmarkEncodeInto(b *testing.B) {
	const dim = 1536
	rng := rand.New(rand.NewSource(1))
	v := randomVec(rng, dim, 1.0)
	var dst Vector
	dst.Codes = make([]int8, dim)
	b.SetBytes(int64(dim * 4))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		EncodeInto(v, &dst)
	}
}

// BenchmarkL2SquaredFromSQ8 measures L2 distance computation from SQ8 bytes.
func BenchmarkL2SquaredFromSQ8(b *testing.B) {
	const dim = 1536
	rng := rand.New(rand.NewSource(2))
	a := randomVec(rng, dim, 1.0)
	bv := randomVec(rng, dim, 1.0)
	qa := Encode(a)
	qb := Encode(bv)
	raw := MarshalBinary(qb, nil)
	b.SetBytes(int64(EncodedSize(dim)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = L2SquaredFromSQ8(qa, raw)
	}
}

// BenchmarkDotInt8Bytes measures raw int8 dot product throughput.
func BenchmarkDotInt8Bytes(b *testing.B) {
	const dim = 1536
	rng := rand.New(rand.NewSource(3))
	qa := Encode(randomVec(rng, dim, 1.0))
	qb := Encode(randomVec(rng, dim, 1.0))
	raw := MarshalBinary(qb, nil)
	codes := raw[headerSize:]
	b.SetBytes(int64(dim))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = DotInt8Bytes(qa.Codes, codes)
	}
}
