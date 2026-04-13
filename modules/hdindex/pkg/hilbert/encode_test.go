package hilbert

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
)

// hilbertIndex extracts the integer Hilbert index from the key for small cases.
// totalBits = numDims * order must fit in an int.
func hilbertIndex(key []byte, totalBits int) int {
	result := 0
	for i := 0; i < totalBits; i++ {
		byteIdx := i / 8
		bitIdx := 7 - (i % 8)
		if (key[byteIdx]>>uint(bitIdx))&1 == 1 {
			result |= 1 << uint(totalBits-1-i)
		}
	}
	return result
}

// The canonical 2D order=2 Hilbert curve sequence (x, y) -> index.
// Derived from the standard U-shaped Hilbert curve on a 4x4 grid.
var known2DOrder2 = []struct {
	x, y, h int
}{
	{0, 0, 0}, {1, 0, 1}, {1, 1, 2}, {0, 1, 3},
	{0, 2, 4}, {0, 3, 5}, {1, 3, 6}, {1, 2, 7},
	{2, 2, 8}, {2, 3, 9}, {3, 3, 10}, {3, 2, 11},
	{3, 1, 12}, {2, 1, 13}, {2, 0, 14}, {3, 0, 15},
}

func TestEncode_2D_Order2(t *testing.T) {
	t.Parallel()
	for _, tc := range known2DOrder2 {
		key := Encode([]uint32{uint32(tc.x), uint32(tc.y)}, 2)
		if len(key) != 1 {
			t.Fatalf("(%d,%d): expected 1 byte key, got %d", tc.x, tc.y, len(key))
		}
		got := hilbertIndex(key, 4)
		if got != tc.h {
			t.Errorf("(%d,%d): expected Hilbert index %d, got %d", tc.x, tc.y, tc.h, got)
		}
	}
}

func TestEncode_2D_Order3(t *testing.T) {
	t.Parallel()

	// Spot-check known points on the 8x8 Hilbert curve.
	// Actual values confirmed against the implementation's canonical ordering.
	checks := []struct {
		x, y, h int
	}{
		{0, 0, 0},
		{0, 7, 21},
		{7, 7, 42},
		{7, 0, 63},
	}

	for _, tc := range checks {
		key := Encode([]uint32{uint32(tc.x), uint32(tc.y)}, 3)
		got := hilbertIndex(key, 6)
		if got != tc.h {
			t.Errorf("2D order=3 (%d,%d): expected %d, got %d", tc.x, tc.y, tc.h, got)
		}
	}

	// Verify all 64 points produce unique indices in [0,63].
	seen := make(map[int]bool, 64)
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			key := Encode([]uint32{uint32(x), uint32(y)}, 3)
			idx := hilbertIndex(key, 6)
			if seen[idx] {
				t.Errorf("duplicate Hilbert index %d for (%d,%d)", idx, x, y)
			}
			if idx < 0 || idx >= 64 {
				t.Errorf("out-of-range Hilbert index %d for (%d,%d)", idx, x, y)
			}
			seen[idx] = true
		}
	}
	if len(seen) != 64 {
		t.Errorf("expected 64 unique indices, got %d", len(seen))
	}
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	t.Parallel()

	type testCase struct {
		numDims int
		order   int
	}
	cases := []testCase{
		{2, 4},
		{2, 8},
		{3, 4},
		{3, 8},
		{4, 4},
		{4, 8},
		{8, 4},
		{8, 8},
		{16, 4},
		{16, 8},
	}

	const iters = 200

	for idx, tc := range cases {
		tc := tc
		seed := int64(42 + idx)
		t.Run("", func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewSource(seed))
			maxCoord := uint32((1 << tc.order) - 1)
			for i := 0; i < iters; i++ {
				coords := make([]uint32, tc.numDims)
				for d := range coords {
					coords[d] = rng.Uint32() & maxCoord
				}
				key := Encode(coords, tc.order)
				expectedLen := (tc.numDims*tc.order + 7) / 8
				if len(key) != expectedLen {
					t.Errorf("dims=%d order=%d: key length %d != expected %d",
						tc.numDims, tc.order, len(key), expectedLen)
				}
				decoded := Decode(key, tc.numDims, tc.order)
				if len(decoded) != tc.numDims {
					t.Fatalf("dims=%d order=%d: decoded length %d != %d",
						tc.numDims, tc.order, len(decoded), tc.numDims)
				}
				for d := range coords {
					if decoded[d] != coords[d] {
						t.Errorf("dims=%d order=%d iter=%d dim=%d: decoded %d != original %d",
							tc.numDims, tc.order, i, d, decoded[d], coords[d])
						break
					}
				}
			}
		})
	}
}

func TestEncodeLocality(t *testing.T) {
	t.Parallel()

	const (
		numDims = 4
		order   = 8
		iters   = 2000
	)
	maxCoord := uint32((1 << order) - 1)
	rng := rand.New(rand.NewSource(99))

	l2 := func(a, b []uint32) float64 {
		sum := 0.0
		for i := range a {
			d := float64(a[i]) - float64(b[i])
			sum += d * d
		}
		return math.Sqrt(sum)
	}

	keyDiff := func(a, b []byte) float64 {
		// Treat keys as big-endian integers and compute absolute difference.
		// For this statistical test we use the numeric value of the byte slice.
		var va, vb float64
		for i, by := range a {
			va += float64(by) * math.Pow(256, float64(len(a)-1-i))
		}
		for i, by := range b {
			vb += float64(by) * math.Pow(256, float64(len(b)-1-i))
		}
		if va > vb {
			return va - vb
		}
		return vb - va
	}

	var (
		nearL2Sum, nearKeySum float64
		farL2Sum, farKeySum   float64
	)
	threshold := float64(maxCoord) * 0.1 // 10% of max range

	for i := 0; i < iters; i++ {
		// Generate base point.
		base := make([]uint32, numDims)
		for d := range base {
			base[d] = rng.Uint32() & maxCoord
		}

		// Near neighbor: perturb by small amount.
		near := make([]uint32, numDims)
		for d := range near {
			delta := rng.Int31n(int32(maxCoord)/20 + 1)
			v := int32(base[d]) + delta - int32(maxCoord)/40
			if v < 0 {
				v = 0
			} else if uint32(v) > maxCoord {
				v = int32(maxCoord)
			}
			near[d] = uint32(v)
		}

		// Far neighbor: ensure L2 > threshold.
		far := make([]uint32, numDims)
		for l2(base, far) < threshold {
			for d := range far {
				far[d] = rng.Uint32() & maxCoord
			}
		}

		kBase := Encode(base, order)
		kNear := Encode(near, order)
		kFar := Encode(far, order)

		nearL2Sum += l2(base, near)
		nearKeySum += keyDiff(kBase, kNear)
		farL2Sum += l2(base, far)
		farKeySum += keyDiff(kBase, kFar)
	}

	avgNearKeyDiff := nearKeySum / float64(iters)
	avgFarKeyDiff := farKeySum / float64(iters)

	if avgNearKeyDiff >= avgFarKeyDiff {
		t.Errorf("locality violation: avg key diff for nearby points (%.2f) >= far points (%.2f)",
			avgNearKeyDiff, avgFarKeyDiff)
	}
}

func TestEncodeOrdering(t *testing.T) {
	t.Parallel()

	// For 2D order=2, the Hilbert path must visit all 16 points such that
	// consecutive points differ in exactly one coordinate by exactly 1.
	byIndex := make([][2]int, 16)
	for _, tc := range known2DOrder2 {
		byIndex[tc.h] = [2]int{tc.x, tc.y}
	}

	for i := 1; i < 16; i++ {
		prev := byIndex[i-1]
		curr := byIndex[i]
		dx := abs(curr[0] - prev[0])
		dy := abs(curr[1] - prev[1])
		if dx+dy != 1 {
			t.Errorf("step %d->%d: (%d,%d)->(%d,%d) is not a unit step (dx=%d, dy=%d)",
				i-1, i, prev[0], prev[1], curr[0], curr[1], dx, dy)
		}
	}
}

func TestEncode_SingleDim(t *testing.T) {
	t.Parallel()

	// 1D Hilbert curve is trivially the identity: index = coordinate.
	for order := 1; order <= 8; order++ {
		maxCoord := (1 << order) - 1
		for v := 0; v <= maxCoord; v++ {
			key := Encode([]uint32{uint32(v)}, order)
			expectedLen := (order + 7) / 8
			if len(key) != expectedLen {
				t.Errorf("order=%d v=%d: key length %d != %d", order, v, len(key), expectedLen)
			}
			// Decode must recover the original value.
			decoded := Decode(key, 1, order)
			if decoded[0] != uint32(v) {
				t.Errorf("order=%d v=%d: decoded %d != original", order, v, decoded[0])
			}
		}
	}
}

func TestEncode_HighDim(t *testing.T) {
	t.Parallel()

	const (
		numDims = 96
		order   = 8
		iters   = 50
	)
	maxCoord := uint32((1 << order) - 1)
	rng := rand.New(rand.NewSource(7777))
	expectedLen := (numDims*order + 7) / 8 // 96 bytes

	for i := 0; i < iters; i++ {
		coords := make([]uint32, numDims)
		for d := range coords {
			coords[d] = rng.Uint32() & maxCoord
		}
		key := Encode(coords, order)
		if len(key) != expectedLen {
			t.Fatalf("iter=%d: key length %d != expected %d", i, len(key), expectedLen)
		}
		decoded := Decode(key, numDims, order)
		for d := range coords {
			if decoded[d] != coords[d] {
				t.Errorf("iter=%d dim=%d: decoded %d != original %d", i, d, decoded[d], coords[d])
				break
			}
		}

		// Verify key is ordered relative to itself.
		key2 := Encode(coords, order)
		if !bytes.Equal(key, key2) {
			t.Error("encode is not deterministic")
		}
	}
}

func BenchmarkEncode_96dim_order8(b *testing.B) {
	const (
		numDims = 96
		order   = 8
	)
	maxCoord := uint32((1 << order) - 1)
	rng := rand.New(rand.NewSource(12345))

	coords := make([]uint32, numDims)
	for d := range coords {
		coords[d] = rng.Uint32() & maxCoord
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Encode(coords, order)
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
