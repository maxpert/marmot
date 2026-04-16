package db

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// sampleReservoir drives reservoirSlot over an integer stream of size n and
// returns the final reservoir contents. Pure helper for tests — exercises
// the same append/overwrite logic as computeCentroids but without SQLite.
func sampleReservoir(t *testing.T, n, capacity int, seed int64) []int {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	out := make([]int, 0, capacity)
	for i := 1; i <= n; i++ {
		slot := reservoirSlot(i, capacity, rng)
		if slot < 0 {
			continue
		}
		if slot < len(out) {
			out[slot] = i
		} else {
			out = append(out, i)
		}
	}
	return out
}

// TestReservoirSlot_DeterministicGivenSeed is the core property we rely on
// for concurrent-CREATE convergence: two independent runs with the same seed
// and input length produce element-by-element identical reservoirs.
func TestReservoirSlot_DeterministicGivenSeed(t *testing.T) {
	t.Parallel()

	const n = 10_000
	const capacity = 128
	const seed int64 = 0xDEADBEEF

	a := sampleReservoir(t, n, capacity, seed)
	b := sampleReservoir(t, n, capacity, seed)

	require.Equal(t, a, b, "same seed must produce byte-identical reservoirs")
	require.Len(t, a, capacity)
}

// TestReservoirSlot_PassthroughWhenCapacityGTE_N locks the append-in-order
// path: when capacity >= n we keep every item in input order.
func TestReservoirSlot_PassthroughWhenCapacityGTE_N(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		n, capacity int
	}{
		{"exact_fit", 50, 50},
		{"capacity_larger", 10, 100},
		{"single_item", 1, 64},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := sampleReservoir(t, tc.n, tc.capacity, 42)
			require.Len(t, got, tc.n)
			for i := 0; i < tc.n; i++ {
				require.Equal(t, i+1, got[i],
					"passthrough must preserve input order at position %d", i)
			}
		})
	}
}

// TestReservoirSlot_DifferentSeedsDifferentSamples sanity-checks that we
// actually use the seed — a flat bug (ignore seed, always pick the same
// slots) would still produce deterministic output but every seed would
// agree.
func TestReservoirSlot_DifferentSeedsDifferentSamples(t *testing.T) {
	t.Parallel()

	const n = 5_000
	const capacity = 32

	a := sampleReservoir(t, n, capacity, 1)
	b := sampleReservoir(t, n, capacity, 2)

	require.NotEqual(t, a, b,
		"different seeds must produce different reservoirs on a reasonable stream")
}

// TestReservoirSlot_UniformInclusion runs the sampler across many seeds and
// asserts every input index is selected with empirical frequency close to
// capacity/n. This is the classic correctness check for Algorithm-R: each
// item has equal selection probability regardless of position in the stream.
//
// With n=500, capacity=50, trials=5000 the expected per-item count is 500.
// Binomial std-dev ≈ sqrt(5000 * 0.1 * 0.9) ≈ 21.2 → 5σ band ≈ ±106. We
// allow ±25% (±125) which is well above 5σ and keeps the test flake-free.
func TestReservoirSlot_UniformInclusion(t *testing.T) {
	t.Parallel()

	const (
		n        = 500
		capacity = 50
		trials   = 5_000
	)

	counts := make([]int, n+1) // 1-indexed
	for seed := int64(1); seed <= trials; seed++ {
		sample := sampleReservoir(t, n, capacity, seed)
		for _, v := range sample {
			counts[v]++
		}
	}

	expected := float64(trials) * float64(capacity) / float64(n)
	tol := 0.25 * expected
	for i := 1; i <= n; i++ {
		diff := math.Abs(float64(counts[i]) - expected)
		require.LessOrEqualf(t, diff, tol,
			"item %d selected %d times over %d trials, expected ~%.0f (tol ±%.0f)",
			i, counts[i], trials, expected, tol)
	}
}

// TestReservoirSlot_ZeroCapacity guards the degenerate path — a reservoir of
// zero capacity rejects every item without consuming the PRNG, so the caller
// cannot accidentally shift downstream draws by routing a stream through a
// dead reservoir.
func TestReservoirSlot_ZeroCapacity(t *testing.T) {
	t.Parallel()

	rngUsed := rand.New(rand.NewSource(11))
	for i := 1; i <= 100; i++ {
		require.Equal(t, -1, reservoirSlot(i, 0, rngUsed),
			"capacity=0 must reject every item")
	}
	// After 100 capacity=0 calls rngUsed must still match a fresh rng seeded
	// identically — capacity=0 short-circuits before any PRNG draw.
	rngFresh := rand.New(rand.NewSource(11))
	for i := 0; i < 5; i++ {
		require.Equal(t, rngFresh.Int63(), rngUsed.Int63(),
			"capacity=0 must not consume the PRNG (divergence at draw %d)", i)
	}
}

// TestReservoirSlot_InvalidArgsPanic pins the fail-fast contract for caller
// bugs. Panicking is the right response — silently returning -1 on bad args
// would mask integration mistakes.
func TestReservoirSlot_InvalidArgsPanic(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1))
	require.PanicsWithValue(t,
		"reservoirSlot: seen must be >= 1",
		func() { reservoirSlot(0, 10, rng) })
	require.PanicsWithValue(t,
		"reservoirSlot: capacity must be >= 0",
		func() { reservoirSlot(1, -1, rng) })
}
