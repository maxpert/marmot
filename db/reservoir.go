package db

import "math/rand"

// reservoirSlot implements Algorithm-R (Vitter, 1985) in a streaming form: it
// returns the destination slot for the seen-th (1-indexed) item when filling
// a reservoir of the given capacity, or -1 when the item should be rejected.
//
// The contract:
//   - For the first capacity items the return value equals seen-1, so the
//     caller deterministically appends them in input order.
//   - Beyond capacity the function rolls rng.Intn(seen): if the roll lands in
//     [0, capacity) that becomes the overwrite slot; otherwise the caller
//     rejects the item (returns -1).
//
// One random draw per non-initial item — no work is done for appends. Every
// draw advances the PRNG identically on every node given the same seed, which
// is how two nodes running concurrent CREATE VECTOR INDEX on byte-identical
// data select identical training samples (design §8.1 convergence).
//
// Panics on capacity < 0 or seen < 1 (caller bug).
func reservoirSlot(seen, capacity int, rng *rand.Rand) int {
	if seen < 1 {
		panic("reservoirSlot: seen must be >= 1")
	}
	if capacity < 0 {
		panic("reservoirSlot: capacity must be >= 0")
	}
	if capacity == 0 {
		return -1
	}
	if seen <= capacity {
		return seen - 1
	}
	j := rng.Intn(seen)
	if j < capacity {
		return j
	}
	return -1
}
