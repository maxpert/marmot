package kmeans

import (
	"math/rand"
	"runtime"
	"sync"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// initChunkSize is the fixed partition size used for all deterministic
// parallel passes in the init code paths. A fixed constant (rather than
// n/GOMAXPROCS) is required so the same input produces the same chunk
// boundaries regardless of the host's CPU count — which in turn is required
// for bit-identical output across peers with different GOMAXPROCS.
const initChunkSize = 1024

// kMeansPlusPlusInitIncremental runs the classic D²-weighted k-means++
// selection but with an O(n·k) inner loop: a persistent nearestD2 slice is
// updated in-place as each new centroid is picked, so the cost per pick is
// a single pass over the data rather than a k-way scan of all prior picks.
//
// Byte-identical contract: produces the same centroids as the O(n·k²)
// reference (kMeansPlusPlusInitReference) for any fixed (vectors, k, seed).
// The parent rng consumes Intn and Float64 in the same order and same
// counts as the reference, preserving the global determinism contract.
//
// The per-pick distance update is parallelised across deterministic chunks;
// the reduction for the running total is ordered by chunk index so the
// final total is the same regardless of scheduling.
func kMeansPlusPlusInitIncremental(vectors [][]float32, k, _ int, rng *rand.Rand) [][]float32 {
	n := len(vectors)
	centroids := make([][]float32, 0, k)

	first := copyVec(vectors[rng.Intn(n)])
	centroids = append(centroids, first)

	nearestD2 := make([]float64, n)
	nChunks := (n + initChunkSize - 1) / initChunkSize
	partialSums := make([]float64, nChunks)

	// Initial pass: nearestD2[i] = D²(vectors[i], centroids[0]), while
	// summing per-chunk partials in fixed slot order.
	initNearestD2Parallel(vectors, centroids[0], nearestD2, partialSums)
	total := reducePartials(partialSums)

	for len(centroids) < k {
		target := rng.Float64() * total
		chosen := samplePrefix(nearestD2, target)
		centroids = append(centroids, copyVec(vectors[chosen]))
		// Update nearestD2 with the newly chosen centroid and refresh the
		// global total deterministically.
		updateNearestD2Parallel(vectors, centroids[len(centroids)-1], nearestD2, partialSums)
		total = reducePartials(partialSums)
	}

	return centroids
}

// samplePrefix returns the smallest index i such that the cumulative sum
// of nearestD2 up through i is >= target. Matches the behaviour of the
// reference implementation, including the "chosen=n-1 if target overshoots"
// fallback for float rounding.
func samplePrefix(nearestD2 []float64, target float64) int {
	var cumulative float64
	n := len(nearestD2)
	for i, d := range nearestD2 {
		cumulative += d
		if cumulative >= target {
			return i
		}
	}
	return n - 1
}

// initNearestD2Parallel populates nearestD2[i] with D²(vectors[i], c) and
// fills partialSums[chunkIdx] with the chunk's fixed-order sum. Workers
// write only to disjoint slices of each output buffer; there is no shared
// accumulator.
func initNearestD2Parallel(vectors [][]float32, c []float32, nearestD2 []float64, partialSums []float64) {
	n := len(vectors)
	if n == 0 {
		return
	}
	nChunks := (n + initChunkSize - 1) / initChunkSize

	workers := runtime.GOMAXPROCS(0)
	if workers > nChunks {
		workers = nChunks
	}
	if workers <= 1 {
		for ci := 0; ci < nChunks; ci++ {
			lo, hi := chunkBounds(ci, n)
			var s float64
			for i := lo; i < hi; i++ {
				d := float64(metric.L2Squared(vectors[i], c))
				nearestD2[i] = d
				s += d
			}
			partialSums[ci] = s
		}
		return
	}

	var next int
	var mu sync.Mutex
	claim := func() (int, bool) {
		mu.Lock()
		defer mu.Unlock()
		if next >= nChunks {
			return 0, false
		}
		idx := next
		next++
		return idx, true
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ci, ok := claim()
				if !ok {
					return
				}
				lo, hi := chunkBounds(ci, n)
				var s float64
				for i := lo; i < hi; i++ {
					d := float64(metric.L2Squared(vectors[i], c))
					nearestD2[i] = d
					s += d
				}
				partialSums[ci] = s
			}
		}()
	}
	wg.Wait()
}

// updateNearestD2Parallel refreshes nearestD2[i] = min(nearestD2[i], D²(x, c))
// and rewrites partialSums[chunkIdx] to the new per-chunk sum. Same
// determinism guarantees as initNearestD2Parallel.
func updateNearestD2Parallel(vectors [][]float32, c []float32, nearestD2 []float64, partialSums []float64) {
	n := len(vectors)
	if n == 0 {
		return
	}
	nChunks := (n + initChunkSize - 1) / initChunkSize

	workers := runtime.GOMAXPROCS(0)
	if workers > nChunks {
		workers = nChunks
	}
	if workers <= 1 {
		for ci := 0; ci < nChunks; ci++ {
			lo, hi := chunkBounds(ci, n)
			var s float64
			for i := lo; i < hi; i++ {
				d := float64(metric.L2Squared(vectors[i], c))
				if d < nearestD2[i] {
					nearestD2[i] = d
				}
				s += nearestD2[i]
			}
			partialSums[ci] = s
		}
		return
	}

	var next int
	var mu sync.Mutex
	claim := func() (int, bool) {
		mu.Lock()
		defer mu.Unlock()
		if next >= nChunks {
			return 0, false
		}
		idx := next
		next++
		return idx, true
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ci, ok := claim()
				if !ok {
					return
				}
				lo, hi := chunkBounds(ci, n)
				var s float64
				for i := lo; i < hi; i++ {
					d := float64(metric.L2Squared(vectors[i], c))
					if d < nearestD2[i] {
						nearestD2[i] = d
					}
					s += nearestD2[i]
				}
				partialSums[ci] = s
			}
		}()
	}
	wg.Wait()
}

// reducePartials sums partialSums in fixed index order. Float64 addition is
// not associative, so the reduction order is part of the determinism
// contract — it must never depend on goroutine scheduling.
func reducePartials(partialSums []float64) float64 {
	var total float64
	for _, s := range partialSums {
		total += s
	}
	return total
}

// chunkBounds returns [lo, hi) for chunk index ci of n items partitioned
// with fixed chunk size initChunkSize.
func chunkBounds(ci, n int) (int, int) {
	lo := ci * initChunkSize
	hi := lo + initChunkSize
	if hi > n {
		hi = n
	}
	return lo, hi
}
