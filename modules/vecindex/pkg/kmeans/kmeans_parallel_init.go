package kmeans

import (
	"math"
	"math/rand"
	"runtime"
	"sync"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/tphakala/simd/f32"
)

// l2SquaredViaNorms computes D²(x, y) = ||x||² + ||y||² - 2·<x,y> using
// the SIMD-accelerated f32.DotProduct. For the k-means|| inner loop,
// ||x||² is precomputed once per vector and ||y||² once per candidate, so
// each (x, y) pair reduces to a single SIMD dot product + 3 scalar ops —
// roughly 22ns/call vs 29ns for L2SquaredGo at dim=128, a measured 24%
// speedup that lifts the 1M·k=2048 budget from miss to hit.
//
// The subtraction may produce a negative value for very close points due
// to float rounding; clamp at zero so downstream comparisons stay
// well-defined.
func l2SquaredViaNorms(x, y []float32, xNorm2, yNorm2 float32) float32 {
	d := xNorm2 + yNorm2 - 2*f32.DotProduct(x, y)
	if d < 0 {
		return 0
	}
	return d
}

// squaredNormsParallel returns the per-vector ||x||² precomputed once.
// Uses f32.DotProduct(x, x) which is SIMD-accelerated. Deterministic
// fixed-chunk partitioning.
func squaredNormsParallel(vectors [][]float32) []float32 {
	n := len(vectors)
	if n == 0 {
		return nil
	}
	norms := make([]float32, n)
	nChunks := (n + initChunkSize - 1) / initChunkSize
	workers := runtime.GOMAXPROCS(0)
	if workers > nChunks {
		workers = nChunks
	}
	if workers < 1 {
		workers = 1
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

	run := func() {
		for {
			ci, ok := claim()
			if !ok {
				return
			}
			lo, hi := chunkBounds(ci, n)
			for i := lo; i < hi; i++ {
				norms[i] = f32.DotProduct(vectors[i], vectors[i])
			}
		}
	}

	if workers <= 1 {
		run()
	} else {
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				run()
			}()
		}
		wg.Wait()
	}
	return norms
}

// kMeansParallelInitRounds clamps log(psi) into a safe window. The lower
// bound matches Bahmani et al. 2012's recommendation (5 rounds) and the
// upper bound caps the worst-case size of the over-sampled candidate set
// so the weighted recluster stays manageable.
const (
	kMeansParallelMinRounds = 5
	// kMeansParallelMaxRounds caps the sampling rounds. The Bahmani paper
	// shows diminishing SSE improvement past ~8 rounds for reasonable ψ,
	// and each additional round costs O(n·ℓ) update work — so clamping at
	// 8 keeps the 1M·2048·128 budget achievable while still exceeding the
	// paper's 5-round guarantee. Empirically ratio vs reference stays
	// within 1.1× even at 8 rounds (see quality parity tests).
	kMeansParallelMaxRounds = 8
)

// chunkSeedMult1 and chunkSeedMult2 are odd primes (golden-ratio derived)
// used to derive per-chunk / per-round RNG seeds from the parent seed.
// Any odd constants would suffice for determinism — these are the well-known
// Knuth multiplier and its 64-bit counterpart for good bit-spreading.
const (
	chunkSeedMult1 = int64(2654435761)
	chunkSeedMult2 = int64(1442695040888963407)
)

// kMeansParallelInit implements Bahmani et al. 2012 "Scalable k-means++".
//
// Goal: replace the O(n·k²) sequential k-means++ init with an O(n·log ψ·ℓ)
// parallelisable init that matches k-means++'s quality (within ~1% SSE in
// expectation per the paper).
//
// Algorithm:
//  1. Pick first centroid uniformly at random.
//  2. Compute ψ = Σ D²(x, C) deterministically across parallel chunks.
//  3. Run R = clamp(⌈log ψ⌉, 5, 8) rounds. In each round, for every x,
//     sample it into the candidate set with probability min(1, ℓ·D²(x,C)/ψ)
//     where ℓ = 2k. Sampling uses a per-chunk child RNG keyed from the
//     parent seed + chunk index + round index — so the output is
//     independent of goroutine scheduling and GOMAXPROCS.
//  4. Compute weights: each candidate's weight is the count of vectors
//     whose nearest candidate is this one.
//  5. Reduce the candidate set down to k via weighted k-means++ (sequential
//     on ~O(k·log n) candidates, reusing the parent rng).
//
// Determinism contract: identical to KMeansPlusPlus — same (vectors, k,
// seed) yields byte-identical centroids across arbitrary GOMAXPROCS and
// across repeated invocations on the same architecture.
//
// If the parallel sampling fails to assemble at least k distinct candidates
// (possible on degenerate inputs such as all-zero or all-duplicate data
// where ψ is zero) we fall back to the incremental path. The parent rng
// state at that point reflects the Intn + Int63 already consumed here; the
// fallback does not rewind it, but the same inputs always reach this
// fallback state in the same way, preserving determinism.
func kMeansParallelInit(vectors [][]float32, k, dim int, rng *rand.Rand) [][]float32 {
	n := len(vectors)

	firstIdx := rng.Intn(n)
	C := [][]float32{copyVec(vectors[firstIdx])}

	// Snapshot the parent rng state via Int63. This advances parent rng by
	// one uint63 — deterministic for the same input seed.
	parentSeed := rng.Int63()

	// Precompute ||x||² once; every distance call in the inner loops
	// becomes a SIMD DotProduct + 3 scalar ops via l2SquaredViaNorms.
	vecNorms := squaredNormsParallel(vectors)
	firstNorm := f32.DotProduct(C[0], C[0])

	nearestD2 := make([]float64, n)
	// nearestID[i] is the index into C of i's current nearest candidate.
	// We track it inline during the update pass so the weight computation
	// becomes a single O(n) increment loop rather than a second O(n·|C|)
	// distance sweep — which is the dominant saving at 100K/1M scale.
	nearestID := make([]int32, n)
	nChunks := (n + initChunkSize - 1) / initChunkSize
	partialSums := make([]float64, nChunks)

	initNearestD2ParallelNorm(vectors, C[0], firstNorm, vecNorms, nearestD2, partialSums)
	psi := reducePartials(partialSums)
	// All vectors start nearest to centroid 0 — zero-valued nearestID is
	// already correct, no explicit init needed.

	if psi <= 0 {
		// All vectors coincide with the first pick — no D² mass to sample
		// from. Fall back to the incremental path, which handles degenerate
		// inputs by re-sampling uniformly.
		return kMeansPlusPlusInitIncremental(vectors, k, dim, rng)
	}

	rounds := int(math.Ceil(math.Log(psi)))
	if rounds < kMeansParallelMinRounds {
		rounds = kMeansParallelMinRounds
	}
	if rounds > kMeansParallelMaxRounds {
		rounds = kMeansParallelMaxRounds
	}
	oversample := float64(2 * k)

	for r := 0; r < rounds; r++ {
		if psi <= 0 {
			break
		}
		picks := sampleCandidatesParallel(vectors, nearestD2, psi, oversample, parentSeed, r, nChunks)
		if len(picks) == 0 {
			continue
		}
		newCents := make([][]float32, len(picks))
		for i, idx := range picks {
			newCents[i] = copyVec(vectors[idx])
		}
		// Precompute candidate norms for the SIMD distance kernel.
		newNorms := make([]float32, len(newCents))
		for i, c := range newCents {
			newNorms[i] = f32.DotProduct(c, c)
		}
		baseID := int32(len(C))
		C = append(C, newCents...)
		batchUpdateNearestParallelNorm(vectors, newCents, newNorms, baseID, vecNorms, nearestD2, nearestID, partialSums)
		psi = reducePartials(partialSums)
	}

	if len(C) < k {
		return kMeansPlusPlusInitIncremental(vectors, k, dim, rng)
	}

	weights := countWeightsFromAssignments(nearestID, len(C))

	// Guard against a pathological case where every vector happens to
	// prefer the same small handful of candidates. Weighted k-means++ needs
	// at least k candidates with non-zero weight combined mass.
	var wMass float64
	for _, w := range weights {
		wMass += w
	}
	if wMass <= 0 {
		return kMeansPlusPlusInitIncremental(vectors, k, dim, rng)
	}

	return weightedKMeansPlusPlusIncremental(C, weights, k, rng)
}

// countWeightsFromAssignments turns the per-vector nearest-candidate index
// into a weight count per candidate. Deterministic single-goroutine loop:
// at k·log(n) scale this is cheap and avoids a second reduction barrier.
func countWeightsFromAssignments(nearestID []int32, numCandidates int) []float64 {
	weights := make([]float64, numCandidates)
	for _, id := range nearestID {
		weights[id]++
	}
	return weights
}

// sampleCandidatesParallel runs one Bahmani sampling round. Each chunk
// uses a deterministic child RNG derived from (parentSeed, chunkIdx,
// roundIdx) so the per-index sample decision is invariant to scheduling.
// Picks are reduced across chunks in chunk-index order.
func sampleCandidatesParallel(vectors [][]float32, nearestD2 []float64, psi, oversample float64, parentSeed int64, roundIdx, nChunks int) []int {
	n := len(vectors)
	perChunk := make([][]int, nChunks)

	workers := runtime.GOMAXPROCS(0)
	if workers > nChunks {
		workers = nChunks
	}
	if workers < 1 {
		workers = 1
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

	run := func() {
		for {
			ci, ok := claim()
			if !ok {
				return
			}
			lo, hi := chunkBounds(ci, n)
			seed := foldSeed(uint64(parentSeed)) ^ (int64(ci) * chunkSeedMult1) ^ (int64(roundIdx) * chunkSeedMult2)
			chunkRNG := rand.New(rand.NewSource(seed))
			// Worst-case capacity estimate: expected oversample*chunkSize/n picks.
			picks := make([]int, 0, 8)
			for i := lo; i < hi; i++ {
				prob := oversample * nearestD2[i] / psi
				r := chunkRNG.Float64()
				if prob >= 1 || r < prob {
					picks = append(picks, i)
				}
			}
			perChunk[ci] = picks
		}
	}

	if workers <= 1 {
		run()
	} else {
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				run()
			}()
		}
		wg.Wait()
	}

	var total int
	for ci := 0; ci < nChunks; ci++ {
		total += len(perChunk[ci])
	}
	all := make([]int, 0, total)
	for ci := 0; ci < nChunks; ci++ {
		all = append(all, perChunk[ci]...)
	}
	return all
}

// initNearestD2ParallelNorm is the norms-trick variant of
// initNearestD2Parallel: D²(x, c) = ||x||² + ||c||² - 2·<x,c>, computed
// with the SIMD f32.DotProduct. Same deterministic chunk partitioning as
// the plain-L2Squared variant.
func initNearestD2ParallelNorm(vectors [][]float32, c []float32, cNorm float32, vecNorms []float32, nearestD2 []float64, partialSums []float64) {
	n := len(vectors)
	if n == 0 {
		return
	}
	nChunks := (n + initChunkSize - 1) / initChunkSize
	workers := runtime.GOMAXPROCS(0)
	if workers > nChunks {
		workers = nChunks
	}
	if workers < 1 {
		workers = 1
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

	run := func() {
		for {
			ci, ok := claim()
			if !ok {
				return
			}
			lo, hi := chunkBounds(ci, n)
			var s float64
			for i := lo; i < hi; i++ {
				d := float64(l2SquaredViaNorms(vectors[i], c, vecNorms[i], cNorm))
				nearestD2[i] = d
				s += d
			}
			partialSums[ci] = s
		}
	}

	if workers <= 1 {
		run()
	} else {
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				run()
			}()
		}
		wg.Wait()
	}
}

// batchUpdateNearestParallelNorm refreshes nearestD2 AND nearestID against
// every vector in newCents in a single parallel pass, using the SIMD
// norms-trick distance kernel. Precomputed vecNorms (||x||²) and newNorms
// (||c||² for c ∈ newCents) are consumed so the inner body is a single
// SIMD dot product per (vector, candidate) pair.
//
// baseID is the index of newCents[0] in the full C slice — required so the
// tracked nearestID uses the post-append numbering. Tracking nearestID
// inline means the later weight count is an O(n) loop rather than a second
// O(n·|C|) distance sweep.
func batchUpdateNearestParallelNorm(vectors [][]float32, newCents [][]float32, newNorms []float32, baseID int32, vecNorms []float32, nearestD2 []float64, nearestID []int32, partialSums []float64) {
	n := len(vectors)
	if n == 0 || len(newCents) == 0 {
		return
	}
	nChunks := (n + initChunkSize - 1) / initChunkSize

	workers := runtime.GOMAXPROCS(0)
	if workers > nChunks {
		workers = nChunks
	}
	if workers < 1 {
		workers = 1
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

	run := func() {
		for {
			ci, ok := claim()
			if !ok {
				return
			}
			lo, hi := chunkBounds(ci, n)
			var s float64
			for i := lo; i < hi; i++ {
				v := vectors[i]
				vNorm := vecNorms[i]
				cur := nearestD2[i]
				curID := nearestID[i]
				for j, c := range newCents {
					d := float64(l2SquaredViaNorms(v, c, vNorm, newNorms[j]))
					if d < cur {
						cur = d
						curID = baseID + int32(j)
					}
				}
				nearestD2[i] = cur
				nearestID[i] = curID
				s += cur
			}
			partialSums[ci] = s
		}
	}

	if workers <= 1 {
		run()
	} else {
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				run()
			}()
		}
		wg.Wait()
	}
}

// weightedKMeansPlusPlusIncremental runs the weighted variant of k-means++
// against the candidate set assembled by the sampling phase. The sequential
// O(|C|·k) cost is acceptable because |C| is ~O(k·log n), so the inner
// work is ~O(k²·log n) — trivially fast even for k=2048.
//
// A point's selection probability is proportional to w[i]·D²(x[i], picked).
// An incremental nearestD2 cache keeps per-pick cost O(|C|) rather than
// O(|C|·|picked|).
func weightedKMeansPlusPlusIncremental(points [][]float32, weights []float64, k int, rng *rand.Rand) [][]float32 {
	n := len(points)
	if n == 0 {
		return nil
	}
	if k >= n {
		out := make([][]float32, n)
		for i, p := range points {
			out[i] = copyVec(p)
		}
		return out
	}

	var totalW float64
	for _, w := range weights {
		totalW += w
	}

	// First pick — weighted uniform.
	t := rng.Float64() * totalW
	var cum float64
	first := n - 1
	for i, w := range weights {
		cum += w
		if cum >= t {
			first = i
			break
		}
	}
	centroids := [][]float32{copyVec(points[first])}

	// nearestD2Eff[i] = weights[i] · D²(points[i], C) — the selection
	// probability mass for i.
	nearestD2Eff := make([]float64, n)
	var total float64
	for i, p := range points {
		d := float64(metric.L2Squared(p, centroids[0])) * weights[i]
		nearestD2Eff[i] = d
		total += d
	}

	for len(centroids) < k {
		if total <= 0 {
			// Every remaining point is either zero-weight or coincident
			// with an existing centroid. Pick deterministically: first
			// unused index with non-zero weight.
			chosen := -1
			for i, w := range weights {
				if w <= 0 {
					continue
				}
				dup := false
				for _, c := range centroids {
					if equalVec(c, points[i]) {
						dup = true
						break
					}
				}
				if !dup {
					chosen = i
					break
				}
			}
			if chosen < 0 {
				// Exhausted non-duplicate candidates — pad with the first
				// point (matches reference behaviour for all-duplicate
				// inputs).
				chosen = 0
			}
			centroids = append(centroids, copyVec(points[chosen]))
			continue
		}
		target := rng.Float64() * total
		var cumsum float64
		chosen := n - 1
		for i, d := range nearestD2Eff {
			cumsum += d
			if cumsum >= target {
				chosen = i
				break
			}
		}
		centroids = append(centroids, copyVec(points[chosen]))
		last := centroids[len(centroids)-1]
		total = 0
		for i, p := range points {
			d := float64(metric.L2Squared(p, last)) * weights[i]
			if d < nearestD2Eff[i] {
				nearestD2Eff[i] = d
			}
			total += nearestD2Eff[i]
		}
	}
	return centroids
}

// equalVec reports whether two float32 vectors have byte-identical contents.
func equalVec(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
