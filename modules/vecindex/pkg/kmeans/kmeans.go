package kmeans

import (
	"errors"
	"math/rand"
	"runtime"
	"sort"
	"sync"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

const convergenceThreshold = 1e-4

// foldSeed maps any uint64 seed into a non-negative int64 suitable for
// rand.NewSource, guaranteeing that the SAME uint64 input always produces the
// SAME int64 output on every node. Distinct uint64 values may collide with
// probability ~1/2^63, which is acceptable because the only contract is
// same-in → same-out, not collision-freedom.
//
// Folding strategy: mask off the sign bit, then XOR with the value shifted
// right by 1 so that seeds that differ only in the high bit still produce
// distinct outputs in almost all practical cases.
func foldSeed(s uint64) int64 {
	return int64(s&0x7FFFFFFFFFFFFFFF) ^ int64(s>>1)
}

// KMeansPlusPlus runs k-means++ initialisation followed by Lloyd's algorithm.
// Returns k centroids or an error if the inputs are invalid.
// The algorithm is deterministic: same (vectors, k, seed) always produces the
// same byte-identical centroid output. maxIter must be >= 1.
func KMeansPlusPlus(vectors [][]float32, k int, seed uint64, maxIter int) ([][]float32, error) {
	if err := validateInputs(vectors, k, maxIter); err != nil {
		return nil, err
	}

	dim := len(vectors[0])
	rng := rand.New(rand.NewSource(foldSeed(seed)))

	centroids := kMeansPlusPlusInit(vectors, k, dim, rng)
	centroids = lloydIterations(vectors, centroids, k, dim, maxIter, rng)

	return centroids, nil
}

// Assign returns the index and distance of the nearest centroid for vec.
// Returns an error if centroids is empty or dimensions are mismatched.
func Assign(vec []float32, centroids [][]float32, m metric.Metric) (clusterID uint32, dist float32, err error) {
	if len(centroids) == 0 {
		return 0, 0, errors.New("kmeans: centroids must not be empty")
	}
	if len(vec) != len(centroids[0]) {
		return 0, 0, errors.New("kmeans: dimension mismatch between vec and centroids")
	}

	bestID := uint32(0)
	bestDist := metric.Distance(m, vec, centroids[0])

	for i := 1; i < len(centroids); i++ {
		d := metric.Distance(m, vec, centroids[i])
		if d < bestDist {
			bestDist = d
			bestID = uint32(i)
		}
	}

	return bestID, bestDist, nil
}

// AssignTopN returns the n nearest centroids sorted by ascending distance.
// If n >= len(centroids), all centroids are returned sorted.
// If n == 0, an empty result is returned.
// Returns an error if centroids is empty or dimensions are mismatched.
func AssignTopN(vec []float32, centroids [][]float32, n int, m metric.Metric) (ids []uint32, dists []float32, err error) {
	if n == 0 {
		return []uint32{}, []float32{}, nil
	}
	if len(centroids) == 0 {
		return nil, nil, errors.New("kmeans: centroids must not be empty")
	}
	if len(vec) != len(centroids[0]) {
		return nil, nil, errors.New("kmeans: dimension mismatch between vec and centroids")
	}

	type entry struct {
		id   uint32
		dist float32
	}
	entries := make([]entry, len(centroids))
	for i := range centroids {
		entries[i] = entry{uint32(i), metric.Distance(m, vec, centroids[i])}
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].dist != entries[j].dist {
			return entries[i].dist < entries[j].dist
		}
		return entries[i].id < entries[j].id
	})

	count := n
	if count > len(entries) {
		count = len(entries)
	}

	ids = make([]uint32, count)
	dists = make([]float32, count)
	for i := 0; i < count; i++ {
		ids[i] = entries[i].id
		dists[i] = entries[i].dist
	}

	return ids, dists, nil
}

// validateInputs checks that vectors, k, and maxIter are valid for k-means.
func validateInputs(vectors [][]float32, k int, maxIter int) error {
	if len(vectors) == 0 {
		return errors.New("kmeans: vectors must not be empty")
	}
	if k <= 0 {
		return errors.New("kmeans: k must be >= 1")
	}
	if k > len(vectors) {
		return errors.New("kmeans: k must not exceed the number of vectors")
	}
	if maxIter < 1 {
		return errors.New("kmeans: maxIter must be >= 1")
	}

	dim := len(vectors[0])
	for i, v := range vectors {
		if len(v) != dim {
			return errors.New("kmeans: all vectors must have the same dimension (mismatch at index " + itoa(i) + ")")
		}
	}

	return nil
}

// kMeansPlusPlusInit selects k initial centroids using the k-means++ algorithm.
func kMeansPlusPlusInit(vectors [][]float32, k, dim int, rng *rand.Rand) [][]float32 {
	n := len(vectors)
	centroids := make([][]float32, 0, k)

	// Choose first centroid uniformly at random.
	first := copyVec(vectors[rng.Intn(n)])
	centroids = append(centroids, first)

	dists := make([]float64, n)

	for len(centroids) < k {
		// Compute D^2 weights.
		var total float64
		for i, v := range vectors {
			minD := float64(metric.L2Squared(v, centroids[0]))
			for _, c := range centroids[1:] {
				if d := float64(metric.L2Squared(v, c)); d < minD {
					minD = d
				}
			}
			dists[i] = minD
			total += minD
		}

		// Sample next centroid proportional to D^2.
		target := rng.Float64() * total
		var cumulative float64
		chosen := n - 1
		for i, d := range dists {
			cumulative += d
			if cumulative >= target {
				chosen = i
				break
			}
		}
		centroids = append(centroids, copyVec(vectors[chosen]))
	}

	return centroids
}

// lloydIterations runs Lloyd's algorithm for at most maxIter iterations.
func lloydIterations(vectors [][]float32, centroids [][]float32, k, dim, maxIter int, rng *rand.Rand) [][]float32 {
	n := len(vectors)
	assignments := make([]int, n)

	nWorkers := runtime.GOMAXPROCS(0)
	chunkSize := (n + nWorkers - 1) / nWorkers

	for iter := 0; iter < maxIter; iter++ {
		// Assignment step (parallelised).
		var wg sync.WaitGroup
		for w := 0; w < nWorkers; w++ {
			start := w * chunkSize
			if start >= n {
				break
			}
			end := start + chunkSize
			if end > n {
				end = n
			}
			wg.Add(1)
			go func(lo, hi int) {
				defer wg.Done()
				for i := lo; i < hi; i++ {
					best := 0
					bestD := metric.L2Squared(vectors[i], centroids[0])
					for j := 1; j < k; j++ {
						if d := metric.L2Squared(vectors[i], centroids[j]); d < bestD {
							bestD = d
							best = j
						}
					}
					assignments[i] = best
				}
			}(start, end)
		}
		wg.Wait()

		// Update step: compute new centroids.
		sums := make([][]float64, k)
		counts := make([]int, k)
		for j := range sums {
			sums[j] = make([]float64, dim)
		}
		for i, v := range vectors {
			j := assignments[i]
			counts[j]++
			for d := range v {
				sums[j][d] += float64(v[d])
			}
		}

		newCentroids := make([][]float32, k)
		for j := 0; j < k; j++ {
			newCentroids[j] = make([]float32, dim)
			if counts[j] == 0 {
				// Empty cluster: reinitialise to a random input vector.
				copy(newCentroids[j], vectors[rng.Intn(n)])
			} else {
				inv := 1.0 / float64(counts[j])
				for d := range newCentroids[j] {
					newCentroids[j][d] = float32(sums[j][d] * inv)
				}
			}
		}

		// Check convergence: max centroid shift.
		converged := true
		for j := 0; j < k; j++ {
			var shift float32
			for d := range newCentroids[j] {
				diff := newCentroids[j][d] - centroids[j][d]
				shift += diff * diff
			}
			if shift >= convergenceThreshold*convergenceThreshold {
				converged = false
				break
			}
		}

		centroids = newCentroids
		if converged {
			break
		}
	}

	return centroids
}

// copyVec returns a deep copy of v.
func copyVec(v []float32) []float32 {
	cp := make([]float32, len(v))
	copy(cp, v)
	return cp
}

// itoa converts a non-negative int to a decimal string without fmt dependency.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 10)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}
