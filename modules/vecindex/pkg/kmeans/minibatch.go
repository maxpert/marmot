package kmeans

import (
	"errors"
	"math"
	"math/rand"
	"runtime"
	"slices"
	"sync"
)

const (
	DefaultMiniBatchSize         = 2048
	DefaultMiniBatchMaxIter      = 6
	DefaultMiniBatchInitFactor   = 4
	DefaultBalancePenalty        = 0.25
	DefaultHardClusterFactor     = 2
	DefaultUnderfilledFraction   = 0.25
	DefaultConvergenceEpsilon    = 1e-3
	miniBatchDistanceTileEntries = 1 << 20
	miniBatchParallelChunkSize   = 64
	maxRepairCandidatesPerBucket = 4
)

type MiniBatchBalancedOptions struct {
	BatchSize           int
	MaxIter             int
	TargetClusterSize   int
	BalancePenalty      float32
	HardClusterFactor   int
	UnderfilledFraction float32
	ConvergenceEpsilon  float32
}

type MiniBatchPassResult struct {
	Converged bool
	Repaired  bool
	MaxShift  float32
}

type MiniBatchBalancedTrainer struct {
	centroids [][]float32
	counts    []int64
	opts      MiniBatchBalancedOptions

	passSums       [][]float64
	passCounts     []int64
	passRawCounts  []int64
	passCandidates [][]repairCandidate
	inPass         bool
}

type repairCandidate struct {
	vec  []float32
	dist float32
}

type clusterBucket struct {
	clusterID int
	count     int64
}

func normalizeMiniBatchOptions(opts MiniBatchBalancedOptions) MiniBatchBalancedOptions {
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultMiniBatchSize
	}
	if opts.MaxIter <= 0 {
		opts.MaxIter = DefaultMiniBatchMaxIter
	}
	if opts.BalancePenalty <= 0 {
		opts.BalancePenalty = DefaultBalancePenalty
	}
	if opts.HardClusterFactor <= 0 {
		opts.HardClusterFactor = DefaultHardClusterFactor
	}
	if opts.UnderfilledFraction <= 0 {
		opts.UnderfilledFraction = DefaultUnderfilledFraction
	}
	if opts.ConvergenceEpsilon <= 0 {
		opts.ConvergenceEpsilon = DefaultConvergenceEpsilon
	}
	return opts
}

func NewMiniBatchBalancedTrainer(initCentroids [][]float32, opts MiniBatchBalancedOptions) (*MiniBatchBalancedTrainer, error) {
	if len(initCentroids) == 0 {
		return nil, errors.New("kmeans: initCentroids must not be empty")
	}
	dim := len(initCentroids[0])
	if dim == 0 {
		return nil, errors.New("kmeans: initCentroids must not be empty")
	}
	centroids := make([][]float32, len(initCentroids))
	counts := make([]int64, len(initCentroids))
	for i, centroid := range initCentroids {
		if len(centroid) != dim {
			return nil, errors.New("kmeans: init centroid dim mismatch at index " + itoa(i))
		}
		cp := make([]float32, dim)
		copy(cp, centroid)
		centroids[i] = cp
	}
	opts = normalizeMiniBatchOptions(opts)
	return &MiniBatchBalancedTrainer{
		centroids:      centroids,
		counts:         counts,
		opts:           opts,
		passSums:       make([][]float64, len(initCentroids)),
		passCounts:     make([]int64, len(initCentroids)),
		passRawCounts:  make([]int64, len(initCentroids)),
		passCandidates: make([][]repairCandidate, len(initCentroids)),
	}, nil
}

func (t *MiniBatchBalancedTrainer) BeginPass() error {
	if t == nil {
		return errors.New("kmeans: trainer is nil")
	}
	for i := range t.passSums {
		t.passSums[i] = nil
		t.passCounts[i] = 0
		t.passRawCounts[i] = 0
		t.passCandidates[i] = t.passCandidates[i][:0]
	}
	t.inPass = true
	return nil
}

func (t *MiniBatchBalancedTrainer) ObserveBatch(vectors [][]float32) error {
	if t == nil {
		return errors.New("kmeans: trainer is nil")
	}
	if !t.inPass {
		return errors.New("kmeans: BeginPass must be called before ObserveBatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	dim := len(t.centroids[0])
	k := len(t.centroids)
	for _, vec := range vectors {
		if len(vec) != dim {
			return errors.New("kmeans: vector dim mismatch in mini-batch")
		}
	}
	tileRows := len(vectors)
	maxRows := miniBatchDistanceTileEntries / k
	if maxRows < 1 {
		maxRows = 1
	}
	if tileRows > maxRows {
		tileRows = maxRows
	}
	distances := make([]float32, tileRows*k)
	for start := 0; start < len(vectors); start += tileRows {
		end := start + tileRows
		if end > len(vectors) {
			end = len(vectors)
		}
		batch := vectors[start:end]
		rows := distances[:len(batch)*k]
		fillMiniBatchDistanceRows(batch, t.centroids, rows)
		for i, vec := range batch {
			row := rows[i*k : (i+1)*k]
			rawClusterID := nearestCluster(row)
			t.passRawCounts[rawClusterID]++
			t.recordRepairCandidate(rawClusterID, vec, row[rawClusterID])
			clusterID := assignBalancedDistances(row, t.counts, t.passCounts, t.opts.TargetClusterSize, t.opts.BalancePenalty, t.opts.HardClusterFactor)
			if t.passSums[clusterID] == nil {
				t.passSums[clusterID] = make([]float64, dim)
			}
			for d, value := range vec {
				t.passSums[clusterID][d] += float64(value)
			}
			t.passCounts[clusterID]++
		}
	}
	return nil
}

func (t *MiniBatchBalancedTrainer) EndPass(seed uint64) (MiniBatchPassResult, error) {
	if t == nil {
		return MiniBatchPassResult{}, errors.New("kmeans: trainer is nil")
	}
	if !t.inPass {
		return MiniBatchPassResult{}, errors.New("kmeans: BeginPass must be called before EndPass")
	}
	t.inPass = false

	result := MiniBatchPassResult{}
	for clusterID := range t.centroids {
		n := t.passCounts[clusterID]
		if n == 0 {
			continue
		}
		centroid := t.centroids[clusterID]
		sum := t.passSums[clusterID]
		var shift float32
		inv := 1.0 / float64(n)
		for d := range centroid {
			next := float32(sum[d] * inv)
			diff := centroid[d] - next
			shift += diff * diff
			centroid[d] = next
		}
		if sqrt := float32(math.Sqrt(float64(shift))); sqrt > result.MaxShift {
			result.MaxShift = sqrt
		}
	}
	t.counts = append(t.counts[:0], t.passCounts...)
	repaired := t.repairSkew(seed)
	result.Repaired = repaired
	result.Converged = !repaired && result.MaxShift <= t.opts.ConvergenceEpsilon
	return result, nil
}

func (t *MiniBatchBalancedTrainer) recordRepairCandidate(clusterID int, vec []float32, dist float32) {
	candidates := t.passCandidates[clusterID]
	if len(candidates) >= maxRepairCandidatesPerBucket && dist <= candidates[len(candidates)-1].dist {
		return
	}
	entry := repairCandidate{vec: append([]float32(nil), vec...), dist: dist}
	candidates = append(candidates, entry)
	slices.SortFunc(candidates, func(a, b repairCandidate) int {
		switch {
		case a.dist > b.dist:
			return -1
		case a.dist < b.dist:
			return 1
		default:
			return 0
		}
	})
	if len(candidates) > maxRepairCandidatesPerBucket {
		candidates = candidates[:maxRepairCandidatesPerBucket]
	}
	t.passCandidates[clusterID] = candidates
}

func (t *MiniBatchBalancedTrainer) repairSkew(seed uint64) bool {
	if t == nil || len(t.centroids) == 0 || t.opts.TargetClusterSize <= 0 {
		return false
	}
	underfilledLimit := int64(math.Ceil(float64(t.opts.TargetClusterSize) * float64(t.opts.UnderfilledFraction)))
	if underfilledLimit < 1 {
		underfilledLimit = 1
	}
	oversizedLimit := int64(t.opts.TargetClusterSize * t.opts.HardClusterFactor)

	var underfilled []clusterBucket
	var repairSources []int
	for clusterID, count := range t.counts {
		switch {
		case count <= underfilledLimit:
			underfilled = append(underfilled, clusterBucket{clusterID: clusterID, count: count})
		}
	}
	for clusterID, count := range t.passRawCounts {
		if count <= oversizedLimit || len(t.passCandidates[clusterID]) == 0 {
			continue
		}
		extraSplits := int((count - oversizedLimit + int64(t.opts.TargetClusterSize) - 1) / int64(t.opts.TargetClusterSize))
		if extraSplits < 1 {
			extraSplits = 1
		}
		for i := 0; i < extraSplits; i++ {
			repairSources = append(repairSources, clusterID)
		}
	}
	if len(underfilled) == 0 || len(repairSources) == 0 {
		return false
	}
	slices.SortFunc(underfilled, func(a, b clusterBucket) int {
		switch {
		case a.count < b.count:
			return -1
		case a.count > b.count:
			return 1
		case a.clusterID < b.clusterID:
			return -1
		default:
			return 1
		}
	})

	repaired := false
	for i, dst := range underfilled {
		if i >= len(repairSources) {
			break
		}
		src := repairSources[i]
		t.mergeSparseCluster(dst.clusterID, underfilled, i)
		if !t.splitOversizedCluster(src, dst.clusterID) {
			continue
		}
		repaired = true
	}
	return repaired
}

func (t *MiniBatchBalancedTrainer) mergeSparseCluster(clusterID int, underfilled []clusterBucket, repurposeIdx int) {
	if t == nil || clusterID < 0 || clusterID >= len(t.centroids) {
		return
	}
	mergeTarget := -1
	bestDist := float32(0)
	for i, candidate := range underfilled {
		if i == repurposeIdx || candidate.clusterID == clusterID {
			continue
		}
		dist := metricL2Squared(t.centroids[clusterID], t.centroids[candidate.clusterID])
		if mergeTarget == -1 || dist < bestDist {
			mergeTarget = candidate.clusterID
			bestDist = dist
		}
	}
	if mergeTarget == -1 {
		for candidateID := range t.centroids {
			if candidateID == clusterID {
				continue
			}
			dist := metricL2Squared(t.centroids[clusterID], t.centroids[candidateID])
			if mergeTarget == -1 || dist < bestDist {
				mergeTarget = candidateID
				bestDist = dist
			}
		}
	}
	if mergeTarget == -1 {
		return
	}
	dstCount := t.counts[clusterID]
	targetCount := t.counts[mergeTarget]
	total := dstCount + targetCount
	if total <= 0 {
		return
	}
	for dim := range t.centroids[mergeTarget] {
		t.centroids[mergeTarget][dim] = float32(
			(float64(t.centroids[mergeTarget][dim])*float64(targetCount) +
				float64(t.centroids[clusterID][dim])*float64(dstCount)) / float64(total),
		)
	}
	t.counts[mergeTarget] = total
}

func (t *MiniBatchBalancedTrainer) splitOversizedCluster(srcClusterID, dstClusterID int) bool {
	if t == nil || srcClusterID < 0 || srcClusterID >= len(t.centroids) || dstClusterID < 0 || dstClusterID >= len(t.centroids) {
		return false
	}
	candidates := t.passCandidates[srcClusterID]
	if len(candidates) == 0 {
		return false
	}
	order := miniBatchOrder(len(candidates), uint64(srcClusterID+1)^uint64(dstClusterID+1))
	first := candidates[order[0]]
	var second []float32
	if len(candidates) > 1 {
		second = candidates[order[1]].vec
	} else {
		second = make([]float32, len(first.vec))
		for i := range second {
			second[i] = 2*t.centroids[srcClusterID][i] - first.vec[i]
		}
	}
	copy(t.centroids[srcClusterID], first.vec)
	copy(t.centroids[dstClusterID], second)
	t.counts[srcClusterID] = 0
	t.counts[dstClusterID] = 0
	return true
}

func (t *MiniBatchBalancedTrainer) Centroids() [][]float32 {
	if t == nil {
		return nil
	}
	out := make([][]float32, len(t.centroids))
	for i, centroid := range t.centroids {
		cp := make([]float32, len(centroid))
		copy(cp, centroid)
		out[i] = cp
	}
	return out
}

func (t *MiniBatchBalancedTrainer) Counts() []int64 {
	if t == nil {
		return nil
	}
	out := make([]int64, len(t.counts))
	copy(out, t.counts)
	return out
}

func assignBalancedDistances(distances []float32, counts, passCounts []int64, targetClusterSize int, balancePenalty float32, hardClusterFactor int) int {
	hardLimit := int64(targetClusterSize * hardClusterFactor)
	best := -1
	bestScore := float32(0)
	for i := range distances {
		passCount := passCounts[i]
		if targetClusterSize > 0 && hardClusterFactor > 0 && hardLimit > 0 && passCount >= hardLimit {
			continue
		}
		score := balancedScore(distances[i], counts[i]+passCounts[i], targetClusterSize, balancePenalty)
		if best == -1 || score < bestScore {
			best = i
			bestScore = score
		}
	}
	if best >= 0 {
		return best
	}
	best = 0
	bestScore = balancedScore(distances[0], counts[0]+passCounts[0], targetClusterSize, balancePenalty)
	for i := 1; i < len(distances); i++ {
		score := balancedScore(distances[i], counts[i]+passCounts[i], targetClusterSize, balancePenalty)
		if score < bestScore {
			best = i
			bestScore = score
		}
	}
	return best
}

func nearestCluster(distances []float32) int {
	best := 0
	bestDist := distances[0]
	for i := 1; i < len(distances); i++ {
		if distances[i] < bestDist {
			best = i
			bestDist = distances[i]
		}
	}
	return best
}

func balancedScore(dist float32, count int64, targetClusterSize int, balancePenalty float32) float32 {
	if targetClusterSize <= 0 || balancePenalty <= 0 {
		return dist
	}
	target := int64(targetClusterSize)
	over := float32(count+1-target) / float32(targetClusterSize)
	if over <= 0 {
		return dist
	}
	return dist + balancePenalty*over
}

func fillMiniBatchDistanceRows(vectors [][]float32, centroids [][]float32, rows []float32) {
	n := len(vectors)
	k := len(centroids)
	if n == 0 || k == 0 {
		return
	}
	nChunks := (n + miniBatchParallelChunkSize - 1) / miniBatchParallelChunkSize
	workers := runtime.GOMAXPROCS(0)
	if workers > nChunks {
		workers = nChunks
	}
	if workers < 1 {
		workers = 1
	}
	run := func(worker int) {
		for chunk := worker; chunk < nChunks; chunk += workers {
			start := chunk * miniBatchParallelChunkSize
			end := start + miniBatchParallelChunkSize
			if end > n {
				end = n
			}
			for i := start; i < end; i++ {
				row := rows[i*k : (i+1)*k]
				for j, centroid := range centroids {
					row[j] = metricL2Squared(vectors[i], centroid)
				}
			}
		}
	}
	if workers == 1 {
		run(0)
		return
	}
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			run(worker)
		}(worker)
	}
	wg.Wait()
}

func metricL2Squared(a, b []float32) float32 {
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

func farthestReferenceScore(vec []float32, centroids [][]float32, skip int) float32 {
	best := float32(0)
	initialized := false
	for i, centroid := range centroids {
		if i == skip {
			continue
		}
		dist := metricL2Squared(vec, centroid)
		if !initialized || dist < best {
			best = dist
			initialized = true
		}
	}
	if !initialized {
		return 0
	}
	return best
}

func MiniBatchBalanced(vectors [][]float32, k int, seed uint64, opts MiniBatchBalancedOptions) ([][]float32, error) {
	opts = normalizeMiniBatchOptions(opts)
	if err := validateInputs(vectors, k, opts.MaxIter); err != nil {
		return nil, err
	}
	initCentroids, err := initialMiniBatchCentroids(vectors, k, seed, opts)
	if err != nil {
		return nil, err
	}
	return MiniBatchBalancedFromInit(vectors, initCentroids, seed, opts)
}

func MiniBatchBalancedFromInit(vectors [][]float32, initCentroids [][]float32, seed uint64, opts MiniBatchBalancedOptions) ([][]float32, error) {
	if len(vectors) == 0 {
		return nil, errors.New("kmeans: vectors must not be empty")
	}
	opts = normalizeMiniBatchOptions(opts)
	trainer, err := NewMiniBatchBalancedTrainer(initCentroids, opts)
	if err != nil {
		return nil, err
	}
	if len(trainer.centroids) > len(vectors) {
		trainer.centroids = trainer.centroids[:len(vectors)]
		trainer.counts = trainer.counts[:len(vectors)]
	}

	batch := make([][]float32, 0, opts.BatchSize)
	stablePasses := 0
	for iter := 0; iter < opts.MaxIter; iter++ {
		if err := trainer.BeginPass(); err != nil {
			return nil, err
		}
		order := miniBatchOrder(len(vectors), seed^uint64(iter+1))
		for _, idx := range order {
			batch = append(batch, vectors[idx])
			if len(batch) < opts.BatchSize {
				continue
			}
			if err := trainer.ObserveBatch(batch); err != nil {
				return nil, err
			}
			batch = batch[:0]
		}
		if len(batch) > 0 {
			if err := trainer.ObserveBatch(batch); err != nil {
				return nil, err
			}
			batch = batch[:0]
		}
		result, err := trainer.EndPass(seed ^ uint64(iter+1))
		if err != nil {
			return nil, err
		}
		if result.Converged {
			stablePasses++
			if stablePasses >= 2 {
				break
			}
		} else {
			stablePasses = 0
		}
	}
	return trainer.Centroids(), nil
}

func initialMiniBatchCentroids(vectors [][]float32, k int, seed uint64, opts MiniBatchBalancedOptions) ([][]float32, error) {
	initCap := opts.BatchSize * DefaultMiniBatchInitFactor
	if initCap < k {
		initCap = k
	}
	if initCap > len(vectors) {
		initCap = len(vectors)
	}
	order := miniBatchOrder(len(vectors), seed^0x9e3779b97f4a7c15)
	initSample := make([][]float32, initCap)
	for i := 0; i < initCap; i++ {
		initSample[i] = vectors[order[i]]
	}
	centroids, err := KMeansPlusPlus(initSample, k, seed, 1)
	if err != nil {
		return nil, err
	}
	rebalanceCap := initCap * DefaultMiniBatchInitFactor
	if rebalanceCap < k {
		rebalanceCap = k
	}
	if rebalanceCap > len(vectors) {
		rebalanceCap = len(vectors)
	}
	rebalanceSample := make([][]float32, rebalanceCap)
	for i := 0; i < rebalanceCap; i++ {
		rebalanceSample[i] = vectors[order[i]]
	}
	return RebalanceInitialCentroids(rebalanceSample, centroids, opts, seed)
}

func RebalanceInitialCentroids(vectors [][]float32, initCentroids [][]float32, opts MiniBatchBalancedOptions, seed uint64) ([][]float32, error) {
	if len(initCentroids) == 0 || len(vectors) == 0 {
		return initCentroids, nil
	}
	opts = normalizeMiniBatchOptions(opts)
	centroids := make([][]float32, len(initCentroids))
	for i, centroid := range initCentroids {
		centroids[i] = append([]float32(nil), centroid...)
	}
	underfilledLimit := int64(math.Ceil(float64(opts.TargetClusterSize) * float64(opts.UnderfilledFraction)))
	if underfilledLimit < 1 {
		underfilledLimit = 1
	}
	hardLimit := int64(opts.TargetClusterSize * opts.HardClusterFactor)
	if hardLimit < 1 {
		hardLimit = 1
	}
	for iter := 0; iter < len(centroids); iter++ {
		counts, members := assignSampleMembers(vectors, centroids)
		underfilled, oversized := findSkewedSampleClusters(counts, members, underfilledLimit, hardLimit)
		if len(underfilled) == 0 || len(oversized) == 0 {
			break
		}
		dst := underfilled[0].clusterID
		src := oversized[0].clusterID
		mergeTarget := nearestSparseMergeTarget(centroids, underfilled, dst, src)
		if mergeTarget == -1 {
			mergeTarget = nearestCentroidExcluding(centroids, dst, src)
		}
		if mergeTarget != -1 && len(members[dst]) > 0 {
			merged := append(append([]int(nil), members[mergeTarget]...), members[dst]...)
			centroids[mergeTarget] = meanOfMembers(vectors, merged, len(centroids[mergeTarget]))
		}
		if len(members[src]) < 2 {
			break
		}
		srcVectors := make([][]float32, 0, len(members[src]))
		for _, idx := range members[src] {
			srcVectors = append(srcVectors, vectors[idx])
		}
		split, err := KMeansPlusPlus(srcVectors, 2, seed^uint64(iter+1)^uint64(src+1)^uint64(dst+1), 2)
		if err != nil {
			return nil, err
		}
		centroids[src] = append(centroids[src][:0], split[0]...)
		centroids[dst] = append(centroids[dst][:0], split[1]...)
	}
	return centroids, nil
}

func assignSampleMembers(vectors [][]float32, centroids [][]float32) ([]int64, [][]int) {
	counts := make([]int64, len(centroids))
	members := make([][]int, len(centroids))
	for idx, vec := range vectors {
		clusterID := nearestByVector(vec, centroids)
		counts[clusterID]++
		members[clusterID] = append(members[clusterID], idx)
	}
	return counts, members
}

func findSkewedSampleClusters(counts []int64, members [][]int, underfilledLimit, hardLimit int64) ([]clusterBucket, []clusterBucket) {
	var underfilled []clusterBucket
	var oversized []clusterBucket
	for clusterID, count := range counts {
		switch {
		case count <= underfilledLimit:
			underfilled = append(underfilled, clusterBucket{clusterID: clusterID, count: count})
		case count > hardLimit && len(members[clusterID]) > 1:
			oversized = append(oversized, clusterBucket{clusterID: clusterID, count: count})
		}
	}
	slices.SortFunc(underfilled, func(a, b clusterBucket) int {
		switch {
		case a.count < b.count:
			return -1
		case a.count > b.count:
			return 1
		case a.clusterID < b.clusterID:
			return -1
		default:
			return 1
		}
	})
	slices.SortFunc(oversized, func(a, b clusterBucket) int {
		switch {
		case a.count > b.count:
			return -1
		case a.count < b.count:
			return 1
		case a.clusterID < b.clusterID:
			return -1
		default:
			return 1
		}
	})
	return underfilled, oversized
}

func nearestSparseMergeTarget(centroids [][]float32, underfilled []clusterBucket, clusterID, exclude int) int {
	mergeTarget := -1
	bestDist := float32(0)
	for _, candidate := range underfilled {
		if candidate.clusterID == clusterID || candidate.clusterID == exclude {
			continue
		}
		dist := metricL2Squared(centroids[clusterID], centroids[candidate.clusterID])
		if mergeTarget == -1 || dist < bestDist {
			mergeTarget = candidate.clusterID
			bestDist = dist
		}
	}
	return mergeTarget
}

func nearestCentroidExcluding(centroids [][]float32, clusterID int, exclude ...int) int {
	blocked := make(map[int]struct{}, len(exclude)+1)
	blocked[clusterID] = struct{}{}
	for _, candidate := range exclude {
		blocked[candidate] = struct{}{}
	}
	mergeTarget := -1
	bestDist := float32(0)
	for candidateID := range centroids {
		if _, ok := blocked[candidateID]; ok {
			continue
		}
		dist := metricL2Squared(centroids[clusterID], centroids[candidateID])
		if mergeTarget == -1 || dist < bestDist {
			mergeTarget = candidateID
			bestDist = dist
		}
	}
	return mergeTarget
}

func meanOfMembers(vectors [][]float32, members []int, dim int) []float32 {
	centroid := make([]float32, dim)
	if len(members) == 0 {
		return centroid
	}
	for _, idx := range members {
		for d, value := range vectors[idx] {
			centroid[d] += value
		}
	}
	inv := 1 / float32(len(members))
	for d := range centroid {
		centroid[d] *= inv
	}
	return centroid
}

func nearestByVector(vec []float32, centroids [][]float32) int {
	best := 0
	bestDist := metricL2Squared(vec, centroids[0])
	for i := 1; i < len(centroids); i++ {
		dist := metricL2Squared(vec, centroids[i])
		if dist < bestDist {
			best = i
			bestDist = dist
		}
	}
	return best
}

func miniBatchOrder(n int, seed uint64) []int {
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	if n <= 1 {
		return order
	}
	rng := rand.New(rand.NewSource(foldSeed(seed ^ uint64(n))))
	rng.Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})
	return order
}
