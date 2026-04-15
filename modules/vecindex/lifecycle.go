package vecindex

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
)

// kmeansMaxIter is the Lloyd iteration cap passed to KMeansPlusPlus.
// Convergence detection terminates early when centroids stabilise.
const kmeansMaxIter = 3

// flatScanThreshold is the minimum vector count required to leave flat-scan phase.
const flatScanThreshold = 6400

// kmeansTrainSampleFactor controls subsampling: training uses min(n, nlist*factor) vectors.
const kmeansTrainSampleFactor = 2

// Graduate promotes an index from flat-scan to IVF by training centroids.
// Returns an error if count < flatScanThreshold (below IVF viable range).
func Graduate(ctx context.Context, idx *Index, targetNlist int) error {
	if targetNlist <= 0 || targetNlist > MaxNlist {
		return fmt.Errorf("vecindex: targetNlist %d out of range [1, %d]", targetNlist, MaxNlist)
	}

	count := idx.vectorCount.Load()
	if int(count) < flatScanThreshold {
		return fmt.Errorf(
			"vecindex: need at least %d vectors to graduate from flat-scan, have %d",
			flatScanThreshold, count,
		)
	}
	if minRequired := minVectorsForNlist(targetNlist); int(count) < minRequired {
		return fmt.Errorf(
			"vecindex: need at least %d vectors for nlist=%d, have %d",
			minRequired, targetNlist, count,
		)
	}
	if int(count) < targetNlist {
		return fmt.Errorf(
			"vecindex: need at least %d vectors for nlist=%d (k-means requirement), have %d",
			targetNlist, targetNlist, count,
		)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	vecs, docIDs, oldClusterIDs, err := gatherAllVectors(idx)
	if err != nil {
		return fmt.Errorf("vecindex: gather vectors for graduation: %w", err)
	}
	if len(vecs) == 0 {
		return errors.New("vecindex: no vectors to graduate on")
	}

	nlist := targetNlist
	if nlist > len(vecs) {
		nlist = len(vecs)
	}

	maxSamples := nlist * kmeansTrainSampleFactor
	if maxSamples < nlist+1 {
		maxSamples = nlist + 1
	}
	trainVecs := vecs
	if len(vecs) > maxSamples {
		trainVecs = subsampleVecs(vecs, maxSamples, idx.spec.Seed)
	}
	if nlist > len(trainVecs) {
		nlist = len(trainVecs)
	}

	centroids, err := kmeans.KMeansPlusPlus(trainVecs, nlist, idx.spec.Seed, kmeansMaxIter)
	if err != nil {
		return fmt.Errorf("vecindex: k-means: %w", err)
	}

	epoch := idx.spec.Epoch + 1

	newClusterIDs := make([]uint32, nlist)
	for i := range newClusterIDs {
		cid, allocErr := idx.st.AllocateClusterID()
		if allocErr != nil {
			return fmt.Errorf("vecindex: allocate cluster ID: %w", allocErr)
		}
		newClusterIDs[i] = cid
	}

	for i, c := range centroids {
		if err := idx.st.PutCentroid(newClusterIDs[i], c, idx.spec.InternalDim()); err != nil {
			return fmt.Errorf("vecindex: put centroid %d: %w", i, err)
		}
	}

	// Parallel assignment: GOMAXPROCS workers each write their own Pebble batch.
	nWorkers := runtime.GOMAXPROCS(0)
	chunkSize := (len(vecs) + nWorkers - 1) / nWorkers

	newMetas := make([]store.ClusterMeta, nlist)
	for i := range newMetas {
		newMetas[i] = store.ClusterMeta{State: store.ClusterStateActive, Epoch: epoch}
	}
	var metaMu sync.Mutex

	type workerErr struct{ err error }
	errs := make([]workerErr, nWorkers)

	var wg sync.WaitGroup
	for w := 0; w < nWorkers; w++ {
		start := w * chunkSize
		if start >= len(vecs) {
			break
		}
		end := start + chunkSize
		if end > len(vecs) {
			end = len(vecs)
		}
		wg.Add(1)
		go func(lo, hi, slot int) {
			defer wg.Done()
			b := idx.st.NewBatch()
			for vi := lo; vi < hi; vi++ {
				if ctx.Err() != nil {
					_ = b.Close()
					errs[slot].err = ctx.Err()
					return
				}
				vec := vecs[vi]
				centIdx, _, assignErr := kmeans.Assign(vec, centroids, idx.storageMetric())
				if assignErr != nil {
					_ = b.Close()
					errs[slot].err = assignErr
					return
				}
				cid := newClusterIDs[centIdx]
				docID := docIDs[vi]

				if bErr := b.BatchPutPosting(cid, docID, vec); bErr != nil {
					_ = b.Close()
					errs[slot].err = bErr
					return
				}
				if bErr := b.BatchPutReverseMap(docID, cid); bErr != nil {
					_ = b.Close()
					errs[slot].err = bErr
					return
				}

				metaMu.Lock()
				newMetas[centIdx].Size++
				metaMu.Unlock()

				if (vi-lo+1)%bulkBatchSize == 0 {
					if cErr := b.Commit(); cErr != nil {
						errs[slot].err = cErr
						return
					}
					b = idx.st.NewBatch()
				}
			}
			if cErr := b.Commit(); cErr != nil {
				errs[slot].err = cErr
			}
		}(start, end, w)
	}
	wg.Wait()

	for _, we := range errs {
		if we.err != nil {
			return fmt.Errorf("vecindex: parallel graduation worker: %w", we.err)
		}
	}

	for i, cid := range newClusterIDs {
		if err := idx.st.PutClusterMeta(cid, newMetas[i]); err != nil {
			return fmt.Errorf("vecindex: put cluster meta %d: %w", i, err)
		}
	}

	cs, err := kmeans.NewCentroidSet(epoch, centroids)
	if err != nil {
		return fmt.Errorf("vecindex: build centroid set: %w", err)
	}

	newState := &centroidState{cs: cs, clusterIDs: newClusterIDs}

	// Seed online MacQueen state for new clusters.
	idx.initClusterStats(newClusterIDs, centroids, vecs)

	oldState := idx.centroids.Load()
	idx.centroids.Store(newState)

	idx.specMu.Lock()
	idx.spec.Nlist = nlist
	idx.spec.Epoch = epoch
	updatedSpec := idx.spec
	idx.specMu.Unlock()

	if err := persistSpec(idx.st, updatedSpec); err != nil {
		idx.logger.Error().Err(err).Msg("vecindex: failed to persist spec after graduation")
	}

	if oldState != nil {
		for _, oldCID := range oldState.clusterIDs {
			cleanupOldCluster(idx, oldCID)
		}
	} else if len(oldClusterIDs) > 0 {
		uniqueOld := uniqueUint32(oldClusterIDs)
		for _, oldCID := range uniqueOld {
			if !containsUint32(newClusterIDs, oldCID) {
				cleanupOldCluster(idx, oldCID)
			}
		}
	}

	return nil
}

// cleanupOldCluster deletes all posting entries for clusterID.
func cleanupOldCluster(idx *Index, clusterID uint32) {
	entries, err := idx.st.ScanCluster(clusterID)
	if err != nil {
		return
	}
	b := idx.st.NewBatch()
	for _, e := range entries {
		_ = b.BatchDeletePosting(clusterID, e.DocID)
	}
	_ = b.Commit()
	_ = idx.st.DeleteCentroid(clusterID)
	meta := store.ClusterMeta{State: store.ClusterStateRetired}
	_ = idx.st.PutClusterMeta(clusterID, meta)
}

// gatherAllVectors collects all live vectors from the index.
func gatherAllVectors(idx *Index) ([][]float32, []uint64, []uint32, error) {
	clusters, err := idx.st.ListActiveClusters()
	if err != nil {
		return nil, nil, nil, err
	}
	if len(clusters) == 0 {
		clusters = []uint32{0}
	}

	var vecs [][]float32
	var docIDs []uint64
	var clusterIDs []uint32

	for _, cid := range clusters {
		entries, err := idx.st.ScanCluster(cid)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, e := range entries {
			vecs = append(vecs, e.Vector)
			docIDs = append(docIDs, e.DocID)
			clusterIDs = append(clusterIDs, cid)
		}
	}
	return vecs, docIDs, clusterIDs, nil
}

// CheckSplit splits the cluster at centroid index centIdx if it has grown beyond
// 3× the mean cluster size. Returns nil without splitting when the cluster has < 2 vectors.
func CheckSplit(idx *Index, centIdx uint32) error {
	cs := idx.centroids.Load()
	if cs == nil {
		return nil
	}
	if int(centIdx) >= len(cs.clusterIDs) {
		return nil
	}
	storeClusterID := cs.clusterIDs[centIdx]

	meta, err := idx.st.GetClusterMeta(storeClusterID)
	if err != nil {
		return err
	}
	if int(meta.Size) < 2 {
		return nil
	}

	lk := idx.clusterLock(storeClusterID)
	lk.Lock()
	defer lk.Unlock()

	return splitCluster(idx, storeClusterID, cs)
}

// splitCluster splits clusterID into two child clusters using k-means(k=2).
func splitCluster(idx *Index, clusterID uint32, cs *centroidState) error {
	entries, err := idx.st.ScanCluster(clusterID)
	if err != nil {
		return err
	}
	if len(entries) < 2 {
		return nil
	}

	vecs := make([][]float32, len(entries))
	for i, e := range entries {
		vecs[i] = e.Vector
	}

	clusterSeed := idx.spec.Seed ^ (uint64(clusterID) * 0x9E3779B97F4A7C15)
	newCentroids, err := kmeans.KMeansPlusPlus(vecs, 2, clusterSeed, kmeansMaxIter)
	if err != nil {
		return fmt.Errorf("vecindex: split k-means: %w", err)
	}

	newCID1, err := idx.st.AllocateClusterID()
	if err != nil {
		return err
	}
	newCID2, err := idx.st.AllocateClusterID()
	if err != nil {
		return err
	}

	if err := idx.st.PutCentroid(newCID1, newCentroids[0], idx.spec.InternalDim()); err != nil {
		return err
	}
	if err := idx.st.PutCentroid(newCID2, newCentroids[1], idx.spec.InternalDim()); err != nil {
		return err
	}

	meta1 := store.ClusterMeta{State: store.ClusterStateActive, Epoch: idx.spec.Epoch}
	meta2 := store.ClusterMeta{State: store.ClusterStateActive, Epoch: idx.spec.Epoch}

	b := idx.st.NewBatch()
	for _, e := range entries {
		c, _, _ := kmeans.Assign(e.Vector, newCentroids, idx.storageMetric())
		var targetCID uint32
		if c == 0 {
			targetCID = newCID1
			meta1.Size++
		} else {
			targetCID = newCID2
			meta2.Size++
		}
		if err := b.BatchPutPosting(targetCID, e.DocID, e.Vector); err != nil {
			_ = b.Close()
			return fmt.Errorf("vecindex: split put posting: %w", err)
		}
		if err := b.BatchPutReverseMap(e.DocID, targetCID); err != nil {
			_ = b.Close()
			return fmt.Errorf("vecindex: split put reverse map: %w", err)
		}
	}
	if err := b.BatchPutClusterMeta(newCID1, meta1); err != nil {
		_ = b.Close()
		return fmt.Errorf("vecindex: split put meta1: %w", err)
	}
	if err := b.BatchPutClusterMeta(newCID2, meta2); err != nil {
		_ = b.Close()
		return fmt.Errorf("vecindex: split put meta2: %w", err)
	}
	if err := b.Commit(); err != nil {
		return fmt.Errorf("vecindex: split batch commit: %w", err)
	}

	centIdx := -1
	for i, cid := range cs.clusterIDs {
		if cid == clusterID {
			centIdx = i
			break
		}
	}

	oldCentroids := make([][]float32, cs.cs.Len())
	for i := range oldCentroids {
		v, _ := cs.cs.Get(uint32(i))
		oldCentroids[i] = v
	}
	newCentVecs := make([][]float32, 0, len(oldCentroids)+1)
	newCentIDs := make([]uint32, 0, len(cs.clusterIDs)+1)
	for i, v := range oldCentroids {
		if i == centIdx {
			newCentVecs = append(newCentVecs, newCentroids[0], newCentroids[1])
			newCentIDs = append(newCentIDs, newCID1, newCID2)
		} else {
			newCentVecs = append(newCentVecs, v)
			newCentIDs = append(newCentIDs, cs.clusterIDs[i])
		}
	}
	if centIdx == -1 {
		newCentVecs = append(newCentVecs, newCentroids[0], newCentroids[1])
		newCentIDs = append(newCentIDs, newCID1, newCID2)
	}

	newCS, err := kmeans.NewCentroidSet(idx.spec.Epoch, newCentVecs)
	if err != nil {
		return err
	}
	idx.centroids.Store(&centroidState{cs: newCS, clusterIDs: newCentIDs})

	oldMeta := store.ClusterMeta{State: store.ClusterStateRetired}
	_ = idx.st.PutClusterMeta(clusterID, oldMeta)
	cleanupOldClusterPostings(idx, clusterID)

	idx.seedClusterStats(newCID1, newCentroids[0], meta1.Size)
	idx.seedClusterStats(newCID2, newCentroids[1], meta2.Size)

	return nil
}

// cleanupOldClusterPostings removes posting entries for clusterID after a split.
func cleanupOldClusterPostings(idx *Index, clusterID uint32) {
	entries, err := idx.st.ScanCluster(clusterID)
	if err != nil {
		return
	}
	b := idx.st.NewBatch()
	for _, e := range entries {
		_ = b.BatchDeletePosting(clusterID, e.DocID)
	}
	_ = b.Commit()
	_ = idx.st.DeleteCentroid(clusterID)
}

// CheckMerge checks if the cluster at centroid index centIdx has shrunk below
// 0.25× mean cluster size and merges it into its nearest neighbour if so.
func CheckMerge(idx *Index, centIdx uint32) error {
	cs := idx.centroids.Load()
	if cs == nil {
		return nil
	}
	if int(centIdx) >= len(cs.clusterIDs) {
		return nil
	}
	clusterID := cs.clusterIDs[centIdx]

	meta, err := idx.st.GetClusterMeta(clusterID)
	if err != nil {
		return err
	}

	meanSize := meanClusterSize(idx)
	threshold := meanSize / 4
	if int(meta.Size) > threshold {
		return nil
	}

	srcVec, err := cs.cs.Get(centIdx)
	if err != nil {
		return err
	}

	otherVecs := make([][]float32, 0, cs.cs.Len()-1)
	otherIndices := make([]int, 0, cs.cs.Len()-1)
	for i := 0; i < cs.cs.Len(); i++ {
		if uint32(i) == centIdx {
			continue
		}
		v, _ := cs.cs.Get(uint32(i))
		otherVecs = append(otherVecs, v)
		otherIndices = append(otherIndices, i)
	}
	if len(otherVecs) == 0 {
		return nil
	}

	// Use storageMetric (L2 on augmented space for MetricDot) to find nearest neighbour.
	nearestIdx, _, err := kmeans.Assign(srcVec, otherVecs, idx.storageMetric())
	if err != nil {
		return err
	}
	targetCentIdx := otherIndices[nearestIdx]
	targetCID := cs.clusterIDs[targetCentIdx]

	// Acquire both shard locks in canonical order (lower shard index first) to
	// prevent deadlock with concurrent Upserts holding one lock and waiting for
	// the other (HR-06 fix).
	lkSrc := idx.clusterLock(clusterID)
	lkDst := idx.clusterLock(targetCID)
	srcShard := clusterID % clusterShards
	dstShard := targetCID % clusterShards
	switch {
	case srcShard < dstShard:
		lkSrc.Lock()
		lkDst.Lock()
	case srcShard > dstShard:
		lkDst.Lock()
		lkSrc.Lock()
	default:
		// Same shard — one lock covers both clusters.
		lkSrc.Lock()
	}
	defer func() {
		lkSrc.Unlock()
		if srcShard != dstShard {
			lkDst.Unlock()
		}
	}()

	entries, err := idx.st.ScanCluster(clusterID)
	if err != nil {
		return err
	}

	// Re-read targetMeta under the lock to get the current size.
	targetMeta, err := idx.st.GetClusterMeta(targetCID)
	if errors.Is(err, store.ErrNotFound) {
		targetMeta = store.ClusterMeta{State: store.ClusterStateActive}
	} else if err != nil {
		return err
	}

	b := idx.st.NewBatch()
	for _, e := range entries {
		if err := b.BatchDeletePosting(clusterID, e.DocID); err != nil {
			_ = b.Close()
			return fmt.Errorf("vecindex: merge delete posting: %w", err)
		}
		if err := b.BatchPutPosting(targetCID, e.DocID, e.Vector); err != nil {
			_ = b.Close()
			return fmt.Errorf("vecindex: merge put posting: %w", err)
		}
		if err := b.BatchPutReverseMap(e.DocID, targetCID); err != nil {
			_ = b.Close()
			return fmt.Errorf("vecindex: merge put reverse map: %w", err)
		}
		targetMeta.Size++
	}
	if err := b.BatchPutClusterMeta(targetCID, targetMeta); err != nil {
		_ = b.Close()
		return fmt.Errorf("vecindex: merge put cluster meta: %w", err)
	}
	if err := b.Commit(); err != nil {
		return fmt.Errorf("vecindex: merge batch commit: %w", err)
	}

	retiredMeta := store.ClusterMeta{State: store.ClusterStateRetired}
	_ = idx.st.PutClusterMeta(clusterID, retiredMeta)
	_ = idx.st.DeleteCentroid(clusterID)

	newCentVecs := make([][]float32, 0, cs.cs.Len()-1)
	newCentIDs := make([]uint32, 0, len(cs.clusterIDs)-1)
	for i := 0; i < cs.cs.Len(); i++ {
		if uint32(i) == centIdx {
			continue
		}
		v, _ := cs.cs.Get(uint32(i))
		newCentVecs = append(newCentVecs, v)
		newCentIDs = append(newCentIDs, cs.clusterIDs[i])
	}

	if len(newCentVecs) > 0 {
		newCS, err := kmeans.NewCentroidSet(idx.spec.Epoch, newCentVecs)
		if err != nil {
			return err
		}
		idx.centroids.Store(&centroidState{cs: newCS, clusterIDs: newCentIDs})
	} else {
		idx.centroids.Store(nil)
	}

	return nil
}

// meanClusterSize returns the mean size across all active clusters.
func meanClusterSize(idx *Index) int {
	cs := idx.centroids.Load()
	if cs == nil {
		return 0
	}
	total := 0
	count := 0
	for _, cid := range cs.clusterIDs {
		m, err := idx.st.GetClusterMeta(cid)
		if err != nil {
			continue
		}
		if m.State == store.ClusterStateActive {
			total += int(m.Size)
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return total / count
}

// minVectorsForNlist returns the minimum vector count for a given nlist tier.
func minVectorsForNlist(nlist int) int {
	switch {
	case nlist <= 64:
		return 6400
	case nlist <= 256:
		return 25600
	case nlist <= 1024:
		return 102400
	case nlist <= 4096:
		return 409600
	case nlist <= 16384:
		return 3276800
	default:
		return 3276800
	}
}

// nprobeDefault returns the canonical nprobe for a given nlist tier.
func nprobeDefault(nlist int) int {
	switch {
	case nlist <= 64:
		return 6
	case nlist <= 256:
		return 12
	case nlist <= 1024:
		return 32
	case nlist <= 4096:
		return 64
	default:
		return 128
	}
}

// normalizeVec returns a unit-normalised copy of vec for cosine metric.
// Returns vec unchanged for other metrics.
func normalizeVec(vec []float32, m metric.Metric) []float32 {
	if m != metric.MetricCosine {
		return vec
	}
	n := metric.Norm(vec)
	if n == 0 {
		return vec
	}
	out := make([]float32, len(vec))
	inv := float32(1.0 / float64(n))
	for i, x := range vec {
		out[i] = x * inv
	}
	return out
}

// uniqueUint32 returns deduplicated elements of s.
func uniqueUint32(s []uint32) []uint32 {
	seen := make(map[uint32]struct{}, len(s))
	out := s[:0:0]
	for _, v := range s {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}

// containsUint32 reports whether v appears in s.
func containsUint32(s []uint32, v uint32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// subsampleVecs returns a deterministic subsample of size m from vecs.
func subsampleVecs(vecs [][]float32, m int, seed uint64) [][]float32 {
	n := len(vecs)
	if m >= n {
		return vecs
	}
	out := make([][]float32, m)
	step := float64(n) / float64(m)
	offset := int(seed % uint64(n))
	for i := 0; i < m; i++ {
		vidx := (offset + int(float64(i)*step)) % n
		out[i] = vecs[vidx]
	}
	return out
}
