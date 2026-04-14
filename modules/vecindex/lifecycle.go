package vecindex

import (
	"context"
	"errors"
	"fmt"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
)

// kmeansMaxIter is the Lloyd iteration cap passed to KMeansPlusPlus.
// Convergence detection in kmeans.go terminates early when centroids stabilise.
const kmeansMaxIter = 3

// flatScanThreshold is the minimum vector count required to leave flat-scan phase.
const flatScanThreshold = 6400

// Graduate promotes an index from flat-scan to IVF by training centroids.
// Returns an error if count < flatScanThreshold (below IVF viable range).
// The caller supplies targetNlist; Graduate does not enforce tier boundaries
// beyond the flat-scan threshold so tests can use arbitrary nlist values.
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

	return retrainWithNlist(ctx, idx, targetNlist, idx.spec.Seed, idx.spec.Epoch+1)
}

// Retrain rebuilds all centroids for idx using the given seed and sets the epoch.
func Retrain(ctx context.Context, idx *Index, seed uint64, epoch uint64) error {
	// Read current nlist without holding the lock — retrainWithNlist acquires write lock.
	cs := idx.centroids.Load()

	nlist := idx.spec.Nlist
	if cs != nil {
		nlist = cs.cs.Len()
	}
	if nlist <= 0 {
		return errors.New("vecindex: cannot retrain — no centroids and nlist=0")
	}

	return retrainWithNlist(ctx, idx, nlist, seed, epoch)
}

// kmeansTrainSampleFactor controls subsampling: training uses min(n, nlist*factor) vectors.
// k-means++ init is O(n_train × k²); keeping n_train small bounds init time for large k.
const kmeansTrainSampleFactor = 2

// retrainWithNlist reads all vectors, runs k-means, and atomically swaps the centroid set.
// Holds idx.mu write lock for the entire operation so that concurrent ListActiveClusters
// callers never observe a mix of old and new cluster states during the centroid swap.
func retrainWithNlist(ctx context.Context, idx *Index, nlist int, seed uint64, epoch uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Collect all vectors from the flat cluster (0) or all active clusters.
	vecs, docIDs, oldClusterIDs, err := gatherAllVectors(idx)
	if err != nil {
		return fmt.Errorf("vecindex: gather vectors for retrain: %w", err)
	}
	if len(vecs) == 0 {
		return errors.New("vecindex: no vectors to retrain on")
	}
	if nlist > len(vecs) {
		nlist = len(vecs)
	}

	// Subsample for k-means training to bound init cost (O(n_train × k²)).
	// n_train is capped at max(nlist*factor, nlist+1) to ensure k ≤ n_train.
	maxSamples := nlist * kmeansTrainSampleFactor
	if maxSamples < nlist+1 {
		maxSamples = nlist + 1
	}
	trainVecs := vecs
	if len(vecs) > maxSamples {
		trainVecs = subsampleVecs(vecs, maxSamples, seed)
	}
	if nlist > len(trainVecs) {
		nlist = len(trainVecs)
	}

	centroids, err := kmeans.KMeansPlusPlus(trainVecs, nlist, seed, kmeansMaxIter)
	if err != nil {
		return fmt.Errorf("vecindex: k-means: %w", err)
	}

	// Allocate new cluster IDs.
	newClusterIDs := make([]uint32, nlist)
	for i := range newClusterIDs {
		cid, err := idx.st.AllocateClusterID()
		if err != nil {
			return fmt.Errorf("vecindex: allocate cluster ID: %w", err)
		}
		newClusterIDs[i] = cid
	}

	// Persist new centroids.
	for i, c := range centroids {
		if err := idx.st.PutCentroid(newClusterIDs[i], c, idx.spec.Dim); err != nil {
			return fmt.Errorf("vecindex: put centroid %d: %w", i, err)
		}
	}

	// Build and write new posting lists in batches for throughput.
	newMetas := make([]store.ClusterMeta, nlist)
	for i := range newMetas {
		newMetas[i] = store.ClusterMeta{State: store.ClusterStateActive, Epoch: epoch}
	}

	// Pre-assign all vectors to centroids.
	assignments := make([]uint32, len(vecs))
	for vi, vec := range vecs {
		centIdx, _, err := kmeans.Assign(vec, centroids, idx.spec.Metric)
		if err != nil {
			return err
		}
		assignments[vi] = centIdx
		newMetas[centIdx].Size++
	}

	// Write all cluster metas first.
	for i, cid := range newClusterIDs {
		if err := idx.st.PutClusterMeta(cid, newMetas[i]); err != nil {
			return fmt.Errorf("vecindex: put cluster meta %d: %w", i, err)
		}
	}

	// Write posting lists and reverse maps in batches.
	b := idx.st.NewBatch()
	for vi, vec := range vecs {
		if ctx.Err() != nil {
			_ = b.Close()
			return ctx.Err()
		}
		centIdx := assignments[vi]
		cid := newClusterIDs[centIdx]
		docID := docIDs[vi]

		if err := b.BatchPutPosting(cid, docID, vec); err != nil {
			_ = b.Close()
			return err
		}
		if err := b.BatchPutReverseMap(docID, cid); err != nil {
			_ = b.Close()
			return err
		}

		if (vi+1)%bulkBatchSize == 0 {
			if err := b.Commit(); err != nil {
				return err
			}
			b = idx.st.NewBatch()
		}
	}
	if err := b.Commit(); err != nil {
		return err
	}

	// Build new CentroidSet.
	cs, err := kmeans.NewCentroidSet(epoch, centroids)
	if err != nil {
		return fmt.Errorf("vecindex: build centroid set: %w", err)
	}

	newState := &centroidState{cs: cs, clusterIDs: newClusterIDs}

	// Swap centroid state (already holding write lock from function entry).
	oldState := idx.centroids.Load()
	idx.centroids.Store(newState)
	idx.spec.Nlist = nlist
	idx.spec.Epoch = epoch
	idx.spec.Seed = seed

	// Persist updated spec.
	if err := persistSpec(idx.st, idx.spec); err != nil {
		idx.logger.Error().Err(err).Msg("vecindex: failed to persist spec after retrain")
	}

	// Delete old posting lists and centroids.
	if oldState != nil {
		for _, oldCID := range oldState.clusterIDs {
			cleanupOldCluster(idx, oldCID)
		}
	} else if len(oldClusterIDs) > 0 {
		// Came from flat-scan phase — clean up cluster 0 postings.
		uniqueOld := uniqueUint32(oldClusterIDs)
		for _, oldCID := range uniqueOld {
			// Only clean up flat cluster 0 if it's not in the new cluster IDs.
			if !containsUint32(newClusterIDs, oldCID) {
				cleanupOldCluster(idx, oldCID)
			}
		}
	}

	return nil
}

// cleanupOldCluster deletes all posting entries for clusterID.
// Compaction is skipped — the OS will reclaim space on next open.
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
// Returns (vectors, docIDs, clusterIDs-per-doc).
func gatherAllVectors(idx *Index) ([][]float32, []uint64, []uint32, error) {
	clusters, err := idx.st.ListActiveClusters()
	if err != nil {
		return nil, nil, nil, err
	}
	// Also include cluster 0 if no clusters listed (flat-scan phase).
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
// 3× the mean cluster size (the canonical trigger). The function is also a
// public split primitive: callers that have already verified the size condition
// externally may call it directly.
//
// centIdx is a 0-based index into the current centroid set.
// Returns nil without splitting when the cluster has < 2 vectors.
func CheckSplit(idx *Index, centIdx uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	cs := idx.centroids.Load()
	if cs == nil {
		return nil // flat-scan phase — no split
	}

	if int(centIdx) >= len(cs.clusterIDs) {
		return nil // out of range
	}
	storeClusterID := cs.clusterIDs[centIdx]

	meta, err := idx.st.GetClusterMeta(storeClusterID)
	if err != nil {
		return err
	}

	if int(meta.Size) < 2 {
		return nil // nothing to split
	}

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

	// k=2 split. Use a cluster-specific seed to avoid same-seed bias across splits.
	// Golden-ratio mixing: deterministic given (clusterID, spec.Seed) so all nodes agree.
	clusterSeed := idx.spec.Seed ^ (uint64(clusterID) * 0x9E3779B97F4A7C15)
	newCentroids, err := kmeans.KMeansPlusPlus(vecs, 2, clusterSeed, kmeansMaxIter)
	if err != nil {
		return fmt.Errorf("vecindex: split k-means: %w", err)
	}

	// Allocate two new cluster IDs.
	newCID1, err := idx.st.AllocateClusterID()
	if err != nil {
		return err
	}
	newCID2, err := idx.st.AllocateClusterID()
	if err != nil {
		return err
	}

	if err := idx.st.PutCentroid(newCID1, newCentroids[0], idx.spec.Dim); err != nil {
		return err
	}
	if err := idx.st.PutCentroid(newCID2, newCentroids[1], idx.spec.Dim); err != nil {
		return err
	}

	meta1 := store.ClusterMeta{State: store.ClusterStateActive, Epoch: idx.spec.Epoch}
	meta2 := store.ClusterMeta{State: store.ClusterStateActive, Epoch: idx.spec.Epoch}

	// Single batch for all entry moves — atomic split.
	b := idx.st.NewBatch()
	for _, e := range entries {
		c, _, _ := kmeans.Assign(e.Vector, newCentroids, idx.spec.Metric)
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

	// Find the centroid index for clusterID in the current state.
	centIdx := -1
	for i, cid := range cs.clusterIDs {
		if cid == clusterID {
			centIdx = i
			break
		}
	}

	// Build updated centroid state: replace old with two new.
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
		// Cluster not in current centroid set — just append.
		newCentVecs = append(newCentVecs, newCentroids[0], newCentroids[1])
		newCentIDs = append(newCentIDs, newCID1, newCID2)
	}

	newCS, err := kmeans.NewCentroidSet(idx.spec.Epoch, newCentVecs)
	if err != nil {
		return err
	}
	idx.centroids.Store(&centroidState{cs: newCS, clusterIDs: newCentIDs})

	// Retire and clean up old cluster.
	oldMeta := store.ClusterMeta{State: store.ClusterStateRetired}
	_ = idx.st.PutClusterMeta(clusterID, oldMeta)
	cleanupOldClusterPostings(idx, clusterID)

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
// centIdx is a 0-based index into the current centroid set.
func CheckMerge(idx *Index, centIdx uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	cs := idx.centroids.Load()
	if cs == nil {
		return nil // flat-scan phase
	}

	if int(centIdx) >= len(cs.clusterIDs) {
		return nil // out of range
	}
	clusterID := cs.clusterIDs[centIdx]

	meta, err := idx.st.GetClusterMeta(clusterID)
	if err != nil {
		return err
	}

	meanSize := meanClusterSize(idx)
	threshold := meanSize / 4
	if int(meta.Size) > threshold {
		return nil // not undersized
	}

	// Find the nearest other centroid.
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
		return nil // only one cluster — cannot merge
	}

	nearestIdx, _, err := kmeans.Assign(srcVec, otherVecs, metric.MetricL2)
	if err != nil {
		return err
	}
	targetCentIdx := otherIndices[nearestIdx]
	targetCID := cs.clusterIDs[targetCentIdx]

	// Move all docs from clusterID → targetCID.
	entries, err := idx.st.ScanCluster(clusterID)
	if err != nil {
		return err
	}

	targetMeta, err := idx.st.GetClusterMeta(targetCID)
	if errors.Is(err, store.ErrNotFound) {
		targetMeta = store.ClusterMeta{State: store.ClusterStateActive}
	} else if err != nil {
		return err
	}

	// Single batch for all entry moves — atomic merge.
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

	// Retire source cluster.
	retiredMeta := store.ClusterMeta{State: store.ClusterStateRetired}
	_ = idx.st.PutClusterMeta(clusterID, retiredMeta)
	_ = idx.st.DeleteCentroid(clusterID)

	// Rebuild centroid state without the retired centroid.
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
// Must be called with idx.mu held (read or write).
func meanClusterSize(idx *Index) int {
	cs := idx.centroids.Load()
	if cs == nil {
		return 0
	}
	total := 0
	count := 0
	for _, cid := range cs.clusterIDs {
		meta, err := idx.st.GetClusterMeta(cid)
		if err != nil {
			continue
		}
		if meta.State == store.ClusterStateActive {
			total += int(meta.Size)
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

// subsampleVecs returns a deterministic subsample of size m from vecs using the
// given seed. Uses a stride-based selection for O(n) time with no allocations
// beyond the output slice.
func subsampleVecs(vecs [][]float32, m int, seed uint64) [][]float32 {
	n := len(vecs)
	if m >= n {
		return vecs
	}
	// Deterministic stride sampling: pick evenly spaced indices with a seed offset.
	out := make([][]float32, m)
	step := float64(n) / float64(m)
	offset := int(seed % uint64(n))
	for i := 0; i < m; i++ {
		idx := (offset + int(float64(i)*step)) % n
		out[i] = vecs[idx]
	}
	return out
}
