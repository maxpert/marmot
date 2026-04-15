package vecindex

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/rs/zerolog"
)

// clusterShards is the number of shards for per-cluster mutex striping.
// 256 shards reduces contention to ~0.4% collision probability for 64 clusters.
const clusterShards = 256

// bulkBlockSize is the docID block size reserved per worker in parallel bulk insert.
const bulkBlockSize = 1024

// bulkBatchSize is the number of entries committed per Pebble batch.
const bulkBatchSize = 4096

// publishInterval is how often the background centroid publisher checks for dirty clusters.
const publishInterval = 50 * time.Millisecond

// publishDirtyThreshold is the minimum count increase since last publish to trigger a centroid update.
const publishDirtyThreshold = 64

// clusterStats holds in-memory MacQueen online k-means state for one cluster.
type clusterStats struct {
	sum   []float64 // running sum of all vectors assigned
	count uint64    // number of vectors assigned
}

// LOCK ORDER: onlineMu must never be acquired while holding a shard lock.
// Correct order when both are needed: (1) shard lock, (2) onlineMu.
// publishDirtyCentroids takes onlineMu for a snapshot copy, releases it, then
// acquires shard locks individually — this is the only safe pattern.
// macQueenUpdate is called under the shard lock and acquires onlineMu internally;
// callers must never hold onlineMu before calling macQueenUpdate.

// Index is an open IVF vector index backed by a Pebble store.
type Index struct {
	spec          IVFSpec
	st            *store.Store
	logger        zerolog.Logger
	centroids     atomic.Pointer[centroidState]
	vectorCount   atomic.Uint64
	lastNprobe    atomic.Uint64
	closed        atomic.Bool
	compactNotify chan uint32 // single-slot channel; background worker drains
	compactDone   chan struct{}
	publishDone   chan struct{}
	stopPublish   chan struct{} // closed by Close to immediately stop publishWorker

	// clusterLocks: 256-shard striped mutex for per-cluster state mutations.
	locks [clusterShards]sync.Mutex

	// onlineMu guards onlineStats map mutations (insertion of new cluster entries only).
	onlineMu    sync.Mutex
	onlineStats map[uint32]*clusterStats // clusterID → MacQueen state

	// publishCounts tracks the count value at the last centroid publish per cluster.
	// Keyed by clusterID. Access under per-shard lock.
	publishCounts map[uint32]uint64

	// docIDCounter is the in-memory next docID to allocate (block-allocated).
	// docIDBlockEnd is the exclusive upper bound of the currently-reserved block.
	// docIDAllocMu serialises Pebble block reservations.
	docIDCounter  atomic.Uint64
	docIDBlockEnd atomic.Uint64
	docIDAllocMu  sync.Mutex
}

// centroidState bundles a CentroidSet with the corresponding store cluster IDs.
// centroidIDs[i] is the store cluster ID for centroid index i.
type centroidState struct {
	cs         *kmeans.CentroidSet
	clusterIDs []uint32
}

// clusterLock returns the mutex shard for clusterID.
func (idx *Index) clusterLock(clusterID uint32) *sync.Mutex {
	return &idx.locks[clusterID%clusterShards]
}

// docIDBlockSize is the number of docIDs reserved per Pebble write.
// Unused IDs in the last block are abandoned on close (no collision risk).
const docIDBlockSize = 1024

// newIndex constructs an Index without loading existing data.
// It loads the persisted docID counter and reserves the first block.
func newIndex(spec IVFSpec, st *store.Store, logger zerolog.Logger) *Index {
	idx := &Index{
		spec:          spec,
		st:            st,
		logger:        logger,
		compactNotify: make(chan uint32, 1),
		compactDone:   make(chan struct{}),
		publishDone:   make(chan struct{}),
		stopPublish:   make(chan struct{}),
		onlineStats:   make(map[uint32]*clusterStats),
		publishCounts: make(map[uint32]uint64),
	}
	idx.reserveDocIDBlock()
	go idx.compactWorker()
	go idx.publishWorker()
	return idx
}

// reserveDocIDBlock reads the persisted counter, reserves the next block of
// docIDBlockSize IDs via a Sync Pebble write, and sets in-memory bounds.
// Must be called with docIDAllocMu held, or during single-threaded init.
func (idx *Index) reserveDocIDBlock() {
	val, closer, err := idx.st.DB().Get(keyDocIDCounter)
	var persisted uint64
	if err == nil {
		persisted = binary.BigEndian.Uint64(val)
		closer.Close()
	}
	blockEnd := persisted + docIDBlockSize
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, blockEnd)
	_ = idx.st.DB().Set(keyDocIDCounter, buf, pebble.Sync)
	idx.docIDCounter.Store(persisted)
	idx.docIDBlockEnd.Store(blockEnd)
}

// compactWorker drains compactNotify and runs Pebble compaction asynchronously.
func (idx *Index) compactWorker() {
	defer close(idx.compactDone)
	for clusterID := range idx.compactNotify {
		if idx.closed.Load() {
			return
		}
		_ = idx.st.CompactCluster(clusterID)
	}
}

// publishWorker periodically recomputes centroids from online MacQueen state
// and swaps the atomic centroidState pointer when clusters are sufficiently dirty.
func (idx *Index) publishWorker() {
	defer close(idx.publishDone)
	ticker := time.NewTicker(publishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-idx.stopPublish:
			return
		case <-ticker.C:
			idx.publishDirtyCentroids()
		}
	}
}

// publishDirtyCentroids recomputes centroids for clusters whose count has grown
// by at least publishDirtyThreshold since the last publish, then swaps the
// atomic centroidState pointer.
func (idx *Index) publishDirtyCentroids() {
	cs := idx.centroids.Load()
	if cs == nil {
		return
	}

	// Deep-copy the stats under onlineMu so the publisher never races with
	// macQueenUpdate writes to clusterStats.count/sum.
	idx.onlineMu.Lock()
	stats := make(map[uint32]*clusterStats, len(idx.onlineStats))
	for k, v := range idx.onlineStats {
		cp := &clusterStats{
			count: v.count,
			sum:   make([]float64, len(v.sum)),
		}
		copy(cp.sum, v.sum)
		stats[k] = cp
	}
	idx.onlineMu.Unlock()

	if len(stats) == 0 {
		return
	}

	dirty := false
	newCentVecs := make([][]float32, cs.cs.Len())
	for i := range newCentVecs {
		v, err := cs.cs.GetReadOnly(uint32(i))
		if err != nil {
			return
		}
		newCentVecs[i] = v
	}

	for i, cid := range cs.clusterIDs {
		st, ok := stats[cid]
		if !ok || st.count == 0 {
			continue
		}

		lk := idx.clusterLock(cid)
		lk.Lock()
		lastCount := idx.publishCounts[cid]
		currentCount := st.count
		lk.Unlock()

		if currentCount-lastCount < publishDirtyThreshold {
			continue
		}

		// Recompute centroid = sum/count.
		dim := len(st.sum)
		centroid := make([]float32, dim)
		inv := 1.0 / float64(currentCount)
		for d := range centroid {
			centroid[d] = float32(st.sum[d] * inv)
		}
		newCentVecs[i] = centroid
		dirty = true

		lk.Lock()
		idx.publishCounts[cid] = currentCount
		lk.Unlock()

		// Persist updated centroid to store.
		if err := idx.st.PutCentroid(cid, centroid, idx.spec.InternalDim()); err != nil {
			idx.logger.Error().Err(err).Uint32("clusterID", cid).Msg("vecindex: persist centroid update failed")
		}
	}

	if !dirty {
		return
	}

	newCS, err := kmeans.NewCentroidSet(cs.cs.Epoch(), newCentVecs)
	if err != nil {
		return
	}
	newState := &centroidState{cs: newCS, clusterIDs: cs.clusterIDs}
	// Only publish if no split/merge has changed the centroid set since we loaded it.
	idx.centroids.CompareAndSwap(cs, newState)
}

// initClusterStats initialises online MacQueen state from a full vector assignment
// after graduation. Called once per graduation; not on hot path.
func (idx *Index) initClusterStats(clusterIDs []uint32, centroids [][]float32, vecs [][]float32) {
	stats := make(map[uint32]*clusterStats, len(clusterIDs))
	for i, cid := range clusterIDs {
		stats[cid] = &clusterStats{
			sum:   make([]float64, idx.spec.InternalDim()),
			count: 0,
		}
		// Seed sum from the k-means centroid itself.
		for d, x := range centroids[i] {
			stats[cid].sum[d] = float64(x)
		}
		stats[cid].count = 1
	}

	// Accumulate each vector into its assigned cluster.
	for _, vec := range vecs {
		centIdx, _, err := kmeans.Assign(vec, centroids, idx.storageMetric())
		if err != nil {
			continue
		}
		if int(centIdx) >= len(clusterIDs) {
			continue
		}
		cid := clusterIDs[centIdx]
		st := stats[cid]
		st.count++
		for d, x := range vec {
			st.sum[d] += float64(x)
		}
	}
	idx.onlineMu.Lock()
	for k, v := range stats {
		idx.onlineStats[k] = v
	}
	idx.onlineMu.Unlock()
}

// seedClusterStats seeds online MacQueen state for a newly split cluster.
func (idx *Index) seedClusterStats(clusterID uint32, centroid []float32, size uint32) {
	dim := len(centroid)
	st := &clusterStats{
		sum:   make([]float64, dim),
		count: uint64(size),
	}
	inv := float64(size)
	for d, x := range centroid {
		st.sum[d] = float64(x) * inv
	}

	idx.onlineMu.Lock()
	idx.onlineStats[clusterID] = st
	idx.onlineMu.Unlock()
}

// macQueenUpdate applies the online MacQueen centroid update for clusterID.
// Must be called while holding the per-cluster shard lock.
func (idx *Index) macQueenUpdate(clusterID uint32, vec []float32) {
	idx.onlineMu.Lock()
	st, ok := idx.onlineStats[clusterID]
	if !ok {
		dim := len(vec)
		st = &clusterStats{
			sum:   make([]float64, dim),
			count: 0,
		}
		idx.onlineStats[clusterID] = st
	}
	// n_k ← n_k + 1; c_k ← c_k + (x − c_k) / n_k — all under onlineMu.
	st.count++
	for d, x := range vec {
		st.sum[d] += float64(x)
	}
	idx.onlineMu.Unlock()
}

// loadCentroids reads centroids from the store and populates the atomic pointer.
func (idx *Index) loadCentroids() error {
	clusterIDs, vecs, err := idx.st.ListCentroids()
	if err != nil {
		return err
	}
	if len(clusterIDs) == 0 {
		idx.vectorCount.Store(idx.countVectors())
		return nil
	}

	cs, err := kmeans.NewCentroidSet(idx.spec.Epoch, vecs)
	if err != nil {
		return fmt.Errorf("index: build centroid set: %w", err)
	}
	idx.centroids.Store(&centroidState{cs: cs, clusterIDs: clusterIDs})
	idx.vectorCount.Store(idx.countVectors())
	return nil
}

// countVectors tallies live vectors across all active clusters.
func (idx *Index) countVectors() uint64 {
	clusterIDs, err := idx.st.ListActiveClusters()
	if err != nil {
		return 0
	}
	var total uint64
	for _, cid := range clusterIDs {
		meta, err := idx.st.GetClusterMeta(cid)
		if err != nil {
			continue
		}
		total += uint64(meta.Size)
	}
	return total
}

// bulkLoad inserts all entries using GOMAXPROCS parallel workers.
// Each worker reserves a block of bulkBlockSize docIDs via atomic CAS on the
// persistent counter and writes its own Pebble batch.
func (idx *Index) bulkLoad(ctx context.Context, bulk []BulkEntry) error {
	if len(bulk) == 0 {
		return nil
	}

	// Ensure cluster 0 meta exists.
	meta, err := idx.st.GetClusterMeta(0)
	if errors.Is(err, store.ErrNotFound) {
		meta = store.ClusterMeta{State: store.ClusterStateActive}
	} else if err != nil {
		return err
	}

	n := len(bulk)
	nWorkers := runtime.GOMAXPROCS(0)
	chunkSize := (n + nWorkers - 1) / nWorkers

	type workerResult struct {
		count uint32
		err   error
	}
	results := make([]workerResult, nWorkers)

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
		go func(lo, hi, slot int) {
			defer wg.Done()

			b := idx.st.NewBatch()
			batchCount := 0
			var workerCount uint32

			for i := lo; i < hi; i++ {
				if ctx.Err() != nil {
					_ = b.Close()
					results[slot].err = ctx.Err()
					return
				}
				e := bulk[i]
				docID := uint64(i) // 0-indexed to match brute-force truth indices

				if bErr := b.BatchPutPosting(0, docID, e.Vector); bErr != nil {
					_ = b.Close()
					results[slot].err = bErr
					return
				}
				if bErr := b.BatchPutReverseMap(docID, 0); bErr != nil {
					_ = b.Close()
					results[slot].err = bErr
					return
				}
				if bErr := b.BatchPutExtToDoc(e.ExternalID, docID); bErr != nil {
					_ = b.Close()
					results[slot].err = bErr
					return
				}
				if bErr := b.BatchPutDocToExt(docID, e.ExternalID); bErr != nil {
					_ = b.Close()
					results[slot].err = bErr
					return
				}
				workerCount++
				batchCount++

				if batchCount%bulkBatchSize == 0 {
					if cErr := b.Commit(); cErr != nil {
						results[slot].err = cErr
						return
					}
					b = idx.st.NewBatch()
					batchCount = 0
				}
			}
			if cErr := b.Commit(); cErr != nil {
				results[slot].err = cErr
				return
			}
			results[slot].count = workerCount
		}(start, end, w)
	}
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		meta.Size += r.count
	}

	// Persist cluster meta with final size.
	if err := idx.st.PutClusterMeta(0, meta); err != nil {
		return err
	}

	// Seed docID counter beyond bulk range so nextDocID never collides with
	// bulk-assigned IDs. Use Sync to ensure durability before any subsequent
	// Upserts read the counter.
	if n > 0 {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(n-1))
		_ = idx.st.DB().Set(keyDocIDCounter, buf, pebble.Sync)
		// Reset the in-memory block so the next nextDocID() call allocates a
		// fresh block starting above the bulk range.
		idx.docIDCounter.Store(uint64(n - 1))
		idx.docIDBlockEnd.Store(uint64(n - 1))
	}

	idx.vectorCount.Store(uint64(n))
	return nil
}

// Search returns up to req.K nearest neighbours for the given query vector.
// For MetricDot, query is MIPS-augmented and returned distances are -⟨q,v⟩
// (lower is more similar, consistent with other metrics).
func (idx *Index) Search(ctx context.Context, req SearchRequest) ([]SearchHit, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var queryNorm2 float32
	if idx.spec.Metric == MetricDot {
		queryNorm2 = metric.Norm2(req.Vector)
		req.Vector = metric.AugmentQuery(req.Vector, nil)
	} else {
		// Normalize query for cosine metric.
		req.Vector = normalizeVec(req.Vector, idx.spec.Metric)
	}

	cs := idx.centroids.Load()
	var hits []SearchHit
	var err error
	if cs == nil {
		hits, err = idx.flatSearch(ctx, req)
	} else {
		hits, err = idx.ivfSearch(ctx, req, cs)
	}
	if err != nil || idx.spec.Metric != MetricDot {
		return hits, err
	}

	// Convert L2² on augmented space back to -⟨q,v⟩ so callers get meaningful
	// inner-product distances (lower = more similar).
	maxNorm := idx.spec.MaxNorm
	for i := range hits {
		dot := metric.RecoverDotFromL2Sq(hits[i].Distance, queryNorm2, maxNorm)
		hits[i].Distance = -dot
	}
	return hits, nil
}

// flatSearch scans all vectors in cluster 0 and returns exact top-k.
func (idx *Index) flatSearch(ctx context.Context, req SearchRequest) ([]SearchHit, error) {
	k := req.K
	idx.lastNprobe.Store(1)

	h := &hitHeap{}
	heap.Init(h)

	scanErr := idx.st.ScanClusterFunc(0, func(docID uint64, vecBytes []byte) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d := metric.DistanceFromBytes(idx.storageMetric(), req.Vector, vecBytes)
		heap.Push(h, SearchHit{DocID: docID, Distance: d})
		if h.Len() > k {
			heap.Pop(h)
		}
		return nil
	})
	if scanErr != nil {
		return nil, scanErr
	}
	if h.Len() == 0 {
		return nil, nil
	}

	hits := sortedHits(h, k)
	for i := range hits {
		extID, err := idx.st.GetDocToExt(hits[i].DocID)
		if err != nil {
			continue
		}
		hits[i].ExternalID = extID
	}
	return hits, nil
}

// ivfSearch performs IVF-based approximate nearest-neighbour search.
// Lock-free: reads centroidState via atomic.Pointer; no mutex acquired.
func (idx *Index) ivfSearch(ctx context.Context, req SearchRequest, cs *centroidState) ([]SearchHit, error) {
	nprobe := idx.spec.Nprobe
	if req.NprobeOverride > 0 {
		nprobe = req.NprobeOverride
	}
	if nprobe <= 0 {
		nprobe = nprobeDefault(cs.cs.Len())
	}

	centVecs := make([][]float32, cs.cs.Len())
	for i := range centVecs {
		v, err := cs.cs.GetReadOnly(uint32(i))
		if err != nil {
			return nil, fmt.Errorf("index: get centroid %d: %w", i, err)
		}
		centVecs[i] = v
	}

	fetchN := nprobe + 1
	if fetchN > cs.cs.Len() {
		fetchN = cs.cs.Len()
	}
	ids, dists, err := kmeans.AssignTopN(req.Vector, centVecs, fetchN, idx.storageMetric())
	if err != nil {
		return nil, fmt.Errorf("index: assign centroids: %w", err)
	}

	// Adaptive multi-probe: one-shot conservative bump when the query sits near a
	// Voronoi boundary (2nd centroid within 5% of nearest).
	if len(dists) >= 2 && dists[0] > 0 && dists[1]/dists[0] < 1.05 {
		extra := nprobe / 4
		if extra < 2 {
			extra = 2
		}
		bump := nprobe + extra
		if bump > cs.cs.Len() {
			bump = cs.cs.Len()
		}
		if bump > nprobe {
			fetchBump := bump + 1
			if fetchBump > cs.cs.Len() {
				fetchBump = cs.cs.Len()
			}
			newIDs, newDists, assignErr := kmeans.AssignTopN(req.Vector, centVecs, fetchBump, idx.storageMetric())
			if assignErr != nil {
				return nil, assignErr
			}
			ids, dists = newIDs, newDists
			nprobe = bump
		}
	}

	if len(ids) > nprobe {
		ids = ids[:nprobe]
	}

	idx.lastNprobe.Store(uint64(len(ids)))

	k := req.K

	// Parallel scan: cap goroutines at min(nprobe, GOMAXPROCS×2).
	concurrency := runtime.GOMAXPROCS(0) * 2
	if concurrency > len(ids) {
		concurrency = len(ids)
	}
	sem := make(chan struct{}, concurrency)

	type clusterResult struct {
		hits []SearchHit
		err  error
	}
	results := make([]clusterResult, len(ids))

	var wg sync.WaitGroup
	for i, centIdx := range ids {
		if int(centIdx) >= len(cs.clusterIDs) {
			continue
		}
		clusterID := cs.clusterIDs[centIdx]
		wg.Add(1)
		sem <- struct{}{}
		go func(slot int, cid uint32) {
			defer wg.Done()
			defer func() { <-sem }()

			if ctx.Err() != nil {
				results[slot].err = ctx.Err()
				return
			}
			lh := &hitHeap{}
			heap.Init(lh)
			scanErr := idx.st.ScanClusterFunc(cid, func(docID uint64, vecBytes []byte) error {
				d := metric.DistanceFromBytes(idx.storageMetric(), req.Vector, vecBytes)
				heap.Push(lh, SearchHit{DocID: docID, Distance: d})
				if lh.Len() > k {
					heap.Pop(lh)
				}
				return nil
			})
			results[slot].err = scanErr
			results[slot].hits = sortedHits(lh, k)
		}(i, clusterID)
	}
	wg.Wait()

	nlist := cs.cs.Len()
	avgClusterSize := 1
	if nlist > 0 {
		total := idx.vectorCount.Load()
		avgClusterSize = int(total)/nlist + 1
	}
	seen := make(map[uint64]struct{}, len(ids)*avgClusterSize)
	h := &hitHeap{}
	heap.Init(h)
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		for _, hit := range r.hits {
			if _, dup := seen[hit.DocID]; dup {
				continue
			}
			seen[hit.DocID] = struct{}{}
			heap.Push(h, hit)
			if h.Len() > k {
				heap.Pop(h)
			}
		}
	}

	hits := sortedHits(h, k)
	for i := range hits {
		extID, err := idx.st.GetDocToExt(hits[i].DocID)
		if err != nil {
			continue
		}
		hits[i].ExternalID = extID
	}
	return hits, nil
}

// Upsert inserts or updates the vector associated with externalID.
// For MetricDot, the vector is MIPS-augmented before storage; any vector whose
// L2 norm exceeds IVFSpec.MaxNorm is rejected.
func (idx *Index) Upsert(ctx context.Context, externalID []byte, vec []float32, txnID, seqID uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	var err error
	switch idx.spec.Metric {
	case MetricDot:
		vec, err = metric.AugmentData(vec, idx.spec.MaxNorm, nil)
		if err != nil {
			return fmt.Errorf("vecindex: upsert %s: %w", externalID, err)
		}
	case MetricCosine:
		vec = normalizeVec(vec, idx.spec.Metric)
	}

	// Check watermark for idempotency.
	wmTxn, wmSeq, wmFound := idx.getWatermark(externalID)
	if wmFound && (txnID < wmTxn || (txnID == wmTxn && seqID <= wmSeq)) {
		return nil // stale or duplicate
	}

	existingDocID, err := idx.st.GetExtToDoc(externalID)
	isUpdate := err == nil

	var oldClusterID uint32
	var oldMeta store.ClusterMeta
	if isUpdate {
		if cid, cerr := idx.st.GetClusterForDoc(existingDocID); cerr == nil {
			oldClusterID = cid
			if m, merr := idx.st.GetClusterMeta(cid); merr == nil {
				oldMeta = m
			}
		}
	}

	// Assign new cluster (lock-free read of centroid state).
	var clusterID uint32
	cs := idx.centroids.Load()
	if cs != nil {
		centVecs := make([][]float32, cs.cs.Len())
		for i := range centVecs {
			v, _ := cs.cs.GetReadOnly(uint32(i))
			centVecs[i] = v
		}
		centIdx, _, assignErr := kmeans.Assign(vec, centVecs, idx.storageMetric())
		if assignErr == nil && int(centIdx) < len(cs.clusterIDs) {
			clusterID = cs.clusterIDs[centIdx]
		}
	}

	var docID uint64
	if isUpdate {
		docID = existingDocID
	} else {
		docID = idx.nextDocID()
	}

	// Hold the shard lock for the entire read-meta → increment → batch-commit
	// sequence so concurrent Upserts to the same cluster never overwrite each
	// other's size writes (HR-01 TOCTOU fix).
	lk := idx.clusterLock(clusterID)
	lk.Lock()

	newMeta, err := idx.st.GetClusterMeta(clusterID)
	if errors.Is(err, store.ErrNotFound) {
		newMeta = store.ClusterMeta{State: store.ClusterStateActive}
	} else if err != nil {
		lk.Unlock()
		return err
	}

	b := idx.st.NewBatch()
	if isUpdate {
		if batchErr := b.BatchDeletePosting(oldClusterID, existingDocID); batchErr != nil {
			_ = b.Close()
			lk.Unlock()
			return batchErr
		}
		if oldClusterID != clusterID {
			// Acquire old-cluster lock using canonical order (lower ID first) to
			// prevent deadlock with concurrent merges/upserts on the same pair.
			lkOld := idx.clusterLock(oldClusterID)
			if oldClusterID%clusterShards < clusterID%clusterShards {
				// lkOld maps to a lower or equal shard index — must be taken first,
				// but lk is already held. Unlock lk, take lkOld, retake lk.
				lk.Unlock()
				lkOld.Lock()
				lk.Lock()
				// Re-read newMeta because another goroutine may have modified it.
				newMeta, err = idx.st.GetClusterMeta(clusterID)
				if errors.Is(err, store.ErrNotFound) {
					newMeta = store.ClusterMeta{State: store.ClusterStateActive}
				} else if err != nil {
					lkOld.Unlock()
					lk.Unlock()
					_ = b.Close()
					return err
				}
			} else {
				lkOld.Lock()
			}
			if oldMeta.Size > 0 {
				oldMeta.Size--
			}
			if batchErr := b.BatchPutClusterMeta(oldClusterID, oldMeta); batchErr != nil {
				lkOld.Unlock()
				lk.Unlock()
				_ = b.Close()
				return batchErr
			}
			lkOld.Unlock()
			newMeta.Size++
		}
		// Same cluster: net size unchanged — no Size mutation needed.
	} else {
		newMeta.Size++
	}

	if err := b.BatchPutPosting(clusterID, docID, vec); err != nil {
		lk.Unlock()
		_ = b.Close()
		return err
	}
	if err := b.BatchPutReverseMap(docID, clusterID); err != nil {
		lk.Unlock()
		_ = b.Close()
		return err
	}
	if err := b.BatchPutExtToDoc(externalID, docID); err != nil {
		lk.Unlock()
		_ = b.Close()
		return err
	}
	if err := b.BatchPutDocToExt(docID, externalID); err != nil {
		lk.Unlock()
		_ = b.Close()
		return err
	}
	if err := b.BatchPutClusterMeta(clusterID, newMeta); err != nil {
		lk.Unlock()
		_ = b.Close()
		return err
	}
	if err := b.BatchPutWatermark(externalID, txnID, seqID); err != nil {
		lk.Unlock()
		_ = b.Close()
		return err
	}
	if err := b.Commit(); err != nil {
		lk.Unlock()
		_ = b.Close()
		return err
	}

	// MacQueen online centroid update — after successful Pebble commit, still
	// under the shard lock (macQueenUpdate acquires onlineMu internally; see
	// LOCK ORDER comment above the Index struct definition).
	idx.macQueenUpdate(clusterID, vec)
	lk.Unlock()

	if !isUpdate {
		idx.vectorCount.Add(1)
	}
	return nil
}

// Delete removes the vector associated with externalID from the index.
func (idx *Index) Delete(ctx context.Context, externalID []byte, txnID, seqID uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	docID, err := idx.st.GetExtToDoc(externalID)
	if errors.Is(err, store.ErrNotFound) {
		b := idx.st.NewBatch()
		if batchErr := b.BatchPutWatermark(externalID, txnID, seqID); batchErr != nil {
			_ = b.Close()
			return batchErr
		}
		return b.Commit()
	}
	if err != nil {
		return err
	}

	clusterID, err := idx.st.GetClusterForDoc(docID)
	if err != nil {
		return err
	}

	b := idx.st.NewBatch()
	if err := b.BatchDeletePosting(clusterID, docID); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.BatchDeleteExtMapping(externalID, docID); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.BatchDeleteReverseMap(docID); err != nil {
		_ = b.Close()
		return err
	}

	var meta store.ClusterMeta
	var metaErr error
	meta, metaErr = idx.st.GetClusterMeta(clusterID)
	if metaErr == nil {
		lk := idx.clusterLock(clusterID)
		lk.Lock()
		if meta.Size > 0 {
			meta.Size--
		}
		meta.TombstoneCount++
		lk.Unlock()
		if err := b.BatchPutClusterMeta(clusterID, meta); err != nil {
			_ = b.Close()
			return err
		}
	}
	if err := b.BatchPutWatermark(externalID, txnID, seqID); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.Commit(); err != nil {
		_ = b.Close()
		return err
	}

	idx.vectorCount.Add(^uint64(0))

	if metaErr == nil && store.ShouldCompact(meta) && !idx.closed.Load() {
		select {
		case idx.compactNotify <- clusterID:
		default:
		}
	}

	return nil
}

// Stats returns point-in-time statistics for this index.
func (idx *Index) Stats() Stats {
	cs := idx.centroids.Load()
	var centCount, epoch uint64
	if cs != nil {
		centCount = uint64(cs.cs.Len())
		epoch = cs.cs.Epoch()
	}
	return Stats{
		VectorCount:     idx.vectorCount.Load(),
		CentroidCount:   centCount,
		Epoch:           epoch,
		LastQueryNprobe: idx.lastNprobe.Load(),
	}
}

// Close releases resources held by this index.
func (idx *Index) Close() error {
	idx.closed.Store(true)
	close(idx.stopPublish)
	close(idx.compactNotify)
	<-idx.compactDone
	<-idx.publishDone
	return idx.st.Close()
}

// keyDocIDCounter is the pebble key for the monotonic docID counter.
var keyDocIDCounter = []byte{0x09, 0x01}

// nextDocID allocates a new monotonically-increasing document ID.
// Uses a block-allocation scheme: IDs are dispensed from an in-memory counter
// (atomic increment); when the in-memory block is exhausted a new block is
// reserved via a single Sync Pebble write under docIDAllocMu.  Concurrent
// callers never collide — each gets a unique value from the atomic counter.
func (idx *Index) nextDocID() uint64 {
	id := idx.docIDCounter.Add(1)
	if id <= idx.docIDBlockEnd.Load() {
		return id
	}
	// Block exhausted — reserve a new one under the allocation mutex.
	idx.docIDAllocMu.Lock()
	defer idx.docIDAllocMu.Unlock()
	// Re-check after acquiring the lock: another goroutine may have already
	// reserved a new block while we were waiting.
	id = idx.docIDCounter.Add(1)
	if id <= idx.docIDBlockEnd.Load() {
		return id
	}
	// We are the designated block allocator for this round.
	// Roll back the spurious Add before reserveDocIDBlock resets the counter.
	idx.docIDCounter.Add(^uint64(0)) // subtract 1
	idx.reserveDocIDBlock()
	return idx.docIDCounter.Add(1)
}

// getWatermark reads (txnID, seqID) for externalID from the 0x08 namespace.
func (idx *Index) getWatermark(externalID []byte) (txnID, seqID uint64, found bool) {
	key := store.EncodeWatermarkKey(externalID)
	val, closer, err := idx.st.DB().Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, 0, false
	}
	if err != nil || len(val) < 16 {
		return 0, 0, false
	}
	defer closer.Close()
	txnID = binary.BigEndian.Uint64(val[:8])
	seqID = binary.BigEndian.Uint64(val[8:16])
	return txnID, seqID, true
}

// storageMetric returns the metric used for distance computations on stored vectors.
// For MetricDot, vectors are MIPS-augmented so L2Squared is the correct storage metric.
// For all other metrics it equals idx.spec.Metric.
func (idx *Index) storageMetric() metric.Metric {
	if idx.spec.Metric == MetricDot {
		return MetricL2
	}
	return idx.spec.Metric
}

// hitHeap is a max-heap of SearchHit ordered by Distance (largest distance at top).
type hitHeap []SearchHit

func (h hitHeap) Len() int            { return len(h) }
func (h hitHeap) Less(i, j int) bool  { return h[i].Distance > h[j].Distance }
func (h hitHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *hitHeap) Push(x interface{}) { *h = append(*h, x.(SearchHit)) }
func (h *hitHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// sortedHits drains the max-heap and returns hits sorted ascending by distance.
func sortedHits(h *hitHeap, k int) []SearchHit {
	n := h.Len()
	if n > k {
		n = k
	}
	result := make([]SearchHit, n)
	for i := n - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(SearchHit)
	}
	return result
}
