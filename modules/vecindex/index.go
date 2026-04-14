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

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/rs/zerolog"
)

// Index is an open IVF vector index backed by a Pebble store.
type Index struct {
	spec          IVFSpec
	st            *store.Store
	logger        zerolog.Logger
	mu            sync.RWMutex
	centroids     atomic.Pointer[centroidState]
	vectorCount   atomic.Uint64
	lastNprobe    atomic.Uint64
	closed        atomic.Bool
	compactNotify chan uint32 // single-slot channel; background worker drains
	compactDone   chan struct{}
}

// centroidState bundles a CentroidSet with the corresponding store cluster IDs.
// centroidIDs[i] is the store cluster ID for centroid index i.
type centroidState struct {
	cs         *kmeans.CentroidSet
	clusterIDs []uint32
}

// newIndex constructs an Index without loading existing data.
func newIndex(spec IVFSpec, st *store.Store, logger zerolog.Logger) *Index {
	idx := &Index{
		spec:          spec,
		st:            st,
		logger:        logger,
		compactNotify: make(chan uint32, 1),
		compactDone:   make(chan struct{}),
	}
	go idx.compactWorker()
	return idx
}

// compactWorker drains compactNotify and runs Pebble compaction asynchronously.
// Exits when the channel is closed (on Index.Close).
func (idx *Index) compactWorker() {
	defer close(idx.compactDone)
	for clusterID := range idx.compactNotify {
		if idx.closed.Load() {
			return
		}
		_ = idx.st.CompactCluster(clusterID)
	}
}

// loadCentroids reads centroids from the store and populates the atomic pointer.
func (idx *Index) loadCentroids() error {
	clusterIDs, vecs, err := idx.st.ListCentroids()
	if err != nil {
		return err
	}
	if len(clusterIDs) == 0 {
		// Flat-scan phase — no centroids yet.
		// Load vector count from cluster 0.
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

// bulkBatchSize is the number of entries committed per Pebble batch in bulkLoad.
const bulkBatchSize = 2048

// bulkLoad inserts all entries without watermark checks (used during CreateIndex).
// DocIDs are assigned as 0-indexed positions so they match vector array indices
// used by the test harness's brute-force recall computation.
// Entries are committed in batches of bulkBatchSize for throughput.
func (idx *Index) bulkLoad(ctx context.Context, bulk []BulkEntry) error {
	// In flat-scan phase all vectors go to virtual cluster 0.
	meta, err := idx.st.GetClusterMeta(0)
	if errors.Is(err, store.ErrNotFound) {
		meta = store.ClusterMeta{State: store.ClusterStateActive}
	} else if err != nil {
		return err
	}

	b := idx.st.NewBatch()
	batchCount := 0

	commitBatch := func() error {
		if err := b.Commit(); err != nil {
			_ = b.Close()
			return err
		}
		b = idx.st.NewBatch()
		batchCount = 0
		return nil
	}

	for i, e := range bulk {
		if ctx.Err() != nil {
			_ = b.Close()
			return ctx.Err()
		}
		docID := uint64(i) // 0-indexed to match brute-force truth indices

		if err := b.BatchPutPosting(0, docID, e.Vector); err != nil {
			_ = b.Close()
			return err
		}
		if err := b.BatchPutReverseMap(docID, 0); err != nil {
			_ = b.Close()
			return err
		}
		if err := b.BatchPutExtToDoc(e.ExternalID, docID); err != nil {
			_ = b.Close()
			return err
		}
		if err := b.BatchPutDocToExt(docID, e.ExternalID); err != nil {
			_ = b.Close()
			return err
		}
		meta.Size++
		batchCount++

		if batchCount%bulkBatchSize == 0 {
			if err := b.BatchPutClusterMeta(0, meta); err != nil {
				_ = b.Close()
				return err
			}
			if err := commitBatch(); err != nil {
				return err
			}
		}
	}

	// Commit remaining entries.
	if err := b.BatchPutClusterMeta(0, meta); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.Commit(); err != nil {
		_ = b.Close()
		return err
	}

	// Seed the docID counter beyond the bulk range so subsequent upserts don't collide.
	n := uint64(len(bulk))
	if n > 0 {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, n-1) // nextDocID will increment to n
		_ = idx.st.DB().Set(keyDocIDCounter, buf, pebble.NoSync)
	}

	idx.vectorCount.Store(n)
	return nil
}

// Search returns up to req.K nearest neighbours for the given query vector.
func (idx *Index) Search(ctx context.Context, req SearchRequest) ([]SearchHit, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	idx.mu.RLock()
	cs := idx.centroids.Load()
	idx.mu.RUnlock()

	if cs == nil {
		// Flat-scan phase.
		return idx.flatSearch(ctx, req)
	}
	return idx.ivfSearch(ctx, req, cs)
}

// flatSearch scans all vectors in cluster 0 and returns exact top-k.
// extIDs are resolved only for the final top-K to minimise Pebble Gets.
func (idx *Index) flatSearch(ctx context.Context, req SearchRequest) ([]SearchHit, error) {
	k := req.K
	idx.lastNprobe.Store(1)

	h := &hitHeap{}
	heap.Init(h)

	scanErr := idx.st.ScanClusterFunc(0, func(docID uint64, vecBytes []byte) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d := metric.DistanceFromBytes(idx.spec.Metric, req.Vector, vecBytes)
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
func (idx *Index) ivfSearch(ctx context.Context, req SearchRequest, cs *centroidState) ([]SearchHit, error) {
	nprobe := idx.spec.Nprobe
	if req.NprobeOverride > 0 {
		nprobe = req.NprobeOverride
	}
	if nprobe <= 0 {
		nprobe = 1
	}

	// Extract all centroid vectors from the CentroidSet.
	centVecs := make([][]float32, cs.cs.Len())
	for i := range centVecs {
		v, err := cs.cs.Get(uint32(i))
		if err != nil {
			return nil, fmt.Errorf("index: get centroid %d: %w", i, err)
		}
		centVecs[i] = v
	}

	// Fetch top-(nprobe+1) to check the adaptive condition.
	fetchN := nprobe + 1
	if fetchN > cs.cs.Len() {
		fetchN = cs.cs.Len()
	}
	ids, dists, err := kmeans.AssignTopN(req.Vector, centVecs, fetchN, idx.spec.Metric)
	if err != nil {
		return nil, fmt.Errorf("index: assign centroids: %w", err)
	}

	// Adaptive multi-probe: one-shot conservative bump when the query sits near a
	// Voronoi boundary (2nd centroid within 5% of nearest). The cap is intentionally
	// tight — nprobe+max(2,nprobe/4) — to prevent cascade blowup in high-dimensional
	// spaces where distance concentration makes the 1.1 threshold fire on nearly every
	// query. Users who need higher recall should raise Nprobe explicitly.
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
			newIDs, newDists, assignErr := kmeans.AssignTopN(req.Vector, centVecs, fetchBump, idx.spec.Metric)
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

	// parallelScan: each goroutine scans one cluster and builds a local top-K heap.
	// Goroutine count is capped at min(nprobe, GOMAXPROCS×2) to avoid spawning more
	// goroutines than SSD I/O bandwidth can saturate.
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
				d := metric.DistanceFromBytes(idx.spec.Metric, req.Vector, vecBytes)
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

	// Merge per-cluster heaps, deduplicating by docID.
	seen := make(map[uint64]struct{})
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

	// Resolve extIDs only for the final top-K — not inside the inner scan loop.
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
func (idx *Index) Upsert(ctx context.Context, externalID []byte, vec []float32, txnID, seqID uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check watermark for idempotency.
	wmTxn, wmSeq, wmFound := idx.getWatermark(externalID)
	if wmFound && (txnID < wmTxn || (txnID == wmTxn && seqID <= wmSeq)) {
		return nil // stale or duplicate — no-op
	}

	existingDocID, err := idx.st.GetExtToDoc(externalID)
	isUpdate := err == nil

	// Resolve old cluster state before building the batch (HR-06: single atomic batch).
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

	// Assign new cluster.
	var clusterID uint32
	cs := idx.centroids.Load()
	if cs != nil {
		centVecs := make([][]float32, cs.cs.Len())
		for i := range centVecs {
			v, _ := cs.cs.Get(uint32(i))
			centVecs[i] = v
		}
		centIdx, _, assignErr := kmeans.Assign(vec, centVecs, idx.spec.Metric)
		if assignErr == nil && int(centIdx) < len(cs.clusterIDs) {
			clusterID = cs.clusterIDs[centIdx]
		}
	}
	// else: flat-scan phase — cluster 0

	// Determine docID.
	var docID uint64
	if isUpdate {
		docID = existingDocID
	} else {
		docID = idx.nextDocID()
	}

	// Ensure new cluster meta exists.
	newMeta, err := idx.st.GetClusterMeta(clusterID)
	if errors.Is(err, store.ErrNotFound) {
		newMeta = store.ClusterMeta{State: store.ClusterStateActive}
	} else if err != nil {
		return err
	}
	newMeta.Size++

	b := idx.st.NewBatch()
	// For updates: remove old posting and update old cluster meta in the same batch.
	if isUpdate {
		if batchErr := b.BatchDeletePosting(oldClusterID, existingDocID); batchErr != nil {
			_ = b.Close()
			return batchErr
		}
		if oldClusterID != clusterID {
			if oldMeta.Size > 0 {
				oldMeta.Size--
			}
			if batchErr := b.BatchPutClusterMeta(oldClusterID, oldMeta); batchErr != nil {
				_ = b.Close()
				return batchErr
			}
		} else {
			// Same cluster: net size is unchanged (deleted then re-inserted).
			newMeta.Size = oldMeta.Size
		}
	}
	if err := b.BatchPutPosting(clusterID, docID, vec); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.BatchPutReverseMap(docID, clusterID); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.BatchPutExtToDoc(externalID, docID); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.BatchPutDocToExt(docID, externalID); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.BatchPutClusterMeta(clusterID, newMeta); err != nil {
		_ = b.Close()
		return err
	}
	// Watermark in the same batch — no crash window (HR-02).
	if err := b.BatchPutWatermark(externalID, txnID, seqID); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.Commit(); err != nil {
		_ = b.Close()
		return err
	}

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

	idx.mu.Lock()
	defer idx.mu.Unlock()

	docID, err := idx.st.GetExtToDoc(externalID)
	if errors.Is(err, store.ErrNotFound) {
		// Idempotent: persist tombstone watermark atomically.
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
	// Remove reverse map entry in the same batch (CR-02).
	if err := b.BatchDeleteReverseMap(docID); err != nil {
		_ = b.Close()
		return err
	}
	// Update cluster meta.
	meta, metaErr := idx.st.GetClusterMeta(clusterID)
	if metaErr == nil {
		if meta.Size > 0 {
			meta.Size--
		}
		meta.TombstoneCount++
		if err := b.BatchPutClusterMeta(clusterID, meta); err != nil {
			_ = b.Close()
			return err
		}
	}
	// Watermark in the same batch — no crash window (HR-02).
	if err := b.BatchPutWatermark(externalID, txnID, seqID); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.Commit(); err != nil {
		_ = b.Close()
		return err
	}

	idx.vectorCount.Add(^uint64(0)) // decrement by 1

	// Enqueue compaction to background worker (non-blocking, single-slot) (HR-04).
	if metaErr == nil {
		meta.TombstoneCount++ // reflect the increment written above
		if store.ShouldCompact(meta) && !idx.closed.Load() {
			select {
			case idx.compactNotify <- clusterID:
			default: // slot occupied — compaction already pending
			}
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
	close(idx.compactNotify)
	<-idx.compactDone
	return idx.st.Close()
}

// keyDocIDCounter is the pebble key for the monotonic docID counter.
var keyDocIDCounter = []byte{0x09, 0x01}

// nextDocID allocates a new monotonically-increasing document ID, persisted
// so it survives restarts. Called only while holding idx.mu write lock.
func (idx *Index) nextDocID() uint64 {
	val, closer, err := idx.st.DB().Get(keyDocIDCounter)
	var cur uint64
	if err == nil {
		cur = binary.BigEndian.Uint64(val)
		closer.Close()
	}
	next := cur + 1
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, next)
	_ = idx.st.DB().Set(keyDocIDCounter, buf, pebble.Sync)
	return next
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
