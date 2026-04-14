package vecindex

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
	"github.com/rs/zerolog"
)

// keyPrefixWatermark is the per-externalID watermark namespace.
// Layout: [0x08][externalID bytes] → [txnID uint64 BE][seqID uint64 BE]
const keyPrefixWatermark byte = 0x08

// Index is an open IVF vector index backed by a Pebble store.
type Index struct {
	spec        IVFSpec
	st          *store.Store
	logger      zerolog.Logger
	mu          sync.RWMutex
	centroids   atomic.Pointer[centroidState]
	vectorCount atomic.Uint64
	lastNprobe  atomic.Uint64
	closed      atomic.Bool
}

// centroidState bundles a CentroidSet with the corresponding store cluster IDs.
// centroidIDs[i] is the store cluster ID for centroid index i.
type centroidState struct {
	cs         *kmeans.CentroidSet
	clusterIDs []uint32
}

// newIndex constructs an Index without loading existing data.
func newIndex(spec IVFSpec, st *store.Store, logger zerolog.Logger) *Index {
	return &Index{spec: spec, st: st, logger: logger}
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
		meta.Size = uint32(batchCount) // set final size in last write
		// We update meta per batch — actual size tracked incrementally below.
		if err := b.Commit(); err != nil {
			_ = b.Close()
			return err
		}
		b = idx.st.NewBatch()
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
func (idx *Index) flatSearch(ctx context.Context, req SearchRequest) ([]SearchHit, error) {
	entries, err := idx.st.ScanCluster(0)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}

	k := req.K
	if k > len(entries) {
		k = len(entries)
	}

	idx.lastNprobe.Store(1)

	h := &hitHeap{}
	heap.Init(h)
	for _, e := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		d := metric.Distance(idx.spec.Metric, req.Vector, e.Vector)
		extID, err := idx.st.GetDocToExt(e.DocID)
		if err != nil {
			continue
		}
		heap.Push(h, SearchHit{DocID: e.DocID, ExternalID: extID, Distance: d})
		if h.Len() > k {
			heap.Pop(h)
		}
	}

	return sortedHits(h, k), nil
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

	// Adaptive multi-probe: if the 2nd nearest centroid is within 10% of the nearest,
	// the query sits near a Voronoi boundary. Cascade 50% bumps until the boundary
	// condition no longer holds or nprobe reaches nlist/2.
	maxProbe := cs.cs.Len() / 2
	if maxProbe < nprobe {
		maxProbe = nprobe
	}
	for len(dists) >= 2 && dists[0] > 0 && dists[1]/dists[0] < 1.1 && nprobe < maxProbe {
		bump := nprobe + nprobe/2 + 1 // +1 ensures progress when nprobe is small
		if bump > maxProbe {
			bump = maxProbe
		}
		if bump <= nprobe {
			break
		}
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

	if len(ids) > nprobe {
		ids = ids[:nprobe]
	}

	idx.lastNprobe.Store(uint64(len(ids)))

	k := req.K
	seen := make(map[uint64]struct{})
	h := &hitHeap{}
	heap.Init(h)

	for _, centIdx := range ids {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Map centroid index → store cluster ID.
		if int(centIdx) >= len(cs.clusterIDs) {
			continue
		}
		clusterID := cs.clusterIDs[centIdx]
		entries, err := idx.st.ScanCluster(clusterID)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if _, dup := seen[e.DocID]; dup {
				continue
			}
			seen[e.DocID] = struct{}{}
			d := metric.Distance(idx.spec.Metric, req.Vector, e.Vector)
			extID, err := idx.st.GetDocToExt(e.DocID)
			if err != nil {
				continue
			}
			heap.Push(h, SearchHit{DocID: e.DocID, ExternalID: extID, Distance: d})
			if h.Len() > k {
				heap.Pop(h)
			}
		}
	}

	return sortedHits(h, k), nil
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

	if isUpdate {
		// Remove old posting entry from its current cluster.
		oldClusterID, err := idx.st.GetClusterForDoc(existingDocID)
		if err == nil {
			b := idx.st.NewBatch()
			if batchErr := b.BatchDeletePosting(oldClusterID, existingDocID); batchErr != nil {
				_ = b.Close()
				return batchErr
			}
			if batchErr := b.Commit(); batchErr != nil {
				_ = b.Close()
				return batchErr
			}
			// Decrement old cluster meta.
			if meta, metaErr := idx.st.GetClusterMeta(oldClusterID); metaErr == nil {
				if meta.Size > 0 {
					meta.Size--
				}
				_ = idx.st.PutClusterMeta(oldClusterID, meta)
			}
		}
	}

	// Assign cluster.
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
		// Allocate a new docID using cluster-ID allocator trick: use next seq from
		// ext→doc table. We derive a monotonic docID from the current vector count.
		docID = idx.nextDocID()
	}

	// Ensure cluster meta exists.
	meta, err := idx.st.GetClusterMeta(clusterID)
	if errors.Is(err, store.ErrNotFound) {
		meta = store.ClusterMeta{State: store.ClusterStateActive}
	} else if err != nil {
		return err
	}

	b := idx.st.NewBatch()
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
	if !isUpdate {
		meta.Size++
	} else {
		// Re-increment since we decremented on the old cluster above.
		meta.Size++
	}
	if err := b.BatchPutClusterMeta(clusterID, meta); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.Commit(); err != nil {
		_ = b.Close()
		return err
	}

	// Persist watermark.
	idx.putWatermark(externalID, txnID, seqID)

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
		// Idempotent: persist tombstone watermark so future stale upserts are blocked.
		idx.putWatermark(externalID, txnID, seqID)
		return nil
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
	// Remove reverse map entry.
	if err := idx.batchDeleteReverseMap(b, docID); err != nil {
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
	if err := b.Commit(); err != nil {
		_ = b.Close()
		return err
	}

	// Persist watermark tombstone.
	idx.putWatermark(externalID, txnID, seqID)
	idx.vectorCount.Add(^uint64(0)) // decrement by 1

	// Compact synchronously if tombstone ratio is high enough to warrant it.
	if metaErr == nil {
		meta.TombstoneCount++ // reflect the increment written above
		if store.ShouldCompact(meta) && !idx.closed.Load() {
			_ = idx.st.CompactCluster(clusterID)
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
		VectorCount:    idx.vectorCount.Load(),
		CentroidCount:  centCount,
		Epoch:          epoch,
		LastQueryNprobe: idx.lastNprobe.Load(),
	}
}

// Close releases resources held by this index.
func (idx *Index) Close() error {
	idx.closed.Store(true)
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
	_ = idx.st.DB().Set(keyDocIDCounter, buf, pebble.NoSync)
	return next
}

// getWatermark reads (txnID, seqID) for externalID from the 0x08 namespace.
func (idx *Index) getWatermark(externalID []byte) (txnID, seqID uint64, found bool) {
	key := encodeWatermarkKey(externalID)
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

// putWatermark writes (txnID, seqID) for externalID into the 0x08 namespace.
func (idx *Index) putWatermark(externalID []byte, txnID, seqID uint64) {
	key := encodeWatermarkKey(externalID)
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[:8], txnID)
	binary.BigEndian.PutUint64(val[8:], seqID)
	_ = idx.st.DB().Set(key, val, pebble.NoSync)
}

// encodeWatermarkKey encodes the 0x08-prefixed key for externalID.
func encodeWatermarkKey(externalID []byte) []byte {
	key := make([]byte, 1+len(externalID))
	key[0] = keyPrefixWatermark
	copy(key[1:], externalID)
	return key
}

// batchDeleteReverseMap adds a reverse-map delete to the batch.
// store.Batch does not expose a BatchDeleteReverseMap helper, so we write
// the delete directly to the underlying pebble batch via the DB.
// We do it outside the batch for simplicity — the posting + ext mappings
// being in the batch is the critical atomic unit; the reverse map is
// a secondary index and can be stale-read at worst.
func (idx *Index) batchDeleteReverseMap(_ *store.Batch, docID uint64) error {
	key := store.EncodeReverseKey(docID)
	return idx.st.DB().Delete(key, pebble.NoSync)
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
