package hdindex

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/hdindex/pkg/hilbert"
	"github.com/maxpert/marmot/modules/hdindex/pkg/metric"
	"github.com/maxpert/marmot/modules/hdindex/pkg/prune"
	"github.com/maxpert/marmot/modules/hdindex/pkg/rdb"
	"github.com/maxpert/marmot/modules/hdindex/pkg/refobj"
	"github.com/maxpert/marmot/modules/hdindex/pkg/vecstore"
)

// Index represents an open HD-Index instance.
type Index struct {
	spec     HDIndexSpec
	refs     *refobj.ReferenceSet
	rdbStore *rdb.Store
	vecStore *vecstore.Store
	db       *pebble.DB
	mu       sync.RWMutex
}

// partitionResult holds the output of a single partition scan + prune.
// entries carries the raw RDB scan result; keptIdx holds indices into entries
// that survived triangle pruning. This avoids copying Entry→Candidate structs.
type partitionResult struct {
	entries []rdb.Entry
	keptIdx []int
	scanned int
	pruned  int
}

// Search performs a kNN query using the HD-Index algorithm (Algorithm 2 from paper).
// Partition scans run in parallel across available cores.
func (idx *Index) Search(ctx context.Context, req SearchRequest) (*SearchResult, error) {
	if len(req.VectorFP32) != idx.spec.Dim {
		return nil, fmt.Errorf("hdindex: query dimension %d != index dimension %d", len(req.VectorFP32), idx.spec.Dim)
	}
	if req.TopK <= 0 {
		return nil, errors.New("hdindex: TopK must be > 0")
	}

	alpha := idx.spec.Alpha
	explicitAlpha := req.Alpha > 0
	if explicitAlpha {
		alpha = req.Alpha
	}
	gamma := idx.spec.Gamma
	if req.Gamma > 0 {
		gamma = req.Gamma
	}

	// Adaptive alpha: scale proportionally to dataset size when using spec
	// defaults. Scanning >5% per partition degrades to a random scan with no
	// Hilbert locality benefit. When alpha is reduced, gamma scales down to
	// maintain the paper's alpha/gamma ratio for effective triangle pruning.
	// Skipped for per-query overrides (caller knows what they want).
	vecCount, _ := idx.vecStore.GetVectorCount()
	if vc := int(vecCount); vc > 0 && !explicitAlpha {
		maxAlpha := max(int(float64(vc)*0.05), req.TopK*2)
		if alpha > maxAlpha {
			scale := float64(maxAlpha) / float64(alpha)
			alpha = maxAlpha
			gamma = max(int(float64(gamma)*scale), req.TopK*2)
		}
	}

	// Transform query according to metric.
	queryOrig := req.VectorFP32
	queryTransformed, err := idx.transformVector(queryOrig)
	if err != nil {
		return nil, fmt.Errorf("hdindex: transform query: %w", err)
	}

	// Compute query distances to all m reference objects.
	queryRefDists := idx.refs.ComputeRefDists(queryTransformed)

	// Scan all τ partitions in parallel.
	tau := idx.spec.Tau
	partResults := make([]partitionResult, tau)
	partErrors := make([]error, tau)

	var wg sync.WaitGroup
	sem := make(chan struct{}, max(1, min(tau, runtime.GOMAXPROCS(0))))

	for i := range tau {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if ctx.Err() != nil {
				partErrors[i] = ctx.Err()
				return
			}

			pr, err := idx.searchPartition(i, queryTransformed, queryRefDists, alpha, gamma)
			if err != nil {
				partErrors[i] = err
				return
			}
			partResults[i] = pr
		}()
	}
	wg.Wait()

	// Check for errors from any partition.
	for i, err := range partErrors {
		if err != nil {
			return nil, fmt.Errorf("hdindex: scan partition %d: %w", i, err)
		}
	}

	// Merge and deduplicate candidates across partitions.
	var stats SearchStats
	seen := make(map[uint64]struct{}, gamma*tau)
	docIDs := make([]uint64, 0, gamma*tau)

	for _, pr := range partResults {
		stats.PartitionsSearched++
		stats.CandidatesScanned += pr.scanned
		stats.CandidatesAfterTriangle += pr.pruned

		for _, ki := range pr.keptIdx {
			did := pr.entries[ki].DocID
			if _, exists := seen[did]; !exists {
				seen[did] = struct{}{}
				docIDs = append(docIDs, did)
			}
		}
	}

	vecs, err := idx.vecStore.GetVectors(docIDs)
	if err != nil {
		return nil, fmt.Errorf("hdindex: load vectors: %w", err)
	}
	stats.CandidatesExactScored = len(vecs)

	type scored struct {
		docID    uint64
		distance float32
	}
	results := make([]scored, 0, len(vecs))
	for docID, vec := range vecs {
		d := idx.exactDistance(queryOrig, vec)
		results = append(results, scored{docID: docID, distance: d})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	topK := min(req.TopK, len(results))
	hits := make([]SearchHit, 0, topK)
	for _, r := range results[:topK] {
		extID, err := idx.vecStore.GetExternalID(r.docID)
		if err != nil {
			return nil, fmt.Errorf("hdindex: get external id for doc %d: %w", r.docID, err)
		}
		hits = append(hits, SearchHit{
			ExternalID: extID,
			Distance:   r.distance,
			Score:      -r.distance, // higher score = closer
		})
	}

	return &SearchResult{Hits: hits, Stats: stats}, nil
}

// searchPartition runs the Hilbert scan + triangle prune for a single partition.
func (idx *Index) searchPartition(partIdx int, queryTransformed, queryRefDists []float32, alpha, gamma int) (partitionResult, error) {
	etaDims := idx.spec.Eta
	start := partIdx * etaDims
	end := start + etaDims

	coords := metric.QuantizeDims(queryTransformed[start:end], idx.spec.DomainMin[start:end], idx.spec.DomainMax[start:end], idx.spec.Omega)
	hilbertKey := hilbert.Encode(coords, idx.spec.Omega)

	entries, err := idx.rdbStore.ScanNearest(partIdx, hilbertKey, alpha)
	if err != nil {
		return partitionResult{}, err
	}

	// Extract ref distances slice headers (no data copy — they point into the
	// ScanNearest arena). This avoids constructing prune.Candidate structs.
	refDists := make([][]float32, len(entries))
	for j := range entries {
		refDists[j] = entries[j].RefDists
	}

	// Per HD-Index paper Section 5.2.5: triangle inequality prune α → γ.
	keptIdx := prune.TrianglePruneRefDists(queryRefDists, refDists, gamma)

	return partitionResult{
		entries: entries,
		keptIdx: keptIdx,
		scanned: len(entries),
		pruned:  len(keptIdx),
	}, nil
}

// Upsert inserts or updates a single vector. Idempotent via (TxnID, SeqID).
func (idx *Index) Upsert(ctx context.Context, mut Mutation) error {
	if len(mut.VectorFP32) != idx.spec.Dim {
		return fmt.Errorf("hdindex: vector dimension %d != index dimension %d", len(mut.VectorFP32), idx.spec.Dim)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check if this is an update (external ID already exists).
	existingDocID, err := idx.vecStore.GetDocID(mut.ExternalID)
	isUpdate := err == nil
	if err != nil && err != pebble.ErrNotFound {
		return fmt.Errorf("hdindex: lookup external id: %w", err)
	}

	transformed, err := idx.transformDataVector(mut.VectorFP32)
	if err != nil {
		return fmt.Errorf("hdindex: transform vector: %w", err)
	}

	refDists := idx.refs.ComputeRefDists(transformed)
	hilbertKeys := idx.computeHilbertKeys(transformed)

	batch := idx.db.NewBatch()
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()

	if isUpdate {
		// Delete old RDB entries using stored reverse Hilbert keys.
		oldHilbert, err := idx.vecStore.GetReverseHilbert(existingDocID)
		if err != nil {
			return fmt.Errorf("hdindex: get reverse hilbert: %w", err)
		}
		hilbertKeyLen := len(hilbertKeys[0])
		for i := range idx.spec.Tau {
			offset := i * hilbertKeyLen
			oldKey := oldHilbert[offset : offset+hilbertKeyLen]
			if err := idx.rdbStore.Delete(batch, i, oldKey, existingDocID); err != nil {
				return fmt.Errorf("hdindex: delete old rdb entry partition %d: %w", i, err)
			}
		}
	}

	var docID uint64
	if isUpdate {
		docID = existingDocID
	} else {
		docID, err = idx.vecStore.NextDocID(batch)
		if err != nil {
			return fmt.Errorf("hdindex: alloc doc id: %w", err)
		}
	}

	// Build concatenated reverse hilbert keys.
	concatenatedHilbert := concatHilbertKeys(hilbertKeys)

	// Write vector, id mappings, reverse hilbert.
	if err := idx.vecStore.PutVector(batch, docID, mut.VectorFP32); err != nil {
		return fmt.Errorf("hdindex: put vector: %w", err)
	}
	if err := idx.vecStore.PutIDMapping(batch, mut.ExternalID, docID); err != nil {
		return fmt.Errorf("hdindex: put id mapping: %w", err)
	}
	if err := idx.vecStore.PutReverseHilbert(batch, docID, concatenatedHilbert); err != nil {
		return fmt.Errorf("hdindex: put reverse hilbert: %w", err)
	}

	// Write RDB entries for all partitions.
	for i, hk := range hilbertKeys {
		if err := idx.rdbStore.Put(batch, i, hk, docID, refDists); err != nil {
			return fmt.Errorf("hdindex: put rdb partition %d: %w", i, err)
		}
	}

	// Increment vector count only on insert.
	if !isUpdate {
		if err := idx.vecStore.IncrementVectorCount(batch, 1); err != nil {
			return fmt.Errorf("hdindex: increment vector count: %w", err)
		}
	}

	if err := idx.vecStore.SetWatermark(batch, mut.TxnID, mut.SeqID); err != nil {
		return fmt.Errorf("hdindex: set watermark: %w", err)
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("hdindex: commit upsert batch: %w", err)
	}
	batch = nil
	return nil
}

// Delete removes a vector by external ID.
func (idx *Index) Delete(ctx context.Context, mut DeleteMutation) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	docID, err := idx.vecStore.GetDocID(mut.ExternalID)
	if err == pebble.ErrNotFound {
		return nil // idempotent
	}
	if err != nil {
		return fmt.Errorf("hdindex: lookup external id: %w", err)
	}

	reverseHilbert, err := idx.vecStore.GetReverseHilbert(docID)
	if err != nil {
		return fmt.Errorf("hdindex: get reverse hilbert: %w", err)
	}

	// Determine hilbert key length per partition.
	etaDims := idx.spec.Eta
	hilbertKeyLen := (etaDims*idx.spec.Omega + 7) / 8

	batch := idx.db.NewBatch()
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()

	// Delete RDB entries.
	for i := range idx.spec.Tau {
		offset := i * hilbertKeyLen
		hk := reverseHilbert[offset : offset+hilbertKeyLen]
		if err := idx.rdbStore.Delete(batch, i, hk, docID); err != nil {
			return fmt.Errorf("hdindex: delete rdb partition %d: %w", i, err)
		}
	}

	// Delete vector, id mappings, reverse hilbert.
	if err := idx.vecStore.DeleteVector(batch, docID); err != nil {
		return fmt.Errorf("hdindex: delete vector: %w", err)
	}
	if err := idx.vecStore.DeleteIDMapping(batch, mut.ExternalID, docID); err != nil {
		return fmt.Errorf("hdindex: delete id mapping: %w", err)
	}
	if err := idx.vecStore.DeleteReverseHilbert(batch, docID); err != nil {
		return fmt.Errorf("hdindex: delete reverse hilbert: %w", err)
	}

	if err := idx.vecStore.IncrementVectorCount(batch, -1); err != nil {
		return fmt.Errorf("hdindex: decrement vector count: %w", err)
	}

	if err := idx.vecStore.SetWatermark(batch, mut.TxnID, mut.SeqID); err != nil {
		return fmt.Errorf("hdindex: set watermark: %w", err)
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("hdindex: commit delete batch: %w", err)
	}
	batch = nil
	return nil
}

// Stats returns index statistics.
func (idx *Index) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	count, _ := idx.vecStore.GetVectorCount()
	txnID, seqID, _ := idx.vecStore.GetWatermark()
	return IndexStats{
		VectorCount:    count,
		WatermarkTxnID: txnID,
		WatermarkSeqID: seqID,
	}
}

// Spec returns the index specification.
func (idx *Index) Spec() HDIndexSpec {
	return idx.spec
}

// Checkpoint creates a consistent point-in-time snapshot of the index's Pebble
// database at destDir. The checkpoint uses hard links when possible, making it
// very fast. The caller should tar/compress destDir for transport and remove it
// when done.
func (idx *Index) Checkpoint(destDir string) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.db.Checkpoint(destDir)
}

// Close flushes and closes the index.
func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.db.Close()
}

// transformVector applies the metric-specific transformation to a QUERY vector.
// For Cosine: normalize. For Dot: MIPS query augmentation (appends 0). For Euclidean: no-op.
func (idx *Index) transformVector(v []float32) ([]float32, error) {
	return transformQueryVectorWithSpec(idx.spec, v)
}

// transformDataVector applies the metric-specific transformation to a DATA vector.
// For Cosine: normalize. For Dot: MIPS data augmentation (appends sqrt(M²-||x||²)). For Euclidean: no-op.
func (idx *Index) transformDataVector(v []float32) ([]float32, error) {
	return transformVectorWithSpec(idx.spec, v)
}

// exactDistance computes the true metric distance between two original (non-transformed) vectors.
func (idx *Index) exactDistance(query, candidate []float32) float32 {
	switch idx.spec.Metric {
	case MetricCosine:
		return 1.0 - metric.CosineSimilarity(query, candidate)
	case MetricDot:
		return -metric.DotProduct(query, candidate)
	default:
		return float32(math.Sqrt(float64(metric.L2Squared(query, candidate))))
	}
}

// computeHilbertKeys computes tau Hilbert keys from a transformed vector.
func (idx *Index) computeHilbertKeys(transformed []float32) [][]byte {
	keys := make([][]byte, idx.spec.Tau)
	etaDims := idx.spec.Eta
	for i := range idx.spec.Tau {
		start := i * etaDims
		end := start + etaDims
		partSlice := transformed[start:end]
		domainMin := idx.spec.DomainMin[start:end]
		domainMax := idx.spec.DomainMax[start:end]
		coords := metric.QuantizeDims(partSlice, domainMin, domainMax, idx.spec.Omega)
		keys[i] = hilbert.Encode(coords, idx.spec.Omega)
	}
	return keys
}

// concatHilbertKeys concatenates tau Hilbert keys into a single byte slice.
func concatHilbertKeys(keys [][]byte) []byte {
	if len(keys) == 0 {
		return nil
	}
	keyLen := len(keys[0])
	out := make([]byte, len(keys)*keyLen)
	for i, k := range keys {
		copy(out[i*keyLen:], k)
	}
	return out
}
