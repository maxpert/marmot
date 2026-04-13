package hdindex

import (
	"context"
	"errors"
	"fmt"
	"math"
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

// Search performs a kNN query using the HD-Index algorithm (Algorithm 2 from paper).
func (idx *Index) Search(ctx context.Context, req SearchRequest) (*SearchResult, error) {
	if len(req.VectorFP32) != idx.spec.Dim {
		return nil, fmt.Errorf("hdindex: query dimension %d != index dimension %d", len(req.VectorFP32), idx.spec.Dim)
	}
	if req.TopK <= 0 {
		return nil, errors.New("hdindex: TopK must be > 0")
	}

	alpha := idx.spec.Alpha
	if req.Alpha > 0 {
		alpha = req.Alpha
	}
	gamma := idx.spec.Gamma
	if req.Gamma > 0 {
		gamma = req.Gamma
	}

	// Cap alpha to half the dataset size — scanning >50% per partition wastes I/O
	// without improving recall (the Hilbert scan only helps when alpha << n).
	vecCount, _ := idx.vecStore.GetVectorCount()
	if vc := int(vecCount); vc > 0 && alpha > vc/2 {
		alpha = max(vc/2, gamma)
	}

	// Transform query according to metric.
	queryOrig := req.VectorFP32
	queryTransformed, err := idx.transformVector(queryOrig)
	if err != nil {
		return nil, fmt.Errorf("hdindex: transform query: %w", err)
	}

	// Compute query distances to all m reference objects.
	queryRefDists := idx.refs.ComputeRefDists(queryTransformed)

	var stats SearchStats
	seen := make(map[uint64]struct{})
	allCandidates := make([]prune.Candidate, 0, gamma*idx.spec.Tau)

	etaDims := idx.spec.Eta

	for i := range idx.spec.Tau {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		start := i * etaDims
		end := start + etaDims
		partSlice := queryTransformed[start:end]
		domainMinSlice := idx.spec.DomainMin[start:end]
		domainMaxSlice := idx.spec.DomainMax[start:end]

		coords := metric.QuantizeDims(partSlice, domainMinSlice, domainMaxSlice, idx.spec.Omega)
		hilbertKey := hilbert.Encode(coords, idx.spec.Omega)

		entries, err := idx.rdbStore.ScanNearest(i, hilbertKey, alpha)
		if err != nil {
			return nil, fmt.Errorf("hdindex: scan partition %d: %w", i, err)
		}
		stats.PartitionsSearched++
		stats.CandidatesScanned += len(entries)

		// Per HD-Index paper Section 5.2.5: use triangle inequality alone as the
		// filter (not Ptolemaic). Triangle prune from α candidates directly to γ.
		// This is the paper's recommended configuration: α/γ = 4.
		candidates := make([]prune.Candidate, len(entries))
		for j, e := range entries {
			candidates[j] = prune.Candidate{DocID: e.DocID, RefDists: e.RefDists}
		}
		pruned := prune.TrianglePrune(queryRefDists, candidates, gamma)
		stats.CandidatesAfterTriangle += len(pruned)
		stats.CandidatesAfterPtolemaic += len(pruned)

		for _, c := range pruned {
			if _, exists := seen[c.DocID]; !exists {
				seen[c.DocID] = struct{}{}
				allCandidates = append(allCandidates, c)
			}
		}
	}

	// Collect unique doc IDs and load their original vectors.
	docIDs := make([]uint64, 0, len(allCandidates))
	for _, c := range allCandidates {
		docIDs = append(docIDs, c.DocID)
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

	transformed, err := idx.transformVector(mut.VectorFP32)
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

// Close flushes and closes the index.
func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.db.Close()
}

// transformVector applies the metric-specific transformation to a vector.
// For Cosine: normalize. For Dot: MIPS augmentation. For Euclidean: no-op.
func (idx *Index) transformVector(v []float32) ([]float32, error) {
	switch idx.spec.Metric {
	case MetricCosine:
		return metric.NormalizeCopy(v), nil
	case MetricDot:
		return metric.AugmentForMIPS(v, idx.spec.NormMax)
	default:
		out := make([]float32, len(v))
		copy(out, v)
		return out, nil
	}
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
