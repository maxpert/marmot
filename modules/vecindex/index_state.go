package vecindex

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// IndexState holds the in-memory state for a single vector index.
//
// probeState is frozen at REINDEX time; query transpile and UDF assignment use
// this pointer. driftState is updated online via MacQueen increments and
// seeds the next k-means warm start. driftTracker accumulates the running
// sum/count statistics that produce the drifted centroids.
//
// vectorCache is the optional in-memory (cluster_id → entries) snapshot that
// lets the coordinator rank candidates in Go without a SQLite cursor (task
// #16). Epoch-tagged so searches can fall back to SQL when the cache has not
// yet caught up to a fresh probeState.
//
// All pointers are stored as atomic.Pointer so readers are lock-free.
type IndexState struct {
	spec         IVFSpec
	probeState   atomic.Pointer[kmeans.CentroidSet]
	driftState   atomic.Pointer[kmeans.CentroidSet]
	driftTracker atomic.Pointer[DriftTracker]
	vectorCache  atomic.Pointer[VectorCache]
}

// NewIndexState creates an IndexState with probe and drift both pointing at cs.
// The drift tracker is initialized from cs's centroids for MacQueen tracking.
func NewIndexState(spec IVFSpec, cs *kmeans.CentroidSet) *IndexState {
	s := &IndexState{spec: spec}
	s.probeState.Store(cs)
	s.driftState.Store(cs)
	if cs != nil {
		s.driftTracker.Store(NewDriftTracker(cs.Snapshot()))
	}
	return s
}

// Spec returns the immutable IVF configuration for this index.
func (s *IndexState) Spec() IVFSpec { return s.spec }

// ProbeState returns the current probe centroid set. Nil when no centroids
// are loaded (empty-table bootstrap). The returned CentroidSet is immutable.
func (s *IndexState) ProbeState() *kmeans.CentroidSet {
	return s.probeState.Load()
}

// ProbeVersion returns the epoch of the active probe centroid set.
// Returns 0 when no centroid set is loaded.
func (s *IndexState) ProbeVersion() uint64 {
	if cs := s.probeState.Load(); cs != nil {
		return cs.Epoch()
	}
	return 0
}

// SwapProbeState atomically replaces the probe centroid set and returns the
// previous one. Called at REINDEX commit (design §8.3 step 7 in-txn swap).
func (s *IndexState) SwapProbeState(cs *kmeans.CentroidSet) *kmeans.CentroidSet {
	return s.probeState.Swap(cs)
}

// DriftState returns the current drift centroid set (design §8.5). The
// drift state carries MacQueen-updated centroids — it is the warm-start
// source for the next REINDEX k-means (§8.3 step 2, fix G).
//
// Returns nil before any centroids have been installed. The returned
// CentroidSet is immutable; callers that want to fork it must call
// CentroidSet.Snapshot to get a mutable copy.
func (s *IndexState) DriftState() *kmeans.CentroidSet {
	return s.driftState.Load()
}

// ResetDriftState atomically replaces the drift centroid set and reinitializes
// the MacQueen drift tracker from the new centroids. Called at REINDEX commit
// so drift tracking starts from the newly-swapped probe state — subsequent
// DriftUpdate calls then fork drift cleanly from the new baseline.
//
// Returns the previous drift pointer.
func (s *IndexState) ResetDriftState(cs *kmeans.CentroidSet) *kmeans.CentroidSet {
	if cs != nil {
		s.driftTracker.Store(NewDriftTracker(cs.Snapshot()))
	}
	return s.driftState.Swap(cs)
}

// DriftUpdate accumulates a vector into the MacQueen drift tracker for the
// given 1-based clusterID. Uses CAS-loop copy-on-write so concurrent callers
// never block. No-op if the tracker is nil or clusterID is out of range.
func (s *IndexState) DriftUpdate(clusterID int64, vec []float32) {
	idx := int(clusterID - 1) // convert 1-based to 0-based
	for {
		old := s.driftTracker.Load()
		if old == nil || idx < 0 || idx >= old.Len() {
			return
		}
		updated := old.Update(idx, vec)
		if s.driftTracker.CompareAndSwap(old, updated) {
			return
		}
	}
}

// DriftCentroids returns the MacQueen-drifted centroid positions computed from
// the running sum/count tracker. Returns nil if no tracker is initialized.
// Used as the warm-start seed for the next REINDEX k-means (design §8.3 fix G).
func (s *IndexState) DriftCentroids() [][]float32 {
	t := s.driftTracker.Load()
	if t == nil {
		return nil
	}
	return t.Centroids()
}

// LoadDriftTracker returns the current drift tracker for external inspection
// (e.g., growth-ratio sensor in the auto-retrain monitor). The returned
// tracker is immutable; safe for concurrent reads.
func (s *IndexState) LoadDriftTracker() *DriftTracker {
	return s.driftTracker.Load()
}

// LoadCache returns the current in-memory vector cache snapshot, or nil when
// no cache has been installed. The returned *VectorCache is immutable.
func (s *IndexState) LoadCache() *VectorCache {
	return s.vectorCache.Load()
}

// StoreCache atomically installs c as the active vector cache, replacing any
// prior snapshot. Passing nil clears the cache.
func (s *IndexState) StoreCache(c *VectorCache) {
	s.vectorCache.Store(c)
}

// CacheClear atomically drops the cache. No-op if no cache is installed.
func (s *IndexState) CacheClear() {
	s.vectorCache.Store(nil)
}

// CacheInsertBatch COW-inserts the batch into the active cache. The batch's
// expected epoch must match the cache's epoch; on mismatch this is a no-op so
// stale delta-flush writes cannot corrupt a newly-swapped post-reindex cache.
// The cache caller transfers ownership of each entry.Vec — it must not be
// mutated after this call.
func (s *IndexState) CacheInsertBatch(expectedEpoch uint64, entries []CacheEntry) {
	if len(entries) == 0 {
		return
	}
	for {
		old := s.vectorCache.Load()
		if old == nil {
			return
		}
		if old.epoch != expectedEpoch {
			return
		}
		updated := old.withBatchInsert(entries)
		if s.vectorCache.CompareAndSwap(old, updated) {
			return
		}
	}
}

// CacheDelete COW-removes the entry for rowid from whichever cluster holds
// it. Safe when rowid is absent (no-op). Intended for DELETE-triggered
// removals; bulk purges should replace the cache wholesale via StoreCache.
func (s *IndexState) CacheDelete(rowid int64) {
	for {
		old := s.vectorCache.Load()
		if old == nil {
			return
		}
		updated := old.withDelete(rowid)
		if updated == old {
			return
		}
		if s.vectorCache.CompareAndSwap(old, updated) {
			return
		}
	}
}

// AssignNearest returns the 1-based cluster ID for the nearest centroid.
// cluster_id=0 is reserved for delta (unassigned) rows per design §3.3.
//
// vecBytes must be a little-endian float32 BLOB of exactly spec.Dim*4 bytes
// for L2/Cosine, or spec.Dim*4 bytes for Dot (augmentation is applied here).
// Returns an error on dimension mismatch or missing centroids.
func (s *IndexState) AssignNearest(vecBytes []byte) (int64, error) {
	if len(vecBytes) == 0 || len(vecBytes)%4 != 0 {
		return 0, fmt.Errorf("MARMOT-VEC-014: invalid vector blob length %d for index %q", len(vecBytes), s.spec.ID)
	}
	rawDim := len(vecBytes) / 4
	if rawDim != s.spec.Dim {
		return 0, fmt.Errorf("MARMOT-VEC-014: dimension mismatch for index %q: got %d, want %d",
			s.spec.ID, rawDim, s.spec.Dim)
	}

	raw := metric.BytesToFloat32(vecBytes)

	// For MIPS→L2: augment data vector to internal D+1 space.
	var vec []float32
	if s.spec.Metric == MetricDot {
		augmented, err := metric.AugmentData(raw, s.spec.MaxNorm, nil)
		if err != nil {
			return 0, fmt.Errorf("vecindex: augment for index %q: %w", s.spec.ID, err)
		}
		vec = augmented
	} else {
		vec = raw
	}

	cs := s.probeState.Load()
	if cs == nil || cs.Len() == 0 {
		return 0, fmt.Errorf("vecindex: no centroids loaded for index %q", s.spec.ID)
	}

	clusterID, _, err := cs.AssignNearest(vec, s.spec.InternalMetric())
	if err != nil {
		return 0, fmt.Errorf("vecindex: assign nearest for index %q: %w", s.spec.ID, err)
	}

	return int64(clusterID) + 1, nil // 1-based; 0 reserved for delta
}

// TopNprobeClusters returns the top-n 1-based cluster IDs ordered by ascending
// distance to the query vector (closest first). Thin wrapper over
// TopNprobeClustersWithEpoch that discards the epoch.
func (s *IndexState) TopNprobeClusters(vecBytes []byte, n int) ([]int64, error) {
	ids, _, err := s.TopNprobeClustersWithEpoch(vecBytes, n)
	return ids, err
}

// TopNprobeClustersWithEpoch is TopNprobeClusters + the probe-state epoch the
// cluster IDs were computed against. The epoch is read from the SAME
// probeState pointer used for the top-N computation, so callers can later
// gate cache reads on epoch equality and avoid indexing IDs computed under
// the old probe into a freshly-rebuilt cache (task #16 coherence).
func (s *IndexState) TopNprobeClustersWithEpoch(vecBytes []byte, n int) ([]int64, uint64, error) {
	if len(vecBytes) == 0 || len(vecBytes)%4 != 0 {
		return nil, 0, fmt.Errorf("MARMOT-VEC-014: invalid vector blob length %d for index %q", len(vecBytes), s.spec.ID)
	}
	rawDim := len(vecBytes) / 4
	if rawDim != s.spec.Dim {
		return nil, 0, fmt.Errorf("MARMOT-VEC-014: dimension mismatch for index %q: got %d, want %d",
			s.spec.ID, rawDim, s.spec.Dim)
	}

	raw := metric.BytesToFloat32(vecBytes)

	var vec []float32
	if s.spec.Metric == MetricDot {
		augmented, err := metric.AugmentData(raw, s.spec.MaxNorm, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("vecindex: augment for index %q: %w", s.spec.ID, err)
		}
		vec = augmented
	} else {
		vec = raw
	}

	cs := s.probeState.Load()
	if cs == nil || cs.Len() == 0 {
		return nil, 0, fmt.Errorf("vecindex: no centroids loaded for index %q", s.spec.ID)
	}

	if n < 1 {
		n = 1
	}
	if n > cs.Len() {
		n = cs.Len()
	}

	ids, _, err := cs.AssignTopN(vec, n, s.spec.InternalMetric())
	if err != nil {
		return nil, 0, fmt.Errorf("vecindex: top-n probe for index %q: %w", s.spec.ID, err)
	}

	result := make([]int64, len(ids))
	for i, id := range ids {
		result[i] = int64(id) + 1
	}
	return result, cs.Epoch(), nil
}

// Float32ToBytes encodes a []float32 as little-endian bytes.
// Used during sampling for k-means to round-trip through SQL BLOB columns.
func Float32ToBytes(v []float32) []byte {
	if len(v) == 0 {
		return nil
	}
	out := make([]byte, len(v)*4)
	for i, f := range v {
		bits := math.Float32bits(f)
		out[i*4] = byte(bits)
		out[i*4+1] = byte(bits >> 8)
		out[i*4+2] = byte(bits >> 16)
		out[i*4+3] = byte(bits >> 24)
	}
	return out
}
