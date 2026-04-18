package vecindex

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

var ErrNoCentroidsLoaded = errors.New("vecindex: no centroids loaded")

// IndexState holds the in-memory state for a single vector index.
//
// probeState is frozen at REINDEX time; query transpile and UDF assignment use
// this pointer. driftState is updated online via MacQueen increments and
// seeds the next k-means warm start. driftTracker accumulates the running
// sum/count statistics that produce the drifted centroids.
//
// All pointers are stored as atomic.Pointer so readers are lock-free.
type IndexState struct {
	spec          IVFSpec
	probeState    atomic.Pointer[kmeans.CentroidSet]
	driftState    atomic.Pointer[kmeans.CentroidSet]
	driftTracker  atomic.Pointer[DriftTracker]
	residentDelta atomic.Pointer[DeltaBuffer]
	packedStore   atomic.Pointer[PackedPartitionStore]
	packedDirty   atomic.Pointer[packedDirtySet]
}

type packedDirtySet struct {
	clusters map[int64]struct{}
}

// NewIndexState creates an IndexState with probe and drift both pointing at cs.
// The drift tracker is initialized from cs's centroids for MacQueen tracking.
func NewIndexState(spec IVFSpec, cs *kmeans.CentroidSet) *IndexState {
	s := &IndexState{spec: spec}
	s.probeState.Store(cs)
	s.driftState.Store(cs)
	s.residentDelta.Store(NewDeltaBuffer())
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

// LoadResidentDelta returns the always-resident cluster_id=0 buffer used by
// the packed streaming path. The returned buffer is immutable to callers.
func (s *IndexState) LoadResidentDelta() *DeltaBuffer {
	return s.residentDelta.Load()
}

// StoreResidentDelta installs delta as the active always-resident
// cluster_id=0 buffer. Passing nil resets it to an empty buffer.
func (s *IndexState) StoreResidentDelta(delta *DeltaBuffer) {
	if delta == nil {
		delta = NewDeltaBuffer()
	}
	s.residentDelta.Store(delta)
}

// LoadPackedStore returns the current mmap-backed stable partition store.
func (s *IndexState) LoadPackedStore() *PackedPartitionStore {
	return s.packedStore.Load()
}

// StorePackedStore installs store as the active packed partition snapshot and
// clears any dirty-cluster bookkeeping.
func (s *IndexState) StorePackedStore(store *PackedPartitionStore) {
	old := s.packedStore.Swap(store)
	s.packedDirty.Store(nil)
	if old != nil && old != store {
		_ = old.Close()
	}
}

// ClearPackedStore drops the active packed partition snapshot.
func (s *IndexState) ClearPackedStore() {
	old := s.packedStore.Swap(nil)
	s.packedDirty.Store(nil)
	if old != nil {
		_ = old.Close()
	}
}

// PackedClusterDirty reports whether the stable packed snapshot should be
// bypassed for clusterID and SQLite should be consulted instead.
func (s *IndexState) PackedClusterDirty(clusterID int64) bool {
	if clusterID <= 0 {
		return true
	}
	dirty := s.packedDirty.Load()
	if dirty == nil {
		return false
	}
	_, ok := dirty.clusters[clusterID]
	return ok
}

// ApplyDeltaFlushUpdates reflects a committed delta-flush batch in memory:
// rowids are removed from the resident delta buffer and touched clusters are
// marked dirty so subsequent packed reads consult SQLite until the next
// snapshot rebuild.
func (s *IndexState) ApplyDeltaFlushUpdates(expectedEpoch uint64, entries []PartitionUpdate) {
	if len(entries) == 0 {
		return
	}
	if s.ProbeVersion() != expectedEpoch {
		return
	}
	if delta := s.residentDelta.Load(); delta != nil {
		for _, e := range entries {
			delta.Remove(e.RowID)
		}
	}
	s.markPackedClustersDirty(entries)
}

func (s *IndexState) markPackedClustersDirty(entries []PartitionUpdate) {
	if s.packedStore.Load() == nil || len(entries) == 0 {
		return
	}
	for {
		old := s.packedDirty.Load()
		next := &packedDirtySet{clusters: make(map[int64]struct{}, len(entries))}
		if old != nil {
			for cid := range old.clusters {
				next.clusters[cid] = struct{}{}
			}
		}
		for _, entry := range entries {
			if entry.ClusterID > 0 {
				next.clusters[entry.ClusterID] = struct{}{}
			}
		}
		if s.packedDirty.CompareAndSwap(old, next) {
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
		return 0, fmt.Errorf("%w for index %q", ErrNoCentroidsLoaded, s.spec.ID)
	}

	clusterID, _, err := cs.AssignNearest(vec, s.spec.InternalMetric())
	if err != nil {
		return 0, fmt.Errorf("vecindex: assign nearest for index %q: %w", s.spec.ID, err)
	}

	return int64(clusterID) + 1, nil // 1-based; 0 reserved for delta
}

// AssignNearestPrepared returns the 1-based cluster ID for the nearest
// centroid for a vector that is already in the internal search space.
//
// vecBytes must be a little-endian float32 BLOB of exactly spec.InternalDim()*4
// bytes. Unlike AssignNearest, dot-metric vectors are not augmented here.
func (s *IndexState) AssignNearestPrepared(vecBytes []byte) (int64, error) {
	if len(vecBytes) == 0 || len(vecBytes)%4 != 0 {
		return 0, fmt.Errorf("MARMOT-VEC-014: invalid vector blob length %d for index %q", len(vecBytes), s.spec.ID)
	}
	internalDim := s.spec.InternalDim()
	if got := len(vecBytes) / 4; got != internalDim {
		return 0, fmt.Errorf("MARMOT-VEC-014: internal dimension mismatch for index %q: got %d, want %d",
			s.spec.ID, got, internalDim)
	}

	cs := s.probeState.Load()
	if cs == nil || cs.Len() == 0 {
		return 0, fmt.Errorf("%w for index %q", ErrNoCentroidsLoaded, s.spec.ID)
	}

	clusterID, _, err := cs.AssignNearest(metric.BytesToFloat32(vecBytes), s.spec.InternalMetric())
	if err != nil {
		return 0, fmt.Errorf("vecindex: assign nearest prepared for index %q: %w", s.spec.ID, err)
	}
	return int64(clusterID) + 1, nil
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
		return nil, 0, fmt.Errorf("%w for index %q", ErrNoCentroidsLoaded, s.spec.ID)
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
