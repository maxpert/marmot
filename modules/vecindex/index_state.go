package vecindex

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

var ErrNoCentroidsLoaded = errors.New("vecindex: no centroids loaded")

// IndexState holds the in-memory state for a single vector index.
//
// probeState is the active routing centroid set used by query transpile and
// assignment. All pointers are stored as atomic.Pointer so readers are
// lock-free.
type IndexState struct {
	spec         IVFSpec
	probeState   atomic.Pointer[kmeans.CentroidSet]
	overlay      atomic.Pointer[JournaledOverlay]
	segmentStore atomic.Pointer[SegmentGeneration]
	maintenance  atomic.Pointer[MaintenanceState]
	readers      atomic.Int64
	retired      atomic.Bool
	closeOnce    sync.Once
	closeMu      sync.Mutex
	closeHooks   []func()
	clusterHits  []atomic.Uint64
}

// NewIndexState creates an IndexState rooted at cs.
func NewIndexState(spec IVFSpec, cs *kmeans.CentroidSet) *IndexState {
	s := &IndexState{spec: spec}
	if spec.Nlist > 0 {
		s.clusterHits = make([]atomic.Uint64, spec.Nlist+1)
	}
	s.probeState.Store(cs)
	s.overlay.Store(nil)
	s.maintenance.Store(&MaintenanceState{})
	return s
}

// Spec returns the immutable IVF configuration for this index.
func (s *IndexState) Spec() IVFSpec { return s.spec }

// Acquire pins the state for a serving reader. It returns false when the state
// has already been retired and its file-backed resources may be closing.
func (s *IndexState) Acquire() bool {
	if s == nil {
		return false
	}
	if s.retired.Load() {
		return false
	}
	s.readers.Add(1)
	if s.retired.Load() {
		s.Release()
		return false
	}
	return true
}

// Release drops a serving reader pin acquired by Acquire.
func (s *IndexState) Release() {
	if s == nil {
		return
	}
	if s.readers.Add(-1) == 0 && s.retired.Load() {
		s.closeServingResources()
	}
}

// Retire prevents new serving readers from pinning this state and closes
// file-backed resources as soon as all existing readers have left.
func (s *IndexState) Retire() {
	if s == nil {
		return
	}
	if s.retired.Swap(true) {
		return
	}
	if s.readers.Load() == 0 {
		s.closeServingResources()
	}
}

// AddRetireCallback registers fn to run after all readers have released and
// file-backed serving resources have been closed.
func (s *IndexState) AddRetireCallback(fn func()) {
	if s == nil || fn == nil {
		return
	}
	s.closeMu.Lock()
	s.closeHooks = append(s.closeHooks, fn)
	s.closeMu.Unlock()
}

func (s *IndexState) closeServingResources() {
	s.closeOnce.Do(func() {
		s.ClearOverlay()
		s.ClearSegmentStore()
		s.closeMu.Lock()
		hooks := append([]func(){}, s.closeHooks...)
		s.closeHooks = nil
		s.closeMu.Unlock()
		for _, hook := range hooks {
			hook()
		}
	})
}

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

func (s *IndexState) LoadOverlay() *JournaledOverlay {
	return s.overlay.Load()
}

func (s *IndexState) StoreOverlay(overlay *JournaledOverlay) {
	old := s.overlay.Swap(overlay)
	if old != nil && old != overlay {
		_ = old.Close()
	}
}

func (s *IndexState) ClearOverlay() {
	old := s.overlay.Swap(nil)
	if old != nil {
		_ = old.Close()
	}
}

func (s *IndexState) LoadMaintenanceState() *MaintenanceState {
	return s.maintenance.Load()
}

func (s *IndexState) StoreMaintenanceState(ms *MaintenanceState) {
	if ms == nil {
		ms = &MaintenanceState{}
	}
	s.maintenance.Store(ms.Clone())
}

func (s *IndexState) RecordRowsModified(delta uint64) {
	if delta == 0 {
		return
	}
	if current := s.maintenance.Load(); current != nil {
		current.RecordRowsModified(delta)
	}
}

func (s *IndexState) RecordClusterMutation(oldCluster int64, oldVec []float32, newCluster int64, newVec []float32) {
	if oldCluster <= 0 && newCluster <= 0 {
		return
	}
	if current := s.maintenance.Load(); current != nil {
		current.RecordClusterMutation(oldCluster, oldVec, newCluster, newVec)
	}
}

// LoadSegmentStore returns the active stable on-disk generation snapshot.
func (s *IndexState) LoadSegmentStore() *SegmentGeneration {
	return s.segmentStore.Load()
}

// StoreSegmentStore installs generation as the active stable on-disk snapshot.
func (s *IndexState) StoreSegmentStore(generation *SegmentGeneration) {
	if generation != nil && generation.Data != nil {
		warmClusters := s.HotClusters(32)
		if len(warmClusters) == 0 && len(generation.LayoutHotClusters) > 0 {
			warmClusters = append([]int64(nil), generation.LayoutHotClusters...)
		}
		if len(warmClusters) > 0 {
			_ = generation.Data.WarmClusters(warmClusters, 16<<20)
		}
	}
	if generation != nil {
		nextMaintenance := &MaintenanceState{
			ClusterRowCounts:         append([]uint64(nil), generation.ClusterRowCounts...),
			ClusterVectorSums:        cloneVectorSums(generation.ClusterVectorSums),
			RowsModifiedSinceRebuild: generation.RowsModifiedSinceRebuild,
			LastRebuildRowCount:      generation.LastRebuildRowCount,
			ConsecutiveSkewCycles:    generation.ConsecutiveSkewCycles,
		}
		s.StoreMaintenanceState(nextMaintenance)
	}
	old := s.segmentStore.Swap(generation)
	if old != nil && old != generation {
		_ = old.Close()
	}
}

// ClearSegmentStore drops the active stable on-disk snapshot.
func (s *IndexState) ClearSegmentStore() {
	old := s.segmentStore.Swap(nil)
	if old != nil {
		_ = old.Close()
	}
}

func (s *IndexState) RecordClusterHits(clusterIDs []int64) {
	if len(clusterIDs) == 0 || len(s.clusterHits) == 0 {
		return
	}
	for _, clusterID := range clusterIDs {
		if clusterID <= 0 || int(clusterID) >= len(s.clusterHits) {
			continue
		}
		s.clusterHits[clusterID].Add(1)
	}
}

func (s *IndexState) HotClusters(limit int) []int64 {
	scores := s.hotClusterScores(limit)
	if len(scores) == 0 {
		return nil
	}
	clusters := make([]int64, len(scores))
	for i, score := range scores {
		clusters[i] = score.clusterID
	}
	return clusters
}

func (s *IndexState) HotClusterScores(limit int) map[int64]uint64 {
	scores := s.hotClusterScores(limit)
	if len(scores) == 0 {
		return nil
	}
	out := make(map[int64]uint64, len(scores))
	for _, score := range scores {
		out[score.clusterID] = score.hits
	}
	return out
}

type clusterHitScore struct {
	clusterID int64
	hits      uint64
}

func (s *IndexState) hotClusterScores(limit int) []clusterHitScore {
	if len(s.clusterHits) == 0 {
		return nil
	}
	scores := make([]clusterHitScore, 0, len(s.clusterHits)-1)
	for clusterID := 1; clusterID < len(s.clusterHits); clusterID++ {
		hits := s.clusterHits[clusterID].Load()
		if hits == 0 {
			continue
		}
		scores = append(scores, clusterHitScore{
			clusterID: int64(clusterID),
			hits:      hits,
		})
	}
	if len(scores) == 0 {
		return nil
	}
	slices.SortFunc(scores, func(a, b clusterHitScore) int {
		switch {
		case a.hits > b.hits:
			return -1
		case a.hits < b.hits:
			return 1
		case a.clusterID < b.clusterID:
			return -1
		case a.clusterID > b.clusterID:
			return 1
		default:
			return 0
		}
	})
	if limit > 0 && len(scores) > limit {
		scores = scores[:limit]
	}
	return scores
}

// AssignNearest returns the 1-based cluster ID for the nearest centroid.
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

	return int64(clusterID) + 1, nil // 1-based cluster IDs for stable partitions
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
