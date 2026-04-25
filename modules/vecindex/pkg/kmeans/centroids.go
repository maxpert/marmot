// Package kmeans provides k-means++ clustering and centroid management for IVF indexes.
package kmeans

import (
	"errors"
	"fmt"
	"math"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/vmihailenco/msgpack/v5"
)

// CentroidSet holds a snapshot of cluster centroids at a given epoch.
// Index uses atomic.Pointer[CentroidSet] to allow lock-free reads.
// CentroidSet is immutable after creation via NewCentroidSet.
type CentroidSet struct {
	epoch        uint64
	centroids    [][]float32
	unit         [][]float32
	dim          int
	centroidFlat []float32
	unitFlat     []float32
}

// centroidSetMsg is the msgpack wire format for CentroidSet.
type centroidSetMsg struct {
	Epoch     uint64      `msgpack:"epoch"`
	Centroids [][]float32 `msgpack:"centroids"`
}

// NewCentroidSet creates an immutable CentroidSet from the given epoch and centroid vectors.
// The centroids slice is deep-copied so the caller may reuse it.
func NewCentroidSet(epoch uint64, centroids [][]float32) (*CentroidSet, error) {
	copied := make([][]float32, len(centroids))
	unit := make([][]float32, len(centroids))
	dim := 0
	if len(centroids) > 0 {
		dim = len(centroids[0])
	}
	centroidFlat := make([]float32, 0, len(centroids)*dim)
	unitFlat := make([]float32, 0, len(centroids)*dim)
	for i, c := range centroids {
		if i > 0 && len(c) != dim {
			return nil, fmt.Errorf("kmeans: centroid %d dimension mismatch", i)
		}
		cp := make([]float32, len(c))
		copy(cp, c)
		copied[i] = cp
		centroidFlat = append(centroidFlat, cp...)
		if len(cp) == 0 {
			continue
		}
		up := make([]float32, len(cp))
		copy(up, cp)
		n := metric.Norm(up)
		if n != 0 {
			inv := 1.0 / n
			for j := range up {
				up[j] *= inv
			}
		}
		unit[i] = up
		unitFlat = append(unitFlat, up...)
	}
	return &CentroidSet{epoch: epoch, centroids: copied, unit: unit, dim: dim, centroidFlat: centroidFlat, unitFlat: unitFlat}, nil
}

// Len returns the number of centroids in the set.
func (cs *CentroidSet) Len() int {
	return len(cs.centroids)
}

// Epoch returns the generation counter for this centroid set.
func (cs *CentroidSet) Epoch() uint64 {
	return cs.epoch
}

// Get returns a mutable copy of the centroid vector for the given cluster ID.
// Use this when the caller needs to modify the returned slice.
// Returns an error if clusterID is out of range.
func (cs *CentroidSet) Get(clusterID uint32) ([]float32, error) {
	if int(clusterID) >= len(cs.centroids) {
		return nil, fmt.Errorf("kmeans: cluster ID %d out of range (len=%d)", clusterID, len(cs.centroids))
	}
	src := cs.centroids[clusterID]
	cp := make([]float32, len(src))
	copy(cp, src)
	return cp, nil
}

// GetReadOnly returns the internal centroid slice for read-only access.
// Callers MUST NOT mutate the returned slice — it aliases the CentroidSet's
// internal state and is shared across concurrent readers. For a mutable copy
// use Get instead.
// Returns an error if clusterID is out of range.
func (cs *CentroidSet) GetReadOnly(clusterID uint32) ([]float32, error) {
	if int(clusterID) >= len(cs.centroids) {
		return nil, fmt.Errorf("kmeans: cluster ID %d out of range (len=%d)", clusterID, len(cs.centroids))
	}
	return cs.centroids[clusterID], nil
}

// Encode serialises the CentroidSet to msgpack bytes.
func (cs *CentroidSet) Encode() ([]byte, error) {
	return msgpack.Marshal(&centroidSetMsg{
		Epoch:     cs.epoch,
		Centroids: cs.centroids,
	})
}

// AssignNearest returns the 0-based cluster ID and distance for the nearest
// centroid in the set. Delegates to Assign using the caller-supplied metric.
// Returns an error if vec length mismatches centroid dimensionality.
func (cs *CentroidSet) AssignNearest(vec []float32, m metric.Metric) (uint32, float32, error) {
	if m == metric.MetricCosine {
		return cs.assignNearestUnit(vec)
	}
	return Assign(vec, cs.centroids, m)
}

// AssignTopN returns the n nearest 0-based cluster IDs sorted by ascending
// distance. Delegates to AssignTopN using the caller-supplied metric. n is
// clamped to [0, cs.Len()] by the package-level implementation.
// Returns an error if vec length mismatches centroid dimensionality.
func (cs *CentroidSet) AssignTopN(vec []float32, n int, m metric.Metric) ([]uint32, []float32, error) {
	if m == metric.MetricCosine {
		return cs.assignTopNUnit(vec, n)
	}
	return AssignTopN(vec, cs.centroids, n, m)
}

// AssignTopNUntilBudget returns the nearest centroid prefix needed to satisfy
// budgetRows, ordered by ascending distance. rowCounts must be 0-based and
// aligned with centroid IDs. minProbe and maxProbe bound the selected prefix.
func (cs *CentroidSet) AssignTopNUntilBudget(
	vec []float32,
	rowCounts []uint64,
	budgetRows uint64,
	minProbe int,
	maxProbe int,
	m metric.Metric,
) ([]uint32, []float32, error) {
	if cs == nil || len(cs.centroids) == 0 {
		return nil, nil, errors.New("kmeans: centroids must not be empty")
	}
	if len(rowCounts) != len(cs.centroids) {
		return nil, nil, fmt.Errorf("kmeans: rowCounts length %d does not match centroids %d", len(rowCounts), len(cs.centroids))
	}
	if len(vec) != cs.dim {
		return nil, nil, errors.New("kmeans: dimension mismatch between vec and centroids")
	}
	if minProbe < 1 {
		minProbe = 1
	}
	if minProbe > len(cs.centroids) {
		minProbe = len(cs.centroids)
	}
	if maxProbe <= 0 || maxProbe > len(cs.centroids) {
		maxProbe = len(cs.centroids)
	}
	if maxProbe < minProbe {
		maxProbe = minProbe
	}

	q := vec
	var err error
	if m == metric.MetricCosine {
		q, err = normalizeQuery(vec, cs.unit)
		if err != nil {
			return nil, nil, err
		}
	}
	entries := make([]centroidTopNEntry, len(cs.centroids))
	for i := range cs.centroids {
		var dist float32
		if m == metric.MetricCosine && cs.dim > 0 && len(cs.unitFlat) == len(cs.centroids)*cs.dim {
			start := i * cs.dim
			dist = metric.CosineDistanceUnit(q, cs.unitFlat[start:start+cs.dim])
		} else if cs.dim > 0 && len(cs.centroidFlat) == len(cs.centroids)*cs.dim {
			start := i * cs.dim
			dist = metric.Distance(m, q, cs.centroidFlat[start:start+cs.dim])
		} else {
			dist = metric.Distance(m, q, cs.centroids[i])
		}
		entries[i] = centroidTopNEntry{id: uint32(i), dist: dist}
	}
	heapifyBest(entries)

	ids := make([]uint32, 0, maxProbe)
	dists := make([]float32, 0, maxProbe)
	var cumulative uint64
	for len(entries) > 0 && len(ids) < maxProbe {
		entry := popBest(&entries)
		ids = append(ids, entry.id)
		dists = append(dists, entry.dist)
		cumulative += rowCounts[entry.id]
		if len(ids) >= minProbe && budgetRows > 0 && cumulative >= budgetRows {
			break
		}
	}
	return ids, dists, nil
}

// Snapshot returns a deep copy of all centroid vectors. Use this when the
// caller needs an independent mutable copy, e.g. as a warm start for a
// subsequent k-means run.
func (cs *CentroidSet) Snapshot() [][]float32 {
	out := make([][]float32, len(cs.centroids))
	for i, c := range cs.centroids {
		cp := make([]float32, len(c))
		copy(cp, c)
		out[i] = cp
	}
	return out
}

// DecodeCentroidSet deserialises a CentroidSet from msgpack bytes produced by Encode.
func DecodeCentroidSet(data []byte) (*CentroidSet, error) {
	if len(data) == 0 {
		return nil, errors.New("kmeans: cannot decode empty data")
	}
	var msg centroidSetMsg
	if err := msgpack.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("kmeans: decode centroid set: %w", err)
	}
	return NewCentroidSet(msg.Epoch, msg.Centroids)
}

func (cs *CentroidSet) assignNearestUnit(vec []float32) (uint32, float32, error) {
	q, err := normalizeQuery(vec, cs.unit)
	if err != nil {
		return 0, 0, err
	}
	bestID := uint32(0)
	bestDist := metric.CosineDistanceUnit(q, cs.unitSlice(0))
	for i := 1; i < len(cs.unit); i++ {
		d := metric.CosineDistanceUnit(q, cs.unitSlice(i))
		if d < bestDist {
			bestDist = d
			bestID = uint32(i)
		}
	}
	return bestID, bestDist, nil
}

func (cs *CentroidSet) assignTopNUnit(vec []float32, n int) ([]uint32, []float32, error) {
	if n == 0 {
		return []uint32{}, []float32{}, nil
	}
	q, err := normalizeQuery(vec, cs.unit)
	if err != nil {
		return nil, nil, err
	}
	if n > len(cs.unit) {
		n = len(cs.unit)
	}
	h := newCentroidTopNHeap(n)
	for i := range cs.unit {
		h.Push(uint32(i), metric.CosineDistanceUnit(q, cs.unitSlice(i)))
	}
	return h.Drain()
}

func (cs *CentroidSet) unitSlice(i int) []float32 {
	if cs != nil && cs.dim > 0 && len(cs.unitFlat) == len(cs.unit)*cs.dim {
		start := i * cs.dim
		return cs.unitFlat[start : start+cs.dim]
	}
	return cs.unit[i]
}

func normalizeQuery(vec []float32, centroids [][]float32) ([]float32, error) {
	if len(centroids) == 0 {
		return nil, errors.New("kmeans: centroids must not be empty")
	}
	if len(vec) != len(centroids[0]) {
		return nil, errors.New("kmeans: dimension mismatch between vec and centroids")
	}
	n := metric.Norm(vec)
	if n == 0 {
		return vec, nil
	}
	if math.Abs(float64(n-1)) <= 1e-5 {
		return vec, nil
	}
	q := make([]float32, len(vec))
	inv := 1.0 / n
	for i, value := range vec {
		q[i] = value * inv
	}
	return q, nil
}

type centroidTopNEntry struct {
	id   uint32
	dist float32
}

type centroidTopNHeap struct {
	items []centroidTopNEntry
	limit int
}

func newCentroidTopNHeap(limit int) *centroidTopNHeap {
	return &centroidTopNHeap{
		items: make([]centroidTopNEntry, 0, limit),
		limit: limit,
	}
}

func (h *centroidTopNHeap) Push(id uint32, dist float32) {
	entry := centroidTopNEntry{id: id, dist: dist}
	if len(h.items) < h.limit {
		h.items = append(h.items, entry)
		h.siftUp(len(h.items) - 1)
		return
	}
	if !worseThan(h.items[0], entry) {
		return
	}
	h.items[0] = entry
	h.siftDown(0)
}

func (h *centroidTopNHeap) Drain() ([]uint32, []float32, error) {
	sortCentroidEntries(h.items)
	ids := make([]uint32, len(h.items))
	dists := make([]float32, len(h.items))
	for i, item := range h.items {
		ids[i] = item.id
		dists[i] = item.dist
	}
	return ids, dists, nil
}

func (h *centroidTopNHeap) siftUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !worseThan(h.items[i], h.items[p]) {
			break
		}
		h.items[i], h.items[p] = h.items[p], h.items[i]
		i = p
	}
}

func (h *centroidTopNHeap) siftDown(i int) {
	for {
		l := 2*i + 1
		if l >= len(h.items) {
			return
		}
		worst := l
		r := l + 1
		if r < len(h.items) && worseThan(h.items[r], h.items[l]) {
			worst = r
		}
		if !worseThan(h.items[worst], h.items[i]) {
			return
		}
		h.items[i], h.items[worst] = h.items[worst], h.items[i]
		i = worst
	}
}

func worseThan(a, b centroidTopNEntry) bool {
	if a.dist != b.dist {
		return a.dist > b.dist
	}
	return a.id > b.id
}

func sortCentroidEntries(items []centroidTopNEntry) {
	for i := 1; i < len(items); i++ {
		j := i
		for j > 0 {
			prev := j - 1
			if !worseThan(items[prev], items[j]) {
				break
			}
			items[prev], items[j] = items[j], items[prev]
			j--
		}
	}
}

func betterThan(a, b centroidTopNEntry) bool {
	if a.dist != b.dist {
		return a.dist < b.dist
	}
	return a.id < b.id
}

func heapifyBest(items []centroidTopNEntry) {
	for i := len(items)/2 - 1; i >= 0; i-- {
		siftBestDown(items, i)
	}
}

func popBest(items *[]centroidTopNEntry) centroidTopNEntry {
	h := *items
	best := h[0]
	last := len(h) - 1
	h[0] = h[last]
	h = h[:last]
	if len(h) > 0 {
		siftBestDown(h, 0)
	}
	*items = h
	return best
}

func siftBestDown(items []centroidTopNEntry, i int) {
	for {
		l := 2*i + 1
		if l >= len(items) {
			return
		}
		best := l
		r := l + 1
		if r < len(items) && betterThan(items[r], items[l]) {
			best = r
		}
		if !betterThan(items[best], items[i]) {
			return
		}
		items[i], items[best] = items[best], items[i]
		i = best
	}
}
