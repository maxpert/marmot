// Package kmeans provides k-means++ clustering and centroid management for IVF indexes.
package kmeans

import (
	"errors"
	"fmt"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/vmihailenco/msgpack/v5"
)

// CentroidSet holds a snapshot of cluster centroids at a given epoch.
// Index uses atomic.Pointer[CentroidSet] to allow lock-free reads.
// CentroidSet is immutable after creation via NewCentroidSet.
type CentroidSet struct {
	epoch     uint64
	centroids [][]float32
	unit      [][]float32
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
	for i, c := range centroids {
		cp := make([]float32, len(c))
		copy(cp, c)
		copied[i] = cp
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
	}
	return &CentroidSet{epoch: epoch, centroids: copied, unit: unit}, nil
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
	bestDist := metric.CosineDistanceUnit(q, cs.unit[0])
	for i := 1; i < len(cs.unit); i++ {
		d := metric.CosineDistanceUnit(q, cs.unit[i])
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
		h.Push(uint32(i), metric.CosineDistanceUnit(q, cs.unit[i]))
	}
	return h.Drain()
}

func normalizeQuery(vec []float32, centroids [][]float32) ([]float32, error) {
	if len(centroids) == 0 {
		return nil, errors.New("kmeans: centroids must not be empty")
	}
	if len(vec) != len(centroids[0]) {
		return nil, errors.New("kmeans: dimension mismatch between vec and centroids")
	}
	q := make([]float32, len(vec))
	copy(q, vec)
	n := metric.Norm(q)
	if n == 0 {
		return q, nil
	}
	inv := 1.0 / n
	for i := range q {
		q[i] *= inv
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
