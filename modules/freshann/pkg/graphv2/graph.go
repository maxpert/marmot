package graphv2

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
)

// State is the persisted graph representation for the V2 format.
type State struct {
	Metric api.Metric
	R      int
	Start  []uint64
	Adj    map[uint64][]uint64
}

// Index is an in-memory mutable ANN graph.
type Index struct {
	mu    sync.RWMutex
	state State
}

func New(metric api.Metric, r int) *Index {
	if r <= 0 {
		r = 16
	}
	return &Index{state: State{Metric: metric, R: r, Adj: make(map[uint64][]uint64)}}
}

func FromState(st State) *Index {
	if st.R <= 0 {
		st.R = 16
	}
	if st.Adj == nil {
		st.Adj = make(map[uint64][]uint64)
	}
	return &Index{state: st}
}

func (g *Index) SnapshotState() State {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := State{
		Metric: g.state.Metric,
		R:      g.state.R,
		Start:  append([]uint64(nil), g.state.Start...),
		Adj:    make(map[uint64][]uint64, len(g.state.Adj)),
	}
	for docID, neighbors := range g.state.Adj {
		out.Adj[docID] = append([]uint64(nil), neighbors...)
	}
	return out
}

func (g *Index) Build(vectors map[uint64][]float32) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(vectors) == 0 {
		g.state.Start = nil
		g.state.Adj = map[uint64][]uint64{}
		return nil
	}

	ids := make([]uint64, 0, len(vectors))
	for id := range vectors {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	adj := make(map[uint64][]uint64, len(ids))
	for _, id := range ids {
		h := &minHeap{}
		heap.Init(h)
		for _, other := range ids {
			if other == id {
				continue
			}
			s := score(g.state.Metric, vectors[id], vectors[other])
			cand := pair{docID: other, score: s}
			if h.Len() < g.state.R {
				heap.Push(h, cand)
				continue
			}
			if h.Len() > 0 && (*h)[0].score < cand.score {
				heap.Pop(h)
				heap.Push(h, cand)
			}
		}
		neighbors := make([]pair, h.Len())
		for i := len(neighbors) - 1; i >= 0; i-- {
			neighbors[i] = heap.Pop(h).(pair)
		}
		adj[id] = make([]uint64, len(neighbors))
		for i := range neighbors {
			adj[id][i] = neighbors[i].docID
		}
	}

	g.state.Adj = adj
	startCount := 8
	if len(ids) < startCount {
		startCount = len(ids)
	}
	g.state.Start = append([]uint64(nil), ids[:startCount]...)
	return nil
}

func (g *Index) Insert(docID uint64, vec []float32, lSearch int, beam int, getVec func(docID uint64) ([]float32, bool)) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.state.Adj == nil {
		g.state.Adj = make(map[uint64][]uint64)
	}
	if _, exists := g.state.Adj[docID]; exists {
		g.removeNodeLocked(docID)
	}
	if len(g.state.Adj) == 0 {
		g.state.Adj[docID] = nil
		g.state.Start = []uint64{docID}
		return
	}
	if lSearch <= 0 {
		lSearch = 64
	}
	if beam <= 0 {
		beam = 8
	}

	candidates := g.searchUnlocked(vec, g.state.R*4, lSearch, beam, getVec)
	neighbors := make([]uint64, 0, g.state.R)
	for _, cand := range candidates {
		if cand.docID == docID {
			continue
		}
		neighbors = append(neighbors, cand.docID)
		if len(neighbors) >= g.state.R {
			break
		}
	}
	g.state.Adj[docID] = neighbors

	for _, nb := range neighbors {
		list := append(g.state.Adj[nb], docID)
		nbVec, ok := getVec(nb)
		if !ok {
			if len(list) > g.state.R {
				list = list[:g.state.R]
			}
			g.state.Adj[nb] = list
			continue
		}
		g.state.Adj[nb] = g.trimNeighborsByScore(nbVec, list, getVec)
	}

	for _, sid := range g.state.Start {
		if sid == docID {
			return
		}
	}
	if len(g.state.Start) < 8 {
		g.state.Start = append(g.state.Start, docID)
	}
}

func (g *Index) RemoveNode(docID uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeNodeLocked(docID)
}

func (g *Index) removeNodeLocked(docID uint64) {
	delete(g.state.Adj, docID)
	for id, neighbors := range g.state.Adj {
		if len(neighbors) == 0 {
			continue
		}
		filtered := neighbors[:0]
		for _, nb := range neighbors {
			if nb != docID {
				filtered = append(filtered, nb)
			}
		}
		g.state.Adj[id] = filtered
	}
	if len(g.state.Start) > 0 {
		filtered := g.state.Start[:0]
		for _, sid := range g.state.Start {
			if sid != docID {
				filtered = append(filtered, sid)
			}
		}
		g.state.Start = filtered
	}
}

func (g *Index) trimNeighborsByScore(centerVec []float32, neighbors []uint64, getVec func(docID uint64) ([]float32, bool)) []uint64 {
	if len(neighbors) <= g.state.R {
		return dedupe(neighbors)
	}
	unique := dedupe(neighbors)
	type scored struct {
		docID uint64
		score float32
	}
	items := make([]scored, 0, len(unique))
	for _, id := range unique {
		vec, ok := getVec(id)
		if !ok {
			continue
		}
		items = append(items, scored{docID: id, score: score(g.state.Metric, centerVec, vec)})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].score > items[j].score })
	if len(items) > g.state.R {
		items = items[:g.state.R]
	}
	out := make([]uint64, len(items))
	for i := range items {
		out[i] = items[i].docID
	}
	return out
}

func dedupe(in []uint64) []uint64 {
	seen := make(map[uint64]struct{}, len(in))
	out := make([]uint64, 0, len(in))
	for _, id := range in {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func (g *Index) searchUnlocked(queryVec []float32, topK int, lSearch int, beam int, getVec func(docID uint64) ([]float32, bool)) []pair {
	if topK <= 0 {
		topK = 10
	}
	if len(g.state.Adj) == 0 || len(g.state.Start) == 0 {
		return nil
	}

	visited := make(map[uint64]struct{}, lSearch*2)
	frontier := &minHeap{}
	heap.Init(frontier)
	for _, id := range g.state.Start {
		if _, ok := visited[id]; ok {
			continue
		}
		vec, ok := getVec(id)
		if !ok {
			continue
		}
		visited[id] = struct{}{}
		s := score(g.state.Metric, queryVec, vec)
		heap.Push(frontier, pair{docID: id, score: -s})
	}

	best := &topKHeap{}
	heap.Init(&best.h)
	expanded := 0
	for frontier.Len() > 0 && expanded < lSearch {
		current := heap.Pop(frontier).(pair)
		id := current.docID
		expanded++
		if vec, ok := getVec(id); ok {
			s := score(g.state.Metric, queryVec, vec)
			best.push(topK, pair{docID: id, score: s})
		}
		neighbors := g.state.Adj[id]
		limit := len(neighbors)
		if limit > beam {
			limit = beam
		}
		for n := 0; n < limit; n++ {
			cand := neighbors[n]
			if _, ok := visited[cand]; ok {
				continue
			}
			vec, ok := getVec(cand)
			if !ok {
				continue
			}
			visited[cand] = struct{}{}
			s := score(g.state.Metric, queryVec, vec)
			heap.Push(frontier, pair{docID: cand, score: -s})
		}
	}
	return best.sortedDesc()
}

// Search returns matching docIDs, traversal step count, and optional error.
func (g *Index) Search(queryVec []float32, topK int, lSearch int, beam int,
	getVec func(docID uint64) ([]float32, bool), allow func(docID uint64) bool) ([]uint64, int, error) {
	if topK <= 0 {
		topK = 10
	}
	if lSearch <= 0 {
		lSearch = 64
	}
	if beam <= 0 {
		beam = 8
	}

	g.mu.RLock()
	metric := g.state.Metric
	adj := g.state.Adj
	starts := append([]uint64(nil), g.state.Start...)
	g.mu.RUnlock()

	if len(adj) == 0 {
		return nil, 0, nil
	}
	if len(starts) == 0 {
		return nil, 0, fmt.Errorf("graph has no start nodes")
	}

	visited := make(map[uint64]struct{}, lSearch*2)
	frontier := &minHeap{}
	heap.Init(frontier)
	for _, id := range starts {
		if _, ok := visited[id]; ok {
			continue
		}
		vec, ok := getVec(id)
		if !ok {
			continue
		}
		visited[id] = struct{}{}
		s := score(metric, queryVec, vec)
		heap.Push(frontier, pair{docID: id, score: -s})
	}

	best := &topKHeap{}
	heap.Init(&best.h)
	expanded := 0

	for frontier.Len() > 0 && expanded < lSearch {
		current := heap.Pop(frontier).(pair)
		id := current.docID
		expanded++

		if allow(id) {
			if vec, ok := getVec(id); ok {
				s := score(metric, queryVec, vec)
				best.push(topK, pair{docID: id, score: s})
			}
		}

		neighbors := adj[id]
		limit := len(neighbors)
		if limit > beam {
			limit = beam
		}
		for n := 0; n < limit; n++ {
			cand := neighbors[n]
			if _, ok := visited[cand]; ok {
				continue
			}
			vec, ok := getVec(cand)
			if !ok {
				continue
			}
			visited[cand] = struct{}{}
			s := score(metric, queryVec, vec)
			heap.Push(frontier, pair{docID: cand, score: -s})
		}
	}

	pairs := best.sortedDesc()
	out := make([]uint64, len(pairs))
	for i := range pairs {
		out[i] = pairs[i].docID
	}
	return out, expanded, nil
}

func score(metric api.Metric, q, v []float32) float32 {
	if len(q) != len(v) {
		return -1e9
	}
	switch metric {
	case api.MetricDot:
		var s float32
		for i := range q {
			s += q[i] * v[i]
		}
		return s
	case api.MetricCosine:
		var dot, qn, vn float64
		for i := range q {
			qf := float64(q[i])
			vf := float64(v[i])
			dot += qf * vf
			qn += qf * qf
			vn += vf * vf
		}
		if qn == 0 || vn == 0 {
			return 0
		}
		return float32(dot / (math.Sqrt(qn) * math.Sqrt(vn)))
	case api.MetricEuclidean:
		var l2 float32
		for i := range q {
			d := q[i] - v[i]
			l2 += d * d
		}
		return -l2
	default:
		return 0
	}
}

type pair struct {
	docID uint64
	score float32
}

type minHeap []pair

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].score < h[j].score }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(pair)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type topKHeap struct{ h minHeap }

func (t *topKHeap) push(k int, p pair) {
	if t.h.Len() < k {
		heap.Push(&t.h, p)
		return
	}
	if t.h.Len() > 0 && t.h[0].score < p.score {
		heap.Pop(&t.h)
		heap.Push(&t.h, p)
	}
}

func (t *topKHeap) sortedDesc() []pair {
	out := make([]pair, t.h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&t.h).(pair)
	}
	return out
}
