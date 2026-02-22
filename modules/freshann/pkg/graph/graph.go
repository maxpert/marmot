package graph

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
)

type State struct {
	Metric api.Metric          `json:"metric"`
	R      int                 `json:"r"`
	Start  []string            `json:"start"`
	Adj    map[string][]string `json:"adj"`
}

type Index struct {
	mu    sync.RWMutex
	state State
}

func (g *Index) SnapshotState() State {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := State{
		Metric: g.state.Metric,
		R:      g.state.R,
		Start:  append([]string(nil), g.state.Start...),
		Adj:    make(map[string][]string, len(g.state.Adj)),
	}
	for id, nb := range g.state.Adj {
		out.Adj[id] = append([]string(nil), nb...)
	}
	return out
}

func New(metric api.Metric, r int) *Index {
	if r <= 0 {
		r = 16
	}
	return &Index{state: State{Metric: metric, R: r, Adj: make(map[string][]string)}}
}

func FromState(st State) *Index {
	if st.Adj == nil {
		st.Adj = make(map[string][]string)
	}
	if st.R <= 0 {
		st.R = 16
	}
	return &Index{state: st}
}

func (g *Index) Build(vectors map[string][]float32) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(vectors) == 0 {
		g.state.Adj = map[string][]string{}
		g.state.Start = nil
		return nil
	}
	ids := make([]string, 0, len(vectors))
	for id := range vectors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	adj := make(map[string][]string, len(ids))
	for _, id := range ids {
		h := &minHeap{}
		heap.Init(h)
		for _, other := range ids {
			if other == id {
				continue
			}
			s := score(g.state.Metric, vectors[id], vectors[other])
			cand := pair{id: other, score: s}
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
		adj[id] = make([]string, len(neighbors))
		for i := range neighbors {
			adj[id][i] = neighbors[i].id
		}
	}

	g.state.Adj = adj
	// stable entry points: up to 8 IDs
	startCount := 8
	if len(ids) < startCount {
		startCount = len(ids)
	}
	g.state.Start = append([]string(nil), ids[:startCount]...)
	return nil
}

func (g *Index) Insert(id string, vec []float32, lSearch int, beam int, getVec func(id string) ([]float32, bool)) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.state.Adj == nil {
		g.state.Adj = make(map[string][]string)
	}
	if _, exists := g.state.Adj[id]; exists {
		g.removeNodeLocked(id)
	}

	if len(g.state.Adj) == 0 {
		g.state.Adj[id] = nil
		g.state.Start = []string{id}
		return
	}

	if lSearch <= 0 {
		lSearch = 64
	}
	if beam <= 0 {
		beam = 8
	}

	candidates := g.searchUnlocked(vec, g.state.R*4, lSearch, beam, getVec)
	neighbors := make([]string, 0, g.state.R)
	for _, p := range candidates {
		if p.id == id {
			continue
		}
		neighbors = append(neighbors, p.id)
		if len(neighbors) >= g.state.R {
			break
		}
	}
	g.state.Adj[id] = neighbors

	for _, nb := range neighbors {
		list := append(g.state.Adj[nb], id)
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

	seen := false
	for _, sid := range g.state.Start {
		if sid == id {
			seen = true
			break
		}
	}
	if !seen {
		if len(g.state.Start) < 8 {
			g.state.Start = append(g.state.Start, id)
		}
	}
}

func (g *Index) RemoveNode(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeNodeLocked(id)
}

func (g *Index) removeNodeLocked(id string) {
	delete(g.state.Adj, id)
	for n, nb := range g.state.Adj {
		if len(nb) == 0 {
			continue
		}
		filtered := nb[:0]
		for _, c := range nb {
			if c != id {
				filtered = append(filtered, c)
			}
		}
		g.state.Adj[n] = filtered
	}
	if len(g.state.Start) > 0 {
		filtered := g.state.Start[:0]
		for _, s := range g.state.Start {
			if s != id {
				filtered = append(filtered, s)
			}
		}
		g.state.Start = filtered
	}
}

func (g *Index) trimNeighborsByScore(centerVec []float32, neighbors []string, getVec func(id string) ([]float32, bool)) []string {
	if len(neighbors) <= g.state.R {
		return dedupeStrings(neighbors)
	}
	u := dedupeStrings(neighbors)
	type scoredNeighbor struct {
		id    string
		score float32
	}
	scored := make([]scoredNeighbor, 0, len(u))
	for _, id := range u {
		vec, ok := getVec(id)
		if !ok {
			continue
		}
		scored = append(scored, scoredNeighbor{id: id, score: score(g.state.Metric, centerVec, vec)})
	}
	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	if len(scored) > g.state.R {
		scored = scored[:g.state.R]
	}
	out := make([]string, len(scored))
	for i := range scored {
		out[i] = scored[i].id
	}
	return out
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (g *Index) searchUnlocked(queryVec []float32, topK int, lSearch int, beam int,
	getVec func(id string) ([]float32, bool)) []pair {
	if topK <= 0 {
		topK = 10
	}
	if len(g.state.Adj) == 0 || len(g.state.Start) == 0 {
		return nil
	}
	visited := make(map[string]struct{}, lSearch*2)
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
		heap.Push(frontier, pair{id: id, score: -s})
	}
	best := &topKHeap{}
	heap.Init(&best.h)
	expanded := 0
	for frontier.Len() > 0 && expanded < lSearch {
		current := heap.Pop(frontier).(pair)
		id := current.id
		expanded++
		vec, ok := getVec(id)
		if ok {
			s := score(g.state.Metric, queryVec, vec)
			best.push(topK, pair{id: id, score: s})
		}
		nb := g.state.Adj[id]
		limit := len(nb)
		if limit > beam {
			limit = beam
		}
		for i := 0; i < limit; i++ {
			cand := nb[i]
			if _, ok := visited[cand]; ok {
				continue
			}
			vec, ok := getVec(cand)
			if !ok {
				continue
			}
			visited[cand] = struct{}{}
			s := score(g.state.Metric, queryVec, vec)
			heap.Push(frontier, pair{id: cand, score: -s})
		}
	}
	return best.sortedDesc()
}

func (g *Index) Search(queryVec []float32, topK int, lSearch int, beam int,
	getVec func(id string) ([]float32, bool), allow func(id string) bool) ([]string, error) {
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
	starts := append([]string(nil), g.state.Start...)
	g.mu.RUnlock()

	if len(adj) == 0 {
		return nil, nil
	}
	if len(starts) == 0 {
		return nil, fmt.Errorf("graph has no start nodes")
	}

	visited := make(map[string]struct{}, lSearch*2)
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
		heap.Push(frontier, pair{id: id, score: -s})
	}

	best := &topKHeap{}
	heap.Init(&best.h)
	expanded := 0

	for frontier.Len() > 0 && expanded < lSearch {
		current := heap.Pop(frontier).(pair)
		id := current.id
		expanded++

		if allow(id) {
			vec, ok := getVec(id)
			if ok {
				s := score(metric, queryVec, vec)
				best.push(topK, pair{id: id, score: s})
			}
		}

		nb := adj[id]
		limit := len(nb)
		if limit > beam {
			limit = beam
		}
		for i := 0; i < limit; i++ {
			cand := nb[i]
			if _, ok := visited[cand]; ok {
				continue
			}
			vec, ok := getVec(cand)
			if !ok {
				continue
			}
			visited[cand] = struct{}{}
			s := score(metric, queryVec, vec)
			heap.Push(frontier, pair{id: cand, score: -s})
		}
	}

	outPairs := best.sortedDesc()
	out := make([]string, len(outPairs))
	for i := range outPairs {
		out[i] = outPairs[i].id
	}
	return out, nil
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
	id    string
	score float32
}

type maxHeap []pair

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].score > h[j].score }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x interface{}) { *h = append(*h, x.(pair)) }
func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
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

func (t *topKHeap) Push(x interface{}) { heap.Push(&t.h, x) }
func (t *topKHeap) Pop() interface{}   { return heap.Pop(&t.h) }
