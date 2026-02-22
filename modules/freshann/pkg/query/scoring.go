package query

import (
	"container/heap"
	"math"
	"runtime"
	"sync"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
)

func Score(metric api.Metric, q, v []float32) float32 {
	switch metric {
	case api.MetricDot:
		return dot(q, v)
	case api.MetricCosine:
		nq := norm(q)
		nv := norm(v)
		if nq == 0 || nv == 0 {
			return 0
		}
		return dot(q, v) / (nq * nv)
	case api.MetricEuclidean:
		return -l2Squared(q, v)
	default:
		return 0
	}
}

func dot(a, b []float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var s float32
	for i := 0; i < n; i++ {
		s += a[i] * b[i]
	}
	return s
}

func norm(a []float32) float32 {
	var s float64
	for _, v := range a {
		s += float64(v * v)
	}
	return float32(math.Sqrt(s))
}

func l2Squared(a, b []float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var s float32
	for i := 0; i < n; i++ {
		d := a[i] - b[i]
		s += d * d
	}
	return s
}

func DistanceFromScore(metric api.Metric, score float32) float32 {
	switch metric {
	case api.MetricEuclidean:
		if score >= 0 {
			return 0
		}
		return float32(math.Sqrt(float64(-score)))
	default:
		return 1 - score
	}
}

type Candidate struct {
	ExternalID []byte
	Score      float32
}

type CandidateDoc struct {
	DocID uint64
	Score float32
}

type minHeap []Candidate

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].Score < h[j].Score }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(Candidate)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func TopK(metric api.Metric, q []float32, candidates map[string][]float32, k int) []Candidate {
	return TopKWithWorkers(metric, q, candidates, k, 0)
}

func TopKWithWorkers(metric api.Metric, q []float32, candidates map[string][]float32, k int, workers int) []Candidate {
	if k <= 0 {
		return nil
	}
	if len(candidates) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
		if workers > 4 {
			workers = 4
		}
	}
	if workers <= 1 || len(candidates) < 4096 {
		return topKSequential(metric, q, candidates, k)
	}

	entries := make([]CandidateEntry, 0, len(candidates))
	for id, vec := range candidates {
		entries = append(entries, CandidateEntry{ID: id, Vec: vec})
	}
	if workers > len(entries) {
		workers = len(entries)
	}
	chunk := (len(entries) + workers - 1) / workers
	locals := make([][]Candidate, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		start := w * chunk
		end := start + chunk
		if end > len(entries) {
			end = len(entries)
		}
		go func(idx, lo, hi int) {
			defer wg.Done()
			if lo >= hi {
				return
			}
			locals[idx] = topKEntries(metric, q, entries[lo:hi], k)
		}(w, start, end)
	}
	wg.Wait()

	merged := &minHeap{}
	heap.Init(merged)
	for _, partial := range locals {
		for _, cand := range partial {
			if merged.Len() < k {
				heap.Push(merged, cand)
				continue
			}
			if (*merged)[0].Score < cand.Score {
				heap.Pop(merged)
				heap.Push(merged, cand)
			}
		}
	}
	out := make([]Candidate, merged.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(merged).(Candidate)
	}
	return out
}

type CandidateEntry struct {
	ID  string
	Vec []float32
}

func topKEntries(metric api.Metric, q []float32, entries []CandidateEntry, k int) []Candidate {
	if k <= 0 {
		return nil
	}
	h := &minHeap{}
	heap.Init(h)
	for _, ent := range entries {
		s := Score(metric, q, ent.Vec)
		cand := Candidate{ExternalID: []byte(ent.ID), Score: s}
		if h.Len() < k {
			heap.Push(h, cand)
			continue
		}
		if (*h)[0].Score < s {
			heap.Pop(h)
			heap.Push(h, cand)
		}
	}
	out := make([]Candidate, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(Candidate)
	}
	return out
}

func topKSequential(metric api.Metric, q []float32, candidates map[string][]float32, k int) []Candidate {
	if k <= 0 {
		return nil
	}
	h := &minHeap{}
	heap.Init(h)
	for id, vec := range candidates {
		s := Score(metric, q, vec)
		cand := Candidate{ExternalID: []byte(id), Score: s}
		if h.Len() < k {
			heap.Push(h, cand)
			continue
		}
		if (*h)[0].Score < s {
			heap.Pop(h)
			heap.Push(h, cand)
		}
	}
	out := make([]Candidate, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(Candidate)
	}
	return out
}

type minHeapDoc []CandidateDoc

func (h minHeapDoc) Len() int            { return len(h) }
func (h minHeapDoc) Less(i, j int) bool  { return h[i].Score < h[j].Score }
func (h minHeapDoc) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeapDoc) Push(x interface{}) { *h = append(*h, x.(CandidateDoc)) }
func (h *minHeapDoc) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type CandidateDocEntry struct {
	DocID uint64
	Vec   []float32
}

func TopKDocIDsWithWorkers(metric api.Metric, q []float32, candidates map[uint64][]float32, k int, workers int) []CandidateDoc {
	if k <= 0 {
		return nil
	}
	if len(candidates) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
		if workers > 4 {
			workers = 4
		}
	}
	if workers <= 1 || len(candidates) < 4096 {
		return topKDocSequential(metric, q, candidates, k)
	}

	entries := make([]CandidateDocEntry, 0, len(candidates))
	for docID, vec := range candidates {
		entries = append(entries, CandidateDocEntry{DocID: docID, Vec: vec})
	}
	if workers > len(entries) {
		workers = len(entries)
	}
	chunk := (len(entries) + workers - 1) / workers
	locals := make([][]CandidateDoc, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		start := w * chunk
		end := start + chunk
		if end > len(entries) {
			end = len(entries)
		}
		go func(idx, lo, hi int) {
			defer wg.Done()
			if lo >= hi {
				return
			}
			locals[idx] = topKDocEntries(metric, q, entries[lo:hi], k)
		}(w, start, end)
	}
	wg.Wait()

	merged := &minHeapDoc{}
	heap.Init(merged)
	for _, partial := range locals {
		for _, cand := range partial {
			if merged.Len() < k {
				heap.Push(merged, cand)
				continue
			}
			if (*merged)[0].Score < cand.Score {
				heap.Pop(merged)
				heap.Push(merged, cand)
			}
		}
	}
	out := make([]CandidateDoc, merged.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(merged).(CandidateDoc)
	}
	return out
}

func topKDocEntries(metric api.Metric, q []float32, entries []CandidateDocEntry, k int) []CandidateDoc {
	if k <= 0 {
		return nil
	}
	h := &minHeapDoc{}
	heap.Init(h)
	for _, ent := range entries {
		s := Score(metric, q, ent.Vec)
		cand := CandidateDoc{DocID: ent.DocID, Score: s}
		if h.Len() < k {
			heap.Push(h, cand)
			continue
		}
		if (*h)[0].Score < s {
			heap.Pop(h)
			heap.Push(h, cand)
		}
	}
	out := make([]CandidateDoc, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(CandidateDoc)
	}
	return out
}

func topKDocSequential(metric api.Metric, q []float32, candidates map[uint64][]float32, k int) []CandidateDoc {
	if k <= 0 {
		return nil
	}
	h := &minHeapDoc{}
	heap.Init(h)
	for docID, vec := range candidates {
		s := Score(metric, q, vec)
		cand := CandidateDoc{DocID: docID, Score: s}
		if h.Len() < k {
			heap.Push(h, cand)
			continue
		}
		if (*h)[0].Score < s {
			heap.Pop(h)
			heap.Push(h, cand)
		}
	}
	out := make([]CandidateDoc, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(CandidateDoc)
	}
	return out
}
