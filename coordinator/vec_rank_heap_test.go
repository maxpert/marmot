package coordinator

import (
	"math/rand"
	"sort"
	"testing"
)

// --- test helpers --------------------------------------------------------

// pushAll is a tiny helper to push a batch of rankItems into a topKHeap.
func pushAll(t *testing.T, h *topKHeap, items []rankItem) {
	t.Helper()
	for _, it := range items {
		h.Push(it.rowid, it.dist)
	}
}

// sortItemsAsc returns a copy of in sorted by ascending dist; ties broken by
// rowid for deterministic comparison in tests.
func sortItemsAsc(in []rankItem) []rankItem {
	out := make([]rankItem, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool {
		if out[i].dist != out[j].dist {
			return out[i].dist < out[j].dist
		}
		return out[i].rowid < out[j].rowid
	})
	return out
}

// --- tests ---------------------------------------------------------------

// TestTopKHeap_NewIsEmpty verifies a freshly constructed heap reports empty
// state and Drain returns no items.
func TestTopKHeap_NewIsEmpty(t *testing.T) {
	t.Parallel()

	h := newTopKHeap(10)
	if h == nil {
		t.Fatalf("newTopKHeap returned nil")
	}
	if got := h.Len(); got != 0 {
		t.Fatalf("Len()=%d, want 0", got)
	}
	if out := h.Drain(); len(out) != 0 {
		t.Fatalf("Drain() on new heap = %v, want nil/empty", out)
	}
}

// TestTopKHeap_UnderfilledAcceptsAll pushes fewer than k items and checks
// Drain returns them all in ascending dist order.
func TestTopKHeap_UnderfilledAcceptsAll(t *testing.T) {
	t.Parallel()

	h := newTopKHeap(10)
	items := []rankItem{
		{rowid: 1, dist: 0.5},
		{rowid: 2, dist: 0.1},
		{rowid: 3, dist: 0.9},
		{rowid: 4, dist: 0.3},
		{rowid: 5, dist: 0.7},
	}
	pushAll(t, h, items)

	if got := h.Len(); got != 5 {
		t.Fatalf("Len()=%d, want 5", got)
	}

	got := h.Drain()
	want := sortItemsAsc(items)
	if len(got) != len(want) {
		t.Fatalf("Drain() len=%d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Drain()[%d]=%+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestTopKHeap_FullRejectsLarger pushes exactly k items, then pushes one
// with a larger dist; the heap contents must not change.
func TestTopKHeap_FullRejectsLarger(t *testing.T) {
	t.Parallel()

	h := newTopKHeap(10)
	initial := make([]rankItem, 10)
	for i := 0; i < 10; i++ {
		initial[i] = rankItem{rowid: int64(i + 1), dist: float32(i+1) * 0.1}
	}
	pushAll(t, h, initial)

	// Snapshot sorted contents before the rejected push.
	before := sortItemsAsc(initial)

	// Current max is 1.0 (rowid 10). Push strictly larger.
	h.Push(99, 2.0)

	if got := h.Len(); got != 10 {
		t.Fatalf("Len()=%d after rejected Push, want 10", got)
	}

	after := h.Drain()
	if len(after) != len(before) {
		t.Fatalf("Drain() len=%d, want %d", len(after), len(before))
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("post-reject Drain()[%d]=%+v, want %+v", i, after[i], before[i])
		}
	}
}

// TestTopKHeap_FullEvictsMaxOnSmaller pushes k distinct-dist items then a
// strictly smaller one; verifies exactly one eviction — old max gone, new
// item present.
func TestTopKHeap_FullEvictsMaxOnSmaller(t *testing.T) {
	t.Parallel()

	h := newTopKHeap(10)
	initial := make([]rankItem, 10)
	for i := 0; i < 10; i++ {
		initial[i] = rankItem{rowid: int64(i + 1), dist: float32(i+1) * 0.1}
	}
	pushAll(t, h, initial)

	// Current max is rowid=10, dist=1.0.
	const newRowid int64 = 777
	const newDist float32 = 0.05
	h.Push(newRowid, newDist)

	if got := h.Len(); got != 10 {
		t.Fatalf("Len()=%d, want 10 after eviction", got)
	}

	out := h.Drain()
	if len(out) != 10 {
		t.Fatalf("Drain() len=%d, want 10", len(out))
	}

	// New item must be present.
	foundNew := false
	for _, it := range out {
		if it.rowid == newRowid {
			if it.dist != newDist {
				t.Fatalf("new item dist=%v, want %v", it.dist, newDist)
			}
			foundNew = true
			break
		}
	}
	if !foundNew {
		t.Fatalf("new item rowid=%d not found after eviction", newRowid)
	}

	// Old max (rowid=10, dist=1.0) must be gone.
	for _, it := range out {
		if it.rowid == 10 {
			t.Fatalf("old max rowid=10 dist=1.0 still present after eviction: %+v", it)
		}
	}
}

// TestTopKHeap_OrderingOnDrain pushes 50 random items into a k=10 heap and
// verifies Drain returns exactly the 10 smallest in ascending order.
func TestTopKHeap_OrderingOnDrain(t *testing.T) {
	t.Parallel()

	const N = 50
	const K = 10

	rng := rand.New(rand.NewSource(1))
	all := make([]rankItem, N)
	for i := 0; i < N; i++ {
		all[i] = rankItem{rowid: int64(i + 1), dist: rng.Float32()}
	}

	h := newTopKHeap(K)
	pushAll(t, h, all)

	if got := h.Len(); got != K {
		t.Fatalf("Len()=%d, want %d", got, K)
	}

	got := h.Drain()
	if len(got) != K {
		t.Fatalf("Drain() len=%d, want %d", len(got), K)
	}

	sortedAll := sortItemsAsc(all)
	want := sortedAll[:K]

	for i := 0; i < K; i++ {
		if got[i] != want[i] {
			t.Fatalf("Drain()[%d]=%+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestTopKHeap_ResetPreservesCapacity verifies that Reset zeros the logical
// length while leaving the backing array reusable — post-Reset behaviour
// must match a fresh heap, and capacity should not shrink.
func TestTopKHeap_ResetPreservesCapacity(t *testing.T) {
	t.Parallel()

	const K = 10
	h := newTopKHeap(K)
	for i := 0; i < K; i++ {
		h.Push(int64(i+1), float32(i+1)*0.1)
	}
	if h.Len() != K {
		t.Fatalf("pre-Reset Len()=%d, want %d", h.Len(), K)
	}

	capBefore := cap(h.items)

	h.Reset()

	if got := h.Len(); got != 0 {
		t.Fatalf("post-Reset Len()=%d, want 0", got)
	}
	if capAfter := cap(h.items); capAfter < capBefore {
		t.Fatalf("post-Reset cap=%d, want >= %d (capacity must not shrink)", capAfter, capBefore)
	}

	// Push the same K items again and verify behaviour matches a fresh heap.
	replay := make([]rankItem, K)
	for i := 0; i < K; i++ {
		replay[i] = rankItem{rowid: int64(i + 1), dist: float32(i+1) * 0.1}
	}
	pushAll(t, h, replay)

	if h.Len() != K {
		t.Fatalf("post-Reset refill Len()=%d, want %d", h.Len(), K)
	}

	// No growth should happen because we refilled to the same K.
	if capAfter := cap(h.items); capAfter != capBefore {
		t.Fatalf("post-refill cap=%d, want unchanged %d (no growth expected)", capAfter, capBefore)
	}

	got := h.Drain()
	want := sortItemsAsc(replay)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("post-Reset Drain()[%d]=%+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestTopKHeap_DrainOnEmptyReturnsNil verifies Drain on an empty heap yields
// a nil / zero-length slice and leaves Len at 0.
func TestTopKHeap_DrainOnEmptyReturnsNil(t *testing.T) {
	t.Parallel()

	h := newTopKHeap(5)
	out := h.Drain()
	if len(out) != 0 {
		t.Fatalf("Drain() on empty = %v, want empty", out)
	}
	if got := h.Len(); got != 0 {
		t.Fatalf("Len()=%d after empty Drain, want 0", got)
	}
}

// TestTopKHeap_TieBreakOnEqualDist pushes k equal-dist items then a (k+1)-th
// with the same dist. The heap must remain at size k and contain rowids only
// from the original k+1 candidates — whether the new item replaces an old
// one or not is implementation-defined, but state must be deterministic
// and well-formed.
func TestTopKHeap_TieBreakOnEqualDist(t *testing.T) {
	t.Parallel()

	const K = 10
	const tieDist float32 = 1.0

	h := newTopKHeap(K)
	// Original K items, rowids 1..K.
	for i := int64(1); i <= K; i++ {
		h.Push(i, tieDist)
	}

	// (K+1)-th with the same dist.
	const newRowid int64 = 999
	h.Push(newRowid, tieDist)

	if got := h.Len(); got != K {
		t.Fatalf("Len()=%d, want %d", got, K)
	}

	out := h.Drain()
	if len(out) != K {
		t.Fatalf("Drain() len=%d, want %d", len(out), K)
	}

	// All rowids must come from {1..K, 999}. No duplicates allowed.
	allowed := make(map[int64]bool, K+1)
	for i := int64(1); i <= K; i++ {
		allowed[i] = true
	}
	allowed[newRowid] = true

	seen := make(map[int64]bool, K)
	for _, it := range out {
		if it.dist != tieDist {
			t.Fatalf("drained item dist=%v, want %v", it.dist, tieDist)
		}
		if !allowed[it.rowid] {
			t.Fatalf("drained rowid=%d not in allowed set", it.rowid)
		}
		if seen[it.rowid] {
			t.Fatalf("drained rowid=%d appears twice", it.rowid)
		}
		seen[it.rowid] = true
	}
}

// TestTopKHeap_StressRandomInsertions drives 10_000 random inserts through a
// k=32 heap and compares Drain output against a brute-force oracle.
func TestTopKHeap_StressRandomInsertions(t *testing.T) {
	t.Parallel()

	const N = 10000
	const K = 32

	rng := rand.New(rand.NewSource(42))
	all := make([]rankItem, N)
	for i := 0; i < N; i++ {
		all[i] = rankItem{rowid: int64(i + 1), dist: rng.Float32()}
	}

	h := newTopKHeap(K)
	for _, it := range all {
		h.Push(it.rowid, it.dist)
	}

	if got := h.Len(); got != K {
		t.Fatalf("Len()=%d, want %d", got, K)
	}

	got := h.Drain()
	if len(got) != K {
		t.Fatalf("Drain() len=%d, want %d", len(got), K)
	}

	// Build oracle: sort all by dist ascending, take first K. Ties on dist
	// can legitimately resolve in either direction in the heap, so compare
	// by dist only (not rowid) — but cross-check counts match.
	sortedAll := sortItemsAsc(all)
	want := sortedAll[:K]

	// Distances must match element-wise since our RNG produces float32 with
	// vanishingly small tie probability over 10k draws.
	for i := 0; i < K; i++ {
		if got[i].dist != want[i].dist {
			t.Fatalf("stress Drain()[%d].dist=%v, want %v (rowid got=%d want=%d)",
				i, got[i].dist, want[i].dist, got[i].rowid, want[i].rowid)
		}
		if got[i].rowid != want[i].rowid {
			t.Fatalf("stress Drain()[%d].rowid=%d, want %d", i, got[i].rowid, want[i].rowid)
		}
	}
}

// TestTopKHeap_PushIsAllocFree pins the zero-allocation guarantee on the
// hot path: once the heap is full at capacity K, subsequent Push calls must
// not allocate. This is the whole point of the typed heap replacing
// container/heap's interface{} boxing.
func TestTopKHeap_PushIsAllocFree(t *testing.T) {
	// Intentionally not t.Parallel(): testing.AllocsPerRun panics when called
	// concurrently with other parallel tests (Go 1.26 hard check).
	const K = 10
	h := newTopKHeap(K)
	// Fill to capacity with a known max of 1.0.
	for i := 0; i < K; i++ {
		h.Push(int64(i+1), float32(i+1)*0.1)
	}

	// Alternate between an accepted and rejected push to exercise both code
	// paths without ever changing allocation behaviour. Rowids stay bounded.
	var toggle int
	avg := testing.AllocsPerRun(1000, func() {
		toggle++
		if toggle%2 == 0 {
			// Smaller than current max → replace path.
			h.Push(int64(toggle), 0.01)
		} else {
			// Larger than current max → reject path.
			h.Push(int64(toggle), 99.0)
		}
	})

	if avg != 0 {
		t.Fatalf("Push allocs/op=%v, want 0", avg)
	}
}

// BenchmarkTopKHeap_Push_K10_N21000 simulates the query hot path: 21k
// candidates streamed into a k=10 heap, per op. Reports ns/op and B/op.
func BenchmarkTopKHeap_Push_K10_N21000(b *testing.B) {
	const K = 10
	const N = 21000

	// Pre-generate random distances so RNG cost doesn't pollute the bench.
	rng := rand.New(rand.NewSource(7))
	dists := make([]float32, N)
	rowids := make([]int64, N)
	for i := 0; i < N; i++ {
		dists[i] = rng.Float32()
		rowids[i] = int64(i + 1)
	}

	h := newTopKHeap(K)
	var sinkRowid int64
	var sinkDist float32

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Reset()
		for j := 0; j < N; j++ {
			h.Push(rowids[j], dists[j])
		}
		// Sink to prevent DCE of the heap state.
		out := h.Drain()
		if len(out) > 0 {
			sinkRowid ^= out[0].rowid
			sinkDist += out[0].dist
		}
	}
	b.StopTimer()

	// Defeat dead-code elimination on the sink values.
	if sinkRowid == 0x7fffffffffffffff && sinkDist < 0 {
		b.Logf("sink=%d %v", sinkRowid, sinkDist)
	}
}
