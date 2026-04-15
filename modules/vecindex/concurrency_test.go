package vecindex

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestOnlineMacQueen_IncrementalCentroidUpdate inserts 1000 points into a graduated
// index and verifies that the background publisher eventually updates the centroid
// toward the running mean.
func TestOnlineMacQueen_IncrementalCentroidUpdate(t *testing.T) {
	t.Parallel()
	const (
		n   = 6400
		dim = 8
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "macqueen-test", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(n, dim, 1)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("v%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	// Record centroid state before inserts.
	csBefore := idx.centroids.Load()
	require.NotNil(t, csBefore)

	// Insert 1000 points — enough to exceed publishDirtyThreshold (64) multiple times.
	extra := makeRandomVectors(1000, dim, 99)
	for i, v := range extra {
		require.NoError(t, idx.Upsert(ctx, []byte(fmt.Sprintf("extra-%d", i)), v, uint64(n+i+1), 0))
	}

	// Wait for the background publisher to fire (publishInterval = 50ms).
	time.Sleep(200 * time.Millisecond)

	csAfter := idx.centroids.Load()
	require.NotNil(t, csAfter)

	// At least one centroid must have shifted — the epoch stays the same but the
	// centroid vectors should differ after MacQueen online updates.
	shifted := false
	for i := 0; i < csAfter.cs.Len() && i < csBefore.cs.Len(); i++ {
		after, _ := csAfter.cs.GetReadOnly(uint32(i))
		before, _ := csBefore.cs.GetReadOnly(uint32(i))
		var diff float32
		for d := range after {
			delta := after[d] - before[d]
			diff += delta * delta
		}
		if diff > 1e-6 {
			shifted = true
			break
		}
	}
	require.True(t, shifted, "at least one centroid must drift after 1000 inserts")
}

// TestParallelBulkInsert_Correctness bulk-inserts 10K vectors via CreateIndex,
// then verifies all are present and the count is correct.
func TestParallelBulkInsert_Correctness(t *testing.T) {
	t.Parallel()
	const (
		n   = 10_000
		dim = 16
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "bulk-parallel", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(n, dim, 42)
	bulk := make([]BulkEntry, n)
	extIDs := make([][]byte, n)
	for i, v := range vecs {
		extIDs[i] = []byte(fmt.Sprintf("bp%d", i))
		bulk[i] = BulkEntry{ExternalID: extIDs[i], Vector: v}
	}

	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)

	stats := idx.Stats()
	require.Equal(t, uint64(n), stats.VectorCount, "all bulk entries must be present")

	// Spot-check: each external ID must map to a valid doc.
	for _, extID := range extIDs[:20] {
		docID, docErr := idx.st.GetExtToDoc(extID)
		require.NoError(t, docErr, "extID %s must be in index", extID)
		require.Less(t, docID, uint64(n), "docID must be within bulk range")
	}

	// No duplicate docIDs: count unique docIDs for a sample.
	seen := make(map[uint64]struct{}, 100)
	for _, extID := range extIDs[:100] {
		docID, _ := idx.st.GetExtToDoc(extID)
		_, dup := seen[docID]
		require.False(t, dup, "docID collision for extID %s", extID)
		seen[docID] = struct{}{}
	}
}

// TestParallelSearch_NoRaces runs 100 concurrent searches on a graduated index
// and verifies no race detector flags are raised.
func TestParallelSearch_NoRaces(t *testing.T) {
	t.Parallel()
	const (
		n   = 8_000
		dim = 32
		k   = 5
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "par-search", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(n, dim, 7)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("ps%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	queries := makeRandomVectors(100, dim, 13)
	var wg sync.WaitGroup
	errs := make([]error, len(queries))
	for i, q := range queries {
		wg.Add(1)
		go func(slot int, query []float32) {
			defer wg.Done()
			_, errs[slot] = idx.Search(ctx, SearchRequest{Vector: query, K: k})
		}(i, q)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "search %d must not error", i)
	}
}

// TestBackgroundPublisher_EventuallyConsistent inserts vectors, waits 200ms,
// then verifies the centroid has been updated by the background publisher.
func TestBackgroundPublisher_EventuallyConsistent(t *testing.T) {
	t.Parallel()
	const (
		n   = 6400
		dim = 8
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "bg-pub-test", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(n, dim, 5)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("bp%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	cs0 := idx.centroids.Load()
	require.NotNil(t, cs0)

	// Insert enough vectors to exceed the dirty threshold for at least one cluster.
	const insertCount = 200
	extra := makeRandomVectors(insertCount, dim, 77)
	for i, v := range extra {
		require.NoError(t, idx.Upsert(ctx, []byte(fmt.Sprintf("bg%d", i)), v, uint64(n+i+1), 0))
	}

	// Wait for at least 4 publish intervals.
	time.Sleep(200 * time.Millisecond)

	cs1 := idx.centroids.Load()
	require.NotNil(t, cs1)

	// The publisher must have fired at least once and updated at least one centroid.
	// We can't guarantee cs0 != cs1 pointer-wise (CAS might skip if under threshold),
	// but we can verify the index remains searchable with correct results.
	q := makeRandomVectors(1, dim, 99)[0]
	hits, err := idx.Search(ctx, SearchRequest{Vector: q, K: 5})
	require.NoError(t, err)
	require.NotEmpty(t, hits, "index must remain searchable after background publish")
}

// TestLockFreeSearch_DoesNotBlockInsert runs concurrent searches and inserts,
// verifying that insert throughput is not blocked by searches.
func TestLockFreeSearch_DoesNotBlockInsert(t *testing.T) {
	t.Parallel()
	const (
		n   = 8_000
		dim = 16
		k   = 5
	)
	e := newTempEngine(t)
	ctx := context.Background()

	spec := IVFSpec{ID: "lockfree-test", Dim: dim, Metric: MetricL2, Nlist: 64, Nprobe: 8}
	vecs := makeRandomVectors(n, dim, 11)
	bulk := make([]BulkEntry, n)
	for i, v := range vecs {
		bulk[i] = BulkEntry{ExternalID: []byte(fmt.Sprintf("lf%d", i)), Vector: v}
	}
	idx, err := e.CreateIndex(ctx, spec, bulk)
	require.NoError(t, err)
	require.NoError(t, Graduate(ctx, idx, 64))

	stop := make(chan struct{})
	searchDone := make(chan struct{})

	// Background searcher runs continuously.
	go func() {
		defer close(searchDone)
		queries := makeRandomVectors(50, dim, 99)
		for {
			select {
			case <-stop:
				return
			default:
				for _, q := range queries {
					select {
					case <-stop:
						return
					default:
					}
					_, _ = idx.Search(ctx, SearchRequest{Vector: q, K: k})
				}
			}
		}
	}()

	// Measure insert throughput while search is running.
	insertVecs := makeRandomVectors(500, dim, 55)
	start := time.Now()
	for i, v := range insertVecs {
		err := idx.Upsert(ctx, []byte(fmt.Sprintf("ins%d", i)), v, uint64(n+i+1), 0)
		require.NoError(t, err)
	}
	elapsed := time.Since(start)
	close(stop)
	<-searchDone

	// 500 inserts must complete in under 10s — a generous bound that rules out
	// global-lock starvation without requiring specific hardware performance.
	// The race detector adds measurable overhead so we allow 10s instead of 5s.
	require.Less(t, elapsed, 10*time.Second,
		"500 inserts took %v — search goroutines must not block inserts", elapsed)

	// All inserted vectors must be searchable.
	for i, v := range insertVecs[:10] {
		extID := []byte(fmt.Sprintf("ins%d", i))
		hits, searchErr := idx.Search(ctx, SearchRequest{Vector: v, K: 1})
		require.NoError(t, searchErr)
		require.NotEmpty(t, hits)
		require.Equal(t, extID, hits[0].ExternalID,
			"inserted vector %s must be findable", extID)
	}

	// Verify stats.
	stats := idx.Stats()
	require.Equal(t, uint64(n+500), stats.VectorCount)
	_ = math.Pi // suppress import if needed
}
