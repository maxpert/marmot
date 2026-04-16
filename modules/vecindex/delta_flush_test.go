package vecindex

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

// mockFlushDB implements DeltaFlushDB for unit tests.
type mockFlushDB struct {
	mu          sync.Mutex
	deltaRows   []DeltaRow        // rows returned by FetchDeltaEmbeddings
	committed   []DeltaAssignment // all committed assignments
	fetchCalls  int64
	commitCalls int64
	commitErr   error  // if set, CommitFlushBatch returns this error
	onCommit    func() // optional callback after each commit
}

func (m *mockFlushDB) FetchDeltaEmbeddings(_ context.Context, _, _, _ string, limit int) ([]DeltaRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.AddInt64(&m.fetchCalls, 1)
	if len(m.deltaRows) == 0 {
		return nil, nil
	}
	n := limit
	if n > len(m.deltaRows) {
		n = len(m.deltaRows)
	}
	result := make([]DeltaRow, n)
	copy(result, m.deltaRows[:n])
	m.deltaRows = m.deltaRows[n:]
	return result, nil
}

func (m *mockFlushDB) CommitFlushBatch(_ context.Context, _ string, assignments []DeltaAssignment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.AddInt64(&m.commitCalls, 1)
	if m.commitErr != nil {
		return m.commitErr
	}
	m.committed = append(m.committed, assignments...)
	if m.onCommit != nil {
		m.onCommit()
	}
	return nil
}

func (m *mockFlushDB) committedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.committed)
}

func makeDeltaRows(n, dim int) []DeltaRow {
	rows := make([]DeltaRow, n)
	for i := range rows {
		vec := make([]float32, dim)
		for d := range vec {
			vec[d] = float32(i*dim + d)
		}
		rows[i] = DeltaRow{Rowid: int64(i + 1), Embed: Float32ToBytes(vec)}
	}
	return rows
}

func TestDeltaFlushCycle_AssignsAllDelta(t *testing.T) {
	t.Parallel()
	const dim = 4
	const nRows = 1000

	spec := IVFSpec{ID: "test", Dim: dim, Metric: MetricL2, Nlist: 2}
	centroids := [][]float32{
		{0, 0, 0, 0},
		{100, 100, 100, 100},
	}
	cs, err := kmeans.NewCentroidSet(1, centroids)
	require.NoError(t, err)
	state := NewIndexState(spec, cs)

	db := &mockFlushDB{deltaRows: makeDeltaRows(nRows, dim)}
	cfg := DeltaFlushConfig{Interval: time.Millisecond, MaxRows: nRows, BatchSize: 200}

	deltaFlushCycle(context.Background(), cfg, state, db, "test", "docs", "embed")

	// All 1000 rows should have been committed.
	require.Equal(t, nRows, db.committedCount())
	// Each committed row should have cluster_id 1 or 2 (1-based).
	db.mu.Lock()
	for _, a := range db.committed {
		require.True(t, a.ClusterID == 1 || a.ClusterID == 2,
			"cluster_id %d is not 1-based", a.ClusterID)
	}
	db.mu.Unlock()
}

func TestDeltaFlushCycle_VersionMismatchAbortsBatch(t *testing.T) {
	t.Parallel()
	const dim = 2

	spec := IVFSpec{ID: "test", Dim: dim, Metric: MetricL2, Nlist: 2}
	centroids := [][]float32{{1, 0}, {0, 1}}
	cs, err := kmeans.NewCentroidSet(1, centroids)
	require.NoError(t, err)
	state := NewIndexState(spec, cs)

	// 500 rows, batch size 100 → 5 batches.
	db := &mockFlushDB{deltaRows: makeDeltaRows(500, dim)}

	var commitCount int64
	db.onCommit = func() {
		n := atomic.AddInt64(&commitCount, 1)
		if n == 2 {
			// After 2nd batch commit, swap probeState to simulate REINDEX.
			cs2, _ := kmeans.NewCentroidSet(2, centroids)
			state.SwapProbeState(cs2)
		}
	}

	cfg := DeltaFlushConfig{Interval: time.Second, MaxRows: 500, BatchSize: 100}
	deltaFlushCycle(context.Background(), cfg, state, db, "test", "docs", "embed")

	// Should have committed exactly 2 batches (200 rows) before aborting.
	// The 3rd batch sees version mismatch and aborts.
	require.Equal(t, 200, db.committedCount())
}

func TestDeltaFlushCycle_EmptyDeltaNoOp(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 2, Metric: MetricL2, Nlist: 1}
	cs, _ := kmeans.NewCentroidSet(1, [][]float32{{1, 0}})
	state := NewIndexState(spec, cs)

	db := &mockFlushDB{} // no delta rows
	cfg := DefaultDeltaFlushConfig()

	deltaFlushCycle(context.Background(), cfg, state, db, "test", "docs", "embed")

	require.Equal(t, int64(1), atomic.LoadInt64(&db.fetchCalls))
	require.Equal(t, int64(0), atomic.LoadInt64(&db.commitCalls))
}

func TestDeltaFlushLoop_GracefulShutdown(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 2, Metric: MetricL2, Nlist: 1}
	cs, _ := kmeans.NewCentroidSet(1, [][]float32{{1, 0}})
	state := NewIndexState(spec, cs)

	db := &mockFlushDB{}
	cfg := DeltaFlushConfig{Interval: time.Millisecond, MaxRows: 100, BatchSize: 50}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		deltaFlushLoop(ctx, cfg, state, db, "test", "docs", "embed")
		close(done)
	}()

	// Let it tick a few times.
	time.Sleep(20 * time.Millisecond)
	cancel()

	// Goroutine should exit promptly.
	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("delta flush goroutine did not exit after context cancellation")
	}
}

func TestDeltaFlushCycle_UpdatesDriftTracker(t *testing.T) {
	t.Parallel()
	const dim = 2

	spec := IVFSpec{ID: "test", Dim: dim, Metric: MetricL2, Nlist: 2}
	centroids := [][]float32{{0, 0}, {10, 10}}
	cs, err := kmeans.NewCentroidSet(1, centroids)
	require.NoError(t, err)
	state := NewIndexState(spec, cs)

	// 10 rows near cluster 0.
	rows := make([]DeltaRow, 10)
	for i := range rows {
		rows[i] = DeltaRow{
			Rowid: int64(i + 1),
			Embed: Float32ToBytes([]float32{float32(i), 0}),
		}
	}
	db := &mockFlushDB{deltaRows: rows}
	cfg := DeltaFlushConfig{Interval: time.Second, MaxRows: 100, BatchSize: 100}

	deltaFlushCycle(context.Background(), cfg, state, db, "test", "docs", "embed")

	// All 10 should be assigned to cluster 1 (1-based, nearest to {0,0}).
	tracker := state.LoadDriftTracker()
	require.NotNil(t, tracker)
	// Cluster 0 (0-based) should have initial 1 + 10 updates = 11.
	require.Equal(t, int64(11), tracker.ClusterCount(0))
}

func TestDeltaFlushCycle_OnErrorCallbackFires(t *testing.T) {
	t.Parallel()
	spec := IVFSpec{ID: "test", Dim: 2, Metric: MetricL2, Nlist: 1}
	cs, _ := kmeans.NewCentroidSet(1, [][]float32{{1, 0}})
	state := NewIndexState(spec, cs)

	fetchErr := fmt.Errorf("no such table: __marmot_vec_test_members")
	db := &mockFlushDB{deltaRows: makeDeltaRows(10, 2)}
	// Override fetch to return error.
	errDB := &errorFlushDB{err: fetchErr}

	var captured error
	var capturedIndex string
	cfg := DeltaFlushConfig{
		Interval:  time.Second,
		MaxRows:   100,
		BatchSize: 50,
		OnError: func(indexName string, err error) {
			capturedIndex = indexName
			captured = err
		},
	}

	deltaFlushCycle(context.Background(), cfg, state, errDB, "myidx", "docs", "embed")

	require.ErrorIs(t, captured, fetchErr)
	require.Equal(t, "myidx", capturedIndex)
	_ = db // suppress unused
}

// errorFlushDB always returns an error from FetchDeltaEmbeddings.
type errorFlushDB struct {
	err error
}

func (e *errorFlushDB) FetchDeltaEmbeddings(context.Context, string, string, string, int) ([]DeltaRow, error) {
	return nil, e.err
}

func (e *errorFlushDB) CommitFlushBatch(context.Context, string, []DeltaAssignment) error {
	return e.err
}
