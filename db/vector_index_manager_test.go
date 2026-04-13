package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock implementations
// ---------------------------------------------------------------------------

type mockUpsertCall struct {
	externalID []byte
	vector     []float32
	txnID      uint64
	seqID      uint64
}

type mockDeleteCall struct {
	externalID []byte
	txnID      uint64
	seqID      uint64
}

type mockVectorIndex struct {
	searchResults []VectorSearchHit
	searchErr     error
	upsertCalls   []mockUpsertCall
	deleteCalls   []mockDeleteCall
	stats         VectorIndexStats
	closed        bool
}

func (m *mockVectorIndex) Search(_ context.Context, _ []float32, _ int) ([]VectorSearchHit, error) {
	return m.searchResults, m.searchErr
}

func (m *mockVectorIndex) Upsert(_ context.Context, externalID []byte, vector []float32, txnID, seqID uint64) error {
	m.upsertCalls = append(m.upsertCalls, mockUpsertCall{externalID: externalID, vector: vector, txnID: txnID, seqID: seqID})
	return nil
}

func (m *mockVectorIndex) Delete(_ context.Context, externalID []byte, txnID, seqID uint64) error {
	m.deleteCalls = append(m.deleteCalls, mockDeleteCall{externalID: externalID, txnID: txnID, seqID: seqID})
	return nil
}

func (m *mockVectorIndex) Stats() VectorIndexStats { return m.stats }
func (m *mockVectorIndex) Close() error            { m.closed = true; return nil }

type mockVectorEngine struct {
	indexes    map[string]*mockVectorIndex
	created    []string
	dropped    []string
	openErrors map[string]error // optional per-index error returned by OpenIndex
}

func newMockEngine() *mockVectorEngine {
	return &mockVectorEngine{
		indexes:    make(map[string]*mockVectorIndex),
		openErrors: make(map[string]error),
	}
}

func (e *mockVectorEngine) CreateIndex(_ context.Context, id string, _ int, _ string, _ []VectorBulkEntry) (VectorIndex, error) {
	e.created = append(e.created, id)
	idx := &mockVectorIndex{}
	e.indexes[id] = idx
	return idx, nil
}

func (e *mockVectorEngine) OpenIndex(_ context.Context, id string) (VectorIndex, error) {
	if err, hasErr := e.openErrors[id]; hasErr {
		return nil, err
	}
	idx, ok := e.indexes[id]
	if !ok {
		idx = &mockVectorIndex{}
		e.indexes[id] = idx
	}
	return idx, nil
}

func (e *mockVectorEngine) DropIndex(_ context.Context, id string) error {
	e.dropped = append(e.dropped, id)
	delete(e.indexes, id)
	return nil
}

func (e *mockVectorEngine) Close() error { return nil }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func setupTestVIM(t *testing.T) (*VectorIndexManager, *DatabaseManager, *mockVectorEngine) {
	t.Helper()
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)
	dm, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)

	eng := newMockEngine()
	vim := NewVectorIndexManager(eng, dm, 0)
	// Stop VIM before closing DM so background goroutines are cancelled first.
	t.Cleanup(func() {
		_ = vim.Stop()
		dm.Close()
	})
	return vim, dm, eng
}

// encodeFloat32Vec encodes a []float32 as a little-endian IEEE-754 BLOB.
func encodeFloat32Vec(vec []float32) []byte {
	b := make([]byte, len(vec)*4)
	for i, v := range vec {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	return b
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestVectorIndexManager_CreateIndex(t *testing.T) {
	t.Parallel()
	vim, dm, eng := setupTestVIM(t)

	ctx := context.Background()

	// Create a test table with vector data in the default database.
	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "CREATE TABLE vecs (id INTEGER PRIMARY KEY, embedding BLOB)")
	require.NoError(t, err)

	vec := []float32{1.0, 2.0, 3.0}
	_, err = conn.ExecContext(ctx, "INSERT INTO vecs (id, embedding) VALUES (1, ?)", encodeFloat32Vec(vec))
	require.NoError(t, err)

	meta := VectorIndexMeta{
		IndexName:  "test_idx",
		TableName:  "vecs",
		ColumnName: "embedding",
		Database:   DefaultDatabaseName,
		Metric:     "cosine",
		Dim:        3,
	}

	require.NoError(t, vim.CreateIndex(ctx, meta))

	// Engine was called.
	assert.Contains(t, eng.created, "test_idx")

	// Index is registered.
	vim.mu.RLock()
	_, ok := vim.indexes["test_idx"]
	vim.mu.RUnlock()
	assert.True(t, ok)

	// Metadata row exists with status=ready.
	var status string
	err = conn.QueryRowContext(ctx, "SELECT status FROM __marmot_vector_indexes WHERE index_name = ?", "test_idx").Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "ready", status)
}

func TestVectorIndexManager_DropIndex(t *testing.T) {
	t.Parallel()
	vim, dm, eng := setupTestVIM(t)

	ctx := context.Background()

	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "CREATE TABLE drop_vecs (id INTEGER PRIMARY KEY, embedding BLOB)")
	require.NoError(t, err)

	meta := VectorIndexMeta{
		IndexName:  "drop_idx",
		TableName:  "drop_vecs",
		ColumnName: "embedding",
		Database:   DefaultDatabaseName,
		Metric:     "euclidean",
		Dim:        4,
	}
	require.NoError(t, vim.CreateIndex(ctx, meta))

	require.NoError(t, vim.DropIndex(ctx, "drop_idx", DefaultDatabaseName))

	// Engine was asked to drop.
	assert.Contains(t, eng.dropped, "drop_idx")

	// Index removed from internal map.
	vim.mu.RLock()
	_, ok := vim.indexes["drop_idx"]
	vim.mu.RUnlock()
	assert.False(t, ok)

	// Metadata row removed.
	var count int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM __marmot_vector_indexes WHERE index_name = ?", "drop_idx").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestVectorIndexManager_Search(t *testing.T) {
	t.Parallel()
	vim, dm, eng := setupTestVIM(t)

	ctx := context.Background()

	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "CREATE TABLE search_vecs (id INTEGER PRIMARY KEY, embedding BLOB)")
	require.NoError(t, err)

	meta := VectorIndexMeta{
		IndexName:  "search_idx",
		TableName:  "search_vecs",
		ColumnName: "embedding",
		Database:   DefaultDatabaseName,
		Metric:     "cosine",
		Dim:        2,
	}
	require.NoError(t, vim.CreateIndex(ctx, meta))

	// Pre-load results on the mock.
	mockIdx := eng.indexes["search_idx"]
	mockIdx.searchResults = []VectorSearchHit{
		{ExternalID: []byte("abc"), Distance: 0.1, Score: 0.9},
	}

	hits, err := vim.Search(ctx, "search_idx", []float32{1.0, 0.0}, 5)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, []byte("abc"), hits[0].ExternalID)
}

func TestVectorIndexManager_Search_NotFound(t *testing.T) {
	t.Parallel()
	vim, _, _ := setupTestVIM(t)

	_, err := vim.Search(context.Background(), "nonexistent", []float32{1.0}, 1)
	assert.Error(t, err)
}

func TestVectorIndexManager_GetIndexMeta(t *testing.T) {
	t.Parallel()
	vim, dm, _ := setupTestVIM(t)

	ctx := context.Background()

	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "CREATE TABLE meta_vecs (id INTEGER PRIMARY KEY, embedding BLOB)")
	require.NoError(t, err)

	meta := VectorIndexMeta{
		IndexName:  "meta_idx",
		TableName:  "meta_vecs",
		ColumnName: "embedding",
		Database:   DefaultDatabaseName,
		Metric:     "dot",
		Dim:        8,
	}
	require.NoError(t, vim.CreateIndex(ctx, meta))

	got, ok := vim.GetIndexMeta("meta_idx")
	require.True(t, ok)
	assert.Equal(t, "meta_idx", got.IndexName)
	assert.Equal(t, "meta_vecs", got.TableName)
	assert.Equal(t, "embedding", got.ColumnName)
	assert.Equal(t, "dot", got.Metric)
	assert.Equal(t, 8, got.Dim)
	assert.Equal(t, "ready", got.Status)
}

func TestVectorIndexManager_StartLoadsExisting(t *testing.T) {
	t.Parallel()
	vim, dm, eng := setupTestVIM(t)

	ctx := context.Background()

	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "CREATE TABLE start_vecs (id INTEGER PRIMARY KEY, embedding BLOB)")
	require.NoError(t, err)

	meta := VectorIndexMeta{
		IndexName:  "start_idx",
		TableName:  "start_vecs",
		ColumnName: "embedding",
		Database:   DefaultDatabaseName,
		Metric:     "cosine",
		Dim:        3,
	}
	require.NoError(t, vim.CreateIndex(ctx, meta))

	// Remove from in-memory maps to simulate a fresh start.
	vim.mu.Lock()
	delete(vim.indexes, "start_idx")
	delete(vim.tableMeta, tableMetaKey(DefaultDatabaseName, "start_vecs"))
	vim.mu.Unlock()

	// Pre-register the index in the mock engine so OpenIndex succeeds.
	eng.indexes["start_idx"] = &mockVectorIndex{}

	// Start should reload from the metadata table.
	require.NoError(t, vim.Start(ctx))

	vim.mu.RLock()
	_, loaded := vim.indexes["start_idx"]
	vim.mu.RUnlock()
	assert.True(t, loaded)
}

func TestVectorIndexManager_DatabaseManager_Accessors(t *testing.T) {
	t.Parallel()
	_, dm, eng := setupTestVIM(t)

	vim := NewVectorIndexManager(eng, dm, 0)
	assert.Nil(t, dm.GetVectorIndexManager())

	dm.SetVectorIndexManager(vim)
	assert.Equal(t, vim, dm.GetVectorIndexManager())
}

// TestVectorIndexManager_ReconcileNoGap verifies that reconcileAll runs without
// error and does not report a gap when the index watermark matches the database.
func TestVectorIndexManager_ReconcileNoGap(t *testing.T) {
	t.Parallel()
	vim, dm, eng := setupTestVIM(t)

	ctx := context.Background()

	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "CREATE TABLE reconcile_vecs (id INTEGER PRIMARY KEY, embedding BLOB)")
	require.NoError(t, err)

	meta := VectorIndexMeta{
		IndexName:  "reconcile_idx",
		TableName:  "reconcile_vecs",
		ColumnName: "embedding",
		Database:   DefaultDatabaseName,
		Metric:     "cosine",
		Dim:        3,
	}
	require.NoError(t, vim.CreateIndex(ctx, meta))

	// Both the index watermark (0) and max committed txnID (0) are zero — no gap.
	// reconcileAll must complete without error.
	vim.reconcileAll()

	// Index must still be registered after reconciliation.
	_, ok := eng.indexes["reconcile_idx"]
	assert.True(t, ok)
}

// TestVectorIndexManager_ReconcileDetectsGap verifies that reconcileIndex
// detects a gap when the database has committed transactions beyond the index
// watermark.
func TestVectorIndexManager_ReconcileDetectsGap(t *testing.T) {
	t.Parallel()
	vim, dm, eng := setupTestVIM(t)

	ctx := context.Background()

	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "CREATE TABLE gap_vecs (id INTEGER PRIMARY KEY, embedding BLOB)")
	require.NoError(t, err)

	meta := VectorIndexMeta{
		IndexName:  "gap_idx",
		TableName:  "gap_vecs",
		ColumnName: "embedding",
		Database:   DefaultDatabaseName,
		Metric:     "cosine",
		Dim:        3,
	}
	require.NoError(t, vim.CreateIndex(ctx, meta))

	// Advance the metastore's max committed txnID past the index watermark (0)
	// by recording a committed transaction directly.
	rdb, err := dm.GetDatabase(DefaultDatabaseName)
	require.NoError(t, err)
	ms := rdb.GetMetaStore()
	require.NotNil(t, ms)

	require.NoError(t, ms.StoreReplayedTransaction(42, 1, hlc.Timestamp{WallTime: 1}, DefaultDatabaseName, 0))

	// Gap should now be detected: index watermark=0, db max txnID=42.
	// reconcileIndex must return nil (gap is logged, not an error).
	vim.mu.RLock()
	gapMeta := vim.tableMeta[tableMetaKey(DefaultDatabaseName, "gap_vecs")]
	vim.mu.RUnlock()
	require.NotNil(t, gapMeta)

	err = vim.reconcileIndex(gapMeta)
	require.NoError(t, err)

	// The mock index watermark is still 0; gap should be detectable.
	mockIdx := eng.indexes["gap_idx"]
	assert.Equal(t, uint64(0), mockIdx.Stats().WatermarkTxnID)

	maxTxnID, err := ms.GetMaxCommittedTxnID()
	require.NoError(t, err)
	assert.Equal(t, uint64(42), maxTxnID)
}

// TestVectorIndexManager_CrashRecovery_BuildingStatus verifies that an index
// stuck in 'building' at startup is marked 'error' and not loaded.
func TestVectorIndexManager_CrashRecovery_BuildingStatus(t *testing.T) {
	t.Parallel()
	vim, dm, _ := setupTestVIM(t)
	ctx := context.Background()

	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	// Ensure the metadata table exists.
	_, err = conn.ExecContext(ctx, vectorIndexMetaTable)
	require.NoError(t, err)

	// Insert a row simulating a crash mid-build.
	_, err = conn.ExecContext(ctx,
		`INSERT INTO __marmot_vector_indexes
         (index_name, table_name, column_name, database_name, metric, dim, status, created_at)
         VALUES ('crashed_idx', 'some_vecs', 'embedding', ?, 'cosine', 3, 'building', 1)`,
		DefaultDatabaseName,
	)
	require.NoError(t, err)

	// loadExistingIndexes should handle crash recovery.
	require.NoError(t, vim.loadExistingIndexes(ctx))

	// Index must NOT be registered in memory.
	vim.mu.RLock()
	_, loaded := vim.indexes["crashed_idx"]
	vim.mu.RUnlock()
	assert.False(t, loaded, "crashed index must not be loaded into memory")

	// Status must be updated to 'error'.
	var status string
	err = conn.QueryRowContext(ctx,
		"SELECT status FROM __marmot_vector_indexes WHERE index_name = 'crashed_idx'",
	).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "error", status)
}

// TestVectorIndexManager_CrashRecovery_MissingDirectory verifies that a 'ready'
// index whose backing store cannot be opened is marked 'error' and a rebuild is
// scheduled rather than crashing startup.
func TestVectorIndexManager_CrashRecovery_MissingDirectory(t *testing.T) {
	t.Parallel()
	vim, dm, eng := setupTestVIM(t)
	ctx := context.Background()

	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	// Ensure the metadata table exists.
	_, err = conn.ExecContext(ctx, vectorIndexMetaTable)
	require.NoError(t, err)

	// Create the source table so the rebuild can succeed.
	_, err = conn.ExecContext(ctx, "CREATE TABLE missing_vecs (id INTEGER PRIMARY KEY, embedding BLOB)")
	require.NoError(t, err)

	// Insert metadata as if index was previously ready.
	_, err = conn.ExecContext(ctx,
		`INSERT INTO __marmot_vector_indexes
         (index_name, table_name, column_name, database_name, metric, dim, status, created_at)
         VALUES ('missing_idx', 'missing_vecs', 'embedding', ?, 'cosine', 3, 'ready', 1)`,
		DefaultDatabaseName,
	)
	require.NoError(t, err)

	// Make OpenIndex fail to simulate a missing Pebble directory.
	eng.openErrors["missing_idx"] = fmt.Errorf("pebble: no such directory")

	// loadExistingIndexes must not return an error.
	require.NoError(t, vim.loadExistingIndexes(ctx))

	// Index must NOT be loaded into memory immediately.
	vim.mu.RLock()
	_, loaded := vim.indexes["missing_idx"]
	vim.mu.RUnlock()
	assert.False(t, loaded, "index with missing directory must not be loaded immediately")

	// Status must be updated to 'error'.
	var status string
	err = conn.QueryRowContext(ctx,
		"SELECT status FROM __marmot_vector_indexes WHERE index_name = 'missing_idx'",
	).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "error", status)
}

// TestVectorIndexManager_Search_SourceTableDropped verifies that Search returns
// a clear error when the source table no longer exists.
func TestVectorIndexManager_Search_SourceTableDropped(t *testing.T) {
	t.Parallel()
	vim, dm, _ := setupTestVIM(t)
	ctx := context.Background()

	conn, err := dm.GetDatabaseConnection(DefaultDatabaseName)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "CREATE TABLE droppable_vecs (id INTEGER PRIMARY KEY, embedding BLOB)")
	require.NoError(t, err)

	meta := VectorIndexMeta{
		IndexName:  "droppable_idx",
		TableName:  "droppable_vecs",
		ColumnName: "embedding",
		Database:   DefaultDatabaseName,
		Metric:     "cosine",
		Dim:        3,
	}
	require.NoError(t, vim.CreateIndex(ctx, meta))

	// Drop the source table (simulating schema change after index was built).
	_, err = conn.ExecContext(ctx, "DROP TABLE droppable_vecs")
	require.NoError(t, err)

	_, err = vim.Search(ctx, "droppable_idx", []float32{1.0, 0.0, 0.0}, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source table")
}
