package freshann

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/freshann/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateOpenListDrop(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	eng, err := NewEngine(EngineOptions{RootDir: root})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{ID: "users_embeddings", Dim: 4, Metric: MetricCosine})
	require.NoError(t, err)

	list, err := eng.ListIndexes(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, IndexID("users_embeddings"), list[0].ID)

	idx, err := eng.OpenIndex(ctx, "users_embeddings")
	require.NoError(t, err)
	require.NotNil(t, idx)

	err = eng.DropIndex(ctx, "users_embeddings")
	require.NoError(t, err)

	_, err = eng.OpenIndex(ctx, "users_embeddings")
	require.Error(t, err)
}

func TestUpsertIdempotentByToken(t *testing.T) {
	t.Parallel()
	eng, err := NewEngine(EngineOptions{RootDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{ID: "idx", Dim: 3, Metric: MetricDot})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)

	_, err = idx.Upsert(ctx, Mutation{TxnID: 1, SeqID: 1, ExternalID: []byte("a"), VectorFP32: []float32{1, 2, 3}})
	require.NoError(t, err)
	_, err = idx.Upsert(ctx, Mutation{TxnID: 1, SeqID: 1, ExternalID: []byte("a"), VectorFP32: []float32{9, 9, 9}})
	require.NoError(t, err)

	stats, err := idx.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.AppliedMutations)
	require.Equal(t, uint64(1), stats.VectorCount)
}

func TestAsyncApplyAndWait(t *testing.T) {
	t.Parallel()
	eng, err := NewEngine(EngineOptions{RootDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{
		ID:        "idx",
		Dim:       2,
		Metric:    MetricCosine,
		ApplyMode: ApplyModeAsync,
	})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)

	tok, err := idx.Upsert(ctx, Mutation{TxnID: 10, SeqID: 20, ExternalID: []byte("v1"), VectorFP32: []float32{1, 0}})
	require.NoError(t, err)

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	require.NoError(t, idx.WaitApplied(waitCtx, tok))

	res, err := idx.Search(ctx, SearchRequest{VectorFP32: []float32{1, 0}, TopK: 1})
	require.NoError(t, err)
	require.Len(t, res.Hits, 1)
	require.Equal(t, []byte("v1"), res.Hits[0].ExternalID)
}

func TestSearchWithPartitionAndTags(t *testing.T) {
	t.Parallel()
	eng, err := NewEngine(EngineOptions{RootDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{ID: "idx", Dim: 2, Metric: MetricDot})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)

	_, err = idx.Upsert(ctx, Mutation{TxnID: 1, SeqID: 1, ExternalID: []byte("a"), VectorFP32: []float32{1, 0}, PartitionKey: "p1", Tags: map[string]string{"lang": "en"}})
	require.NoError(t, err)
	_, err = idx.Upsert(ctx, Mutation{TxnID: 1, SeqID: 2, ExternalID: []byte("b"), VectorFP32: []float32{0, 1}, PartitionKey: "p2", Tags: map[string]string{"lang": "fr"}})
	require.NoError(t, err)

	res, err := idx.Search(ctx, SearchRequest{VectorFP32: []float32{1, 0}, TopK: 2, PartitionKey: "p1", Tags: map[string]string{"lang": "en"}})
	require.NoError(t, err)
	require.Len(t, res.Hits, 1)
	require.Equal(t, []byte("a"), res.Hits[0].ExternalID)
}

func TestPersistenceAcrossReopen(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	eng, err := NewEngine(EngineOptions{RootDir: root})
	require.NoError(t, err)

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{ID: "idx", Dim: 2, Metric: MetricCosine})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)
	_, err = idx.Upsert(ctx, Mutation{TxnID: 2, SeqID: 3, ExternalID: []byte("x"), VectorFP32: []float32{0.8, 0.2}})
	require.NoError(t, err)
	require.NoError(t, eng.Close())

	eng2, err := NewEngine(EngineOptions{RootDir: root})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng2.Close() })

	idx2, err := eng2.OpenIndex(ctx, "idx")
	require.NoError(t, err)
	res, err := idx2.Search(ctx, SearchRequest{VectorFP32: []float32{1, 0}, TopK: 1})
	require.NoError(t, err)
	require.Len(t, res.Hits, 1)
	require.Equal(t, []byte("x"), res.Hits[0].ExternalID)
}

func TestSnapshot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	eng, err := NewEngine(EngineOptions{RootDir: root})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{ID: "idx", Dim: 2, Metric: MetricDot})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)
	_, err = idx.Upsert(ctx, Mutation{TxnID: 1, SeqID: 1, ExternalID: []byte("a"), VectorFP32: []float32{1, 1}})
	require.NoError(t, err)

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	require.NoError(t, idx.Snapshot(ctx, snapshotDir))
}

func TestCompactionManifestAndVerify(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	eng, err := NewEngine(EngineOptions{RootDir: root})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{ID: "idx", Dim: 2, Metric: MetricDot})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)

	for n := 1; n <= 20; n++ {
		_, err = idx.Upsert(ctx, Mutation{
			TxnID:      1,
			SeqID:      uint64(n),
			ExternalID: []byte{byte(n)},
			VectorFP32: []float32{float32(n), 0},
		})
		require.NoError(t, err)
	}

	require.NoError(t, idx.Flush(ctx))

	manifestPath := filepath.Join(root, "idx", "manifest.json")
	_, err = os.Stat(manifestPath)
	require.NoError(t, err)

	report, err := idx.Verify(ctx, VerifyOptions{Deep: true})
	require.NoError(t, err)
	require.True(t, report.Healthy)
}

func TestSearchEuclidean(t *testing.T) {
	t.Parallel()
	eng, err := NewEngine(EngineOptions{RootDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{ID: "idx", Dim: 2, Metric: MetricEuclidean})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)

	_, err = idx.Upsert(ctx, Mutation{TxnID: 1, SeqID: 1, ExternalID: []byte("near"), VectorFP32: []float32{0, 0}})
	require.NoError(t, err)
	_, err = idx.Upsert(ctx, Mutation{TxnID: 1, SeqID: 2, ExternalID: []byte("far"), VectorFP32: []float32{10, 10}})
	require.NoError(t, err)

	res, err := idx.Search(ctx, SearchRequest{VectorFP32: []float32{1, 1}, TopK: 2})
	require.NoError(t, err)
	require.Len(t, res.Hits, 2)
	require.Equal(t, []byte("near"), res.Hits[0].ExternalID)
	require.Less(t, res.Hits[0].Distance, res.Hits[1].Distance)
}

func TestSearchSteadyStateSkipsFullScanFallback(t *testing.T) {
	root := t.TempDir()
	eng, err := NewEngine(EngineOptions{RootDir: root})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{ID: "idx", Dim: 2, Metric: MetricCosine})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err = idx.Upsert(ctx, Mutation{
			TxnID:      1,
			SeqID:      uint64(i + 1),
			ExternalID: []byte{byte(i)},
			VectorFP32: []float32{float32(i), 1},
		})
		require.NoError(t, err)
	}
	require.NoError(t, idx.Flush(ctx))

	orig := iterateAllVectorsForSearch
	defer func() { iterateAllVectorsForSearch = orig }()
	fullScanCalls := 0
	iterateAllVectorsForSearch = func(store *storage.IndexStore, fn func([]byte, storage.VectorRecord) error) error {
		fullScanCalls++
		return orig(store, fn)
	}

	_, err = idx.Search(ctx, SearchRequest{VectorFP32: []float32{99, 1}, TopK: 1})
	require.NoError(t, err)
	require.Equal(t, 0, fullScanCalls)
}

func TestSearchDirtyStateUsesPendingDeltaWithoutFullScan(t *testing.T) {
	root := t.TempDir()
	eng, err := NewEngine(EngineOptions{RootDir: root})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	ctx := context.Background()
	_, err = eng.CreateIndex(ctx, IndexSpec{ID: "idx", Dim: 2, Metric: MetricDot})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)

	_, err = idx.Upsert(ctx, Mutation{TxnID: 1, SeqID: 1, ExternalID: []byte("a"), VectorFP32: []float32{1, 0}})
	require.NoError(t, err)
	_, err = idx.Upsert(ctx, Mutation{TxnID: 1, SeqID: 2, ExternalID: []byte("b"), VectorFP32: []float32{0, 1}})
	require.NoError(t, err)

	orig := iterateAllVectorsForSearch
	defer func() { iterateAllVectorsForSearch = orig }()
	fullScanCalls := 0
	iterateAllVectorsForSearch = func(store *storage.IndexStore, fn func([]byte, storage.VectorRecord) error) error {
		fullScanCalls++
		return orig(store, fn)
	}

	res, err := idx.Search(ctx, SearchRequest{VectorFP32: []float32{1, 0}, TopK: 1})
	require.NoError(t, err)
	require.NotEmpty(t, res.Hits)
	require.Equal(t, 0, fullScanCalls)
}
