package test

import (
	"context"
	"testing"

	freshann "github.com/maxpert/marmot/modules/freshann"
	"github.com/stretchr/testify/require"
)

func TestDeterministicReplayConformance(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	eng, err := freshann.NewEngine(freshann.EngineOptions{RootDir: root})
	require.NoError(t, err)
	_, err = eng.CreateIndex(ctx, freshann.IndexSpec{ID: "idx", Dim: 2, Metric: freshann.MetricDot})
	require.NoError(t, err)
	idx, err := eng.OpenIndex(ctx, "idx")
	require.NoError(t, err)

	_, err = idx.Upsert(ctx, freshann.Mutation{TxnID: 1, SeqID: 1, ExternalID: []byte("a"), VectorFP32: []float32{1, 0}})
	require.NoError(t, err)
	_, err = idx.Upsert(ctx, freshann.Mutation{TxnID: 1, SeqID: 2, ExternalID: []byte("b"), VectorFP32: []float32{0.5, 0.1}})
	require.NoError(t, err)
	_, err = idx.Delete(ctx, freshann.DeleteMutation{TxnID: 1, SeqID: 3, ExternalID: []byte("b")})
	require.NoError(t, err)

	require.NoError(t, eng.Close())

	eng2, err := freshann.NewEngine(freshann.EngineOptions{RootDir: root})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng2.Close() })
	idx2, err := eng2.OpenIndex(ctx, "idx")
	require.NoError(t, err)

	res, err := idx2.Search(ctx, freshann.SearchRequest{VectorFP32: []float32{1, 0}, TopK: 10})
	require.NoError(t, err)
	require.Len(t, res.Hits, 1)
	require.Equal(t, []byte("a"), res.Hits[0].ExternalID)
}
