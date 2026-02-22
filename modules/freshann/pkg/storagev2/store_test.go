package storagev2

import (
	"testing"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/maxpert/marmot/modules/freshann/pkg/graphv2"
	"github.com/stretchr/testify/require"
)

func TestApplyUpsertDeleteAndCounts(t *testing.T) {
	store, err := Open(t.TempDir(), OpenOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.SaveSpec(api.IndexSpec{ID: "idx", Dim: 2, Metric: api.MetricDot, FormatVersion: 2}))
	spec, err := store.LoadSpec()
	require.NoError(t, err)
	require.Equal(t, 2, spec.FormatVersion)

	docID, applied, err := store.ApplyUpsert(api.ApplyToken{TxnID: 1, SeqID: 1}, []byte("a"), VectorRecord{
		PartitionKey: "p1",
		Tags:         map[string]string{"lang": "en"},
		VectorFP32:   []float32{1, 2},
	}, nil)
	require.NoError(t, err)
	require.True(t, applied)
	require.NotZero(t, docID)

	_, applied, err = store.ApplyUpsert(api.ApplyToken{TxnID: 1, SeqID: 1}, []byte("a"), VectorRecord{VectorFP32: []float32{3, 4}}, nil)
	require.NoError(t, err)
	require.False(t, applied)

	vecCount, err := store.CountVectors()
	require.NoError(t, err)
	require.Equal(t, uint64(1), vecCount)
	appliedCount, err := store.CountApplied()
	require.NoError(t, err)
	require.Equal(t, uint64(1), appliedCount)

	ids, err := store.CandidateDocIDs("p1", map[string]string{"lang": "en"})
	require.NoError(t, err)
	require.Equal(t, []uint64{docID}, ids)

	_, existed, applied, err := store.ApplyDelete(api.ApplyToken{TxnID: 1, SeqID: 2}, []byte("a"), nil)
	require.NoError(t, err)
	require.True(t, existed)
	require.True(t, applied)

	vecCount, err = store.CountVectors()
	require.NoError(t, err)
	require.Equal(t, uint64(0), vecCount)
	appliedCount, err = store.CountApplied()
	require.NoError(t, err)
	require.Equal(t, uint64(2), appliedCount)
}

func TestVectorLookupAndIterate(t *testing.T) {
	store, err := Open(t.TempDir(), OpenOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	docID, _, err := store.ApplyUpsert(api.ApplyToken{TxnID: 1, SeqID: 1}, []byte("k"), VectorRecord{VectorFP32: []float32{1, 0}}, nil)
	require.NoError(t, err)

	lookup, err := store.NewVectorLookup()
	require.NoError(t, err)
	t.Cleanup(func() { _ = lookup.Close() })

	vec, ok, err := lookup.GetVectorFP32ByDocID(docID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []float32{1, 0}, vec)

	count := 0
	err = store.IterateVectorsByDoc(func(got uint64, externalID []byte, rec VectorRecord) error {
		count++
		require.Equal(t, docID, got)
		require.Equal(t, []byte("k"), externalID)
		require.Equal(t, []float32{1, 0}, rec.VectorFP32)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestGraphStateRoundTrip(t *testing.T) {
	store, err := Open(t.TempDir(), OpenOptions{GraphPageSize: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	state := graphv2.State{
		Metric: api.MetricDot,
		R:      2,
		Start:  []uint64{1},
		Adj: map[uint64][]uint64{
			1: {2, 3},
			2: {1},
			3: {1},
		},
	}
	require.NoError(t, store.SaveGraphState(state, nil))
	got, ok, err := store.LoadGraphState()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, state.Metric, got.Metric)
	require.Equal(t, state.R, got.R)
	require.Equal(t, state.Start, got.Start)
	require.Equal(t, state.Adj, got.Adj)
}
