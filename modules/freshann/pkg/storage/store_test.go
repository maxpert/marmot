package storage

import (
	"testing"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestStoreVectorRoundTrip(t *testing.T) {
	store, err := Open(t.TempDir(), OpenOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.SaveSpec(api.IndexSpec{ID: "idx", Dim: 2, Metric: api.MetricDot}))
	spec, err := store.LoadSpec()
	require.NoError(t, err)
	require.Equal(t, 2, spec.Dim)

	rec := VectorRecord{PartitionKey: "p", Tags: map[string]string{"a": "b"}, VectorFP32: []float32{1, 2}}
	require.NoError(t, store.PutVector([]byte("k"), rec, nil))

	var got VectorRecord
	err = store.IterateVectors(func(_ []byte, rec VectorRecord) error {
		got = rec
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, rec.PartitionKey, got.PartitionKey)
	require.Equal(t, rec.Tags, got.Tags)
	require.Equal(t, rec.VectorFP32, got.VectorFP32)
}

func TestCandidateExternalIDs(t *testing.T) {
	store, err := Open(t.TempDir(), OpenOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.PutVector([]byte("a"), VectorRecord{
		PartitionKey: "p1",
		Tags:         map[string]string{"lang": "en", "tier": "gold"},
		VectorFP32:   []float32{1, 0},
	}, nil))
	require.NoError(t, store.PutVector([]byte("b"), VectorRecord{
		PartitionKey: "p2",
		Tags:         map[string]string{"lang": "fr"},
		VectorFP32:   []float32{0, 1},
	}, nil))

	ids, err := store.CandidateExternalIDs("p1", map[string]string{"lang": "en"})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, []byte("a"), ids[0])

	require.NoError(t, store.DeleteVector([]byte("a"), nil))
	ids, err = store.CandidateExternalIDs("p1", map[string]string{"lang": "en"})
	require.NoError(t, err)
	require.Len(t, ids, 0)
}

func TestAppliedAndWatermark(t *testing.T) {
	store, err := Open(t.TempDir(), OpenOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	tok := api.ApplyToken{TxnID: 5, SeqID: 10}
	ok, err := store.IsApplied(tok)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, store.MarkApplied(tok, nil))
	ok, err = store.IsApplied(tok)
	require.NoError(t, err)
	require.True(t, ok)

	wm, err := store.Watermark()
	require.NoError(t, err)
	require.Equal(t, tok, wm)
}
