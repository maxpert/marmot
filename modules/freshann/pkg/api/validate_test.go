package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSpec(t *testing.T) {
	valid := IndexSpec{ID: "idx", Dim: 4, Metric: MetricCosine, ApplyMode: ApplyModeAsync, DurabilityMode: DurabilityPeriodic}
	require.NoError(t, ValidateSpec(valid))
	require.NoError(t, ValidateSpec(IndexSpec{ID: "idx_l2", Dim: 4, Metric: MetricEuclidean}))

	cases := []IndexSpec{
		{Dim: 4, Metric: MetricCosine},
		{ID: "x", Dim: 0, Metric: MetricCosine},
		{ID: "x", Dim: 4, Metric: "l2"},
		{ID: "x", Dim: 4, Metric: MetricDot, ApplyMode: "bad"},
		{ID: "x", Dim: 4, Metric: MetricDot, DurabilityMode: "bad"},
	}
	for _, tc := range cases {
		require.Error(t, ValidateSpec(tc))
	}
}

func TestValidateMutation(t *testing.T) {
	require.NoError(t, ValidateMutation(3, Mutation{TxnID: 1, SeqID: 1, ExternalID: []byte("id"), VectorFP32: []float32{1, 2, 3}}))
	require.Error(t, ValidateMutation(3, Mutation{TxnID: 1, SeqID: 0, ExternalID: []byte("id"), VectorFP32: []float32{1, 2, 3}}))
	require.Error(t, ValidateMutation(3, Mutation{TxnID: 1, SeqID: 1, ExternalID: []byte("id"), VectorFP32: []float32{1}}))
}
