package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateSpec(t *testing.T) {
	valid := IndexSpec{ID: "idx", Dim: 4, Metric: MetricCosine, ApplyMode: ApplyModeAsync, DurabilityMode: DurabilityPeriodic}
	require.NoError(t, ValidateSpec(valid))
	require.NoError(t, ValidateSpec(IndexSpec{ID: "idx_l2", Dim: 4, Metric: MetricEuclidean}))

	cases := []IndexSpec{
		{Dim: 4, Metric: MetricCosine},
		{ID: "x", FormatVersion: 1, Dim: 4, Metric: MetricCosine},
		{ID: "x", Dim: 0, Metric: MetricCosine},
		{ID: "x", Dim: 4, Metric: "l2"},
		{ID: "x", Dim: 4, Metric: MetricDot, ApplyMode: "bad"},
		{ID: "x", Dim: 4, Metric: MetricDot, DurabilityMode: "bad"},
		{ID: "x", Dim: 4, Metric: MetricDot, Storage: StorageSpec{PebbleCacheBytes: -1}},
		{ID: "x", Dim: 4, Metric: MetricDot, Storage: StorageSpec{VectorCacheBytes: -1}},
		{ID: "x", Dim: 4, Metric: MetricDot, Storage: StorageSpec{VectorCacheBytes: MaxVectorCacheBytes + 1}},
		{ID: "x", Dim: 4, Metric: MetricDot, BudgetPolicy: BudgetPolicySpec{Mode: "bad"}},
		{ID: "x", Dim: 4, Metric: MetricDot, BudgetPolicy: BudgetPolicySpec{TargetRecall: 1.1}},
		{ID: "x", Dim: 4, Metric: MetricDot, BudgetPolicy: BudgetPolicySpec{MinEfSearch: 20, MaxEfSearch: 10}},
		{ID: "x", Dim: 4, Metric: MetricDot, BudgetPolicy: BudgetPolicySpec{MinCandidateBudget: 20, MaxCandidateBudget: 10}},
		{ID: "x", Dim: 4, Metric: MetricDot, SearchDefaults: SearchTuning{CandidateBudget: -1}},
		{ID: "x", Dim: 4, Metric: MetricDot, SearchDefaults: SearchTuning{TargetRecall: 1.5}},
		{ID: "x", Dim: 4, Metric: MetricDot, SearchDefaults: SearchTuning{BudgetScale: -1}},
		{ID: "x", Dim: 4, Metric: MetricDot, Graph: GraphSpec{ConsolidateEveryMutations: -1}},
		{ID: "x", Dim: 4, Metric: MetricDot, Graph: GraphSpec{ConsolidateMinInterval: -1 * time.Second}},
		{ID: "x", Dim: 4, Metric: MetricDot, Graph: GraphSpec{ConsolidateDeltaRatio: 1.1}},
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
