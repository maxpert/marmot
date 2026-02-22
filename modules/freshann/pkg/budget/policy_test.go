package budget

import (
	"testing"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestDefaultSearchTuningScalesWithDimension(t *testing.T) {
	low := DefaultSearchTuning(32, api.MetricCosine, 0.90)
	mid := DefaultSearchTuning(256, api.MetricCosine, 0.90)
	high := DefaultSearchTuning(960, api.MetricEuclidean, 0.90)

	require.Greater(t, mid.CandidateBudget, low.CandidateBudget)
	require.Greater(t, high.CandidateBudget, mid.CandidateBudget)
	require.Greater(t, high.EfSearch, mid.EfSearch)
}

func TestAdaptiveResolverReducesBudgetForHighlySelectiveFilter(t *testing.T) {
	spec := api.IndexSpec{
		Dim:    256,
		Metric: api.MetricCosine,
		BudgetPolicy: api.BudgetPolicySpec{
			Mode:         api.BudgetPolicyAdaptive,
			TargetRecall: 0.90,
		},
		SearchDefaults: api.SearchTuning{
			EfSearch:        96,
			Beam:            16,
			CandidateBudget: 4096,
			RerankK:         256,
		},
	}
	r := NewResolver(spec.BudgetPolicy)
	out := r.Resolve(Input{
		Spec:          spec,
		TopK:          10,
		FilteredCount: 200,
		CorpusCount:   20000,
	})
	require.Less(t, out.CandidateBudget, spec.SearchDefaults.CandidateBudget)
	require.GreaterOrEqual(t, out.CandidateBudget, 80)
	require.GreaterOrEqual(t, out.EfSearch, 20)
}

func TestAdaptiveResolverHonorsExplicitOverrides(t *testing.T) {
	spec := api.IndexSpec{
		Dim:    128,
		Metric: api.MetricEuclidean,
		BudgetPolicy: api.BudgetPolicySpec{
			Mode: api.BudgetPolicyAdaptive,
		},
		SearchDefaults: api.SearchTuning{
			EfSearch:        96,
			Beam:            8,
			CandidateBudget: 2048,
			RerankK:         128,
		},
	}
	r := NewResolver(spec.BudgetPolicy)
	out := r.Resolve(Input{
		Spec: spec,
		TopK: 10,
		Requested: api.SearchTuning{
			EfSearch:        320,
			CandidateBudget: 6000,
			RerankK:         900,
			BudgetScale:     0.5,
		},
	})
	require.Equal(t, 320, out.EfSearch)
	require.Equal(t, 6000, out.CandidateBudget)
	require.Equal(t, 900, out.RerankK)
}

func TestFixedResolverUsesRequestAndDefaults(t *testing.T) {
	spec := api.IndexSpec{
		Dim:    128,
		Metric: api.MetricEuclidean,
		BudgetPolicy: api.BudgetPolicySpec{
			Mode: api.BudgetPolicyFixed,
		},
		SearchDefaults: api.SearchTuning{
			EfSearch:        96,
			Beam:            8,
			CandidateBudget: 2048,
			RerankK:         128,
		},
	}
	r := NewResolver(spec.BudgetPolicy)
	out := r.Resolve(Input{
		Spec: spec,
		TopK: 10,
		Requested: api.SearchTuning{
			Beam:    24,
			RerankK: 512,
		},
	})
	require.Equal(t, 96, out.EfSearch)
	require.Equal(t, 24, out.Beam)
	require.Equal(t, 2048, out.CandidateBudget)
	require.Equal(t, 512, out.RerankK)
}

func TestSweepGridProducesOrderedPositiveLists(t *testing.T) {
	ef, beam, cand, rerank := SweepGrid(960, api.MetricEuclidean, 0.90)
	require.NotEmpty(t, ef)
	require.NotEmpty(t, beam)
	require.NotEmpty(t, cand)
	require.NotEmpty(t, rerank)
	require.True(t, ef[0] > 0)
	require.True(t, beam[0] > 0)
	require.True(t, cand[0] > 0)
	require.True(t, rerank[0] > 0)
	require.GreaterOrEqual(t, ef[len(ef)-1], ef[0])
	require.GreaterOrEqual(t, cand[len(cand)-1], cand[0])
}
