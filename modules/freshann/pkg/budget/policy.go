package budget

import (
	"math"
	"sort"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
)

// Input carries query and index context for budget resolution.
type Input struct {
	Spec               api.IndexSpec
	TopK               int
	FilteredCount      int
	CorpusCount        int
	Requested          api.SearchTuning
	AllowExactFallback bool
}

// Resolver computes effective search tuning for a query.
type Resolver interface {
	Resolve(in Input) api.SearchTuning
	Mode() api.BudgetPolicyMode
}

type fixedResolver struct{}

func (fixedResolver) Mode() api.BudgetPolicyMode { return api.BudgetPolicyFixed }

func (fixedResolver) Resolve(in Input) api.SearchTuning {
	return normalizeAndClamp(merge(in.Spec.SearchDefaults, in.Requested, in.AllowExactFallback), in.Spec.BudgetPolicy, in.TopK)
}

type adaptiveResolver struct{}

func (adaptiveResolver) Mode() api.BudgetPolicyMode { return api.BudgetPolicyAdaptive }

func (adaptiveResolver) Resolve(in Input) api.SearchTuning {
	policy := normalizePolicy(in.Spec.BudgetPolicy)
	base := DefaultSearchTuning(in.Spec.Dim, in.Spec.Metric, policy.TargetRecall)

	t := in.Spec.SearchDefaults
	if t.EfSearch <= 0 {
		t.EfSearch = base.EfSearch
	}
	if t.Beam <= 0 {
		t.Beam = base.Beam
	}
	if t.CandidateBudget <= 0 {
		t.CandidateBudget = base.CandidateBudget
	}
	if t.RerankK <= 0 {
		t.RerankK = base.RerankK
	}
	if t.ShardWorkers <= 0 {
		t.ShardWorkers = base.ShardWorkers
	}
	if t.TargetRecall <= 0 {
		t.TargetRecall = policy.TargetRecall
	}
	if t.BudgetScale <= 0 {
		t.BudgetScale = 1
	}

	target := t.TargetRecall
	if in.Requested.TargetRecall > 0 {
		target = in.Requested.TargetRecall
	}
	scale := t.BudgetScale
	if in.Requested.BudgetScale > 0 {
		scale = in.Requested.BudgetScale
	}
	if scale <= 0 {
		scale = 1
	}
	t.EfSearch = int(math.Round(float64(t.EfSearch) * recallScale(target) * scale))
	t.Beam = int(math.Round(float64(t.Beam) * recallScale(target)))
	t.CandidateBudget = int(math.Round(float64(t.CandidateBudget) * recallScale(target) * scale))
	t.RerankK = int(math.Round(float64(t.RerankK) * recallScale(target) * scale))

	if in.FilteredCount > 0 {
		minFilteredBudget := in.TopK * 8
		if minFilteredBudget < 64 {
			minFilteredBudget = 64
		}
		if in.FilteredCount < t.CandidateBudget {
			t.CandidateBudget = max(minFilteredBudget, in.FilteredCount)
		}
		sel := 1.0
		if in.CorpusCount > 0 {
			sel = float64(in.FilteredCount) / float64(in.CorpusCount)
		}
		switch {
		case sel <= 0.05:
			t.EfSearch = int(math.Round(float64(t.EfSearch) * 0.65))
			t.Beam = int(math.Round(float64(t.Beam) * 0.65))
		case sel <= 0.15:
			t.EfSearch = int(math.Round(float64(t.EfSearch) * 0.80))
			t.Beam = int(math.Round(float64(t.Beam) * 0.80))
		}
	}

	t = merge(t, in.Requested, in.AllowExactFallback)
	return normalizeAndClamp(t, policy, in.TopK)
}

// NewResolver returns a policy resolver for the provided mode.
func NewResolver(policy api.BudgetPolicySpec) Resolver {
	switch normalizePolicy(policy).Mode {
	case api.BudgetPolicyFixed:
		return fixedResolver{}
	default:
		return adaptiveResolver{}
	}
}

// DefaultSearchTuning returns a robust dim/metric prior for query tuning.
func DefaultSearchTuning(dim int, metric api.Metric, targetRecall float64) api.SearchTuning {
	recallAdj := recallScale(targetRecall)
	t := api.SearchTuning{
		TargetRecall: targetRecall,
		BudgetScale:  1,
		ShardWorkers: 4,
	}
	switch {
	case dim >= 768:
		t.EfSearch = 256
		t.Beam = 16
		t.CandidateBudget = 8192
		t.RerankK = 512
	case dim >= 384:
		t.EfSearch = 192
		t.Beam = 16
		t.CandidateBudget = 6144
		t.RerankK = 384
	case dim >= 256:
		t.EfSearch = 96
		t.Beam = 16
		t.CandidateBudget = 4096
		t.RerankK = 256
	case dim >= 96:
		t.EfSearch = 96
		t.Beam = 8
		t.CandidateBudget = 2048
		t.RerankK = 128
	default:
		t.EfSearch = 64
		t.Beam = 8
		t.CandidateBudget = 512
		t.RerankK = 128
	}
	if metric == api.MetricCosine {
		t.Beam = int(math.Round(float64(t.Beam) * 1.5))
		if t.Beam < 8 {
			t.Beam = 8
		}
		t.CandidateBudget = int(math.Round(float64(t.CandidateBudget) * 1.25))
	}
	t.EfSearch = int(math.Round(float64(t.EfSearch) * recallAdj))
	t.CandidateBudget = int(math.Round(float64(t.CandidateBudget) * recallAdj))
	t.RerankK = int(math.Round(float64(t.RerankK) * recallAdj))
	if t.RerankK > t.CandidateBudget {
		t.RerankK = t.CandidateBudget
	}
	return t
}

// SweepGrid derives benchmark sweep values around the adaptive prior.
func SweepGrid(dim int, metric api.Metric, targetRecall float64) (ef, beam, candidate, rerank []int) {
	base := DefaultSearchTuning(dim, metric, targetRecall)
	ef = uniquePositive(
		int(math.Round(float64(base.EfSearch)*0.6)),
		base.EfSearch,
		int(math.Round(float64(base.EfSearch)*1.5)),
		int(math.Round(float64(base.EfSearch)*2.2)),
	)
	beam = uniquePositive(
		max(8, base.Beam/2),
		base.Beam,
		base.Beam*2,
	)
	candidate = uniquePositive(
		max(512, base.CandidateBudget/2),
		base.CandidateBudget,
		min(8192, int(math.Round(float64(base.CandidateBudget)*1.5))),
		min(8192, base.CandidateBudget*2),
	)
	rerank = uniquePositive(
		max(128, base.RerankK/2),
		base.RerankK,
		min(base.CandidateBudget, int(math.Round(float64(base.RerankK)*1.5))),
		min(base.CandidateBudget, base.RerankK*2),
	)
	return ef, beam, candidate, rerank
}

func normalizePolicy(p api.BudgetPolicySpec) api.BudgetPolicySpec {
	if p.Mode == "" {
		p.Mode = api.BudgetPolicyAdaptive
	}
	if p.TargetRecall <= 0 {
		p.TargetRecall = 0.90
	}
	if p.MinEfSearch <= 0 {
		p.MinEfSearch = 32
	}
	if p.MaxEfSearch <= 0 {
		p.MaxEfSearch = 1024
	}
	if p.MinBeam <= 0 {
		p.MinBeam = 4
	}
	if p.MaxBeam <= 0 {
		p.MaxBeam = 64
	}
	if p.MinCandidateBudget <= 0 {
		p.MinCandidateBudget = 128
	}
	if p.MaxCandidateBudget <= 0 {
		p.MaxCandidateBudget = 16384
	}
	if p.MinRerankK <= 0 {
		p.MinRerankK = 32
	}
	if p.MaxRerankK <= 0 {
		p.MaxRerankK = 4096
	}
	return p
}

func normalizeAndClamp(t api.SearchTuning, policy api.BudgetPolicySpec, topK int) api.SearchTuning {
	policy = normalizePolicy(policy)
	if topK <= 0 {
		topK = 10
	}
	if t.EfSearch < topK*2 {
		t.EfSearch = topK * 2
	}
	if t.Beam < 1 {
		t.Beam = 1
	}
	if t.CandidateBudget < topK*4 {
		t.CandidateBudget = topK * 4
	}
	if t.RerankK < topK {
		t.RerankK = topK
	}
	if t.RerankK > t.CandidateBudget {
		t.RerankK = t.CandidateBudget
	}
	if t.ShardWorkers <= 0 {
		t.ShardWorkers = 1
	}
	if t.TargetRecall <= 0 {
		t.TargetRecall = policy.TargetRecall
	}
	if t.BudgetScale <= 0 {
		t.BudgetScale = 1
	}
	t.EfSearch = clampInt(t.EfSearch, policy.MinEfSearch, policy.MaxEfSearch)
	t.Beam = clampInt(t.Beam, policy.MinBeam, policy.MaxBeam)
	t.CandidateBudget = clampInt(t.CandidateBudget, policy.MinCandidateBudget, policy.MaxCandidateBudget)
	t.RerankK = clampInt(t.RerankK, policy.MinRerankK, min(policy.MaxRerankK, t.CandidateBudget))
	return t
}

func merge(base, req api.SearchTuning, allowExactFallback bool) api.SearchTuning {
	out := base
	if req.EfSearch > 0 {
		out.EfSearch = req.EfSearch
	}
	if req.Beam > 0 {
		out.Beam = req.Beam
	}
	if req.CandidateBudget > 0 {
		out.CandidateBudget = req.CandidateBudget
	}
	if req.RerankK > 0 {
		out.RerankK = req.RerankK
	}
	if req.ShardWorkers > 0 {
		out.ShardWorkers = req.ShardWorkers
	}
	if req.TargetRecall > 0 {
		out.TargetRecall = req.TargetRecall
	}
	if req.BudgetScale > 0 {
		out.BudgetScale = req.BudgetScale
	}
	if req.AllowExactFallback || allowExactFallback {
		out.AllowExactFallback = true
	}
	return out
}

func recallScale(target float64) float64 {
	switch {
	case target >= 0.985:
		return 1.8
	case target >= 0.97:
		return 1.5
	case target >= 0.95:
		return 1.25
	case target >= 0.92:
		return 1.1
	case target >= 0.90:
		return 1.0
	case target >= 0.85:
		return 0.85
	default:
		return 0.75
	}
}

func uniquePositive(vals ...int) []int {
	seen := make(map[int]struct{}, len(vals))
	out := make([]int, 0, len(vals))
	for _, v := range vals {
		if v <= 0 {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

func clampInt(v, lo, hi int) int {
	if lo > 0 && v < lo {
		v = lo
	}
	if hi > 0 && v > hi {
		v = hi
	}
	return v
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
