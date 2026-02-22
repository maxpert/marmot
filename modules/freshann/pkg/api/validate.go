package api

import "fmt"

const MaxVectorCacheBytes int64 = 128 << 20

func ValidateSpec(spec IndexSpec) error {
	if spec.ID == "" {
		return fmt.Errorf("%w: id is required", ErrInvalidSpec)
	}
	if spec.Dim <= 0 {
		return fmt.Errorf("%w: dim must be > 0", ErrInvalidSpec)
	}
	switch spec.Metric {
	case MetricCosine, MetricDot, MetricEuclidean:
	default:
		return fmt.Errorf("%w: metric %q", ErrUnsupportedMetric, spec.Metric)
	}
	switch spec.ApplyMode {
	case "", ApplyModeSync, ApplyModeAsync:
	default:
		return fmt.Errorf("%w: invalid apply mode %q", ErrInvalidSpec, spec.ApplyMode)
	}
	switch spec.DurabilityMode {
	case "", DurabilitySyncEveryCommit, DurabilityPeriodic, DurabilityAsync:
	default:
		return fmt.Errorf("%w: invalid durability mode %q", ErrInvalidSpec, spec.DurabilityMode)
	}
	if spec.Graph.R < 0 || spec.Graph.LBuild < 0 || spec.Graph.LSearch < 0 || spec.Graph.Beam < 0 {
		return fmt.Errorf("%w: graph values must be >= 0", ErrInvalidSpec)
	}
	if spec.Storage.PebbleCacheBytes < 0 {
		return fmt.Errorf("%w: storage pebble cache bytes must be >= 0", ErrInvalidSpec)
	}
	if spec.Storage.VectorCacheBytes < 0 {
		return fmt.Errorf("%w: storage vector cache bytes must be >= 0", ErrInvalidSpec)
	}
	if spec.Storage.VectorCacheBytes > MaxVectorCacheBytes {
		return fmt.Errorf("%w: storage vector cache bytes must be <= %d", ErrInvalidSpec, MaxVectorCacheBytes)
	}
	if spec.Storage.BloomBitsPerKey < 0 || spec.Storage.VectorBlockSize < 0 || spec.Storage.GraphPageSize < 0 || spec.Storage.PostingChunkSize < 0 {
		return fmt.Errorf("%w: storage values must be >= 0", ErrInvalidSpec)
	}
	switch spec.BudgetPolicy.Mode {
	case "", BudgetPolicyAdaptive, BudgetPolicyFixed:
	default:
		return fmt.Errorf("%w: invalid budget policy mode %q", ErrInvalidSpec, spec.BudgetPolicy.Mode)
	}
	if spec.BudgetPolicy.TargetRecall < 0 || spec.BudgetPolicy.TargetRecall > 1 {
		return fmt.Errorf("%w: budget policy target recall must be between 0 and 1", ErrInvalidSpec)
	}
	if spec.BudgetPolicy.MinEfSearch < 0 || spec.BudgetPolicy.MaxEfSearch < 0 ||
		spec.BudgetPolicy.MinBeam < 0 || spec.BudgetPolicy.MaxBeam < 0 ||
		spec.BudgetPolicy.MinCandidateBudget < 0 || spec.BudgetPolicy.MaxCandidateBudget < 0 ||
		spec.BudgetPolicy.MinRerankK < 0 || spec.BudgetPolicy.MaxRerankK < 0 {
		return fmt.Errorf("%w: budget policy bounds must be >= 0", ErrInvalidSpec)
	}
	if spec.BudgetPolicy.MinEfSearch > 0 && spec.BudgetPolicy.MaxEfSearch > 0 && spec.BudgetPolicy.MinEfSearch > spec.BudgetPolicy.MaxEfSearch {
		return fmt.Errorf("%w: budget policy ef range is invalid", ErrInvalidSpec)
	}
	if spec.BudgetPolicy.MinBeam > 0 && spec.BudgetPolicy.MaxBeam > 0 && spec.BudgetPolicy.MinBeam > spec.BudgetPolicy.MaxBeam {
		return fmt.Errorf("%w: budget policy beam range is invalid", ErrInvalidSpec)
	}
	if spec.BudgetPolicy.MinCandidateBudget > 0 && spec.BudgetPolicy.MaxCandidateBudget > 0 && spec.BudgetPolicy.MinCandidateBudget > spec.BudgetPolicy.MaxCandidateBudget {
		return fmt.Errorf("%w: budget policy candidate range is invalid", ErrInvalidSpec)
	}
	if spec.BudgetPolicy.MinRerankK > 0 && spec.BudgetPolicy.MaxRerankK > 0 && spec.BudgetPolicy.MinRerankK > spec.BudgetPolicy.MaxRerankK {
		return fmt.Errorf("%w: budget policy rerank range is invalid", ErrInvalidSpec)
	}
	if spec.SearchDefaults.EfSearch < 0 || spec.SearchDefaults.Beam < 0 || spec.SearchDefaults.CandidateBudget < 0 || spec.SearchDefaults.RerankK < 0 || spec.SearchDefaults.ShardWorkers < 0 {
		return fmt.Errorf("%w: search defaults must be >= 0", ErrInvalidSpec)
	}
	if spec.SearchDefaults.TargetRecall < 0 || spec.SearchDefaults.TargetRecall > 1 {
		return fmt.Errorf("%w: search defaults target recall must be between 0 and 1", ErrInvalidSpec)
	}
	if spec.SearchDefaults.BudgetScale < 0 {
		return fmt.Errorf("%w: search defaults budget scale must be >= 0", ErrInvalidSpec)
	}
	return nil
}

func ValidateMutation(dim int, mut Mutation) error {
	if mut.TxnID == 0 {
		return fmt.Errorf("%w: txn id must be > 0", ErrInvalidMutation)
	}
	if mut.SeqID == 0 {
		return fmt.Errorf("%w: seq id must be > 0", ErrInvalidMutation)
	}
	if len(mut.ExternalID) == 0 {
		return fmt.Errorf("%w: external id required", ErrInvalidMutation)
	}
	if len(mut.VectorFP32) != dim {
		return fmt.Errorf("%w: vector dim mismatch expected=%d actual=%d", ErrInvalidMutation, dim, len(mut.VectorFP32))
	}
	return nil
}

func ValidateDeleteMutation(mut DeleteMutation) error {
	if mut.TxnID == 0 {
		return fmt.Errorf("%w: txn id must be > 0", ErrInvalidMutation)
	}
	if mut.SeqID == 0 {
		return fmt.Errorf("%w: seq id must be > 0", ErrInvalidMutation)
	}
	if len(mut.ExternalID) == 0 {
		return fmt.Errorf("%w: external id required", ErrInvalidMutation)
	}
	return nil
}
