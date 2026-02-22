package api

import "fmt"

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
