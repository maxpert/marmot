package bench

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeRunner struct{}

func (f fakeRunner) Run(_ context.Context, system System, cfg RunConfig) (Result, error) {
	return Result{System: system, Dataset: cfg.Dataset, RecallAt10: 0.9, P95MS: 10, P99MS: 15, QPS: 500, QPSAtRecall90: 500}, nil
}

func TestRunComparisonAndThreshold(t *testing.T) {
	report, err := RunComparison(context.Background(), fakeRunner{}, RunConfig{Profile: ProfileCISmoke, Dataset: "tiny"}, []System{SystemFreshANN, SystemMilvus})
	require.NoError(t, err)
	require.Len(t, report.Results, 2)
	require.NoError(t, CheckThreshold(report, SystemFreshANN, DefaultThresholds(ProfileCISmoke)))

	path := filepath.Join(t.TempDir(), "report.json")
	require.NoError(t, SaveReport(path, report))
}
