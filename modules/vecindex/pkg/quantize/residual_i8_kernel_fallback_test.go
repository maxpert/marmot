//go:build cgo && (amd64 || arm64) && (darwin || linux)

package quantize

import (
	"runtime"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

func TestResidualInt8Scorer_ScoreSpanFallsBackWhenSIMDKernelUnavailable(t *testing.T) {
	query, centroid := benchResidualInputs(128)
	candidateA, _ := benchResidualInputs(128)
	candidateB, _ := benchResidualInputs(128)
	blobA, err := EncodeResidualInt8(metric.MetricCosine, candidateA, centroid, DefaultResidualBlockSize)
	require.NoError(t, err)
	blobB, err := EncodeResidualInt8(metric.MetricCosine, candidateB, centroid, DefaultResidualBlockSize)
	require.NoError(t, err)

	scorer, err := NewResidualInt8Scorer(metric.MetricCosine, query, 0, centroid, DefaultResidualBlockSize)
	require.NoError(t, err)

	entrySize := 8 + len(blobA)
	rows := make([]byte, entrySize*2)
	copy(rows[8:], blobA)
	copy(rows[entrySize+8:], blobB)

	want0, err := manualResidualInt8Score(metric.MetricCosine, query, 0, centroid, blobA, DefaultResidualBlockSize)
	require.NoError(t, err)
	want1, err := manualResidualInt8Score(metric.MetricCosine, query, 0, centroid, blobB, DefaultResidualBlockSize)
	require.NoError(t, err)

	old := residualSpanKernel
	defer func() { residualSpanKernel = old }()
	if runtime.GOARCH == "arm64" {
		residualSpanKernel = residualKernelAMD64VNNI
	} else {
		residualSpanKernel = residualKernelARM64Dot
	}

	got := make([]float32, 2)
	require.NoError(t, scorer.ScoreSpan(rows, entrySize, got))
	require.InDelta(t, want0, got[0], 1e-6)
	require.InDelta(t, want1, got[1], 1e-6)
}
