package quantize

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/stretchr/testify/require"
)

func TestEncodeResidualInt8_Deterministic(t *testing.T) {
	vec := []float32{1.25, -2.5, 3.75, -4.5}
	centroid := []float32{1, -2, 4, -4}
	a, err := EncodeResidualInt8(metric.MetricL2, vec, centroid, 2)
	require.NoError(t, err)
	b, err := EncodeResidualInt8(metric.MetricL2, vec, centroid, 2)
	require.NoError(t, err)
	require.Equal(t, a, b)
}

func TestEncodeDecodeResidualInt8_L2(t *testing.T) {
	vec := []float32{1.25, -2.5, 3.75, -4.5, 0.25, -0.125}
	centroid := []float32{1, -2, 4, -4, 0, 0}
	blob, err := EncodeResidualInt8(metric.MetricL2, vec, centroid, 2)
	require.NoError(t, err)

	got, norm2, err := DecodeResidualInt8(metric.MetricL2, centroid, blob, 2, nil)
	require.NoError(t, err)
	require.InDelta(t, metric.Norm2(vec), norm2, 1e-6)
	require.Len(t, got, len(vec))
	for i := range vec {
		require.InDelta(t, vec[i], got[i], 0.01)
	}
}

func TestEncodeDecodeResidualInt8_Cosine(t *testing.T) {
	vec := []float32{0.5, 0.5, 0.5, 0.5}
	centroid := []float32{0.4, 0.6, 0.4, 0.6}
	blob, err := EncodeResidualInt8(metric.MetricCosine, vec, centroid, 2)
	require.NoError(t, err)
	got, norm2, err := DecodeResidualInt8(metric.MetricCosine, centroid, blob, 2, nil)
	require.NoError(t, err)
	require.Zero(t, norm2)
	for i := range vec {
		require.InDelta(t, vec[i], got[i], 0.01)
	}
}

func TestDistanceFromResidualInt8_RankingParity(t *testing.T) {
	query := []float32{0.9, 0.1, 0}
	centroid := []float32{0.8, 0.2, 0}
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}

	blobA, err := EncodeResidualInt8(metric.MetricCosine, a, centroid, 2)
	require.NoError(t, err)
	blobB, err := EncodeResidualInt8(metric.MetricCosine, b, centroid, 2)
	require.NoError(t, err)

	exactA := metric.CosineDistanceUnit(query, a)
	exactB := metric.CosineDistanceUnit(query, b)
	approxA, err := DistanceFromResidualInt8(metric.MetricCosine, query, 0, centroid, blobA, 2)
	require.NoError(t, err)
	approxB, err := DistanceFromResidualInt8(metric.MetricCosine, query, 0, centroid, blobB, 2)
	require.NoError(t, err)

	require.Less(t, exactA, exactB)
	require.Less(t, approxA, approxB)
}

func TestDistanceFromResidualInt8_L2UsesStoredNorm(t *testing.T) {
	query := []float32{3, 4}
	queryNorm2 := metric.Norm2(query)
	centroid := []float32{1, 1}
	vec := []float32{2, 2}
	blob, err := EncodeResidualInt8(metric.MetricL2, vec, centroid, 2)
	require.NoError(t, err)

	got, err := DistanceFromResidualInt8(metric.MetricL2, query, queryNorm2, centroid, blob, 2)
	require.NoError(t, err)
	require.InDelta(t, metric.L2Squared(query, vec), got, 0.05)
}

func TestResidualInt8Scorer_MatchesDistanceHelper(t *testing.T) {
	query, centroid := benchResidualInputs(1536)
	candidate, _ := benchResidualInputs(1536)
	blob, err := EncodeResidualInt8(metric.MetricCosine, candidate, centroid, DefaultResidualBlockSize)
	require.NoError(t, err)

	scorer, err := NewResidualInt8Scorer(metric.MetricCosine, query, 0, centroid, DefaultResidualBlockSize)
	require.NoError(t, err)

	got, err := scorer.Distance(blob)
	require.NoError(t, err)
	want, err := manualResidualInt8Score(metric.MetricCosine, query, 0, centroid, blob, DefaultResidualBlockSize)
	require.InDelta(t, want, got, 1e-6)
}

func TestResidualInt8Scorer_ScoreSpanMatchesDistance(t *testing.T) {
	query, centroid := benchResidualInputs(1536)
	candidateA, _ := benchResidualInputs(1536)
	candidateB, _ := benchResidualInputs(1536)
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
	got := make([]float32, 2)
	require.NoError(t, scorer.ScoreSpan(rows, entrySize, got))

	want0, err := manualResidualInt8Score(metric.MetricCosine, query, 0, centroid, blobA, DefaultResidualBlockSize)
	require.NoError(t, err)
	want1, err := manualResidualInt8Score(metric.MetricCosine, query, 0, centroid, blobB, DefaultResidualBlockSize)
	require.NoError(t, err)
	require.InDelta(t, want0, got[0], 1e-6)
	require.InDelta(t, want1, got[1], 1e-6)
}

func manualResidualInt8Score(rankMetric metric.Metric, query []float32, queryNorm2 float32, centroid []float32, blob []byte, blockSize int) (float32, error) {
	queryCodes, queryScales, err := QuantizeQueryInt8(query, blockSize)
	if err != nil {
		return 0, err
	}
	norm2 := float32(0)
	off := 0
	if rankMetric == metric.MetricL2 {
		norm2 = math.Float32frombits(binary.LittleEndian.Uint32(blob[:4]))
		off = 4
	}
	blocks := ResidualBlockCount(len(query), blockSize)
	scaleOff := off
	codeOff := off + blocks*2
	residualDot := float32(0)
	for block := 0; block < blocks; block++ {
		start := block * blockSize
		end := start + blockSize
		if end > len(query) {
			end = len(query)
		}
		residualScale := decodeFloat16(binary.LittleEndian.Uint16(blob[scaleOff+block*2:]))
		queryScale := queryScales[block]
		if residualScale == 0 || queryScale == 0 {
			continue
		}
		residualDot += queryScale * residualScale * float32(dotInt8(queryCodes[start:end], blob[codeOff+start:codeOff+end]))
	}
	dot := metric.DotProduct(query, centroid) + residualDot
	switch rankMetric {
	case metric.MetricCosine:
		return 1 - dot, nil
	case metric.MetricL2:
		return queryNorm2 + norm2 - 2*dot, nil
	default:
		return 0, nil
	}
}

func TestDecodeResidualInt8_RejectsMalformedBlob(t *testing.T) {
	_, _, err := DecodeResidualInt8(metric.MetricL2, []float32{1, 2, 3}, []byte{1, 2, 3}, 2, nil)
	require.Error(t, err)
}

func BenchmarkEncodeResidualInt8_1536(b *testing.B) {
	vec, centroid := benchResidualInputs(1536)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeResidualInt8(metric.MetricCosine, vec, centroid, DefaultResidualBlockSize)
	}
}

func BenchmarkDistanceFromResidualInt8_1536(b *testing.B) {
	query, centroid := benchResidualInputs(1536)
	blob, err := EncodeResidualInt8(metric.MetricCosine, query, centroid, DefaultResidualBlockSize)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = DistanceFromResidualInt8(metric.MetricCosine, query, 0, centroid, blob, DefaultResidualBlockSize)
	}
}

func BenchmarkResidualInt8Scorer_1536(b *testing.B) {
	query, centroid := benchResidualInputs(1536)
	blob, err := EncodeResidualInt8(metric.MetricCosine, query, centroid, DefaultResidualBlockSize)
	if err != nil {
		b.Fatal(err)
	}
	scorer, err := NewResidualInt8Scorer(metric.MetricCosine, query, 0, centroid, DefaultResidualBlockSize)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = scorer.Distance(blob)
	}
}

func benchResidualInputs(dim int) ([]float32, []float32) {
	rng := rand.New(rand.NewSource(7))
	vec := make([]float32, dim)
	centroid := make([]float32, dim)
	for i := range vec {
		centroid[i] = float32(rng.NormFloat64() * 0.5)
		vec[i] = centroid[i] + float32(rng.NormFloat64()*0.05)
	}
	n := metric.Norm(vec)
	if n > 0 {
		inv := float32(1 / float64(n))
		for i := range vec {
			vec[i] *= inv
			centroid[i] *= inv
		}
	}
	return vec, centroid
}

func TestFloat16RoundTrip(t *testing.T) {
	for _, v := range []float32{0, 1, -1, 0.125, -0.125, 13.75, 1e-3} {
		got := decodeFloat16(encodeFloat16(v))
		require.Less(t, math.Abs(float64(v-got)), 0.02)
	}
}
