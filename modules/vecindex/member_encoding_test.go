package vecindex

import (
	"math"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/quantize"
)

func TestNewStableMemberScorer_ResidualInt8(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: 4, Metric: MetricCosine, Nlist: 1, Nprobe: 1}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{{0.4, 0.6, 0.4, 0.6}})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	prepared := []byte{
		0x00, 0x00, 0x00, 0x3f,
		0x00, 0x00, 0x00, 0x3f,
		0x00, 0x00, 0x00, 0x3f,
		0x00, 0x00, 0x00, 0x3f,
	}
	enc, blob, err := EncodeStableMember(spec, cs, 1, prepared)
	if err != nil {
		t.Fatalf("EncodeStableMember: %v", err)
	}
	if enc != MemberEncodingResidualInt8 {
		t.Fatalf("encoding = %d, want residual-int8", enc)
	}
	query := []float32{0.5, 0.5, 0.5, 0.5}
	queryNorm2 := metric.Norm2(query)
	scorer, err := NewStableMemberScorer(spec, cs, query, queryNorm2, 1, enc)
	if err != nil {
		t.Fatalf("NewStableMemberScorer: %v", err)
	}
	got, err := scorer.Score(blob)
	if err != nil {
		t.Fatalf("Score: %v", err)
	}
	want, err := ScoreEncodedMember(spec, cs, query, queryNorm2, 1, enc, blob)
	if err != nil {
		t.Fatalf("ScoreEncodedMember: %v", err)
	}
	if got != want {
		t.Fatalf("score = %v, want %v", got, want)
	}

	rows := make([]byte, 8+len(blob))
	copy(rows[8:], blob)
	out := make([]float32, 1)
	if err := scorer.ScoreSpan(rows, 8+len(blob), out); err != nil {
		t.Fatalf("ScoreSpan: %v", err)
	}
	if out[0] != got {
		t.Fatalf("span score = %v, want %v", out[0], got)
	}
}

func TestStableMemberCodecResidualPQ8RoundTripAndScoreSpan(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: 16, Metric: MetricL2, Nlist: 1, Nprobe: 1}
	centroid := make([]float32, spec.InternalDim())
	for i := range centroid {
		centroid[i] = float32(i%7) * 0.01
	}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{centroid})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	residuals := make([][]float32, 300)
	for i := range residuals {
		residuals[i] = make([]float32, spec.InternalDim())
		for d := range residuals[i] {
			residuals[i][d] = float32(math.Sin(float64((i+1)*(d+3)%101))) * 0.1
		}
	}
	pq, err := quantize.TrainPQ8(residuals, spec.InternalDim(), quantize.PQ8Options{M: 4, MaxIter: 3, Seed: 9})
	if err != nil {
		t.Fatalf("TrainPQ8: %v", err)
	}
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualPQ8, pq)
	if err != nil {
		t.Fatalf("NewStableMemberCodec: %v", err)
	}
	if got, want := codec.EncodedSize(), 8; got != want {
		t.Fatalf("encoded size = %d, want %d", got, want)
	}

	vec := make([]float32, spec.InternalDim())
	for i := range vec {
		vec[i] = centroid[i] + residuals[17][i]
	}
	enc, blob, err := codec.Encode(1, Float32ToBytes(vec))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if enc != MemberEncodingResidualPQ8 {
		t.Fatalf("encoding = %d, want PQ", enc)
	}
	decoded, err := codec.DecodePrepared(1, blob)
	if err != nil {
		t.Fatalf("DecodePrepared: %v", err)
	}
	if len(decoded) != spec.InternalDim() {
		t.Fatalf("decoded dim = %d, want %d", len(decoded), spec.InternalDim())
	}

	query := make([]float32, spec.InternalDim())
	for i := range query {
		query[i] = float32((i*5)%13) * 0.02
	}
	scorer, err := NewStableMemberScorerWithCodec(codec, query, metric.Norm2(query), 1)
	if err != nil {
		t.Fatalf("NewStableMemberScorerWithCodec: %v", err)
	}
	scalar, err := scorer.Score(blob)
	if err != nil {
		t.Fatalf("Score: %v", err)
	}
	row := make([]byte, 8+len(blob))
	copy(row[8:], blob)
	out := make([]float32, 1)
	if err := scorer.ScoreSpan(row, len(row), out); err != nil {
		t.Fatalf("ScoreSpan: %v", err)
	}
	if out[0] != scalar {
		t.Fatalf("span score = %v, want %v", out[0], scalar)
	}
}

func TestStableMemberCodecBlobCompatibility(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: 8, Metric: MetricCosine, Nlist: 1, Nprobe: 1}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{{0, 0, 0, 0, 0, 0, 0, 0}})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	legacy, err := DecodeStableMemberCodecBlob(spec, cs, MemberEncodingResidualInt8, nil)
	if err != nil {
		t.Fatalf("legacy DecodeStableMemberCodecBlob: %v", err)
	}
	if legacy.Encoding() != MemberEncodingResidualInt8 {
		t.Fatalf("legacy encoding = %d, want residual-int8", legacy.Encoding())
	}
	if _, err := DecodeStableMemberCodecBlob(spec, cs, MemberEncodingResidualPQ8, nil); err == nil {
		t.Fatalf("PQ decode with missing blob succeeded, want error")
	}
}
