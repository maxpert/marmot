package vecindex

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
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
