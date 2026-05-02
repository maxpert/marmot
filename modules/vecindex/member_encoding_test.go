package vecindex

import (
	"math"
	"path/filepath"
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
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualInt8, nil)
	if err != nil {
		t.Fatalf("NewStableMemberCodec: %v", err)
	}
	enc, blob, err := codec.Encode(1, prepared)
	if err != nil {
		t.Fatalf("Encode: %v", err)
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

	spec := IVFSpec{ID: "idx", Dim: StablePQMinInternalDim, Metric: MetricL2, Nlist: 1, Nprobe: 1}
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

func TestBuildStableMemberCodecUsesPQForHighDimSmallTrainingSet(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: StablePQMinInternalDim, Metric: MetricL2, Nlist: 1, Nprobe: 1}
	centroid := make([]float32, spec.InternalDim())
	cs, err := kmeans.NewCentroidSet(1, [][]float32{centroid})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	vec := make([]float32, spec.InternalDim())
	for i := range vec {
		vec[i] = float32(i%11) * 0.01
	}
	codec, err := BuildStableMemberCodec(spec, cs, []StableCodecTrainingVector{{ClusterID: 1, Vec: vec}}, 3)
	if err != nil {
		t.Fatalf("BuildStableMemberCodec: %v", err)
	}
	if codec.Encoding() != MemberEncodingResidualPQ8 {
		t.Fatalf("encoding = %d, want PQ", codec.Encoding())
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

func TestStableMemberQueryScorerResidualInt8BlockLowerBound(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		metricKind Metric
	}{
		{name: "l2", metricKind: MetricL2},
		{name: "cosine", metricKind: MetricCosine},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := IVFSpec{ID: "idx", Dim: 8, Metric: tc.metricKind, Nlist: 1, Nprobe: 1}
			centroid := []float32{0.2, 0.1, -0.1, 0.3, 0, 0.2, -0.2, 0.1}
			cs, err := kmeans.NewCentroidSet(1, [][]float32{centroid})
			if err != nil {
				t.Fatalf("NewCentroidSet: %v", err)
			}
			codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualInt8, nil)
			if err != nil {
				t.Fatalf("NewStableMemberCodec: %v", err)
			}
			rows := [][]float32{
				{0.25, 0.05, -0.05, 0.35, 0.1, 0.15, -0.1, 0.2},
				{0.15, 0.2, -0.2, 0.25, -0.1, 0.25, -0.3, 0},
				{0.3, 0.1, -0.15, 0.4, 0.05, 0.1, -0.25, 0.15},
			}
			block, blobs := blockRecordForEncodedRows(t, spec, codec, 1, rows)

			query := []float32{0.4, -0.1, 0.2, 0.3, 0.05, 0.1, -0.4, 0.2}
			if tc.metricKind == MetricCosine {
				n := metric.Norm(query)
				for i := range query {
					query[i] /= n
				}
			}
			assertBlockLowerBoundCoversRows(t, codec, query, metric.Norm2(query), 1, block, blobs)
		})
	}
}

func TestStableMemberQueryScorerPQBlockLowerBound(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: StablePQMinInternalDim, Metric: MetricL2, Nlist: 1, Nprobe: 1}
	centroid := make([]float32, spec.InternalDim())
	for i := range centroid {
		centroid[i] = float32(i%17) * 0.001
	}
	cs, err := kmeans.NewCentroidSet(1, [][]float32{centroid})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}
	pq, err := quantize.TrainPQ8(testResiduals(320, spec.InternalDim()), spec.InternalDim(), quantize.PQ8Options{M: 4, MaxIter: 3, Seed: 11})
	if err != nil {
		t.Fatalf("TrainPQ8: %v", err)
	}
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualPQ8, pq)
	if err != nil {
		t.Fatalf("NewStableMemberCodec: %v", err)
	}

	residuals := testResiduals(8, spec.InternalDim())
	rows := make([][]float32, 3)
	for i := range rows {
		rows[i] = make([]float32, spec.InternalDim())
		for d := range rows[i] {
			rows[i][d] = centroid[d] + residuals[i+2][d]
		}
	}
	block, blobs := blockRecordForEncodedRows(t, spec, codec, 1, rows)
	query := make([]float32, spec.InternalDim())
	for i := range query {
		query[i] = float32((i*7)%19) * 0.002
	}
	assertBlockLowerBoundCoversRows(t, codec, query, metric.Norm2(query), 1, block, blobs)
}

func TestStableMemberQueryScorerPQBlockLowerBoundForDotInternalL2(t *testing.T) {
	t.Parallel()

	spec := IVFSpec{ID: "idx", Dim: StablePQMinInternalDim - 1, Metric: MetricDot, Nlist: 1, Nprobe: 1, MaxNorm: 64}
	centroid := make([]float32, spec.InternalDim())
	cs, err := kmeans.NewCentroidSet(1, [][]float32{centroid})
	if err != nil {
		t.Fatalf("NewCentroidSet: %v", err)
	}

	training := make([][]float32, 320)
	for i := range training {
		raw := make([]float32, spec.Dim)
		for d := range raw {
			raw[d] = float32(math.Sin(float64((i+1)*(d+5)%131))) * 0.01
		}
		prepared, err := metric.AugmentData(raw, spec.MaxNorm, nil)
		if err != nil {
			t.Fatalf("AugmentData training: %v", err)
		}
		training[i] = prepared
	}
	pq, err := quantize.TrainPQ8(training, spec.InternalDim(), quantize.PQ8Options{M: 4, MaxIter: 3, Seed: 13})
	if err != nil {
		t.Fatalf("TrainPQ8: %v", err)
	}
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualPQ8, pq)
	if err != nil {
		t.Fatalf("NewStableMemberCodec: %v", err)
	}

	rows := training[10:14]
	block, blobs := blockRecordForEncodedRows(t, spec, codec, 1, rows)
	rawQuery := make([]float32, spec.Dim)
	for i := range rawQuery {
		rawQuery[i] = float32(math.Cos(float64((i+3)%29))) * 0.5
	}
	query := metric.AugmentQuery(rawQuery, nil)
	assertBlockLowerBoundCoversRows(t, codec, query, metric.Norm2(query), 1, block, blobs)
}

func blockRecordForEncodedRows(t *testing.T, spec IVFSpec, codec *StableMemberCodec, clusterID int64, rows [][]float32) (SegmentBlockRecord, [][]byte) {
	t.Helper()
	writer, err := CreateSegmentBlockMetaWriter(filepath.Join(t.TempDir(), "gen.blk"), spec, codec, len(rows)+1, spec.Nlist, 1, 1)
	if err != nil {
		t.Fatalf("CreateSegmentBlockMetaWriter: %v", err)
	}
	offset := uint64(0)
	blobs := make([][]byte, 0, len(rows))
	for i, row := range rows {
		_, blob, err := codec.Encode(clusterID, Float32ToBytes(row))
		if err != nil {
			t.Fatalf("Encode row %d: %v", i, err)
		}
		if err := writer.Append(clusterID, int64(i+1), offset, 8+len(blob), blob); err != nil {
			t.Fatalf("Append row %d: %v", i, err)
		}
		offset += uint64(8 + len(blob))
		blobs = append(blobs, blob)
	}
	store, err := writer.Close()
	if err != nil {
		t.Fatalf("Close block writer: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	blocks, err := store.ReadClusterBlocks([]int64{clusterID})
	if err != nil {
		t.Fatalf("ReadClusterBlocks: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("blocks = %d, want 1", len(blocks))
	}
	return blocks[0], blobs
}

func assertBlockLowerBoundCoversRows(t *testing.T, codec *StableMemberCodec, query []float32, queryNorm2 float32, clusterID int64, block SegmentBlockRecord, blobs [][]byte) {
	t.Helper()
	queryScorer, err := NewStableMemberQueryScorerWithCodec(codec, query, queryNorm2)
	if err != nil {
		t.Fatalf("NewStableMemberQueryScorerWithCodec: %v", err)
	}
	bound, ok := queryScorer.BlockLowerBound(clusterID, block)
	if !ok {
		t.Fatalf("BlockLowerBound returned !ok")
	}
	scorer, err := queryScorer.ClusterScorer(clusterID)
	if err != nil {
		t.Fatalf("ClusterScorer: %v", err)
	}
	for i, blob := range blobs {
		score, err := scorer.Score(blob)
		if err != nil {
			t.Fatalf("Score row %d: %v", i, err)
		}
		if bound > score+1e-3 {
			t.Fatalf("block lower bound = %v, row %d score = %v", bound, i, score)
		}
	}
}

func testResiduals(n, dim int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		out[i] = make([]float32, dim)
		for d := range out[i] {
			x := float64((i+3)*(d+7)%149) / 149
			out[i][d] = float32(math.Sin(x*math.Pi*2) * 0.05)
		}
	}
	return out
}
