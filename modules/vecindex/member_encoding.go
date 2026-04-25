package vecindex

import (
	"fmt"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/quantize"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	MemberEncodingRawPreparedF32 int64 = iota
	MemberEncodingResidualInt8
	MemberEncodingResidualPQ8
)

const MemberResidualBlockSize = quantize.DefaultResidualBlockSize
const pqTrainingRowFloor = 4096

func StableMemberEncodingSpec(spec IVFSpec) (int64, int) {
	return MemberEncodingResidualInt8, quantize.EncodedResidualSize(spec.InternalMetric(), spec.InternalDim(), MemberResidualBlockSize)
}

type StableCodecTrainingVector struct {
	ClusterID int64
	Vec       []float32
}

type StableMemberCodec struct {
	spec     IVFSpec
	centroid *kmeans.CentroidSet
	enc      int64
	pq       *quantize.PQ8Codec
}

type StableMemberScorer struct {
	enc       int64
	rawMetric metric.Metric
	query     []float32
	residual  *quantize.ResidualInt8Scorer
	pq        *quantize.PQ8Scorer
}

type StableMemberQueryScorer struct {
	codec      *StableMemberCodec
	query      []float32
	queryNorm2 float32
	pq         *quantize.PQ8QueryScorer
}

type stableMemberCodecMsg struct {
	Version  uint32             `msgpack:"version"`
	Encoding int64              `msgpack:"encoding"`
	PQ8      *quantize.PQ8Codec `msgpack:"pq8,omitempty"`
}

func BuildStableMemberCodec(spec IVFSpec, cs *kmeans.CentroidSet, training []StableCodecTrainingVector, seed uint64) (*StableMemberCodec, error) {
	if cs == nil {
		return nil, fmt.Errorf("vecindex: centroid set is nil")
	}
	if spec.InternalDim() >= 512 && len(training) >= pqTrainingRowFloor {
		residuals := make([][]float32, 0, len(training))
		for i, sample := range training {
			if sample.ClusterID <= 0 || int(sample.ClusterID) > cs.Len() {
				return nil, fmt.Errorf("vecindex: PQ training sample %d cluster %d out of range", i, sample.ClusterID)
			}
			if len(sample.Vec) != spec.InternalDim() {
				return nil, fmt.Errorf("vecindex: PQ training sample %d dim=%d want=%d", i, len(sample.Vec), spec.InternalDim())
			}
			centroid, err := cs.GetReadOnly(uint32(sample.ClusterID - 1))
			if err != nil {
				return nil, err
			}
			residual := make([]float32, len(sample.Vec))
			for d, value := range sample.Vec {
				residual[d] = value - centroid[d]
			}
			residuals = append(residuals, residual)
		}
		pq, err := quantize.TrainPQ8(residuals, spec.InternalDim(), quantize.PQ8Options{
			M:         quantize.DefaultPQ8Subquantizers,
			MaxIter:   8,
			Seed:      seed,
			StoreNorm: spec.InternalMetric() == metric.MetricCosine,
		})
		if err != nil {
			return nil, err
		}
		return NewStableMemberCodec(spec, cs, MemberEncodingResidualPQ8, pq)
	}
	return NewStableMemberCodec(spec, cs, MemberEncodingResidualInt8, nil)
}

func NewStableMemberCodec(spec IVFSpec, cs *kmeans.CentroidSet, enc int64, pq *quantize.PQ8Codec) (*StableMemberCodec, error) {
	if cs == nil {
		return nil, fmt.Errorf("vecindex: centroid set is nil")
	}
	codec := &StableMemberCodec{spec: spec, centroid: cs, enc: enc, pq: pq}
	if err := codec.Validate(); err != nil {
		return nil, err
	}
	return codec, nil
}

func DecodeStableMemberCodecBlob(spec IVFSpec, cs *kmeans.CentroidSet, enc int64, blob []byte) (*StableMemberCodec, error) {
	if enc != MemberEncodingResidualPQ8 {
		return NewStableMemberCodec(spec, cs, enc, nil)
	}
	if len(blob) == 0 {
		return nil, fmt.Errorf("vecindex: missing PQ codec blob")
	}
	raw, err := getDecoder().DecodeAll(blob, nil)
	if err != nil {
		return nil, fmt.Errorf("stable codec blob: zstd decompress: %w", err)
	}
	var msg stableMemberCodecMsg
	if err := msgpack.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("stable codec blob: decode: %w", err)
	}
	if msg.Version != 1 || msg.Encoding != MemberEncodingResidualPQ8 || msg.PQ8 == nil {
		return nil, fmt.Errorf("stable codec blob: invalid PQ codec metadata")
	}
	return NewStableMemberCodec(spec, cs, msg.Encoding, msg.PQ8)
}

func EncodeStableMemberCodecBlob(codec *StableMemberCodec) ([]byte, error) {
	if codec == nil || codec.enc != MemberEncodingResidualPQ8 {
		return nil, nil
	}
	msg := stableMemberCodecMsg{Version: 1, Encoding: codec.enc, PQ8: codec.pq}
	raw, err := msgpack.Marshal(&msg)
	if err != nil {
		return nil, fmt.Errorf("stable codec blob: encode msgpack: %w", err)
	}
	blob, err := encodeMetadataBlob(raw)
	if err != nil {
		return nil, fmt.Errorf("stable codec blob: zstd compress: %w", err)
	}
	return blob, nil
}

func (c *StableMemberCodec) Encoding() int64 {
	if c == nil {
		return 0
	}
	return c.enc
}

func (c *StableMemberCodec) EncodedSize() int {
	if c == nil {
		return 0
	}
	switch c.enc {
	case MemberEncodingRawPreparedF32:
		return c.spec.InternalDim() * 4
	case MemberEncodingResidualInt8:
		return quantize.EncodedResidualSize(c.spec.InternalMetric(), c.spec.InternalDim(), MemberResidualBlockSize)
	case MemberEncodingResidualPQ8:
		return c.pq.EncodedSize(c.spec.InternalMetric())
	default:
		return 0
	}
}

func (c *StableMemberCodec) WithCentroids(cs *kmeans.CentroidSet) (*StableMemberCodec, error) {
	if c == nil {
		return nil, fmt.Errorf("vecindex: stable codec is nil")
	}
	return NewStableMemberCodec(c.spec, cs, c.enc, c.pq)
}

func (c *StableMemberCodec) Validate() error {
	if c == nil {
		return fmt.Errorf("vecindex: stable codec is nil")
	}
	if c.centroid == nil {
		return fmt.Errorf("vecindex: stable codec centroid set is nil")
	}
	switch c.enc {
	case MemberEncodingRawPreparedF32, MemberEncodingResidualInt8:
		return nil
	case MemberEncodingResidualPQ8:
		if c.pq == nil {
			return fmt.Errorf("vecindex: PQ stable codec metadata is nil")
		}
		if err := c.pq.Validate(); err != nil {
			return err
		}
		if c.pq.Dim != c.spec.InternalDim() {
			return fmt.Errorf("vecindex: PQ dim=%d want=%d", c.pq.Dim, c.spec.InternalDim())
		}
		return nil
	default:
		return fmt.Errorf("vecindex: unknown stable encoding %d", c.enc)
	}
}

func (c *StableMemberCodec) Encode(clusterID int64, prepared []byte) (int64, []byte, error) {
	if c == nil {
		return 0, nil, fmt.Errorf("vecindex: stable codec is nil")
	}
	if clusterID <= 0 {
		return MemberEncodingRawPreparedF32, append([]byte(nil), prepared...), nil
	}
	if len(prepared) != c.spec.InternalDim()*4 {
		return 0, nil, fmt.Errorf("vecindex: prepared blob length %d does not match internal dim %d", len(prepared), c.spec.InternalDim())
	}
	centroid, err := c.centroid.GetReadOnly(uint32(clusterID - 1))
	if err != nil {
		return 0, nil, err
	}
	vec := metric.BytesToFloat32(prepared)
	switch c.enc {
	case MemberEncodingRawPreparedF32:
		return c.enc, append([]byte(nil), prepared...), nil
	case MemberEncodingResidualInt8:
		blob, err := quantize.EncodeResidualInt8(c.spec.InternalMetric(), vec, centroid, MemberResidualBlockSize)
		if err != nil {
			return 0, nil, err
		}
		return c.enc, blob, nil
	case MemberEncodingResidualPQ8:
		blob, err := c.pq.EncodeResidual(c.spec.InternalMetric(), vec, centroid)
		if err != nil {
			return 0, nil, err
		}
		return c.enc, blob, nil
	default:
		return 0, nil, fmt.Errorf("vecindex: unsupported stable encoding %d", c.enc)
	}
}

func (c *StableMemberCodec) DecodePrepared(clusterID int64, vecBytes []byte) ([]float32, error) {
	if c == nil {
		return nil, fmt.Errorf("vecindex: stable codec is nil")
	}
	switch c.enc {
	case MemberEncodingRawPreparedF32:
		return append([]float32(nil), metric.BytesToFloat32(vecBytes)...), nil
	case MemberEncodingResidualInt8:
		if c.centroid == nil || clusterID <= 0 || int(clusterID) > c.centroid.Len() {
			return nil, fmt.Errorf("vecindex: missing centroid for cluster %d", clusterID)
		}
		centroid, err := c.centroid.GetReadOnly(uint32(clusterID - 1))
		if err != nil {
			return nil, err
		}
		decoded, _, err := quantize.DecodeResidualInt8(c.spec.InternalMetric(), centroid, vecBytes, MemberResidualBlockSize, nil)
		if err != nil {
			return nil, err
		}
		return decoded, nil
	case MemberEncodingResidualPQ8:
		if c.centroid == nil || clusterID <= 0 || int(clusterID) > c.centroid.Len() {
			return nil, fmt.Errorf("vecindex: missing centroid for cluster %d", clusterID)
		}
		centroid, err := c.centroid.GetReadOnly(uint32(clusterID - 1))
		if err != nil {
			return nil, err
		}
		decoded, _, err := c.pq.DecodeResidual(centroid, vecBytes, c.spec.InternalMetric(), nil)
		if err != nil {
			return nil, err
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("vecindex: unsupported stable encoding %d", c.enc)
	}
}

func EncodeStableMember(spec IVFSpec, cs *kmeans.CentroidSet, clusterID int64, prepared []byte) (int64, []byte, error) {
	codec, err := NewStableMemberCodec(spec, cs, MemberEncodingResidualInt8, nil)
	if err != nil {
		return 0, nil, err
	}
	return codec.Encode(clusterID, prepared)
}

func NewStableMemberScorer(spec IVFSpec, cs *kmeans.CentroidSet, query []float32, queryNorm2 float32, clusterID int64, enc int64) (*StableMemberScorer, error) {
	switch enc {
	case MemberEncodingRawPreparedF32:
		return &StableMemberScorer{
			enc:       enc,
			rawMetric: spec.InternalMetric(),
			query:     query,
		}, nil
	case MemberEncodingResidualInt8:
		if clusterID <= 0 {
			return nil, fmt.Errorf("vecindex: residual encoding requires stable cluster id, got %d", clusterID)
		}
		if cs == nil {
			return nil, fmt.Errorf("vecindex: centroid set is nil")
		}
		centroid, err := cs.GetReadOnly(uint32(clusterID - 1))
		if err != nil {
			return nil, err
		}
		residual, err := quantize.NewResidualInt8Scorer(spec.InternalMetric(), query, queryNorm2, centroid, MemberResidualBlockSize)
		if err != nil {
			return nil, err
		}
		return &StableMemberScorer{
			enc:      enc,
			residual: residual,
		}, nil
	case MemberEncodingResidualPQ8:
		return nil, fmt.Errorf("vecindex: PQ scorer requires a stable member codec")
	default:
		return nil, fmt.Errorf("vecindex: unknown member encoding %d", enc)
	}
}

func NewStableMemberScorerWithCodec(codec *StableMemberCodec, query []float32, queryNorm2 float32, clusterID int64) (*StableMemberScorer, error) {
	queryScorer, err := NewStableMemberQueryScorerWithCodec(codec, query, queryNorm2)
	if err != nil {
		return nil, err
	}
	return queryScorer.ClusterScorer(clusterID)
}

func NewStableMemberQueryScorerWithCodec(codec *StableMemberCodec, query []float32, queryNorm2 float32) (*StableMemberQueryScorer, error) {
	if codec == nil {
		return nil, fmt.Errorf("vecindex: stable codec is nil")
	}
	switch codec.enc {
	case MemberEncodingRawPreparedF32, MemberEncodingResidualInt8:
		return &StableMemberQueryScorer{codec: codec, query: query, queryNorm2: queryNorm2}, nil
	case MemberEncodingResidualPQ8:
		pq, err := quantize.NewPQ8QueryScorer(codec.spec.InternalMetric(), query, queryNorm2, codec.pq)
		if err != nil {
			return nil, err
		}
		return &StableMemberQueryScorer{codec: codec, query: query, queryNorm2: queryNorm2, pq: pq}, nil
	default:
		return nil, fmt.Errorf("vecindex: unknown member encoding %d", codec.enc)
	}
}

func (q *StableMemberQueryScorer) ClusterScorer(clusterID int64) (*StableMemberScorer, error) {
	if q == nil || q.codec == nil {
		return nil, fmt.Errorf("vecindex: stable member query scorer is nil")
	}
	switch q.codec.enc {
	case MemberEncodingRawPreparedF32, MemberEncodingResidualInt8:
		return NewStableMemberScorer(q.codec.spec, q.codec.centroid, q.query, q.queryNorm2, clusterID, q.codec.enc)
	case MemberEncodingResidualPQ8:
		if clusterID <= 0 {
			return nil, fmt.Errorf("vecindex: PQ encoding requires stable cluster id, got %d", clusterID)
		}
		centroid, err := q.codec.centroid.GetReadOnly(uint32(clusterID - 1))
		if err != nil {
			return nil, err
		}
		pq, err := q.pq.ClusterScorer(q.query, centroid)
		if err != nil {
			return nil, err
		}
		return &StableMemberScorer{enc: q.codec.enc, pq: pq}, nil
	default:
		return nil, fmt.Errorf("vecindex: unknown member encoding %d", q.codec.enc)
	}
}

func (s *StableMemberScorer) Score(vec []byte) (float32, error) {
	if s == nil {
		return 0, fmt.Errorf("vecindex: stable member scorer is nil")
	}
	switch s.enc {
	case MemberEncodingRawPreparedF32:
		if len(vec) != len(s.query)*4 {
			return 0, fmt.Errorf("vecindex: raw vec length %d does not match query dim %d", len(vec), len(s.query))
		}
		switch s.rawMetric {
		case metric.MetricCosine:
			return metric.CosineDistanceUnitFromBytes(s.query, vec), nil
		case metric.MetricL2:
			return metric.L2SquaredFromBytes(s.query, vec), nil
		default:
			return 0, fmt.Errorf("vecindex: unsupported internal metric %d", s.rawMetric)
		}
	case MemberEncodingResidualInt8:
		return s.residual.Distance(vec)
	case MemberEncodingResidualPQ8:
		return s.pq.Distance(vec)
	default:
		return 0, fmt.Errorf("vecindex: unknown member encoding %d", s.enc)
	}
}

func (s *StableMemberScorer) ScoreSpan(rows []byte, entrySize int, out []float32) error {
	if s == nil {
		return fmt.Errorf("vecindex: stable member scorer is nil")
	}
	switch s.enc {
	case MemberEncodingResidualInt8:
		return s.residual.ScoreSpan(rows, entrySize, out)
	case MemberEncodingResidualPQ8:
		return s.pq.ScoreSpan(rows, entrySize, out)
	default:
		return fmt.Errorf("vecindex: span scoring unsupported for encoding %d", s.enc)
	}
}

func ScoreEncodedMember(spec IVFSpec, cs *kmeans.CentroidSet, query []float32, queryNorm2 float32, clusterID int64, enc int64, vec []byte) (float32, error) {
	scorer, err := NewStableMemberScorer(spec, cs, query, queryNorm2, clusterID, enc)
	if err != nil {
		return 0, err
	}
	return scorer.Score(vec)
}
