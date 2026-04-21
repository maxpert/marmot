package vecindex

import (
	"fmt"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/quantize"
)

const (
	MemberEncodingRawPreparedF32 int64 = iota
	MemberEncodingResidualInt8
)

const MemberResidualBlockSize = quantize.DefaultResidualBlockSize

func StableMemberEncodingSpec(spec IVFSpec) (int64, int) {
	return MemberEncodingResidualInt8, quantize.EncodedResidualSize(spec.InternalMetric(), spec.InternalDim(), MemberResidualBlockSize)
}

type StableMemberScorer struct {
	enc       int64
	rawMetric metric.Metric
	query     []float32
	residual  *quantize.ResidualInt8Scorer
}

func EncodeStableMember(spec IVFSpec, cs *kmeans.CentroidSet, clusterID int64, prepared []byte) (int64, []byte, error) {
	if clusterID <= 0 {
		return MemberEncodingRawPreparedF32, append([]byte(nil), prepared...), nil
	}
	if cs == nil {
		return 0, nil, fmt.Errorf("vecindex: centroid set is nil")
	}
	if len(prepared) != spec.InternalDim()*4 {
		return 0, nil, fmt.Errorf("vecindex: prepared blob length %d does not match internal dim %d", len(prepared), spec.InternalDim())
	}
	centroid, err := cs.GetReadOnly(uint32(clusterID - 1))
	if err != nil {
		return 0, nil, err
	}
	blob, err := quantize.EncodeResidualInt8(spec.InternalMetric(), metric.BytesToFloat32(prepared), centroid, MemberResidualBlockSize)
	if err != nil {
		return 0, nil, err
	}
	return MemberEncodingResidualInt8, blob, nil
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
	default:
		return nil, fmt.Errorf("vecindex: unknown member encoding %d", enc)
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
