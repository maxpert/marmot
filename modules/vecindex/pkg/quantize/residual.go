package quantize

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

const DefaultResidualBlockSize = 64

type ResidualInt8Scorer struct {
	rankMetric   metric.Metric
	queryNorm2   float32
	dim          int
	blocks       int
	blockSize    int
	expectedSize int
	baseDot      float32
	queryScales  []float32
	queryCodes   []int8
}

func ResidualBlockCount(dim, blockSize int) int {
	if dim <= 0 || blockSize <= 0 {
		return 0
	}
	return (dim + blockSize - 1) / blockSize
}

func EncodedResidualSize(rankMetric metric.Metric, dim, blockSize int) int {
	header := 0
	if rankMetric == metric.MetricL2 {
		header = 4 // exact ||x||^2
	}
	return header + ResidualBlockCount(dim, blockSize)*2 + dim
}

func EncodeResidualInt8(rankMetric metric.Metric, vec, centroid []float32, blockSize int) ([]byte, error) {
	if len(vec) == 0 {
		return nil, fmt.Errorf("quantize: empty vector")
	}
	if len(vec) != len(centroid) {
		return nil, fmt.Errorf("quantize: dimension mismatch: vec=%d centroid=%d", len(vec), len(centroid))
	}
	if blockSize <= 0 {
		return nil, fmt.Errorf("quantize: invalid block size %d", blockSize)
	}

	dim := len(vec)
	blocks := ResidualBlockCount(dim, blockSize)
	out := make([]byte, EncodedResidualSize(rankMetric, dim, blockSize))
	off := 0
	if rankMetric == metric.MetricL2 {
		binary.LittleEndian.PutUint32(out[:4], math.Float32bits(metric.Norm2(vec)))
		off = 4
	}
	scaleOff := off
	codeOff := off + blocks*2

	for block := 0; block < blocks; block++ {
		start := block * blockSize
		end := start + blockSize
		if end > dim {
			end = dim
		}

		maxAbs := float32(0)
		for i := start; i < end; i++ {
			residual := vec[i] - centroid[i]
			if residual < 0 {
				residual = -residual
			}
			if residual > maxAbs {
				maxAbs = residual
			}
		}

		scale := float32(0)
		if maxAbs > 0 {
			scale = maxAbs / 127.0
		}
		binary.LittleEndian.PutUint16(out[scaleOff+block*2:], encodeFloat16(scale))
		for i := start; i < end; i++ {
			code := int8(0)
			if scale > 0 {
				q := int(math.Round(float64((vec[i] - centroid[i]) / scale)))
				if q > 127 {
					q = 127
				} else if q < -127 {
					q = -127
				}
				code = int8(q)
			}
			out[codeOff+i] = byte(code)
		}
	}

	return out, nil
}

func DecodeResidualInt8(rankMetric metric.Metric, centroid []float32, blob []byte, blockSize int, dst []float32) ([]float32, float32, error) {
	if len(centroid) == 0 {
		return nil, 0, fmt.Errorf("quantize: empty centroid")
	}
	if blockSize <= 0 {
		return nil, 0, fmt.Errorf("quantize: invalid block size %d", blockSize)
	}
	dim := len(centroid)
	if len(blob) != EncodedResidualSize(rankMetric, dim, blockSize) {
		return nil, 0, fmt.Errorf("quantize: blob size mismatch: got=%d want=%d", len(blob), EncodedResidualSize(rankMetric, dim, blockSize))
	}
	if cap(dst) < dim {
		dst = make([]float32, dim)
	} else {
		dst = dst[:dim]
	}

	norm2 := float32(0)
	off := 0
	if rankMetric == metric.MetricL2 {
		norm2 = math.Float32frombits(binary.LittleEndian.Uint32(blob[:4]))
		off = 4
	}
	blocks := ResidualBlockCount(dim, blockSize)
	scaleOff := off
	codeOff := off + blocks*2

	for block := 0; block < blocks; block++ {
		start := block * blockSize
		end := start + blockSize
		if end > dim {
			end = dim
		}
		scale := decodeFloat16(binary.LittleEndian.Uint16(blob[scaleOff+block*2:]))
		for i := start; i < end; i++ {
			code := int8(blob[codeOff+i])
			dst[i] = centroid[i] + float32(code)*scale
		}
	}

	return dst, norm2, nil
}

func AccumulateResidualInt8Stats(rankMetric metric.Metric, dim, blockSize int, blob []byte, minResidual, maxResidual []float32) (float32, bool, error) {
	if dim <= 0 {
		return 0, false, fmt.Errorf("quantize: invalid dim %d", dim)
	}
	if blockSize <= 0 {
		return 0, false, fmt.Errorf("quantize: invalid block size %d", blockSize)
	}
	if len(minResidual) != dim || len(maxResidual) != dim {
		return 0, false, fmt.Errorf("quantize: residual stats dim mismatch")
	}
	if len(blob) != EncodedResidualSize(rankMetric, dim, blockSize) {
		return 0, false, fmt.Errorf("quantize: blob size mismatch: got=%d want=%d", len(blob), EncodedResidualSize(rankMetric, dim, blockSize))
	}
	norm2 := float32(0)
	hasNorm := false
	off := 0
	if rankMetric == metric.MetricL2 {
		norm2 = math.Float32frombits(binary.LittleEndian.Uint32(blob[:4]))
		hasNorm = true
		off = 4
	}
	blocks := ResidualBlockCount(dim, blockSize)
	scaleOff := off
	codeOff := off + blocks*2
	for block := 0; block < blocks; block++ {
		start := block * blockSize
		end := start + blockSize
		if end > dim {
			end = dim
		}
		scale := decodeFloat16(binary.LittleEndian.Uint16(blob[scaleOff+block*2:]))
		for i := start; i < end; i++ {
			value := float32(int8(blob[codeOff+i])) * scale
			if value < minResidual[i] {
				minResidual[i] = value
			}
			if value > maxResidual[i] {
				maxResidual[i] = value
			}
		}
	}
	return norm2, hasNorm, nil
}

func QuantizeQueryInt8(query []float32, blockSize int) ([]int8, []float32, error) {
	if len(query) == 0 {
		return nil, nil, fmt.Errorf("quantize: empty query")
	}
	if blockSize <= 0 {
		return nil, nil, fmt.Errorf("quantize: invalid block size %d", blockSize)
	}
	blocks := ResidualBlockCount(len(query), blockSize)
	codes := make([]int8, len(query))
	scales := make([]float32, blocks)
	for block := 0; block < blocks; block++ {
		start := block * blockSize
		end := start + blockSize
		if end > len(query) {
			end = len(query)
		}
		maxAbs := float32(0)
		for i := start; i < end; i++ {
			value := query[i]
			if value < 0 {
				value = -value
			}
			if value > maxAbs {
				maxAbs = value
			}
		}
		if maxAbs == 0 {
			continue
		}
		scale := maxAbs / 127.0
		scales[block] = scale
		for i := start; i < end; i++ {
			q := int(math.Round(float64(query[i] / scale)))
			if q > 127 {
				q = 127
			} else if q < -127 {
				q = -127
			}
			codes[i] = int8(q)
		}
	}
	return codes, scales, nil
}

func NewResidualInt8Scorer(rankMetric metric.Metric, query []float32, queryNorm2 float32, centroid []float32, blockSize int) (*ResidualInt8Scorer, error) {
	if len(query) != len(centroid) {
		return nil, fmt.Errorf("quantize: query/centroid dimension mismatch: query=%d centroid=%d", len(query), len(centroid))
	}
	if blockSize <= 0 {
		return nil, fmt.Errorf("quantize: invalid block size %d", blockSize)
	}
	queryCodes, queryScales, err := QuantizeQueryInt8(query, blockSize)
	if err != nil {
		return nil, err
	}
	return &ResidualInt8Scorer{
		rankMetric:   rankMetric,
		queryNorm2:   queryNorm2,
		dim:          len(query),
		blocks:       ResidualBlockCount(len(query), blockSize),
		blockSize:    blockSize,
		expectedSize: EncodedResidualSize(rankMetric, len(query), blockSize),
		baseDot:      metric.DotProduct(query, centroid),
		queryScales:  queryScales,
		queryCodes:   queryCodes,
	}, nil
}

func (s *ResidualInt8Scorer) Distance(blob []byte) (float32, error) {
	if s == nil {
		return 0, fmt.Errorf("quantize: residual scorer is nil")
	}
	if len(blob) != s.expectedSize {
		return 0, fmt.Errorf("quantize: blob size mismatch: got=%d want=%d", len(blob), s.expectedSize)
	}
	return s.distanceBlob(blob)
}

func (s *ResidualInt8Scorer) ScoreSpan(rows []byte, entrySize int, out []float32) error {
	if s == nil {
		return fmt.Errorf("quantize: residual scorer is nil")
	}
	if entrySize <= 8 {
		return fmt.Errorf("quantize: invalid entry size %d", entrySize)
	}
	if len(out) == 0 {
		return nil
	}
	if len(rows) < len(out)*entrySize {
		return fmt.Errorf("quantize: span buffer too small: got=%d need=%d", len(rows), len(out)*entrySize)
	}
	if scoreResidualInt8SpanSIMD(s, rows, entrySize, out) {
		return nil
	}
	cursor := 0
	for i := range out {
		dist, err := s.distanceBlob(rows[cursor+8 : cursor+entrySize])
		if err != nil {
			return err
		}
		out[i] = dist
		cursor += entrySize
	}
	return nil
}

func (s *ResidualInt8Scorer) distanceBlob(blob []byte) (float32, error) {
	norm2 := float32(0)
	off := 0
	if s.rankMetric == metric.MetricL2 {
		norm2 = math.Float32frombits(binary.LittleEndian.Uint32(blob[:4]))
		off = 4
	}
	scaleOff := off
	codeOff := off + s.blocks*2

	residualDot := float32(0)
	for block := 0; block < s.blocks; block++ {
		start := block * s.blockSize
		end := start + s.blockSize
		if end > s.dim {
			end = s.dim
		}
		residualScale := decodeFloat16(binary.LittleEndian.Uint16(blob[scaleOff+block*2:]))
		queryScale := s.queryScales[block]
		if residualScale == 0 || queryScale == 0 {
			continue
		}
		dot := dotInt8(s.queryCodes[start:end], blob[codeOff+start:codeOff+end])
		residualDot += queryScale * residualScale * float32(dot)
	}

	dot := s.baseDot + residualDot
	switch s.rankMetric {
	case metric.MetricCosine:
		return 1 - dot, nil
	case metric.MetricL2:
		return s.queryNorm2 + norm2 - 2*dot, nil
	default:
		return 0, fmt.Errorf("quantize: unsupported rank metric %d", s.rankMetric)
	}
}

func DistanceFromResidualInt8(rankMetric metric.Metric, query []float32, queryNorm2 float32, centroid []float32, blob []byte, blockSize int) (float32, error) {
	scorer, err := NewResidualInt8Scorer(rankMetric, query, queryNorm2, centroid, blockSize)
	if err != nil {
		return 0, err
	}
	return scorer.Distance(blob)
}

// IEEE 754 binary16 conversions adapted for deterministic internal use.
func encodeFloat16(f float32) uint16 {
	bits := math.Float32bits(f)
	sign := uint16((bits >> 16) & 0x8000)
	exp := int((bits >> 23) & 0xff)
	mant := bits & 0x7fffff

	switch exp {
	case 0xff:
		if mant != 0 {
			return sign | 0x7e00
		}
		return sign | 0x7c00
	case 0:
		return sign
	}

	exp = exp - 127 + 15
	if exp >= 0x1f {
		return sign | 0x7c00
	}
	if exp <= 0 {
		if exp < -10 {
			return sign
		}
		mant |= 0x800000
		shift := uint32(14 - exp)
		rounded := mant + (1 << (shift - 1))
		return sign | uint16(rounded>>shift)
	}

	mant += 0x1000
	if mant&0x800000 != 0 {
		mant = 0
		exp++
		if exp >= 0x1f {
			return sign | 0x7c00
		}
	}
	return sign | uint16(exp<<10) | uint16(mant>>13)
}

func decodeFloat16(h uint16) float32 {
	sign := uint32(h&0x8000) << 16
	exp := (h >> 10) & 0x1f
	mant := uint32(h & 0x03ff)

	switch exp {
	case 0:
		if mant == 0 {
			return math.Float32frombits(sign)
		}
		exp32 := uint32(127 - 15 + 1)
		for mant&0x0400 == 0 {
			mant <<= 1
			exp32--
		}
		mant &= 0x03ff
		return math.Float32frombits(sign | (exp32 << 23) | (mant << 13))
	case 0x1f:
		return math.Float32frombits(sign | 0x7f800000 | (mant << 13))
	default:
		exp32 := uint32(exp) - 15 + 127
		return math.Float32frombits(sign | (exp32 << 23) | (mant << 13))
	}
}
