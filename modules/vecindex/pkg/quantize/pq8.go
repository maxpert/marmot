package quantize

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

const (
	DefaultPQ8Subquantizers = 128
	pq8CodebookSize         = 256
	defaultPQ8MaxIter       = 8
)

type PQ8Options struct {
	M       int
	MaxIter int
	Seed    uint64
}

type PQ8Codec struct {
	Dim       int       `msgpack:"dim"`
	M         int       `msgpack:"m"`
	Offsets   []int     `msgpack:"offsets"`
	Codebooks []float32 `msgpack:"codebooks"`

	offsetOnce      sync.Once
	codebookOffsets []int
}

type PQ8QueryScorer struct {
	rankMetric metric.Metric
	queryNorm2 float32
	m          int
	codeOffset int
	lut        []float32
}

type PQ8Scorer struct {
	rankMetric metric.Metric
	queryNorm2 float32
	m          int
	codeOffset int
	baseDot    float32
	lut        []float32
}

func TrainPQ8(residuals [][]float32, dim int, opts PQ8Options) (*PQ8Codec, error) {
	if dim <= 0 {
		return nil, fmt.Errorf("quantize: invalid PQ dim %d", dim)
	}
	if len(residuals) == 0 {
		return nil, fmt.Errorf("quantize: PQ training requires vectors")
	}
	for i, v := range residuals {
		if len(v) != dim {
			return nil, fmt.Errorf("quantize: PQ training dim mismatch at %d: got=%d want=%d", i, len(v), dim)
		}
	}
	if opts.M <= 0 {
		opts.M = DefaultPQ8Subquantizers
	}
	if opts.M > dim {
		opts.M = dim
	}
	if opts.MaxIter <= 0 {
		opts.MaxIter = defaultPQ8MaxIter
	}
	offsets := pqOffsets(dim, opts.M)
	codebookOffsets := pqCodebookOffsets(offsets)
	codebookLen := codebookOffsets[len(codebookOffsets)-1]
	codebooks := make([]float32, codebookLen)

	workers := runtime.GOMAXPROCS(0)
	if workers > opts.M {
		workers = opts.M
	}
	if workers < 1 {
		workers = 1
	}

	jobs := make(chan int)
	errCh := make(chan error, opts.M)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sub := range jobs {
				start, end := offsets[sub], offsets[sub+1]
				dst := codebooks[codebookOffsets[sub]:codebookOffsets[sub+1]]
				if err := trainPQ8Subspace(residuals, start, end, opts.MaxIter, opts.Seed^uint64(sub+1)*0x9e3779b97f4a7c15, dst); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	for sub := 0; sub < opts.M; sub++ {
		jobs <- sub
	}
	close(jobs)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}
	return &PQ8Codec{Dim: dim, M: opts.M, Offsets: offsets, Codebooks: codebooks, codebookOffsets: codebookOffsets}, nil
}

func (c *PQ8Codec) Validate() error {
	if c == nil {
		return fmt.Errorf("quantize: PQ codec is nil")
	}
	if c.Dim <= 0 || c.M <= 0 || c.M > c.Dim {
		return fmt.Errorf("quantize: invalid PQ shape dim=%d m=%d", c.Dim, c.M)
	}
	if len(c.Offsets) != c.M+1 || c.Offsets[0] != 0 || c.Offsets[len(c.Offsets)-1] != c.Dim {
		return fmt.Errorf("quantize: invalid PQ offsets")
	}
	want := 0
	for m := 0; m < c.M; m++ {
		if c.Offsets[m+1] <= c.Offsets[m] {
			return fmt.Errorf("quantize: invalid PQ subspace %d", m)
		}
		want += pq8CodebookSize * (c.Offsets[m+1] - c.Offsets[m])
	}
	if len(c.Codebooks) != want {
		return fmt.Errorf("quantize: PQ codebook length mismatch: got=%d want=%d", len(c.Codebooks), want)
	}
	return nil
}

func (c *PQ8Codec) EncodedSize(rankMetric metric.Metric) int {
	if c == nil {
		return 0
	}
	size := c.M
	if rankMetric == metric.MetricL2 {
		size += 4
	}
	return size
}

func (c *PQ8Codec) EncodeResidual(rankMetric metric.Metric, vec, centroid []float32) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if len(vec) != c.Dim || len(centroid) != c.Dim {
		return nil, fmt.Errorf("quantize: PQ encode dim mismatch")
	}
	out := make([]byte, c.EncodedSize(rankMetric))
	off := 0
	if rankMetric == metric.MetricL2 {
		binary.LittleEndian.PutUint32(out[:4], math.Float32bits(metric.Norm2(vec)))
		off = 4
	}
	for sub := 0; sub < c.M; sub++ {
		out[off+sub] = byte(c.nearestCodeForResidual(sub, vec, centroid))
	}
	return out, nil
}

func (c *PQ8Codec) DecodeResidual(centroid []float32, blob []byte, rankMetric metric.Metric, dst []float32) ([]float32, float32, error) {
	if err := c.Validate(); err != nil {
		return nil, 0, err
	}
	if len(centroid) != c.Dim {
		return nil, 0, fmt.Errorf("quantize: PQ decode centroid dim mismatch")
	}
	if len(blob) != c.EncodedSize(rankMetric) {
		return nil, 0, fmt.Errorf("quantize: PQ blob size mismatch: got=%d want=%d", len(blob), c.EncodedSize(rankMetric))
	}
	if cap(dst) < c.Dim {
		dst = make([]float32, c.Dim)
	} else {
		dst = dst[:c.Dim]
	}
	norm2 := float32(0)
	off := 0
	if rankMetric == metric.MetricL2 {
		norm2 = math.Float32frombits(binary.LittleEndian.Uint32(blob[:4]))
		off = 4
	}
	copy(dst, centroid)
	for sub := 0; sub < c.M; sub++ {
		code := int(blob[off+sub])
		start, end := c.Offsets[sub], c.Offsets[sub+1]
		cb := c.codeword(sub, code)
		for d := start; d < end; d++ {
			dst[d] += cb[d-start]
		}
	}
	return dst, norm2, nil
}

func NewPQ8Scorer(rankMetric metric.Metric, query []float32, queryNorm2 float32, centroid []float32, codec *PQ8Codec) (*PQ8Scorer, error) {
	queryScorer, err := NewPQ8QueryScorer(rankMetric, query, queryNorm2, codec)
	if err != nil {
		return nil, err
	}
	return queryScorer.ClusterScorer(query, centroid)
}

func NewPQ8QueryScorer(rankMetric metric.Metric, query []float32, queryNorm2 float32, codec *PQ8Codec) (*PQ8QueryScorer, error) {
	if err := codec.Validate(); err != nil {
		return nil, err
	}
	if len(query) != codec.Dim {
		return nil, fmt.Errorf("quantize: PQ scorer dim mismatch")
	}
	lut := make([]float32, codec.M*pq8CodebookSize)
	codebookOffsets := codec.codebookBases()
	for sub := 0; sub < codec.M; sub++ {
		start, end := codec.Offsets[sub], codec.Offsets[sub+1]
		width := end - start
		codebookStart := codebookOffsets[sub]
		lutStart := sub * pq8CodebookSize
		for code := 0; code < pq8CodebookSize; code++ {
			cb := codec.Codebooks[codebookStart+code*width : codebookStart+(code+1)*width]
			var dot float32
			for d := start; d < end; d++ {
				dot += query[d] * cb[d-start]
			}
			lut[lutStart+code] = dot
		}
	}
	codeOffset := 0
	if rankMetric == metric.MetricL2 {
		codeOffset = 4
	}
	return &PQ8QueryScorer{
		rankMetric: rankMetric,
		queryNorm2: queryNorm2,
		m:          codec.M,
		codeOffset: codeOffset,
		lut:        lut,
	}, nil
}

func (q *PQ8QueryScorer) ClusterScorer(query, centroid []float32) (*PQ8Scorer, error) {
	if q == nil {
		return nil, fmt.Errorf("quantize: PQ query scorer is nil")
	}
	if len(query) != len(centroid) {
		return nil, fmt.Errorf("quantize: PQ scorer dim mismatch")
	}
	return &PQ8Scorer{
		rankMetric: q.rankMetric,
		queryNorm2: q.queryNorm2,
		m:          q.m,
		codeOffset: q.codeOffset,
		baseDot:    metric.DotProduct(query, centroid),
		lut:        q.lut,
	}, nil
}

func (s *PQ8Scorer) Distance(blob []byte) (float32, error) {
	if s == nil {
		return 0, fmt.Errorf("quantize: PQ scorer is nil")
	}
	if len(blob) != s.codeOffset+s.m {
		return 0, fmt.Errorf("quantize: PQ blob size mismatch: got=%d want=%d", len(blob), s.codeOffset+s.m)
	}
	dot := s.baseDot
	lutOffset := 0
	for sub := 0; sub < s.m; sub++ {
		dot += s.lut[lutOffset+int(blob[s.codeOffset+sub])]
		lutOffset += pq8CodebookSize
	}
	switch s.rankMetric {
	case metric.MetricCosine:
		return 1 - dot, nil
	case metric.MetricL2:
		norm2 := math.Float32frombits(binary.LittleEndian.Uint32(blob[:4]))
		return s.queryNorm2 + norm2 - 2*dot, nil
	default:
		return 0, fmt.Errorf("quantize: unsupported PQ rank metric %d", s.rankMetric)
	}
}

func (s *PQ8Scorer) ScoreSpan(rows []byte, entrySize int, out []float32) error {
	if s == nil {
		return fmt.Errorf("quantize: PQ scorer is nil")
	}
	if entrySize <= 8 {
		return fmt.Errorf("quantize: invalid entry size %d", entrySize)
	}
	if len(rows) < len(out)*entrySize {
		return fmt.Errorf("quantize: span buffer too small: got=%d need=%d", len(rows), len(out)*entrySize)
	}
	if entrySize < 8+s.codeOffset+s.m {
		return fmt.Errorf("quantize: invalid PQ entry size %d", entrySize)
	}
	cursor := 0
	switch s.rankMetric {
	case metric.MetricCosine:
		for i := range out {
			codeCursor := cursor + 8 + s.codeOffset
			dot := s.baseDot
			lutOffset := 0
			for sub := 0; sub < s.m; sub++ {
				dot += s.lut[lutOffset+int(rows[codeCursor+sub])]
				lutOffset += pq8CodebookSize
			}
			out[i] = 1 - dot
			cursor += entrySize
		}
	case metric.MetricL2:
		for i := range out {
			payloadCursor := cursor + 8
			norm2 := math.Float32frombits(binary.LittleEndian.Uint32(rows[payloadCursor : payloadCursor+4]))
			codeCursor := payloadCursor + s.codeOffset
			dot := s.baseDot
			lutOffset := 0
			for sub := 0; sub < s.m; sub++ {
				dot += s.lut[lutOffset+int(rows[codeCursor+sub])]
				lutOffset += pq8CodebookSize
			}
			out[i] = s.queryNorm2 + norm2 - 2*dot
			cursor += entrySize
		}
	default:
		return fmt.Errorf("quantize: unsupported PQ rank metric %d", s.rankMetric)
	}
	return nil
}

func pqOffsets(dim, m int) []int {
	offsets := make([]int, m+1)
	base := dim / m
	rem := dim % m
	cursor := 0
	for i := 0; i < m; i++ {
		offsets[i] = cursor
		width := base
		if i < rem {
			width++
		}
		cursor += width
	}
	offsets[m] = dim
	return offsets
}

func pqCodebookOffset(offsets []int, sub int) int {
	var off int
	for i := 0; i < sub; i++ {
		off += pq8CodebookSize * (offsets[i+1] - offsets[i])
	}
	return off
}

func pqCodebookOffsets(offsets []int) []int {
	out := make([]int, len(offsets))
	for i := 0; i < len(offsets)-1; i++ {
		out[i+1] = out[i] + pq8CodebookSize*(offsets[i+1]-offsets[i])
	}
	return out
}

func (c *PQ8Codec) codebookBases() []int {
	c.offsetOnce.Do(func() {
		c.codebookOffsets = pqCodebookOffsets(c.Offsets)
	})
	return c.codebookOffsets
}

func (c *PQ8Codec) codeword(sub, code int) []float32 {
	start := c.codebookBases()[sub]
	width := c.Offsets[sub+1] - c.Offsets[sub]
	return c.Codebooks[start+code*width : start+(code+1)*width]
}

func (c *PQ8Codec) nearestCodeForResidual(sub int, vec, centroid []float32) int {
	start, end := c.Offsets[sub], c.Offsets[sub+1]
	width := end - start
	codebookStart := c.codebookBases()[sub]
	best := 0
	bestDist := float32(math.MaxFloat32)
	for code := 0; code < pq8CodebookSize; code++ {
		cb := c.Codebooks[codebookStart+code*width : codebookStart+(code+1)*width]
		var dist float32
		for d := start; d < end; d++ {
			diff := (vec[d] - centroid[d]) - cb[d-start]
			dist += diff * diff
		}
		if dist < bestDist {
			best = code
			bestDist = dist
		}
	}
	return best
}

func trainPQ8Subspace(vectors [][]float32, start, end, maxIter int, seed uint64, dst []float32) error {
	width := end - start
	if width <= 0 {
		return fmt.Errorf("quantize: invalid PQ subspace")
	}
	if len(dst) != pq8CodebookSize*width {
		return fmt.Errorf("quantize: PQ subspace dst length mismatch")
	}
	n := len(vectors)
	rng := rand.New(rand.NewSource(int64(seed)))
	perm := rng.Perm(n)
	active := pq8CodebookSize
	if n < active {
		active = n
	}
	for code := 0; code < active; code++ {
		copy(dst[code*width:(code+1)*width], vectors[perm[code]][start:end])
	}
	for code := active; code < pq8CodebookSize; code++ {
		copy(dst[code*width:(code+1)*width], dst[(code%active)*width:((code%active)+1)*width])
	}
	if maxIter <= 0 {
		maxIter = defaultPQ8MaxIter
	}
	sums := make([]float64, active*width)
	counts := make([]int, active)
	for iter := 0; iter < maxIter; iter++ {
		for i := range sums {
			sums[i] = 0
		}
		clear(counts)
		for _, vec := range vectors {
			best := 0
			bestDist := float32(math.MaxFloat32)
			for code := 0; code < active; code++ {
				cb := dst[code*width : (code+1)*width]
				var dist float32
				for d := 0; d < width; d++ {
					diff := vec[start+d] - cb[d]
					dist += diff * diff
				}
				if dist < bestDist {
					best = code
					bestDist = dist
				}
			}
			counts[best]++
			sum := sums[best*width : (best+1)*width]
			for d := 0; d < width; d++ {
				sum[d] += float64(vec[start+d])
			}
		}
		for code := 0; code < active; code++ {
			cb := dst[code*width : (code+1)*width]
			if counts[code] == 0 {
				copy(cb, vectors[perm[(code+iter)%n]][start:end])
				continue
			}
			inv := 1 / float64(counts[code])
			sum := sums[code*width : (code+1)*width]
			for d := 0; d < width; d++ {
				cb[d] = float32(sum[d] * inv)
			}
		}
	}
	for code := active; code < pq8CodebookSize; code++ {
		copy(dst[code*width:(code+1)*width], dst[(code%active)*width:((code%active)+1)*width])
	}
	return nil
}
