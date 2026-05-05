package quantize

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"runtime"
	"sync"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

const (
	DefaultPQ8Subquantizers = 128
	PQ8CodebookSize         = 256
	defaultPQ8MaxIter       = 8
	defaultPQ8Restarts      = 2
	pq8ConvergenceEpsilon   = 1e-5
)

type PQ8Options struct {
	M         int
	MaxIter   int
	Seed      uint64
	StoreNorm bool
	Restarts  int
}

type PQ8Codec struct {
	Dim       int       `msgpack:"dim"`
	M         int       `msgpack:"m"`
	Offsets   []int     `msgpack:"offsets"`
	Codebooks []float32 `msgpack:"codebooks"`
	StoreNorm bool      `msgpack:"store_norm,omitempty"`

	offsetOnce      sync.Once
	codebookOffsets []int
}

type PQ8QueryScorer struct {
	rankMetric metric.Metric
	queryNorm2 float32
	m          int
	codeOffset int
	storeNorm  bool
	lut        []float32
}

type PQ8Scorer struct {
	rankMetric metric.Metric
	queryNorm2 float32
	m          int
	codeOffset int
	storeNorm  bool
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
	if opts.Restarts <= 0 {
		opts.Restarts = defaultPQ8Restarts
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
				if err := trainPQ8Subspace(residuals, start, end, opts.MaxIter, opts.Restarts, opts.Seed^uint64(sub+1)*0x9e3779b97f4a7c15, dst); err != nil {
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
	return &PQ8Codec{Dim: dim, M: opts.M, Offsets: offsets, Codebooks: codebooks, StoreNorm: opts.StoreNorm, codebookOffsets: codebookOffsets}, nil
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
		want += PQ8CodebookSize * (c.Offsets[m+1] - c.Offsets[m])
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
	if rankMetric == metric.MetricL2 || c.StoreNorm {
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
	} else if c.StoreNorm {
		off = 4
	}
	reconstructedNorm2 := float32(0)
	for sub := 0; sub < c.M; sub++ {
		code := c.nearestCodeForResidual(sub, vec, centroid)
		out[off+sub] = byte(code)
		if c.StoreNorm && rankMetric != metric.MetricL2 {
			start, end := c.Offsets[sub], c.Offsets[sub+1]
			cb := c.codeword(sub, code)
			for d := start; d < end; d++ {
				value := centroid[d] + cb[d-start]
				reconstructedNorm2 += value * value
			}
		}
	}
	if c.StoreNorm && rankMetric != metric.MetricL2 {
		binary.LittleEndian.PutUint32(out[:4], math.Float32bits(reconstructedNorm2))
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
	} else if c.StoreNorm {
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
	lut := make([]float32, codec.M*PQ8CodebookSize)
	codebookOffsets := codec.codebookBases()
	for sub := 0; sub < codec.M; sub++ {
		start, end := codec.Offsets[sub], codec.Offsets[sub+1]
		width := end - start
		codebookStart := codebookOffsets[sub]
		lutStart := sub * PQ8CodebookSize
		for code := 0; code < PQ8CodebookSize; code++ {
			cb := codec.Codebooks[codebookStart+code*width : codebookStart+(code+1)*width]
			var dot float32
			for d := start; d < end; d++ {
				dot += query[d] * cb[d-start]
			}
			lut[lutStart+code] = dot
		}
	}
	codeOffset := 0
	storeNorm := codec.StoreNorm
	if rankMetric == metric.MetricL2 || storeNorm {
		codeOffset = 4
	}
	return &PQ8QueryScorer{
		rankMetric: rankMetric,
		queryNorm2: queryNorm2,
		m:          codec.M,
		codeOffset: codeOffset,
		storeNorm:  storeNorm,
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
		storeNorm:  q.storeNorm,
		baseDot:    metric.DotProduct(query, centroid),
		lut:        q.lut,
	}, nil
}

func (q *PQ8QueryScorer) MaxDotForCodeMask(subquantizer int, maskWords []uint64) (float32, bool) {
	if q == nil || subquantizer < 0 || subquantizer >= q.m || len(maskWords) == 0 {
		return 0, false
	}
	lutBase := subquantizer * PQ8CodebookSize
	best := -float32(math.MaxFloat32)
	found := false
	for wordIdx, mask := range maskWords {
		for mask != 0 {
			bit := bits.TrailingZeros64(mask)
			code := wordIdx*64 + bit
			if code < PQ8CodebookSize {
				if value := q.lut[lutBase+code]; value > best {
					best = value
				}
				found = true
			}
			mask &= mask - 1
		}
	}
	return best, found
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
		lutOffset += PQ8CodebookSize
	}
	switch s.rankMetric {
	case metric.MetricCosine:
		if s.storeNorm {
			norm2 := math.Float32frombits(binary.LittleEndian.Uint32(blob[:4]))
			if norm2 <= 0 {
				return 1, nil
			}
			return 1 - dot/float32(math.Sqrt(float64(norm2))), nil
		}
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
			payloadCursor := cursor + 8
			codeCursor := payloadCursor + s.codeOffset
			dot := s.baseDot
			lutOffset := 0
			for sub := 0; sub < s.m; sub++ {
				dot += s.lut[lutOffset+int(rows[codeCursor+sub])]
				lutOffset += PQ8CodebookSize
			}
			if s.storeNorm {
				norm2 := math.Float32frombits(binary.LittleEndian.Uint32(rows[payloadCursor : payloadCursor+4]))
				if norm2 <= 0 {
					out[i] = 1
				} else {
					out[i] = 1 - dot/float32(math.Sqrt(float64(norm2)))
				}
			} else {
				out[i] = 1 - dot
			}
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
				lutOffset += PQ8CodebookSize
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
		off += PQ8CodebookSize * (offsets[i+1] - offsets[i])
	}
	return off
}

func pqCodebookOffsets(offsets []int) []int {
	out := make([]int, len(offsets))
	for i := 0; i < len(offsets)-1; i++ {
		out[i+1] = out[i] + PQ8CodebookSize*(offsets[i+1]-offsets[i])
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
	for code := 0; code < PQ8CodebookSize; code++ {
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

func trainPQ8Subspace(vectors [][]float32, start, end, maxIter, restarts int, seed uint64, dst []float32) error {
	width := end - start
	if width <= 0 {
		return fmt.Errorf("quantize: invalid PQ subspace")
	}
	if len(dst) != PQ8CodebookSize*width {
		return fmt.Errorf("quantize: PQ subspace dst length mismatch")
	}
	if restarts <= 0 {
		restarts = defaultPQ8Restarts
	}
	best := make([]float32, len(dst))
	scratch := make([]float32, len(dst))
	bestInertia := math.Inf(1)
	active := PQ8CodebookSize
	if len(vectors) < active {
		active = len(vectors)
	}
	workspace := pq8SubspaceWorkspace{
		sums:    make([]float64, active*width),
		counts:  make([]int, active),
		errors:  make([]float32, len(vectors)),
		closest: make([]float32, len(vectors)),
	}
	for restart := 0; restart < restarts; restart++ {
		seed := seed ^ uint64(restart+1)*0xbf58476d1ce4e5b9
		inertia, err := trainPQ8SubspaceOnce(vectors, start, end, maxIter, seed, scratch, &workspace)
		if err != nil {
			return err
		}
		if inertia < bestInertia {
			bestInertia = inertia
			copy(best, scratch)
		}
	}
	copy(dst, best)
	return nil
}

type pq8SubspaceWorkspace struct {
	sums    []float64
	counts  []int
	errors  []float32
	closest []float32
}

func trainPQ8SubspaceOnce(vectors [][]float32, start, end, maxIter int, seed uint64, dst []float32, ws *pq8SubspaceWorkspace) (float64, error) {
	width := end - start
	n := len(vectors)
	if n == 0 {
		return 0, fmt.Errorf("quantize: PQ training requires vectors")
	}
	rng := rand.New(rand.NewSource(int64(seed)))
	active := PQ8CodebookSize
	if n < active {
		active = n
	}
	initPQ8KMeansPP(vectors, start, end, active, rng, dst, ws.closest)
	for code := active; code < PQ8CodebookSize; code++ {
		copy(dst[code*width:(code+1)*width], dst[(code%active)*width:((code%active)+1)*width])
	}
	if maxIter <= 0 {
		maxIter = defaultPQ8MaxIter
	}
	sums := ws.sums[:active*width]
	counts := ws.counts[:active]
	errors := ws.errors[:n]
	prevInertia := math.Inf(1)
	inertia := math.Inf(1)
	for iter := 0; iter < maxIter; iter++ {
		for i := range sums {
			sums[i] = 0
		}
		clear(counts)
		inertia = 0
		for vecIdx, vec := range vectors {
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
			errors[vecIdx] = bestDist
			inertia += float64(bestDist)
			sum := sums[best*width : (best+1)*width]
			for d := 0; d < width; d++ {
				sum[d] += float64(vec[start+d])
			}
		}
		for code := 0; code < active; code++ {
			cb := dst[code*width : (code+1)*width]
			if counts[code] == 0 {
				copy(cb, vectors[farthestPQ8TrainingVector(errors, code+iter)][start:end])
				continue
			}
			inv := 1 / float64(counts[code])
			sum := sums[code*width : (code+1)*width]
			for d := 0; d < width; d++ {
				cb[d] = float32(sum[d] * inv)
			}
		}
		if prevInertia < math.Inf(1) {
			improvement := (prevInertia - inertia) / math.Max(prevInertia, 1)
			if improvement >= 0 && improvement < pq8ConvergenceEpsilon {
				break
			}
		}
		prevInertia = inertia
	}
	for code := active; code < PQ8CodebookSize; code++ {
		copy(dst[code*width:(code+1)*width], dst[(code%active)*width:((code%active)+1)*width])
	}
	return inertia, nil
}

func initPQ8KMeansPP(vectors [][]float32, start, end, active int, rng *rand.Rand, dst []float32, closest []float32) {
	width := end - start
	n := len(vectors)
	first := rng.Intn(n)
	copy(dst[:width], vectors[first][start:end])
	closest = closest[:n]
	for i := range closest {
		closest[i] = float32(math.MaxFloat32)
	}
	for code := 1; code < active; code++ {
		last := dst[(code-1)*width : code*width]
		var total float64
		for i, vec := range vectors {
			var dist float32
			for d := 0; d < width; d++ {
				diff := vec[start+d] - last[d]
				dist += diff * diff
			}
			if dist < closest[i] {
				closest[i] = dist
			}
			total += float64(closest[i])
		}
		next := 0
		if total > 0 {
			target := rng.Float64() * total
			var cumulative float64
			for i, dist := range closest {
				cumulative += float64(dist)
				if cumulative >= target {
					next = i
					break
				}
			}
		} else {
			next = code % n
		}
		copy(dst[code*width:(code+1)*width], vectors[next][start:end])
	}
	if active == 1 {
		return
	}
}

func farthestPQ8TrainingVector(errors []float32, offset int) int {
	best := 0
	bestErr := -float32(math.MaxFloat32)
	for i, err := range errors {
		if err > bestErr {
			best = i
			bestErr = err
		}
	}
	if offset <= 0 || len(errors) == 0 {
		return best
	}
	return (best + offset) % len(errors)
}
