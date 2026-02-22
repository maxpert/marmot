package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	freshann "github.com/maxpert/marmot/modules/freshann"
)

type manifest struct {
	DatasetName     string `json:"dataset_name"`
	Metric          string `json:"metric"`
	BaseCount       int    `json:"base_count"`
	QueryCount      int    `json:"query_count"`
	HasOfficialGT   bool   `json:"has_official_gt"`
	OriginalBaseAll int    `json:"original_base_all"`
}

type report struct {
	Dataset        string  `json:"dataset"`
	Metric         string  `json:"metric"`
	BaseCount      int     `json:"base_count"`
	QueryCount     int     `json:"query_count"`
	TopK           int     `json:"top_k"`
	RecallAtK      float64 `json:"recall_at_k"`
	P50MS          float64 `json:"p50_ms"`
	P95MS          float64 `json:"p95_ms"`
	P99MS          float64 `json:"p99_ms"`
	QPS            float64 `json:"qps"`
	BuildSeconds   float64 `json:"build_seconds"`
	QuerySeconds   float64 `json:"query_seconds"`
	GroundTruth    string  `json:"ground_truth"`
	CollectedAtUTC string  `json:"collected_at_utc"`
}

func main() {
	var (
		datasetDir   = flag.String("dataset-dir", "", "dataset directory containing manifest.json/base.f32bin/queries.f32bin")
		indexRoot    = flag.String("index-root", "/tmp/freshann-bench-data/indexes", "index root dir")
		topK         = flag.Int("topk", 10, "top-k")
		baseLimit    = flag.Int("base-limit", 0, "limit base vectors loaded (0 = all available in base.f32bin)")
		queryLimit   = flag.Int("query-limit", 0, "limit query vectors loaded (0 = all available in queries.f32bin)")
		outFile      = flag.String("out", "", "write JSON report to path (optional)")
		exactGT      = flag.Bool("compute-exact-gt", false, "compute exact GT from loaded base set instead of using provided gt.i32bin")
		queryWorkers = flag.Int("query-workers", runtime.GOMAXPROCS(0), "number of concurrent query workers")
	)
	flag.Parse()

	if *datasetDir == "" {
		fatalf("missing --dataset-dir")
	}
	if *topK <= 0 {
		fatalf("--topk must be > 0")
	}

	mf, err := loadManifest(filepath.Join(*datasetDir, "manifest.json"))
	if err != nil {
		fatalf("load manifest: %v", err)
	}

	base, dim, err := readF32Bin(filepath.Join(*datasetDir, "base.f32bin"), *baseLimit)
	if err != nil {
		fatalf("read base.f32bin: %v", err)
	}
	queries, qDim, err := readF32Bin(filepath.Join(*datasetDir, "queries.f32bin"), *queryLimit)
	if err != nil {
		fatalf("read queries.f32bin: %v", err)
	}
	if qDim != dim {
		fatalf("dimension mismatch base=%d queries=%d", dim, qDim)
	}
	if len(base) == 0 || len(queries) == 0 {
		fatalf("empty base (%d) or query (%d) set", len(base), len(queries))
	}

	metric, err := parseMetric(mf.Metric)
	if err != nil {
		fatalf("parse metric: %v", err)
	}

	root := *indexRoot
	if err := os.MkdirAll(root, 0o755); err != nil {
		fatalf("create index root: %v", err)
	}
	runRoot := filepath.Join(root, fmt.Sprintf("%s-%d", sanitizeName(mf.DatasetName), time.Now().Unix()))
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		fatalf("create run root: %v", err)
	}

	ctx := context.Background()
	eng, err := freshann.NewEngine(freshann.EngineOptions{RootDir: runRoot})
	if err != nil {
		fatalf("new engine: %v", err)
	}
	defer eng.Close()

	_, err = eng.CreateIndex(ctx, freshann.IndexSpec{
		ID:             freshann.IndexID("bench"),
		Dim:            dim,
		Metric:         metric,
		ApplyMode:      freshann.ApplyModeSync,
		DurabilityMode: freshann.DurabilityPeriodic,
		Graph: freshann.GraphSpec{
			R:       8,
			LBuild:  len(base) + 1, // keep rebuild disabled during ingest for benchmark throughput
			LSearch: 64,
			Beam:    16,
		},
	})
	if err != nil {
		fatalf("create index: %v", err)
	}
	idx, err := eng.OpenIndex(ctx, freshann.IndexID("bench"))
	if err != nil {
		fatalf("open index: %v", err)
	}
	defer idx.Close()

	buildStart := time.Now()
	for i, vec := range base {
		_, err := idx.Upsert(ctx, freshann.Mutation{
			TxnID:      1,
			SeqID:      uint64(i + 1),
			ExternalID: []byte(strconv.Itoa(i)),
			VectorFP32: vec,
		})
		if err != nil {
			fatalf("upsert #%d: %v", i, err)
		}
	}
	buildDur := time.Since(buildStart)
	if err := idx.Flush(ctx); err != nil {
		fatalf("flush after ingest: %v", err)
	}

	var gt [][]int
	gtSource := "exact_computed"
	if !*exactGT {
		if mf.HasOfficialGT && len(base) == mf.OriginalBaseAll {
			loaded, err := readGT(filepath.Join(*datasetDir, "gt.i32bin"), len(queries), *topK)
			if err == nil && len(loaded) > 0 {
				gt = loaded
				gtSource = "official_gt"
			}
		}
	}
	if len(gt) == 0 {
		gt = computeExactGT(metric, base, queries, *topK)
		gtSource = "exact_computed"
	}

	durations := make([]time.Duration, len(queries))
	hitCounts := make([]int, len(queries))
	queryStart := time.Now()
	workerCount := *queryWorkers
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(queries) {
		workerCount = len(queries)
	}
	if workerCount <= 1 {
		for qi, qv := range queries {
			t0 := time.Now()
			res, err := idx.Search(ctx, freshann.SearchRequest{
				VectorFP32: qv,
				TopK:       *topK,
			})
			durations[qi] = time.Since(t0)
			if err != nil {
				fatalf("search #%d: %v", qi, err)
			}
			pred := make([]int, 0, len(res.Hits))
			for _, h := range res.Hits {
				id, err := strconv.Atoi(string(h.ExternalID))
				if err != nil {
					continue
				}
				pred = append(pred, id)
			}
			hitCounts[qi] = overlapCount(pred, gt[qi])
		}
	} else {
		type job struct {
			qi int
		}
		jobs := make(chan job, len(queries))
		var wg sync.WaitGroup
		var firstErr error
		var errMu sync.Mutex
		for w := 0; w < workerCount; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobs {
					t0 := time.Now()
					res, err := idx.Search(ctx, freshann.SearchRequest{
						VectorFP32: queries[j.qi],
						TopK:       *topK,
					})
					durations[j.qi] = time.Since(t0)
					if err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("search #%d: %w", j.qi, err)
						}
						errMu.Unlock()
						continue
					}
					pred := make([]int, 0, len(res.Hits))
					for _, h := range res.Hits {
						id, err := strconv.Atoi(string(h.ExternalID))
						if err != nil {
							continue
						}
						pred = append(pred, id)
					}
					hitCounts[j.qi] = overlapCount(pred, gt[j.qi])
				}
			}()
		}
		for qi := range queries {
			jobs <- job{qi: qi}
		}
		close(jobs)
		wg.Wait()
		if firstErr != nil {
			fatalf("%v", firstErr)
		}
	}
	queryDur := time.Since(queryStart)

	totalHits := 0
	for _, n := range hitCounts {
		totalHits += n
	}
	recall := float64(totalHits) / float64(len(queries)*(*topK))
	p50, p95, p99 := percentileMS(durations, 50), percentileMS(durations, 95), percentileMS(durations, 99)
	qps := float64(len(queries)) / queryDur.Seconds()

	rep := report{
		Dataset:        mf.DatasetName,
		Metric:         mf.Metric,
		BaseCount:      len(base),
		QueryCount:     len(queries),
		TopK:           *topK,
		RecallAtK:      recall,
		P50MS:          p50,
		P95MS:          p95,
		P99MS:          p99,
		QPS:            qps,
		BuildSeconds:   buildDur.Seconds(),
		QuerySeconds:   queryDur.Seconds(),
		GroundTruth:    gtSource,
		CollectedAtUTC: time.Now().UTC().Format(time.RFC3339),
	}

	out, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(out))
	if *outFile != "" {
		if err := os.WriteFile(*outFile, out, 0o644); err != nil {
			fatalf("write report: %v", err)
		}
	}
}

func parseMetric(s string) (freshann.Metric, error) {
	switch s {
	case "cosine":
		return freshann.MetricCosine, nil
	case "dot":
		return freshann.MetricDot, nil
	case "euclidean", "l2":
		return freshann.MetricEuclidean, nil
	default:
		return "", fmt.Errorf("unsupported metric %q", s)
	}
}

func loadManifest(path string) (manifest, error) {
	var m manifest
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	return m, nil
}

func readF32Bin(path string, limit int) ([][]float32, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	var n, d uint32
	if err := binary.Read(f, binary.LittleEndian, &n); err != nil {
		return nil, 0, err
	}
	if err := binary.Read(f, binary.LittleEndian, &d); err != nil {
		return nil, 0, err
	}
	count := int(n)
	if limit > 0 && limit < count {
		count = limit
	}
	flat := make([]float32, count*int(d))
	if err := binary.Read(f, binary.LittleEndian, flat); err != nil {
		return nil, 0, err
	}
	out := make([][]float32, count)
	step := int(d)
	for i := 0; i < count; i++ {
		out[i] = flat[i*step : (i+1)*step]
	}
	return out, int(d), nil
}

func readGT(path string, queryLimit int, topk int) ([][]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var nq, k uint32
	if err := binary.Read(f, binary.LittleEndian, &nq); err != nil {
		return nil, err
	}
	if err := binary.Read(f, binary.LittleEndian, &k); err != nil {
		return nil, err
	}
	n := int(nq)
	if queryLimit > 0 && queryLimit < n {
		n = queryLimit
	}
	useK := int(k)
	if topk < useK {
		useK = topk
	}
	row := make([]int32, int(k))
	out := make([][]int, n)
	for i := 0; i < n; i++ {
		if err := binary.Read(f, binary.LittleEndian, row); err != nil {
			return nil, err
		}
		ids := make([]int, useK)
		for j := 0; j < useK; j++ {
			ids[j] = int(row[j])
		}
		out[i] = ids
	}
	return out, nil
}

func computeExactGT(metric freshann.Metric, base, queries [][]float32, k int) [][]int {
	out := make([][]int, len(queries))
	for qi, q := range queries {
		h := make([]pair, 0, k)
		for i, b := range base {
			s := score(metric, q, b)
			if len(h) < k {
				h = append(h, pair{id: i, score: s})
				if len(h) == k {
					sort.Slice(h, func(a, b int) bool { return h[a].score < h[b].score })
				}
				continue
			}
			if s > h[0].score {
				h[0] = pair{id: i, score: s}
				sort.Slice(h, func(a, b int) bool { return h[a].score < h[b].score })
			}
		}
		sort.Slice(h, func(a, b int) bool { return h[a].score > h[b].score })
		row := make([]int, len(h))
		for i := range h {
			row[i] = h[i].id
		}
		out[qi] = row
	}
	return out
}

type pair struct {
	id    int
	score float32
}

func score(metric freshann.Metric, a, b []float32) float32 {
	switch metric {
	case freshann.MetricDot:
		var s float32
		for i := range a {
			s += a[i] * b[i]
		}
		return s
	case freshann.MetricCosine:
		var dot, an, bn float64
		for i := range a {
			af := float64(a[i])
			bf := float64(b[i])
			dot += af * bf
			an += af * af
			bn += bf * bf
		}
		if an == 0 || bn == 0 {
			return 0
		}
		return float32(dot / (math.Sqrt(an) * math.Sqrt(bn)))
	case freshann.MetricEuclidean:
		var l2 float32
		for i := range a {
			d := a[i] - b[i]
			l2 += d * d
		}
		return -l2
	default:
		return 0
	}
}

func overlapCount(pred []int, gt []int) int {
	if len(pred) == 0 || len(gt) == 0 {
		return 0
	}
	m := make(map[int]struct{}, len(gt))
	for _, id := range gt {
		m[id] = struct{}{}
	}
	hits := 0
	for _, id := range pred {
		if _, ok := m[id]; ok {
			hits++
		}
	}
	return hits
}

func percentileMS(durs []time.Duration, pct float64) float64 {
	if len(durs) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), durs...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(math.Ceil((pct/100.0)*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return float64(cp[idx]) / float64(time.Millisecond)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func sanitizeName(in string) string {
	if in == "" {
		return "dataset"
	}
	out := make([]rune, 0, len(in))
	for _, r := range in {
		switch {
		case r >= 'a' && r <= 'z':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r)
		case r >= '0' && r <= '9':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}
