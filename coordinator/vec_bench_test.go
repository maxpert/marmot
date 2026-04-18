//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"context"
	"database/sql"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	benchDim       = 128
	benchNRows     = 100_000
	benchNClusters = 128
	// Default bench config prioritises CI runtime over absolute latency.
	// The design auto-tune for 100K rows is nlist=1264, nprobe=35, but k-means++
	// init is O(n·k²) — 46min for nlist=1264 on 100K×128-dim vectors (see task #14).
	// TestSweepRecallLatency explicitly measures higher-nlist configs.
	// Measured sweep results (recall / p50 / p95 / p99):
	//   nlist=128  nprobe=8  → 0.993 / 11.4 / 13.5 / 14.4 ms   (setup 29s)
	//   nlist=512  nprobe=16 → 0.992 /  6.2 /  8.3 /  8.4 ms   (setup 7m24s)
	//   nlist=1024 nprobe=32 → 0.988 /  6.1 /  7.3 /  7.4 ms   (setup 29m30s)
	//   nlist=1264 nprobe=35 → 0.986 /  5.6 / 10.3 / 52.7 ms*  (setup 46m02s, auto-tune)
	//   *n=50 queries — p99 sensitive to outliers; nlist=1024 n=1000+ benchmark is stable.
	// Per-row floor ≈2.4µs (SQLite cursor + UDF + 128-dim cosine). 5ms p99 target requires
	// native SIMD scan bypassing SQLite UDF path — tracked as future optimisation.
	benchNlist     = 128
	benchNprobe    = 8
	benchDBName    = "benchdb"
	benchIndexName = "bench_embed"
	benchReadyPoll = 120 * time.Second
	benchBatchSize = 1000
	benchNQueries  = 100
)

// benchSetup holds all objects for the 100K benchmark tests.
type benchSetup struct {
	dbMgr   *db.DatabaseManager
	vecMgr  *db.VectorIndexManager
	engine  *vecindex.Engine
	handler *coordinator.CoordinatorHandler
	conn    *sql.DB
	vectors [][]float32
	nlist   int
	nprobe  int
}

func setupBench100K(t testing.TB) *benchSetup {
	return setupBench100KWith(t, benchNlist, benchNprobe)
}

func setupBench100KWith(t testing.TB, nlist, nprobe int) *benchSetup {
	t.Helper()

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("NewDatabaseManager: %v", err)
	}
	if err := dbMgr.CreateDatabase(benchDBName); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}

	vecMgr := db.NewVectorIndexManager(dbMgr)
	dbMgr.SetVectorIndexManager(vecMgr)

	engine := vecindex.NewEngine()
	db.SetVectorUDFProvider(engine)

	hook := db.NewEngineHook(engine, dbMgr)
	vecMgr.SetLifecycleHook(hook)
	vecMgr.SetEngineProvider(hook)
	vecMgr.SetReindexHook(hook)

	if err := vecMgr.Start(context.Background()); err != nil {
		t.Fatalf("vecMgr.Start: %v", err)
	}

	conn, err := dbMgr.GetDatabaseConnection(benchDBName)
	if err != nil {
		t.Fatalf("GetDatabaseConnection: %v", err)
	}

	_, err = conn.Exec(`CREATE TABLE IF NOT EXISTS docs (
		id    INTEGER PRIMARY KEY,
		embed BLOB
	)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Generate 100K deterministic clustered unit-norm vectors.
	// Real embeddings have natural cluster structure; purely random high-dim
	// vectors are nearly equidistant (curse of dimensionality) which defeats
	// IVF pruning. We simulate realistic distributions by generating
	// benchNClusters centroids and adding Gaussian noise around each.
	rng := rand.New(rand.NewSource(42))
	clusterCenters := make([][]float32, benchNClusters)
	for i := range clusterCenters {
		c := make([]float32, benchDim)
		for j := range c {
			c[j] = float32(rng.NormFloat64())
		}
		clusterCenters[i] = unitNorm(c)
	}
	vectors := make([][]float32, benchNRows)
	for i := range vectors {
		center := clusterCenters[i%benchNClusters]
		vectors[i] = addNoise(center, rng, 0.15)
	}

	// Batch insert for speed.
	t.Logf("Inserting %d vectors (dim=%d)...", benchNRows, benchDim)
	insertStart := time.Now()
	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin insert txn: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO docs(id, embed) VALUES (?, ?)`)
	if err != nil {
		t.Fatalf("prepare insert: %v", err)
	}
	for i, v := range vectors {
		if _, err := stmt.Exec(i+1, float32sToBlob(v)); err != nil {
			t.Fatalf("insert id=%d: %v", i+1, err)
		}
		if (i+1)%benchBatchSize == 0 {
			if err := stmt.Close(); err != nil {
				t.Fatalf("close stmt: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit batch: %v", err)
			}
			tx, err = conn.Begin()
			if err != nil {
				t.Fatalf("begin next txn: %v", err)
			}
			stmt, err = tx.Prepare(`INSERT INTO docs(id, embed) VALUES (?, ?)`)
			if err != nil {
				t.Fatalf("prepare insert: %v", err)
			}
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit final batch: %v", err)
	}
	t.Logf("Insert complete in %s", time.Since(insertStart))

	// Build coordinator handler for the read path.
	localReader := db.NewLocalReader(dbMgr)
	nodeProvider := coordinator.NewMockNodeProvider([]uint64{1})
	rc := coordinator.NewReadCoordinator(1, nodeProvider, localReader, 30*time.Second)
	handler := coordinator.NewTestHandler(1, rc, dbMgr, clock)
	handler.SetVectorEngine(engine)

	// Create the vector index.
	t.Logf("Creating vector index (nlist=%d, nprobe=%d)...", nlist, nprobe)
	indexStart := time.Now()
	meta := common.VectorIndexMeta{
		IndexName:  benchIndexName,
		TableName:  "docs",
		ColumnName: "embed",
		Database:   benchDBName,
		Metric:     "cosine",
		Dim:        benchDim,
		Nlist:      nlist,
		Nprobe:     nprobe,
		Status:     "building",
		CreatedAt:  time.Now().UnixNano(),
	}
	ctx := context.Background()
	if err := vecMgr.CreateIndex(ctx, meta); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := waitIndexReady(conn, benchIndexName, benchReadyPoll); err != nil {
		t.Fatalf("waitIndexReady: %v", err)
	}
	t.Logf("Index creation + populate complete in %s", time.Since(indexStart))

	// Read back auto-tuned values when caller passed 0.
	if nlist == 0 || nprobe == 0 {
		row := conn.QueryRow(`SELECT nlist, nprobe FROM __marmot_vector_indexes WHERE index_name = ?`, benchIndexName)
		if err := row.Scan(&nlist, &nprobe); err != nil {
			t.Fatalf("read auto-tuned params: %v", err)
		}
		t.Logf("Auto-tuned: nlist=%d, nprobe=%d", nlist, nprobe)
	}

	return &benchSetup{
		dbMgr:   dbMgr,
		vecMgr:  vecMgr,
		engine:  engine,
		handler: handler,
		conn:    conn,
		vectors: vectors,
		nlist:   nlist,
		nprobe:  nprobe,
	}
}

// bruteForceTopK computes ground truth top-K neighbors by cosine distance.
func bruteForceTopK(query []float32, corpus [][]float32, k int) []int64 {
	type dist struct {
		id int64
		d  float32
	}
	dists := make([]dist, len(corpus))
	for i, v := range corpus {
		dists[i] = dist{id: int64(i + 1), d: cosineDistance(query, v)}
	}
	sort.Slice(dists, func(i, j int) bool { return dists[i].d < dists[j].d })
	result := make([]int64, k)
	for i := 0; i < k; i++ {
		result[i] = dists[i].id
	}
	return result
}

func newBenchSession() *protocol.ConnectionSession {
	return &protocol.ConnectionSession{
		CurrentDatabase: benchDBName,
		ConnID:          100,
		VecVars:         vecindex.DefaultVecSessionVars(),
	}
}

// benchVecQuery issues a vec_match query through the bench handler's coordinator path.
func benchVecQuery(t testing.TB, s *benchSetup, sqlTpl string, qVec []byte, session *protocol.ConnectionSession) []int64 {
	t.Helper()

	params := []interface{}{qVec, qVec}
	stmt := protocol.Statement{
		SQL:      sqlTpl,
		Type:     protocol.StatementSelect,
		Database: benchDBName,
	}

	info, rewrittenArgs, err := s.handler.MaybeRewriteVectorSelect(stmt, params, session)
	if err != nil {
		t.Fatalf("MaybeRewriteVectorSelect: %v", err)
	}
	if info == nil {
		t.Fatal("expected rewrite info (is engine installed?)")
	}

	rs, err := s.handler.ExecuteVectorPlan(stmt, info, rewrittenArgs, protocol.ConsistencyLocalOne)
	if err != nil {
		t.Fatalf("ExecuteVectorPlan: %v", err)
	}
	if rs == nil {
		t.Fatal("nil result set")
	}

	ids := make([]int64, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		if len(row) == 0 {
			continue
		}
		ids = append(ids, toInt64(row[0]))
	}
	return ids
}

// TestRecall100K measures recall@10 across 100 queries against 100K vectors
// through the full coordinator rewrite → execute path.
func TestRecall100K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K recall test in short mode")
	}

	s := setupBench100K(t)
	rng := rand.New(rand.NewSource(17))

	const k = 10
	const querySQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

	sess := newBenchSession()
	sess.VecVars.Fallback = false

	totalHits := 0
	perQuery := make([]float64, benchNQueries)

	for q := 0; q < benchNQueries; q++ {
		targetIdx := rng.Intn(len(s.vectors))
		qv := addNoise(s.vectors[targetIdx], rng, 0.005)
		qb := float32sToBlob(qv)

		groundTruth := bruteForceTopK(qv, s.vectors, k)
		truthSet := make(map[int64]bool, k)
		for _, id := range groundTruth {
			truthSet[id] = true
		}

		ids := benchVecQuery(t, s, querySQL, qb, sess)
		require.Len(t, ids, k, "expected %d results for query %d", k, q)

		hits := 0
		for _, id := range ids {
			if truthSet[id] {
				hits++
			}
		}
		perQuery[q] = float64(hits) / float64(k)
		totalHits += hits
	}

	recall := float64(totalHits) / float64(benchNQueries*k)
	t.Logf("Recall@%d over %d queries = %.4f (%d/%d hits)",
		k, benchNQueries, recall, totalHits, benchNQueries*k)
	t.Logf("  nlist=%d nprobe=%d dim=%d N=%d", s.nlist, s.nprobe, benchDim, benchNRows)

	// Per-query breakdown: min, p25, median, p75, max
	sort.Float64s(perQuery)
	t.Logf("  Per-query recall: min=%.2f p25=%.2f median=%.2f p75=%.2f max=%.2f",
		perQuery[0],
		perQuery[benchNQueries/4],
		perQuery[benchNQueries/2],
		perQuery[3*benchNQueries/4],
		perQuery[benchNQueries-1])

	require.GreaterOrEqual(t, recall, 0.94,
		"recall@%d must be >= 0.94 (got %.4f, nlist=%d nprobe=%d dim=%d)",
		k, recall, s.nlist, s.nprobe, benchDim)
}

// BenchmarkSearch100K measures search latency with percentile reporting.
func BenchmarkSearch100K(b *testing.B) {
	s := setupBench100K(b)
	rng := rand.New(rand.NewSource(99))

	const querySQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

	// Pre-generate query vectors.
	nQueries := 1000
	queryVecs := make([][]byte, nQueries)
	for i := range queryVecs {
		idx := rng.Intn(len(s.vectors))
		qv := addNoise(s.vectors[idx], rng, 0.01)
		queryVecs[i] = float32sToBlob(qv)
	}

	b.Run("Serial", func(b *testing.B) {
		latencies := make([]time.Duration, 0, b.N)
		sess := newBenchSession()
		sess.VecVars.Fallback = false

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			qb := queryVecs[i%nQueries]
			params := []interface{}{qb, qb}
			stmt := protocol.Statement{
				SQL:      querySQL,
				Type:     protocol.StatementSelect,
				Database: benchDBName,
			}

			start := time.Now()
			info, rewrittenArgs, err := s.handler.MaybeRewriteVectorSelect(stmt, params, sess)
			if err != nil {
				b.Fatalf("MaybeRewriteVectorSelect: %v", err)
			}
			if info == nil {
				b.Fatal("expected rewrite info")
			}
			rs, err := s.handler.ExecuteVectorPlan(stmt, info, rewrittenArgs, protocol.ConsistencyLocalOne)
			if err != nil {
				b.Fatalf("ExecuteVectorPlan: %v", err)
			}
			elapsed := time.Since(start)
			latencies = append(latencies, elapsed)

			if len(rs.Rows) == 0 {
				b.Fatal("empty result set")
			}
		}
		b.StopTimer()

		reportLatencyPercentiles(b, latencies)
	})

	b.Run("Parallel", func(b *testing.B) {
		var mu sync.Mutex
		allLatencies := make([]time.Duration, 0, b.N)
		var queryCount atomic.Int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			sess := newBenchSession()
			sess.VecVars.Fallback = false
			var local []time.Duration
			idx := int(queryCount.Add(1)) - 1

			for pb.Next() {
				qb := queryVecs[idx%nQueries]
				idx++
				params := []interface{}{qb, qb}
				stmt := protocol.Statement{
					SQL:      querySQL,
					Type:     protocol.StatementSelect,
					Database: benchDBName,
				}

				start := time.Now()
				info, rewrittenArgs, err := s.handler.MaybeRewriteVectorSelect(stmt, params, sess)
				if err != nil {
					b.Fatalf("MaybeRewriteVectorSelect: %v", err)
				}
				if info == nil {
					b.Fatal("expected rewrite info")
				}
				_, err = s.handler.ExecuteVectorPlan(stmt, info, rewrittenArgs, protocol.ConsistencyLocalOne)
				if err != nil {
					b.Fatalf("ExecuteVectorPlan: %v", err)
				}
				local = append(local, time.Since(start))
			}

			mu.Lock()
			allLatencies = append(allLatencies, local...)
			mu.Unlock()
		})
		b.StopTimer()

		reportLatencyPercentiles(b, allLatencies)
	})
}

// reportLatencyPercentiles sorts latencies and reports p50, p95, p99 via b.ReportMetric.
func reportLatencyPercentiles(b *testing.B, latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	p := func(pct float64) time.Duration {
		idx := int(float64(len(latencies)-1) * pct)
		return latencies[idx]
	}
	p50 := p(0.50)
	p95 := p(0.95)
	p99 := p(0.99)

	b.ReportMetric(float64(p50.Microseconds()), "p50-µs")
	b.ReportMetric(float64(p95.Microseconds()), "p95-µs")
	b.ReportMetric(float64(p99.Microseconds()), "p99-µs")

	b.Logf("Latency: p50=%s p95=%s p99=%s (n=%d, GOMAXPROCS=%d)",
		p50, p95, p99, len(latencies), runtime.GOMAXPROCS(0))

	// Task #16 latency contract: Serial p99 ≤ 5ms on the default config
	// (nlist=128/nprobe=8, 128-dim, 100K rows) via the in-memory vector
	// cache path. Scope is Serial only — Parallel latency reflects core
	// saturation / scheduler tail behaviour and is a throughput measurement,
	// not a per-query latency target. Parallel p50/p95/p99 are still emitted
	// via b.ReportMetric for visibility but do not fail the bench. Gate on
	// n ≥ 500 so `-benchtime=1x` smoke runs do not trip on a single sample.
	if len(latencies) >= 500 && strings.Contains(b.Name(), "Serial") && p99 > 5*time.Millisecond {
		b.Fatalf("p99 %s exceeds 5ms cache target (n=%d)", p99, len(latencies))
	}
}

// TestMemoryProfile100K measures heap allocations from index creation + populate.
func TestMemoryProfile100K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K memory test in short mode")
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	s := setupBench100K(t)
	_ = s // keep alive

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	heapDelta := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)
	heapInUseDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)
	totalAllocDelta := memAfter.TotalAlloc - memBefore.TotalAlloc

	heapDeltaMB := float64(heapDelta) / (1024 * 1024)
	heapInUseMB := float64(heapInUseDelta) / (1024 * 1024)
	totalAllocMB := float64(totalAllocDelta) / (1024 * 1024)

	t.Logf("Memory profile (100K vectors, dim=%d, nlist=%d):", benchDim, s.nlist)
	t.Logf("  HeapAlloc delta:  %.2f MB", heapDeltaMB)
	t.Logf("  HeapInuse delta:  %.2f MB", heapInUseMB)
	t.Logf("  TotalAlloc delta: %.2f MB", totalAllocMB)
	t.Logf("  HeapObjects:      %d → %d", memBefore.HeapObjects, memAfter.HeapObjects)

	// Assert < 100MB per index (§11 targets ~50MB at nlist=2048).
	require.Less(t, heapDeltaMB, 100.0,
		"heap delta %.2f MB exceeds 100 MB budget (dim=%d, nlist=%d)",
		heapDeltaMB, benchDim, s.nlist)
}

// TestReindexUnderLoad100K runs REINDEX while concurrent queries execute.
func TestReindexUnderLoad100K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K reindex-under-load test in short mode")
	}

	s := setupBench100K(t)
	rng := rand.New(rand.NewSource(31))

	const querySQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

	// Measure recall before REINDEX.
	recallBefore := measureRecall(t, s, rng, 20)
	t.Logf("Recall before REINDEX: %.4f", recallBefore)

	// Start 10 concurrent query goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var queryErrors atomic.Int64
	var queriesRun atomic.Int64
	var wg sync.WaitGroup

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(seed))
			sess := newBenchSession()
			sess.VecVars.Fallback = false

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				idx := localRng.Intn(len(s.vectors))
				qv := addNoise(s.vectors[idx], localRng, 0.01)
				qb := float32sToBlob(qv)
				params := []interface{}{qb, qb}
				stmt := protocol.Statement{
					SQL:      querySQL,
					Type:     protocol.StatementSelect,
					Database: benchDBName,
				}

				info, args, err := s.handler.MaybeRewriteVectorSelect(stmt, params, sess)
				if err != nil {
					queryErrors.Add(1)
					continue
				}
				if info == nil {
					queryErrors.Add(1)
					continue
				}
				_, err = s.handler.ExecuteVectorPlan(stmt, info, args, protocol.ConsistencyLocalOne)
				if err != nil {
					queryErrors.Add(1)
					continue
				}
				queriesRun.Add(1)
			}
		}(int64(g * 1000))
	}

	// Run REINDEX.
	t.Log("Starting REINDEX under load...")
	reindexStart := time.Now()
	err := s.vecMgr.ReindexIndex(context.Background(), benchIndexName)
	reindexDuration := time.Since(reindexStart)
	require.NoError(t, err, "REINDEX must not error")

	// Stop query goroutines.
	cancel()
	wg.Wait()

	t.Logf("REINDEX completed in %s", reindexDuration)
	t.Logf("Queries executed during REINDEX: %d", queriesRun.Load())
	t.Logf("Query errors during REINDEX: %d", queryErrors.Load())

	// Measure recall after REINDEX.
	recallAfter := measureRecall(t, s, rand.New(rand.NewSource(31)), 20)
	t.Logf("Recall after REINDEX: %.4f", recallAfter)

	require.Zero(t, queryErrors.Load(), "no query errors expected during REINDEX")
	require.InDelta(t, recallBefore, recallAfter, 0.05,
		"recall before/after REINDEX must be within 5%%")
	require.Less(t, reindexDuration, 30*time.Second,
		"REINDEX must complete within 30s")
}

// TestDeltaFlush100K validates that delta flush assigns newly inserted rows.
func TestDeltaFlush100K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K delta flush test in short mode")
	}

	s := setupBench100K(t)
	rng := rand.New(rand.NewSource(77))

	// Verify initial state: no delta (cluster_id=0) rows.
	membersTable := "__marmot_vec_" + benchIndexName + "_members"
	var initialDelta int64
	err := s.conn.QueryRow(
		`SELECT COUNT(*) FROM "` + membersTable + `" WHERE cluster_id = 0`).Scan(&initialDelta)
	require.NoError(t, err)
	t.Logf("Initial delta rows: %d", initialDelta)

	// Wire delta flush with a short interval for the test.
	flushDB := db.NewSQLDeltaFlushDB(s.dbMgr)
	s.engine.SetFlushDB(flushDB)
	s.engine.SetFlushConfig(vecindex.DeltaFlushConfig{
		Interval:  500 * time.Millisecond,
		MaxRows:   20_000,
		BatchSize: 2_000,
	})
	s.engine.StartFlush(benchIndexName, benchDBName, "docs", "embed")
	defer s.engine.StopFlush(benchIndexName)

	// Insert 10K more rows.
	const extraRows = 10_000
	t.Logf("Inserting %d additional rows...", extraRows)
	tx, err := s.conn.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(`INSERT INTO docs(id, embed) VALUES (?, ?)`)
	require.NoError(t, err)
	for i := 0; i < extraRows; i++ {
		v := make([]float32, benchDim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		unitNorm(v)
		_, err = stmt.Exec(benchNRows+i+1, float32sToBlob(v))
		require.NoError(t, err)
	}
	stmt.Close()
	require.NoError(t, tx.Commit())

	// Poll for delta flush completion.
	t.Log("Waiting for delta flush...")
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var deltaCount int64
		err := s.conn.QueryRow(
			`SELECT COUNT(*) FROM "` + membersTable + `" WHERE cluster_id = 0`).Scan(&deltaCount)
		require.NoError(t, err)
		if deltaCount == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	var finalDelta int64
	err = s.conn.QueryRow(
		`SELECT COUNT(*) FROM "` + membersTable + `" WHERE cluster_id = 0`).Scan(&finalDelta)
	require.NoError(t, err)

	var totalMembers int64
	err = s.conn.QueryRow(
		`SELECT COUNT(*) FROM "` + membersTable + `"`).Scan(&totalMembers)
	require.NoError(t, err)

	t.Logf("Delta flush result: delta_remaining=%d total_members=%d", finalDelta, totalMembers)

	require.Zero(t, finalDelta, "all delta rows must be flushed")
	require.Equal(t, int64(benchNRows+extraRows), totalMembers,
		"total members must equal %d", benchNRows+extraRows)
}

// measureRecall runs nQueries queries and returns mean recall@10.
func measureRecall(t testing.TB, s *benchSetup, rng *rand.Rand, nQueries int) float64 {
	t.Helper()

	const k = 10
	const querySQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

	sess := newBenchSession()
	sess.VecVars.Fallback = false

	totalHits := 0
	for q := 0; q < nQueries; q++ {
		idx := rng.Intn(len(s.vectors))
		qv := addNoise(s.vectors[idx], rng, 0.005)
		qb := float32sToBlob(qv)

		groundTruth := bruteForceTopK(qv, s.vectors, k)
		truthSet := make(map[int64]bool, k)
		for _, id := range groundTruth {
			truthSet[id] = true
		}

		ids := benchVecQuery(t, s, querySQL, qb, sess)
		for _, id := range ids {
			if truthSet[id] {
				totalHits++
			}
		}
	}
	return float64(totalHits) / float64(nQueries*k)
}

// TestSweepRecallLatency runs recall + latency measurements across multiple
// nlist/nprobe configurations to find the optimal operating point.
func TestSweepRecallLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sweep in short mode")
	}

	configs := []struct {
		name   string
		nlist  int
		nprobe int
	}{
		{"nlist512_nprobe16", 512, 16},
		{"nlist1024_nprobe32", 1024, 32},
		{"auto_tune", 0, 0},
		{"nlist2048_nprobe45", 2048, 45},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			s := setupBench100KWith(t, cfg.nlist, cfg.nprobe)

			// Measure recall@10 over 50 queries.
			rng := rand.New(rand.NewSource(17))
			const k = 10
			const nQ = 50
			const querySQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

			sess := newBenchSession()
			sess.VecVars.Fallback = false

			totalHits := 0
			latencies := make([]time.Duration, 0, nQ)

			for q := 0; q < nQ; q++ {
				idx := rng.Intn(len(s.vectors))
				qv := addNoise(s.vectors[idx], rng, 0.005)
				qb := float32sToBlob(qv)

				groundTruth := bruteForceTopK(qv, s.vectors, k)
				truthSet := make(map[int64]bool, k)
				for _, id := range groundTruth {
					truthSet[id] = true
				}

				params := []interface{}{qb, qb}
				stmt := protocol.Statement{
					SQL:      querySQL,
					Type:     protocol.StatementSelect,
					Database: benchDBName,
				}

				start := time.Now()
				info, rewrittenArgs, err := s.handler.MaybeRewriteVectorSelect(stmt, params, sess)
				if err != nil {
					t.Fatalf("rewrite: %v", err)
				}
				if info == nil {
					t.Fatal("nil rewrite info")
				}
				rs, err := s.handler.ExecuteVectorPlan(stmt, info, rewrittenArgs, protocol.ConsistencyLocalOne)
				if err != nil {
					t.Fatalf("execute: %v", err)
				}
				latencies = append(latencies, time.Since(start))

				for _, row := range rs.Rows {
					if len(row) > 0 && truthSet[toInt64(row[0])] {
						totalHits++
					}
				}
			}

			recall := float64(totalHits) / float64(nQ*k)
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			p50 := latencies[len(latencies)/2]
			p95 := latencies[int(float64(len(latencies)-1)*0.95)]
			p99 := latencies[int(float64(len(latencies)-1)*0.99)]

			t.Logf("CONFIG nlist=%d nprobe=%d | recall=%.4f | p50=%s p95=%s p99=%s",
				s.nlist, s.nprobe, recall, p50, p95, p99)
		})
	}
}

// BenchmarkSearch100K_GoRank mirrors BenchmarkSearch100K with the Go-side
// ranking path explicitly enabled. Compare p50/p95/p99 against BenchmarkSearch100K
// (which also uses GoRank=true via DefaultVecSessionVars) to verify no regression.
func BenchmarkSearch100K_GoRank(b *testing.B) {
	s := setupBench100K(b)
	rng := rand.New(rand.NewSource(99))

	const querySQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

	nQueries := 1000
	queryVecs := make([][]byte, nQueries)
	for i := range queryVecs {
		idx := rng.Intn(len(s.vectors))
		qv := addNoise(s.vectors[idx], rng, 0.01)
		queryVecs[i] = float32sToBlob(qv)
	}

	b.Run("GoRank_Serial", func(b *testing.B) {
		sess := newBenchSession()
		sess.VecVars.UseGoRank = true
		sess.VecVars.Fallback = false
		latencies := make([]time.Duration, 0, b.N)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			qb := queryVecs[i%nQueries]
			params := []interface{}{qb, qb}
			stmt := protocol.Statement{
				SQL:      querySQL,
				Type:     protocol.StatementSelect,
				Database: benchDBName,
			}

			start := time.Now()
			info, rewrittenArgs, err := s.handler.MaybeRewriteVectorSelect(stmt, params, sess)
			if err != nil {
				b.Fatalf("MaybeRewriteVectorSelect: %v", err)
			}
			if info == nil {
				b.Fatal("expected rewrite info")
			}
			rs, err := s.handler.ExecuteVectorPlan(stmt, info, rewrittenArgs, protocol.ConsistencyLocalOne)
			if err != nil {
				b.Fatalf("ExecuteVectorPlan: %v", err)
			}
			latencies = append(latencies, time.Since(start))
			if len(rs.Rows) == 0 {
				b.Fatal("empty result set")
			}
		}
		b.StopTimer()
		reportLatencyPercentiles(b, latencies)
	})
}

// BenchmarkMaybeRewriteVectorSelect_AllocsPerOp gates the hot rewrite path
// with and without the task-#19 AST-threading optimisation.
//
//   - WithAST: Statement carries a pre-parsed ParsedAST, mirroring the
//     production parser pipeline (protocol.ParseStatementWithOptions threads
//     the AST onto stmt.ParsedAST). The handler only sqlparser.Clones.
//   - WithoutAST: ParsedAST is nil, so the handler falls back to a full
//     Vitess Parse on every call. This is the pre-task-#19 behaviour and
//     acts as a regression baseline.
//
// The allocs/op delta between the two subtests is the contribution of the
// AST-threading change. Subsequent ExecuteVectorPlan is intentionally
// excluded — its allocs (rows, params, cache-key building, result encoding)
// dwarf the rewrite and would drown out the thing we're measuring.
func BenchmarkMaybeRewriteVectorSelect_AllocsPerOp(b *testing.B) {
	s := setupBench100K(b)

	const querySQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

	parsedAST, err := protocol.ParseVitessAST(querySQL)
	if err != nil {
		b.Fatalf("ParseVitessAST: %v", err)
	}

	qb := float32sToBlob(addNoise(s.vectors[0], rand.New(rand.NewSource(1)), 0.005))
	params := []interface{}{qb, qb}

	sess := newBenchSession()
	sess.VecVars.Fallback = false
	sess.VecVars.UseCache = true
	sess.VecVars.UseGoRank = true

	// Warm cache once so the rewrite subtests run against a populated cache.
	warm := protocol.Statement{
		SQL:       querySQL,
		Type:      protocol.StatementSelect,
		Database:  benchDBName,
		ParsedAST: parsedAST,
	}
	info, args, err := s.handler.MaybeRewriteVectorSelect(warm, params, sess)
	if err != nil || info == nil {
		b.Fatalf("warm rewrite failed: info=%v err=%v", info, err)
	}
	if _, err := s.handler.ExecuteVectorPlan(warm, info, args, protocol.ConsistencyLocalOne); err != nil {
		b.Fatalf("warm execute failed: %v", err)
	}

	run := func(b *testing.B, ast sqlparser.Statement) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stmt := protocol.Statement{
				SQL:       querySQL,
				Type:      protocol.StatementSelect,
				Database:  benchDBName,
				ParsedAST: ast,
			}
			info, _, err := s.handler.MaybeRewriteVectorSelect(stmt, params, sess)
			if err != nil {
				b.Fatalf("rewrite: %v", err)
			}
			if info == nil {
				b.Fatal("expected rewrite info")
			}
		}
	}

	b.Run("WithAST", func(b *testing.B) { run(b, parsedAST) })
	b.Run("WithoutAST", func(b *testing.B) { run(b, nil) })
}

// TestRecall100K_UDF forces the SQL-UDF path (UseGoRank=false) to keep that
// code path exercised by CI even though GoRank is now the default.
func TestRecall100K_UDF(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K UDF recall test in short mode")
	}

	s := setupBench100K(t)
	rng := rand.New(rand.NewSource(17))

	const k = 10
	const querySQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

	sess := newBenchSession()
	sess.VecVars.UseGoRank = false
	sess.VecVars.Fallback = false

	totalHits := 0
	for q := 0; q < benchNQueries; q++ {
		targetIdx := rng.Intn(len(s.vectors))
		qv := addNoise(s.vectors[targetIdx], rng, 0.005)
		qb := float32sToBlob(qv)

		groundTruth := bruteForceTopK(qv, s.vectors, k)
		truthSet := make(map[int64]bool, k)
		for _, id := range groundTruth {
			truthSet[id] = true
		}

		ids := benchVecQuery(t, s, querySQL, qb, sess)
		require.Len(t, ids, k, "expected %d results for query %d", k, q)
		for _, id := range ids {
			if truthSet[id] {
				totalHits++
			}
		}
	}

	recall := float64(totalHits) / float64(benchNQueries*k)
	t.Logf("UDF Recall@%d over %d queries = %.4f (%d/%d hits)",
		k, benchNQueries, recall, totalHits, benchNQueries*k)
	require.GreaterOrEqual(t, recall, 0.94,
		"UDF recall@%d must be >= 0.94 (got %.4f)", k, recall)
}
