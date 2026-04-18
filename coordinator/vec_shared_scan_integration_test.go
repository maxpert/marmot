//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

const (
	sharedScanDBName    = "sharedscandb"
	sharedScanIndexName = "shared_scan_embed"
	sharedScanReadyPoll = 30 * time.Second
	sharedScanQuerySQL  = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`
)

type sharedScanOptions struct {
	dim        int
	rows       int
	clusters   int
	nlist      int
	nprobe     int
	cacheBytes uint64
}

type sharedScanSetup struct {
	dbMgr   *db.DatabaseManager
	vecMgr  *db.VectorIndexManager
	engine  *vecindex.Engine
	handler *coordinator.CoordinatorHandler
	conn    *sql.DB
	vectors [][]float32
	opts    sharedScanOptions
}

type sharedScanWorkloadMode string

const (
	sharedScanWorkloadOverlap  sharedScanWorkloadMode = "overlap"
	sharedScanWorkloadDisjoint sharedScanWorkloadMode = "disjoint"
)

type sharedScanQueryCase struct {
	name       string
	queryBlob  []byte
	clusterIDs []int64
	overlap    int
}

func setupSharedScanFixture(tb testing.TB, opts sharedScanOptions) *sharedScanSetup {
	tb.Helper()

	oldCacheBytes := cfg.Config.VectorIndex.CacheBytes
	cfg.Config.VectorIndex.CacheBytes = opts.cacheBytes
	tb.Cleanup(func() {
		cfg.Config.VectorIndex.CacheBytes = oldCacheBytes
	})

	tmpDir := tb.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(tb, err)
	require.NoError(tb, dbMgr.CreateDatabase(sharedScanDBName))

	vecMgr := db.NewVectorIndexManager(dbMgr)
	dbMgr.SetVectorIndexManager(vecMgr)

	engine := vecindex.NewEngine()
	db.SetVectorUDFProvider(engine)
	tb.Cleanup(func() { db.SetVectorUDFProvider(nil) })

	hook := db.NewEngineHook(engine, dbMgr)
	vecMgr.SetLifecycleHook(hook)
	vecMgr.SetEngineProvider(hook)
	vecMgr.SetReindexHook(hook)
	require.NoError(tb, vecMgr.Start(context.Background()))

	conn, err := dbMgr.GetDatabaseConnection(sharedScanDBName)
	require.NoError(tb, err)

	_, err = conn.Exec(`CREATE TABLE IF NOT EXISTS docs (
		id    INTEGER PRIMARY KEY,
		embed BLOB
	)`)
	require.NoError(tb, err)

	rng := rand.New(rand.NewSource(101))
	clusterCenters := make([][]float32, opts.clusters)
	for i := range clusterCenters {
		center := make([]float32, opts.dim)
		for j := range center {
			center[j] = float32(rng.NormFloat64())
		}
		clusterCenters[i] = sharedScanUnitNorm(center)
	}

	vectors := make([][]float32, opts.rows)
	for i := range vectors {
		center := clusterCenters[i%opts.clusters]
		vectors[i] = sharedScanAddNoise(center, rng, 0.08)
	}

	tx, err := conn.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(`INSERT INTO docs(id, embed) VALUES (?, ?)`)
	require.NoError(tb, err)
	for i, vec := range vectors {
		_, err = stmt.Exec(i+1, sharedScanFloat32sToBlob(vec))
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())

	localReader := db.NewLocalReader(dbMgr)
	nodeProvider := coordinator.NewMockNodeProvider([]uint64{1})
	rc := coordinator.NewReadCoordinator(1, nodeProvider, localReader, 10*time.Second)
	handler := coordinator.NewTestHandler(1, rc, dbMgr, clock)
	handler.SetVectorEngine(engine)

	meta := common.VectorIndexMeta{
		IndexName:  sharedScanIndexName,
		TableName:  "docs",
		ColumnName: "embed",
		Database:   sharedScanDBName,
		Metric:     "cosine",
		Dim:        opts.dim,
		Nlist:      opts.nlist,
		Nprobe:     opts.nprobe,
		Status:     "building",
		CreatedAt:  time.Now().UnixNano(),
	}
	require.NoError(tb, vecMgr.CreateIndex(context.Background(), meta))
	require.NoError(tb, sharedScanWaitIndexReady(conn, sharedScanIndexName, sharedScanReadyPoll))

	return &sharedScanSetup{
		dbMgr:   dbMgr,
		vecMgr:  vecMgr,
		engine:  engine,
		handler: handler,
		conn:    conn,
		vectors: vectors,
		opts:    opts,
	}
}

func newSharedScanSession(useCache bool) *protocol.ConnectionSession {
	vars := vecindex.DefaultVecSessionVars()
	vars.UseGoRank = true
	vars.UseCache = useCache
	vars.Fallback = false
	return &protocol.ConnectionSession{
		CurrentDatabase: sharedScanDBName,
		ConnID:          400,
		VecVars:         vars,
	}
}

func sharedScanStatement() protocol.Statement {
	return protocol.Statement{
		SQL:      sharedScanQuerySQL,
		Type:     protocol.StatementSelect,
		Database: sharedScanDBName,
	}
}

func rewriteSharedScanQuery(tb testing.TB, s *sharedScanSetup, queryBlob []byte, useCache bool) (*coordinator.RewriteInfo, []interface{}) {
	tb.Helper()

	stmt := sharedScanStatement()
	params := []interface{}{queryBlob, queryBlob}
	info, args, err := s.handler.MaybeRewriteVectorSelect(stmt, params, newSharedScanSession(useCache))
	require.NoError(tb, err)
	require.NotNil(tb, info)
	require.NotNil(tb, info.GoRank)
	return info, args
}

func runSharedScanQuery(tb testing.TB, s *sharedScanSetup, queryBlob []byte, useCache bool) []int64 {
	tb.Helper()

	stmt := sharedScanStatement()
	params := []interface{}{queryBlob, queryBlob}
	info, args, err := s.handler.MaybeRewriteVectorSelect(stmt, params, newSharedScanSession(useCache))
	require.NoError(tb, err)
	require.NotNil(tb, info)

	rs, err := s.handler.ExecuteVectorPlan(stmt, info, args, protocol.ConsistencyLocalOne)
	require.NoError(tb, err)
	require.NotNil(tb, rs)

	ids := make([]int64, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		if len(row) == 0 {
			continue
		}
		ids = append(ids, sharedScanToInt64(row[0]))
	}
	return ids
}

func buildSharedScanWorkload(tb testing.TB, s *sharedScanSetup, mode sharedScanWorkloadMode, want int) []sharedScanQueryCase {
	tb.Helper()

	require.Greater(tb, want, 0)

	rng := rand.New(rand.NewSource(202))
	pivotBlob := sharedScanFloat32sToBlob(sharedScanAddNoise(s.vectors[0], rng, 0.01))
	pivotInfo, _ := rewriteSharedScanQuery(tb, s, pivotBlob, true)
	pivotSet := make(map[int64]struct{}, len(pivotInfo.GoRank.ClusterIDs))
	for _, cid := range pivotInfo.GoRank.ClusterIDs {
		pivotSet[cid] = struct{}{}
	}

	overlapMin := max(2, len(pivotInfo.GoRank.ClusterIDs)-2)
	if overlapMin > len(pivotInfo.GoRank.ClusterIDs) {
		overlapMin = len(pivotInfo.GoRank.ClusterIDs)
	}

	cases := make([]sharedScanQueryCase, 0, want)
	seenNames := make(map[string]struct{}, want)
	if mode == sharedScanWorkloadOverlap {
		cases = append(cases, sharedScanQueryCase{
			name:       "pivot",
			queryBlob:  pivotBlob,
			clusterIDs: append([]int64(nil), pivotInfo.GoRank.ClusterIDs...),
			overlap:    len(pivotInfo.GoRank.ClusterIDs),
		})
		seenNames["pivot"] = struct{}{}
	}

	for i := 1; i < len(s.vectors) && len(cases) < want; i++ {
		queryVec := sharedScanAddNoise(s.vectors[i], rng, 0.01)
		queryBlob := sharedScanFloat32sToBlob(queryVec)
		info, _ := rewriteSharedScanQuery(tb, s, queryBlob, true)
		overlap := countClusterOverlap(pivotSet, info.GoRank.ClusterIDs)

		matches := false
		switch mode {
		case sharedScanWorkloadOverlap:
			matches = overlap >= overlapMin
		case sharedScanWorkloadDisjoint:
			matches = overlap <= 1
		default:
			tb.Fatalf("unknown workload mode %q", mode)
		}
		if !matches {
			continue
		}

		name := fmt.Sprintf("row_%d", i+1)
		if _, ok := seenNames[name]; ok {
			continue
		}
		seenNames[name] = struct{}{}
		cases = append(cases, sharedScanQueryCase{
			name:       name,
			queryBlob:  queryBlob,
			clusterIDs: append([]int64(nil), info.GoRank.ClusterIDs...),
			overlap:    overlap,
		})
	}

	require.Len(tb, cases, want, "need %d %s queries; got %d", want, mode, len(cases))
	return cases
}

func countClusterOverlap(pivot map[int64]struct{}, clusterIDs []int64) int {
	count := 0
	for _, cid := range clusterIDs {
		if _, ok := pivot[cid]; ok {
			count++
		}
	}
	return count
}

func uniqueClusterCount(cases []sharedScanQueryCase) int {
	seen := make(map[int64]struct{})
	for _, tc := range cases {
		for _, cid := range tc.clusterIDs {
			seen[cid] = struct{}{}
		}
	}
	return len(seen)
}

func waitForResidentPartitions(tb testing.TB, cache *vecindex.VectorCache, maxResident int) int {
	tb.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := cache.Partitions().EstimatedSize(); got <= maxResident {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cache.Partitions().EstimatedSize()
}

func sortedIDs(ids []int64) []int64 {
	out := append([]int64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sharedScanFloat32sToBlob(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

func sharedScanUnitNorm(v []float32) []float32 {
	var sum float32
	for _, x := range v {
		sum += x * x
	}
	inv := float32(1.0 / math.Sqrt(float64(sum)))
	for i := range v {
		v[i] *= inv
	}
	return v
}

func sharedScanAddNoise(v []float32, rng *rand.Rand, scale float32) []float32 {
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x + scale*float32(rng.NormFloat64())
	}
	return sharedScanUnitNorm(out)
}

func sharedScanWaitIndexReady(conn *sql.DB, indexName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := conn.QueryRow(
			`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`, indexName,
		).Scan(&status)
		if err == nil && status == "ready" {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("index %q did not become ready within %s", indexName, timeout)
}

func sharedScanToInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case []byte:
		if len(x) == 8 {
			return int64(binary.LittleEndian.Uint64(x))
		}
	}
	panic(fmt.Sprintf("unsupported integer type %T", v))
}

func TestSharedScanV1_OverlapAndDisjointMatchIndependentNoCache(t *testing.T) {
	opts := sharedScanOptions{
		dim:        32,
		rows:       2048,
		clusters:   64,
		nlist:      64,
		nprobe:     4,
		cacheBytes: 32 << 20,
	}
	s := setupSharedScanFixture(t, opts)

	workloads := []struct {
		name string
		mode sharedScanWorkloadMode
	}{
		{name: "overlap", mode: sharedScanWorkloadOverlap},
		{name: "disjoint", mode: sharedScanWorkloadDisjoint},
	}

	for _, workload := range workloads {
		workload := workload
		t.Run(workload.name, func(t *testing.T) {
			cases := buildSharedScanWorkload(t, s, workload.mode, 8)

			for _, tc := range cases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					info, _ := rewriteSharedScanQuery(t, s, tc.queryBlob, true)
					rowids, hit := s.handler.CacheRankForTest(info.GoRank)
					require.True(t, hit, "cache path must be available for %s", tc.name)
					require.NotEmpty(t, rowids)

					cacheIDs := runSharedScanQuery(t, s, tc.queryBlob, true)
					noCacheIDs := runSharedScanQuery(t, s, tc.queryBlob, false)

					require.Equal(t, noCacheIDs, cacheIDs, "cache and independent no-cache results must match exactly")
					require.Equal(t, sortedIDs(noCacheIDs), sortedIDs(rowids), "cacheRank test hook must expose the same top-k set")
				})
			}
		})
	}
}

func TestSharedScanV1_LowMemoryGuardrails(t *testing.T) {
	opts := sharedScanOptions{
		dim:        32,
		rows:       2048,
		clusters:   64,
		nlist:      64,
		nprobe:     4,
		cacheBytes: 24 << 10,
	}
	s := setupSharedScanFixture(t, opts)
	cases := buildSharedScanWorkload(t, s, sharedScanWorkloadDisjoint, 12)

	cache := s.engine.LookupCache(sharedScanIndexName)
	require.NotNil(t, cache)
	require.NotNil(t, cache.Partitions())

	for _, tc := range cases {
		cacheIDs := runSharedScanQuery(t, s, tc.queryBlob, true)
		noCacheIDs := runSharedScanQuery(t, s, tc.queryBlob, false)
		require.Equal(t, noCacheIDs, cacheIDs, "bounded cache must not change ranking correctness")
	}

	touchedClusters := uniqueClusterCount(cases)
	resident := waitForResidentPartitions(t, cache, 8)

	require.Greater(t, touchedClusters, 16, "disjoint workload must touch many distinct partitions")
	require.LessOrEqual(t, resident, 8, "resident partitions must stay bounded under a %d-byte cache budget", opts.cacheBytes)
	require.Less(t, resident, touchedClusters, "resident set must stay smaller than the touched working set")
}
