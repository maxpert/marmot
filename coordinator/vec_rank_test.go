//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

const rankQuerySQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

const rankQueryWithPredSQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10) AND status = 'published'
ORDER BY vec_distance(embed, ?) LIMIT 10`

func newGoRankSession(useGoRank bool) *protocol.ConnectionSession {
	vars := vecindex.DefaultVecSessionVars()
	vars.UseGoRank = useGoRank
	vars.Fallback = false
	return &protocol.ConnectionSession{
		CurrentDatabase: e2eDBName,
		ConnID:          200,
		VecVars:         vars,
	}
}

// TestGoRank_HappyPath_ReturnsNearestNeighbors checks the active segment/overlay
// path against an exact brute-force baseline.
func TestGoRank_HappyPath_ReturnsNearestNeighbors(t *testing.T) {
	s := setupVecE2E(t)

	// Use zero scale to get an exact copy of the vector.
	rng := rand.New(rand.NewSource(42))
	qv := addNoise(s.vectors[5], rng, 0)
	qb := float32sToBlob(qv)

	ids := runVecQuery(t, s, rankQuerySQL, qb, newGoRankSession(true))
	require.NotEmpty(t, ids, "go-rank: expected non-empty results")

	truth := bruteForceTopK(qv, s.vectors, len(ids))
	require.Equal(t, truth, ids, "go-rank should match the exact nearest neighbors for an exact query vector")
}

// TestGoRank_WithUserPredicate asserts that a Go-rank query with AND status='published'
// only returns published rows.
func TestGoRank_WithUserPredicate(t *testing.T) {
	s := setupVecE2E(t)

	rng := rand.New(rand.NewSource(13))
	qv := addNoise(s.vectors[10], rng, 0)
	qb := float32sToBlob(qv)

	sess := newGoRankSession(true)
	ids := runVecQuery(t, s, rankQueryWithPredSQL, qb, sess)

	require.NotEmpty(t, ids, "expected non-empty results with predicate")

	pubSet := publishedIDSet()
	for _, id := range ids {
		require.True(t, pubSet[id], "id=%d must be published", id)
	}
}

func TestGoRank_WithUserPredicate_ExactRerankFillsLimit(t *testing.T) {
	s := setupVecE2E(t)

	qv := append([]float32(nil), s.vectors[0]...)
	qb := float32sToBlob(qv)

	type dist struct {
		id int64
		d  float32
	}
	dists := make([]dist, len(s.vectors))
	for i, v := range s.vectors {
		dists[i] = dist{id: int64(i + 1), d: cosineDistance(qv, v)}
	}
	sort.Slice(dists, func(i, j int) bool { return dists[i].d < dists[j].d })

	stmt, err := s.conn.Prepare(`UPDATE docs SET status = ? WHERE id = ?`)
	require.NoError(t, err)
	defer stmt.Close()

	for _, item := range dists[:80] {
		_, err := stmt.Exec("draft", item.id)
		require.NoError(t, err)
	}
	for _, item := range dists[80:90] {
		_, err := stmt.Exec("published", item.id)
		require.NoError(t, err)
	}

	ids := runVecQuery(t, s, rankQueryWithPredSQL, qb, newGoRankSession(true))

	require.Len(t, ids, 10, "expected exact rerank to refill the post-filter top-k")

	expected := make([]int64, 10)
	for i, item := range dists[80:90] {
		expected[i] = item.id
	}
	require.Equal(t, expected, ids, "post-filter exact rerank should skip predicate-mismatched nearest rows")
}

// TestGoRank_Recall asserts recall >= 0.95 for Go-rank queries over 1000 rows.
func TestGoRank_Recall(t *testing.T) {
	s := setupVecE2E(t)
	sess := newGoRankSession(true)

	const k = 10
	const nQueries = 50

	totalHits := 0
	for q := 0; q < nQueries; q++ {
		target := s.vectors[q%len(s.vectors)]
		qb := float32sToBlob(target)

		groundTruth := bruteForceTopK(target, s.vectors, k)
		truthSet := make(map[int64]bool, k)
		for _, id := range groundTruth {
			truthSet[id] = true
		}

		ids := runVecQuery(t, s, rankQuerySQL, qb, sess)
		for _, id := range ids {
			if truthSet[id] {
				totalHits++
			}
		}
	}

	recall := float64(totalHits) / float64(nQueries*k)
	t.Logf("GoRank recall@%d over %d queries = %.4f (%d/%d hits)", k, nQueries, recall, totalHits, nQueries*k)
	require.GreaterOrEqual(t, recall, 0.95, "go-rank recall@%d must be >= 0.95 (got %.4f)", k, recall)
}

func TestGoRank_DeltaOnlyAfterEmptyCreate(t *testing.T) {
	requireVectorBenchTests(t, "empty-create delta vector integration test")

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	require.NoError(t, dbMgr.CreateDatabase(e2eDBName))

	vecMgr := db.NewVectorIndexManager(dbMgr)
	dbMgr.SetVectorIndexManager(vecMgr)

	engine := vecindex.NewEngine()
	db.SetVectorUDFProvider(engine)
	t.Cleanup(func() { db.SetVectorUDFProvider(nil) })

	hook := db.NewEngineHook(engine, dbMgr)
	hook.BindVectorIndexManager(vecMgr)
	vecMgr.SetLifecycleHook(hook)
	vecMgr.SetEngineProvider(hook)
	vecMgr.SetReindexHook(hook)
	require.NoError(t, vecMgr.Start(context.Background()))

	conn, err := dbMgr.GetDatabaseConnection(e2eDBName)
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE IF NOT EXISTS docs (
		id     INTEGER PRIMARY KEY,
		embed  BLOB,
		status TEXT
	)`)
	require.NoError(t, err)

	localReader := db.NewLocalReader(dbMgr)
	nodeProvider := coordinator.NewMockNodeProvider([]uint64{1})
	rc := coordinator.NewReadCoordinator(1, nodeProvider, localReader, 10*time.Second)
	localReplicator := db.NewLocalReplicator(1, dbMgr, clock)
	writeCoord := coordinator.NewWriteCoordinator(1, nodeProvider, localReplicator, localReplicator, 5*time.Second, clock)
	handler := coordinator.NewCoordinatorHandler(1, writeCoord, rc, clock, dbMgr, nil, nil, nil)
	handler.SetVectorEngine(engine)

	meta := common.VectorIndexMeta{
		IndexName:  e2eIndexName,
		TableName:  "docs",
		ColumnName: "embed",
		Database:   e2eDBName,
		Metric:     "cosine",
		Dim:        e2eDim,
		Nlist:      e2eNlist,
		Nprobe:     e2eNprobe,
		Status:     "building",
		CreatedAt:  time.Now().UnixNano(),
	}
	require.NoError(t, vecMgr.CreateIndex(context.Background(), meta))
	require.NoError(t, waitIndexReady(conn, e2eIndexName, e2eReadyPoll))

	var status string
	require.NoError(t, conn.QueryRow(
		`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`, e2eIndexName,
	).Scan(&status))
	require.Equal(t, "ready", status)

	rng := rand.New(rand.NewSource(91))
	const bootstrapRows = 4096
	vectors := make([][]float32, bootstrapRows)
	for i := range vectors {
		v := make([]float32, e2eDim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vectors[i] = unitNorm(v)
		_, err := handler.HandleQuery(
			newGoRankSession(true),
			`INSERT INTO docs(id, embed, status) VALUES (?, ?, 'published')`,
			[]interface{}{i + 1, float32sToBlob(vectors[i])},
		)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		state, ok := engine.Lookup(e2eIndexName)
		return ok && state.ProbeVersion() > 0 && state.LoadSegmentStore() != nil
	}, 30*time.Second, 100*time.Millisecond, "empty-table create should bootstrap centroids automatically after inserts")

	s := &e2eSetup{
		dbMgr:   dbMgr,
		vecMgr:  vecMgr,
		engine:  engine,
		handler: handler,
		conn:    conn,
		vectors: vectors,
	}

	qb := float32sToBlob(vectors[0])
	goRankIDs := runVecQuery(t, s, rankQuerySQL, qb, newGoRankSession(true))

	require.NotEmpty(t, goRankIDs, "go-rank should return rows after automatic bootstrap")
	truth := bruteForceTopK(vectors[0], vectors, len(goRankIDs))
	require.Equal(t, truth, goRankIDs)
}
