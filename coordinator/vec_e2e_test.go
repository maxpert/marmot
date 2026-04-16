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

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

const (
	e2eDim       = 32
	e2eNlist     = 16
	e2eNprobe    = 12
	e2eNRows     = 1000
	e2eDBName    = "e2edb"
	e2eIndexName = "embeddings"
	e2eReadyPoll = 5 * time.Second
)

// float32sToBlob encodes a float32 slice as little-endian bytes.
func float32sToBlob(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// blobToFloat32s decodes little-endian bytes to float32 slice.
func blobToFloat32s(b []byte) []float32 {
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return v
}

// unitNorm normalises v in place to unit L2 norm; returns v.
func unitNorm(v []float32) []float32 {
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

// cosineDistance returns 1 − cosine_similarity for unit-norm vectors.
func cosineDistance(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return 1 - dot
}

// addNoise returns a unit-normalised copy of v with small Gaussian noise added.
func addNoise(v []float32, rng *rand.Rand, scale float32) []float32 {
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x + scale*float32(rng.NormFloat64())
	}
	return unitNorm(out)
}

// e2eSetup holds all objects created for the E2E test.
type e2eSetup struct {
	dbMgr   *db.DatabaseManager
	vecMgr  *db.VectorIndexManager
	engine  *vecindex.Engine
	handler *coordinator.CoordinatorHandler
	conn    *sql.DB
	vectors [][]float32 // indexed 0..999, ID = i+1
}

func setupVecE2E(t *testing.T) *e2eSetup {
	t.Helper()

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err, "NewDatabaseManager")

	// Create the test database.
	require.NoError(t, dbMgr.CreateDatabase(e2eDBName))

	// Wire the vector index manager.
	vecMgr := db.NewVectorIndexManager(dbMgr)
	dbMgr.SetVectorIndexManager(vecMgr)

	engine := vecindex.NewEngine()
	db.SetVectorUDFProvider(engine)

	hook := db.NewEngineHook(engine, dbMgr)
	vecMgr.SetLifecycleHook(hook)
	vecMgr.SetEngineProvider(hook)
	vecMgr.SetReindexHook(hook)

	require.NoError(t, vecMgr.Start(context.Background()))

	// Get the raw SQL connection to create DDL and insert rows.
	conn, err := dbMgr.GetDatabaseConnection(e2eDBName)
	require.NoError(t, err)

	_, err = conn.Exec(`CREATE TABLE IF NOT EXISTS docs (
		id     INTEGER PRIMARY KEY,
		embed  BLOB,
		status TEXT
	)`)
	require.NoError(t, err)

	// Generate 1000 deterministic unit-norm vectors.
	rng := rand.New(rand.NewSource(42))
	vectors := make([][]float32, e2eNRows)
	for i := range vectors {
		v := make([]float32, e2eDim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vectors[i] = unitNorm(v)
	}

	// Insert rows: IDs 1..1000; 950 published, 50 rare.
	stmt, err := conn.Prepare(`INSERT INTO docs(id, embed, status) VALUES (?, ?, ?)`)
	require.NoError(t, err)
	defer stmt.Close()

	for i, v := range vectors {
		id := i + 1
		status := "published"
		if i >= 950 {
			status = "rare"
		}
		_, err = stmt.Exec(id, float32sToBlob(v), status)
		require.NoError(t, err, "insert id=%d", id)
	}

	// Build a CoordinatorHandler that can run local reads.
	// The read path goes: handler.handleRead → readCoord.ReadTransaction →
	// LocalReader.ReadSnapshot → ReplicatedDatabase.ExecuteSnapshotRead.
	localReader := db.NewLocalReader(dbMgr)
	nodeProvider := coordinator.NewMockNodeProvider([]uint64{1})
	rc := coordinator.NewReadCoordinator(1, nodeProvider, localReader, 10*time.Second)
	handler := coordinator.NewTestHandler(1, rc, dbMgr, clock)
	handler.SetVectorEngine(engine)

	// Create the vector index (trains k-means + populates members).
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
	ctx := context.Background()
	require.NoError(t, vecMgr.CreateIndex(ctx, meta), "CreateIndex")

	// Wait for index to be 'ready'.
	require.NoError(t, waitIndexReady(conn, e2eIndexName, e2eReadyPoll))

	return &e2eSetup{
		dbMgr:   dbMgr,
		vecMgr:  vecMgr,
		engine:  engine,
		handler: handler,
		conn:    conn,
		vectors: vectors,
	}
}

// waitIndexReady polls __marmot_vector_indexes until status='ready' or timeout.
func waitIndexReady(conn *sql.DB, indexName string, timeout time.Duration) error {
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

// runVecQuery issues a vec_match / vec_distance query through the handler's
// rewrite + execute path. qVec is passed as a bind parameter.
func runVecQuery(t *testing.T, s *e2eSetup, sqlTpl string, qVec []byte, session *protocol.ConnectionSession) []int64 {
	t.Helper()

	// The SQL template uses two '?' placeholders: one for vec_match and one for
	// vec_distance. Both receive the same query vector.
	params := []interface{}{qVec, qVec}

	stmt := protocol.Statement{
		SQL:      sqlTpl,
		Type:     protocol.StatementSelect,
		Database: e2eDBName,
	}

	info, rewrittenArgs, err := s.handler.MaybeRewriteVectorSelect(stmt, params, session)
	require.NoError(t, err, "MaybeRewriteVectorSelect")
	require.NotNil(t, info, "expected rewrite info (is engine installed?)")

	rs, err := s.handler.ExecuteVectorPlan(stmt, info, rewrittenArgs, protocol.ConsistencyLocalOne)
	require.NoError(t, err, "executeVectorPlan")
	require.NotNil(t, rs)

	ids := make([]int64, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		if len(row) == 0 {
			continue
		}
		id := toInt64(row[0])
		ids = append(ids, id)
	}
	return ids
}

func toInt64(v interface{}) int64 {
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
	return 0
}

// publishedIDs builds a set of IDs that have status='published' (1..950).
func publishedIDSet() map[int64]bool {
	s := make(map[int64]bool, 950)
	for i := 1; i <= 950; i++ {
		s[int64(i)] = true
	}
	return s
}

// rareIDSet builds a set of IDs that have status='rare' (951..1000).
func rareIDSet() map[int64]bool {
	s := make(map[int64]bool, 50)
	for i := 951; i <= 1000; i++ {
		s[int64(i)] = true
	}
	return s
}

func newE2ESession() *protocol.ConnectionSession {
	sess := &protocol.ConnectionSession{
		CurrentDatabase: e2eDBName,
		ConnID:          99,
		VecVars:         vecindex.DefaultVecSessionVars(),
	}
	return sess
}

// TestVecRewriteE2E exercises the full rewrite → execute path against real SQLite.
func TestVecRewriteE2E(t *testing.T) {
	s := setupVecE2E(t)
	pubSet := publishedIDSet()
	rareSet := rareIDSet()

	// Use first inserted vector as a query basis (minor noise added for realism).
	rng := rand.New(rand.NewSource(7))
	queryVec := addNoise(s.vectors[0], rng, 0.01)
	qBlob := float32sToBlob(queryVec)

	const postFilterSQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10) AND status = 'published'
ORDER BY vec_distance(embed, ?) LIMIT 10`

	t.Run("PostFilter_HappyPath_K10", func(t *testing.T) {
		sess := newE2ESession()
		// auto plan + fallback off → post-filter (predicate present, large table relative to K)
		sess.VecVars.Fallback = false

		ids := runVecQuery(t, s, postFilterSQL, qBlob, sess)
		require.Len(t, ids, 10, "expected exactly 10 rows")
		for _, id := range ids {
			require.True(t, pubSet[id], "id=%d must be published", id)
		}
	})

	t.Run("PreFilter_ForcedPlan", func(t *testing.T) {
		sess := newE2ESession()
		sess.VecVars.ForcePlan = vecindex.ForcePlanPre
		sess.VecVars.Fallback = false

		ids := runVecQuery(t, s, postFilterSQL, qBlob, sess)
		require.Len(t, ids, 10, "expected exactly 10 rows with pre-filter plan")
		for _, id := range ids {
			require.True(t, pubSet[id], "id=%d must be published", id)
		}
	})

	t.Run("Fallback_NarrowPredicate", func(t *testing.T) {
		sess := newE2ESession()
		sess.VecVars.ForcePlan = vecindex.ForcePlanAuto
		sess.VecVars.Fallback = true

		// Query for 'rare' rows — only 50 exist; K=20 means post-filter may find <20
		// triggering the fallback pre-filter path.
		const rareSQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 20) AND status = 'rare'
ORDER BY vec_distance(embed, ?) LIMIT 20`

		// Use a query vector close to one of the rare vectors (id=951, index 950).
		rareQuery := addNoise(s.vectors[950], rng, 0.01)
		rareBlob := float32sToBlob(rareQuery)

		ids := runVecQuery(t, s, rareSQL, rareBlob, sess)
		require.NotEmpty(t, ids, "fallback must return results for narrow predicate")
		require.LessOrEqual(t, len(ids), 20, "cannot exceed K=20")
		for _, id := range ids {
			require.True(t, rareSet[id], "id=%d must be rare", id)
		}
	})

	t.Run("RecallSanity", func(t *testing.T) {
		// Average recall@10 across multiple queries. Averaging smooths the
		// variance introduced by non-deterministic k-means seeding so the
		// bar is meaningful rather than lucky-seed-dependent.
		recallRng := rand.New(rand.NewSource(17))
		const nQueries = 20

		sess := newE2ESession()
		sess.VecVars.Fallback = false

		const noPredicateSQL = `SELECT id FROM docs
WHERE vec_match(embed, ?, 10)
ORDER BY vec_distance(embed, ?) LIMIT 10`

		type dist struct {
			id int64
			d  float32
		}

		totalHits := 0
		for q := 0; q < nQueries; q++ {
			targetIdx := recallRng.Intn(len(s.vectors))
			qv := addNoise(s.vectors[targetIdx], recallRng, 0.005)
			qb := float32sToBlob(qv)

			dists := make([]dist, len(s.vectors))
			for i, v := range s.vectors {
				dists[i] = dist{id: int64(i + 1), d: cosineDistance(qv, v)}
			}
			sort.Slice(dists, func(i, j int) bool { return dists[i].d < dists[j].d })
			groundTruth := make(map[int64]bool, 10)
			for _, d := range dists[:10] {
				groundTruth[d.id] = true
			}

			ids := runVecQuery(t, s, noPredicateSQL, qb, sess)
			require.Len(t, ids, 10, "expected 10 results for recall check (q=%d)", q)
			for _, id := range ids {
				if groundTruth[id] {
					totalHits++
				}
			}
		}

		recall := float64(totalHits) / float64(nQueries*10)
		t.Logf("Mean Recall@10 over %d queries = %.3f (%d/%d hits)",
			nQueries, recall, totalHits, nQueries*10)
		require.GreaterOrEqual(t, recall, 0.95,
			"mean recall must be >= 0.95 (dim=%d nlist=%d nprobe=%d)",
			e2eDim, e2eNlist, e2eNprobe)
	})
}
