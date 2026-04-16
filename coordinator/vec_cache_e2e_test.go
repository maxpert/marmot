//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// newCacheSession returns a session with cache and go-rank toggles configured.
func newCacheSession(useCache, useGoRank bool) *protocol.ConnectionSession {
	vars := vecindex.DefaultVecSessionVars()
	vars.UseCache = useCache
	vars.UseGoRank = useGoRank
	vars.Fallback = false
	return &protocol.ConnectionSession{
		CurrentDatabase: e2eDBName,
		ConnID:          300,
		VecVars:         vars,
	}
}

// TestCache_ParityWithUDF asserts the cache-ranking path returns the same
// top-K set as the SQL-UDF path across a spread of queries (task #16).
func TestCache_ParityWithUDF(t *testing.T) {
	s := setupVecE2E(t)

	const nQueries = 20
	rng := rand.New(rand.NewSource(1234))

	for q := 0; q < nQueries; q++ {
		qv := addNoise(s.vectors[rng.Intn(len(s.vectors))], rng, 0.02)
		qb := float32sToBlob(qv)

		cacheIDs := runVecQuery(t, s, rankQuerySQL, qb, newCacheSession(true, true))
		udfIDs := runVecQuery(t, s, rankQuerySQL, qb, newCacheSession(false, false))

		require.NotEmpty(t, cacheIDs, "query %d: cache path produced no rows", q)
		require.Equal(t, len(udfIDs), len(cacheIDs),
			"query %d: cache returned %d, udf returned %d", q, len(cacheIDs), len(udfIDs))

		sortedC := append([]int64(nil), cacheIDs...)
		sortedU := append([]int64(nil), udfIDs...)
		sort.Slice(sortedC, func(i, j int) bool { return sortedC[i] < sortedC[j] })
		sort.Slice(sortedU, func(i, j int) bool { return sortedU[i] < sortedU[j] })
		require.Equal(t, sortedU, sortedC, "query %d: cache vs udf result-set mismatch", q)
	}
}

// TestCache_FallsThroughWhenDisabled asserts that the cache session var
// cleanly disables the cache path and the SQL candidate scan still produces
// valid top-K results (task #16 session toggle).
func TestCache_FallsThroughWhenDisabled(t *testing.T) {
	s := setupVecE2E(t)

	rng := rand.New(rand.NewSource(7))
	qv := addNoise(s.vectors[3], rng, 0)
	qb := float32sToBlob(qv)

	// Cache OFF + GoRank ON: uses SQL candidate scan.
	ids := runVecQuery(t, s, rankQuerySQL, qb, newCacheSession(false, true))
	require.NotEmpty(t, ids)
}

// TestCache_EpochMismatchFallsThrough simulates a plan whose cluster IDs were
// captured under an older probe epoch (e.g., search that overlapped a
// REINDEX). The cache path must detect the epoch mismatch and fall back to
// SQL rather than indexing stale cluster IDs into a freshly-rebuilt cache
// (task #16 HIGH-2 fix).
func TestCache_EpochMismatchFallsThrough(t *testing.T) {
	s := setupVecE2E(t)

	rng := rand.New(rand.NewSource(41))
	qv := addNoise(s.vectors[7], rng, 0.01)
	qb := float32sToBlob(qv)

	params := []interface{}{qb, qb}
	stmt := protocol.Statement{
		SQL:      rankQuerySQL,
		Type:     protocol.StatementSelect,
		Database: e2eDBName,
	}

	info, _, err := s.handler.MaybeRewriteVectorSelect(stmt, params, newCacheSession(true, true))
	require.NoError(t, err)
	require.NotNil(t, info)
	require.NotNil(t, info.GoRank)
	require.NotZero(t, info.GoRank.ProbeEpoch, "rewriter must capture the probe epoch")

	// Happy path: plan epoch matches cache epoch → cache ranks and returns K rows.
	_, hit := s.handler.CacheRankForTest(info.GoRank)
	require.True(t, hit, "cache path must be used when epochs match")

	// Simulate a post-REINDEX scenario by mutating the plan's ProbeEpoch to
	// something the cache does not have. cacheRank must refuse.
	stale := *info.GoRank
	stale.ProbeEpoch = info.GoRank.ProbeEpoch + 99
	_, hit = s.handler.CacheRankForTest(&stale)
	require.False(t, hit, "cache path must fall through on epoch mismatch")

	// Zero-epoch (legacy provider path) is also a miss.
	zero := *info.GoRank
	zero.ProbeEpoch = 0
	_, hit = s.handler.CacheRankForTest(&zero)
	require.False(t, hit, "zero probe epoch must be treated as a miss")
}

// TestCache_ConcurrentREINDEX runs cache-path queries in a tight loop on
// multiple goroutines while REINDEX fires, asserting the epoch guard (HIGH-2
// regression) prevents stale cluster IDs from indexing a freshly-rebuilt
// cache. Every query must return K valid, real rowids; no panics; no empty
// result sets; no error surface. Recall may drop during swap but correctness
// (results are valid neighbors) must hold.
func TestCache_ConcurrentREINDEX(t *testing.T) {
	s := setupVecE2E(t)

	const (
		workers     = 4
		k           = 10
		maxRowID    = e2eNRows
		testWindow  = 1500 * time.Millisecond
		queryPeriod = 2 * time.Millisecond
	)

	ctx, cancel := context.WithTimeout(context.Background(), testWindow+5*time.Second)
	defer cancel()

	qCtx, qCancel := context.WithCancel(ctx)
	defer qCancel()

	var (
		queriesRun atomic.Int64
		queryFails atomic.Int64
		badRowID   atomic.Int64
		emptySets  atomic.Int64
	)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			sess := newCacheSession(true, true)

			for {
				select {
				case <-qCtx.Done():
					return
				default:
				}

				qv := addNoise(s.vectors[rng.Intn(len(s.vectors))], rng, 0.01)
				qb := float32sToBlob(qv)
				params := []interface{}{qb, qb}
				stmt := protocol.Statement{
					SQL:      rankQuerySQL,
					Type:     protocol.StatementSelect,
					Database: e2eDBName,
				}

				info, args, err := s.handler.MaybeRewriteVectorSelect(stmt, params, sess)
				if err != nil || info == nil {
					queryFails.Add(1)
					continue
				}
				rs, err := s.handler.ExecuteVectorPlan(stmt, info, args, protocol.ConsistencyLocalOne)
				if err != nil {
					queryFails.Add(1)
					continue
				}
				queriesRun.Add(1)

				if rs == nil || len(rs.Rows) == 0 {
					emptySets.Add(1)
					continue
				}
				for _, row := range rs.Rows {
					if len(row) == 0 {
						badRowID.Add(1)
						break
					}
					id := toInt64(row[0])
					if id < 1 || id > int64(maxRowID) {
						badRowID.Add(1)
						break
					}
				}
				time.Sleep(queryPeriod)
			}
		}(int64(w * 9973))
	}

	// Let a handful of queries hit the steady-state cache first.
	time.Sleep(50 * time.Millisecond)

	// Trigger REINDEX mid-load. This exercises:
	//   - new cache install  -> epoch bump
	//   - probe swap         -> old-plan cluster IDs no longer match
	// Readers in flight with old-epoch plans must fall through to SQL rather
	// than index stale cluster IDs into the post-swap cache.
	reindexErr := s.vecMgr.ReindexIndex(ctx, e2eIndexName)
	require.NoError(t, reindexErr, "REINDEX must complete cleanly under load")

	// Give a few more cycles after swap so both old-plan and new-plan readers
	// are observed post-swap.
	time.Sleep(200 * time.Millisecond)

	qCancel()
	wg.Wait()

	t.Logf("queries=%d fails=%d empty=%d bad_rowid=%d",
		queriesRun.Load(), queryFails.Load(), emptySets.Load(), badRowID.Load())

	require.Greater(t, queriesRun.Load(), int64(50), "not enough queries ran to exercise the race")
	require.Zero(t, queryFails.Load(), "no rewrite/execute errors expected under concurrent REINDEX")
	require.Zero(t, emptySets.Load(), "no empty result sets — epoch guard must keep cacheRank coherent or fall through to SQL")
	require.Zero(t, badRowID.Load(), "returned rowids must all be valid IDs in [1,%d]", maxRowID)
}
