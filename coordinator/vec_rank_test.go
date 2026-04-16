//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"math/rand"
	"sort"
	"testing"

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

// TestGoRank_HappyPath_MatchesSQL_UDF runs the same query with Go-rank and
// SQL-UDF paths and asserts both return the same set of IDs (order may differ
// on tied distances so we compare sorted sets).
func TestGoRank_HappyPath_MatchesSQL_UDF(t *testing.T) {
	s := setupVecE2E(t)

	// Use zero scale to get an exact copy of the vector.
	rng := rand.New(rand.NewSource(42))
	qv := addNoise(s.vectors[5], rng, 0)
	qb := float32sToBlob(qv)

	sessGoRank := newGoRankSession(true)
	sessUDF := newGoRankSession(false)

	goRankIDs := runVecQuery(t, s, rankQuerySQL, qb, sessGoRank)
	udfIDs := runVecQuery(t, s, rankQuerySQL, qb, sessUDF)

	require.NotEmpty(t, goRankIDs, "go-rank: expected non-empty results")
	require.Equal(t, len(udfIDs), len(goRankIDs), "result count must match")

	sortedGR := make([]int64, len(goRankIDs))
	copy(sortedGR, goRankIDs)
	sort.Slice(sortedGR, func(i, j int) bool { return sortedGR[i] < sortedGR[j] })

	sortedUDF := make([]int64, len(udfIDs))
	copy(sortedUDF, udfIDs)
	sort.Slice(sortedUDF, func(i, j int) bool { return sortedUDF[i] < sortedUDF[j] })

	require.Equal(t, sortedUDF, sortedGR, "go-rank and UDF must return the same IDs")
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
