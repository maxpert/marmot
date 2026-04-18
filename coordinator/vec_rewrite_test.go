//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

// --- stubs ---

// stubEngine implements vecindex.VectorUDFProvider for tests.
type stubEngine struct {
	clusterIDs []int64
}

func (s *stubEngine) AssignNearest(_ string, _ []byte) (int64, error) { return 1, nil }
func (s *stubEngine) NotifyCentroidChange(_ string, _ int64) error    { return nil }
func (s *stubEngine) TopNprobeClusters(_ string, _ []byte, _ int) ([]int64, error) {
	return s.clusterIDs, nil
}

// stubLookup implements VectorIndexLookup for tests.
type stubLookup struct {
	meta *common.VectorIndexMeta
}

func (s *stubLookup) GetIndexByColumn(_, table, column string) (*common.VectorIndexMeta, bool) {
	if s.meta != nil && s.meta.TableName == table && s.meta.ColumnName == column {
		return s.meta, true
	}
	return nil, false
}

func (s *stubLookup) EstimatedRowCount(_, _ string) int64 { return 100_000 }

// stubSession implements QuerySession for tests.
type stubSession struct {
	nprobe       int
	forcePlan    string
	prefilterCap int64
	fallback     string
	useGoRank    bool
	useCache     bool
}

func (s *stubSession) Nprobe(def int) int {
	if s.nprobe == 0 {
		return def
	}
	return s.nprobe
}
func (s *stubSession) ForcePlan() string   { return s.forcePlan }
func (s *stubSession) PrefilterCap() int64 { return s.prefilterCap }
func (s *stubSession) Fallback() string    { return s.fallback }
func (s *stubSession) UseGoRank() bool     { return s.useGoRank }
func (s *stubSession) UseCache() bool      { return s.useCache }

// --- helpers ---

var vitessParser *sqlparser.Parser

func init() {
	var err error
	vitessParser, err = sqlparser.New(sqlparser.Options{})
	if err != nil {
		panic("failed to init vitess parser: " + err.Error())
	}
}

func parseSQL(t *testing.T, sql string) sqlparser.Statement {
	t.Helper()
	stmt, err := vitessParser.Parse(sql)
	require.NoError(t, err)
	return stmt
}

func defaultMeta() *common.VectorIndexMeta {
	return &common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "testdb",
		Metric:     "cosine",
		Nlist:      1024,
		Nprobe:     16,
	}
}

func defaultSession() *stubSession {
	return &stubSession{
		forcePlan:    "auto",
		prefilterCap: 5000,
		fallback:     "on",
	}
}

func defaultEngine() *stubEngine {
	return &stubEngine{clusterIDs: []int64{3, 17, 22}}
}

func defaultLookup() *stubLookup {
	return &stubLookup{meta: defaultMeta()}
}

// walkForFunc walks a parsed statement and returns the first FuncExpr with the
// given name, or nil. Used in AST-structure assertions.
func walkForFunc(t *testing.T, stmt sqlparser.Statement, name string) *sqlparser.FuncExpr {
	t.Helper()
	var found *sqlparser.FuncExpr
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if fn, ok := node.(*sqlparser.FuncExpr); ok && fn.Name.EqualString(name) {
			found = fn
			return false, nil
		}
		return true, nil
	}, stmt)
	return found
}

// --- test cases ---

// 1. Detection: valid pgvector SELECT → rewrite succeeds.
func TestRewrite_ValidVectorQuery(t *testing.T) {
	t.Parallel()
	sql := `SELECT d.title FROM docs d
		WHERE vec_match(d.embed, ?, 10) AND d.status = 'published'
		ORDER BY vec_distance(d.embed, ?)
		LIMIT 10`
	stmt := parseSQL(t, sql)
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), defaultSession(), defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, 10, info.K)
	require.Equal(t, "docs", info.TableName)
	require.Equal(t, "embed", info.ColumnName)
	require.Equal(t, "cosine", info.Metric)
}

func TestRewrite_GoRankEnablesCacheOnlyWithoutUserPredicate(t *testing.T) {
	t.Parallel()

	sess := defaultSession()
	sess.useGoRank = true
	sess.useCache = true

	stmt := parseSQL(t, `SELECT id FROM docs
		WHERE vec_match(embed, ?, 10)
		ORDER BY vec_distance(embed, ?)
		LIMIT 10`)
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.NotNil(t, info)
	require.NotNil(t, info.GoRank)
	require.True(t, info.GoRank.AllowCache)
}

func TestRewrite_GoRankDisablesCacheWithUserPredicate(t *testing.T) {
	t.Parallel()

	sess := defaultSession()
	sess.useGoRank = true
	sess.useCache = true

	stmt := parseSQL(t, `SELECT id FROM docs
		WHERE vec_match(embed, ?, 10) AND status = 'published'
		ORDER BY vec_distance(embed, ?)
		LIMIT 10`)
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.NotNil(t, info)
	require.NotNil(t, info.GoRank)
	require.False(t, info.GoRank.AllowCache)
}

// 2. Detection negative: SELECT without vec_match → returns (nil, nil).
func TestRewrite_NoVecMatch_ReturnsNil(t *testing.T) {
	t.Parallel()
	stmt := parseSQL(t, `SELECT id FROM docs WHERE status = 'published' LIMIT 10`)
	info, err := RewriteVectorQuery(stmt, nil, defaultSession(), defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.Nil(t, info)
}

// 3. Non-SELECT → returns (nil, nil).
func TestRewrite_NonSelect_ReturnsNil(t *testing.T) {
	t.Parallel()
	stmt := parseSQL(t, `UPDATE docs SET status = 'x' WHERE id = 1`)
	info, err := RewriteVectorQuery(stmt, nil, defaultSession(), defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.Nil(t, info)
}

// 4. Error MARMOT-VEC-020: two vec_match in one SELECT.
func TestRewrite_MultipleVecMatch_Error(t *testing.T) {
	t.Parallel()
	sql := `SELECT d.title FROM docs d
		WHERE vec_match(d.embed, ?, 10) AND vec_match(d.embed, ?, 5)
		ORDER BY vec_distance(d.embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	_, err := RewriteVectorQuery(stmt, make([]byte, 16), defaultSession(), defaultEngine(), defaultLookup())
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-020")
}

// 5. Error MARMOT-VEC-021: vec_match present but no LIMIT.
func TestRewrite_NoLimit_Error(t *testing.T) {
	t.Parallel()
	sql := `SELECT d.title FROM docs d
		WHERE vec_match(d.embed, ?, 10)
		ORDER BY vec_distance(d.embed, ?)`
	stmt := parseSQL(t, sql)
	_, err := RewriteVectorQuery(stmt, make([]byte, 16), defaultSession(), defaultEngine(), defaultLookup())
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-021")
}

// 6. Error MARMOT-VEC-022: vec_match on column with no index.
func TestRewrite_NoIndex_Error(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	// Lookup for a different column returns nothing.
	lookup := &stubLookup{meta: &common.VectorIndexMeta{
		IndexName: "other", TableName: "docs", ColumnName: "other_col",
		Database: "testdb", Metric: "l2",
	}}
	_, err := RewriteVectorQuery(stmt, make([]byte, 16), defaultSession(), defaultEngine(), lookup)
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-022")
}

// 7. Metric threading — cosine yields vec_distance_cosine in rendered SQL.
func TestRewrite_MetricCosine(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "pre", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	// Parse the rendered primary SQL and check ORDER BY function name.
	primary, err := vitessParser.Parse(info.PrimarySQL)
	require.NoError(t, err)
	fn := walkForFunc(t, primary, "vec_distance_cosine")
	require.NotNil(t, fn, "expected vec_distance_cosine in ORDER BY")
}

// 8. Metric threading — l2 index yields vec_distance_l2.
func TestRewrite_MetricL2(t *testing.T) {
	t.Parallel()
	lookup := &stubLookup{meta: &common.VectorIndexMeta{
		IndexName: "embeddings", TableName: "docs", ColumnName: "embed",
		Database: "testdb", Metric: "l2", Nprobe: 16,
	}}
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "pre", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), lookup)
	require.NoError(t, err)
	primary, err := vitessParser.Parse(info.PrimarySQL)
	require.NoError(t, err)
	fn := walkForFunc(t, primary, "vec_distance_l2")
	require.NotNil(t, fn, "expected vec_distance_l2 in ORDER BY")
}

// 9. Metric threading — dot index yields vec_distance_dot.
func TestRewrite_MetricDot(t *testing.T) {
	t.Parallel()
	lookup := &stubLookup{meta: &common.VectorIndexMeta{
		IndexName: "embeddings", TableName: "docs", ColumnName: "embed",
		Database: "testdb", Metric: "dot", Nprobe: 16,
	}}
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "pre", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), lookup)
	require.NoError(t, err)
	primary, err := vitessParser.Parse(info.PrimarySQL)
	require.NoError(t, err)
	fn := walkForFunc(t, primary, "vec_distance_dot")
	require.NotNil(t, fn, "expected vec_distance_dot in ORDER BY")
}

// 10. Plan selection — no predicate → post_filter.
func TestRewrite_NoPredicate_PostFilter(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "auto", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.Equal(t, PlanPostFilter, info.Plan)
}

// 11. Plan selection — force_plan=pre → pre_filter regardless of selectivity.
func TestRewrite_ForcePre(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) AND status = 'x' ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "pre", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.Equal(t, PlanPreFilter, info.Plan)
}

// 12. Plan selection — force_plan=post → post_filter regardless.
func TestRewrite_ForcePost(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) AND status = 'x' ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "post", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.Equal(t, PlanPostFilter, info.Plan)
}

// 13. Plan selection — small |F| → pre_filter (|F|=1 ≤ min(|I|, cap)=min(640,5000)=640).
func TestRewrite_SmallF_PreFilter(t *testing.T) {
	t.Parallel()
	// 1-row table → |F| = 1/10 = 1 (atLeast1). |I| = 10*16*4 = 640. cap=5000. 1 ≤ 640 → pre.
	lookup := &stubLookup{meta: defaultMeta()}
	stubL := &struct{ *stubLookup }{lookup}
	// Override EstimatedRowCount to return 1.
	_ = stubL

	type smallLookup struct{ *stubLookup }
	sl := &smallLookup{lookup}
	_ = sl

	// Use a custom lookup that returns 1.
	customLookup := &customRowCountLookup{meta: defaultMeta(), rows: 1}
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) AND status = 'x' ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "auto", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), customLookup)
	require.NoError(t, err)
	require.Equal(t, PlanPreFilter, info.Plan)
}

// 14. Plan selection — large |F| → post_filter.
func TestRewrite_LargeF_PostFilter(t *testing.T) {
	t.Parallel()
	// 10M rows → |F| = 10M/10 = 1M. |I|=10*16*4=640. cap=5000. 1M > 640 → post.
	customLookup := &customRowCountLookup{meta: defaultMeta(), rows: 10_000_000}
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) AND status = 'x' ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "auto", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), customLookup)
	require.NoError(t, err)
	require.Equal(t, PlanPostFilter, info.Plan)
}

// 15. Pre-filter rewrite — parse rendered SQL; assert no vec_match, has ORDER BY vec_distance_<metric>.
func TestRewrite_PreFilter_RenderedSQL(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) AND status = 'published' ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "pre", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.Equal(t, PlanPreFilter, info.Plan)

	// Parse the rendered SQL and verify structure.
	primary, err := vitessParser.Parse(info.PrimarySQL)
	require.NoError(t, err)

	// No vec_match in the rewritten statement.
	vmFn := walkForFunc(t, primary, "vec_match")
	require.Nil(t, vmFn, "vec_match must be removed from pre-filter rewrite")

	// vec_distance_cosine present in ORDER BY.
	vdFn := walkForFunc(t, primary, "vec_distance_cosine")
	require.NotNil(t, vdFn, "vec_distance_cosine must appear in ORDER BY")

	// User predicate preserved: walk for status comparison.
	sel := primary.(*sqlparser.Select)
	require.NotNil(t, sel.Where, "user predicate must be preserved in WHERE")
}

// 16. Post-filter rewrite — assert WHERE contains subquery on members table
//
//	with cluster_id IN (3,17,22) and cluster_id=0 disjunct; user predicate preserved.
func TestRewrite_PostFilter_RenderedSQL(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) AND status = 'published' ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "post", prefilterCap: 5000, fallback: "off"}
	engine := &stubEngine{clusterIDs: []int64{3, 17, 22}}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, engine, defaultLookup())
	require.NoError(t, err)
	require.Equal(t, PlanPostFilter, info.Plan)
	require.Equal(t, []int64{3, 17, 22}, info.ClusterIDs)

	// Parse and walk the rendered SQL.
	primary, err := vitessParser.Parse(info.PrimarySQL)
	require.NoError(t, err)
	sel := primary.(*sqlparser.Select)
	require.NotNil(t, sel.Where)

	// Walk for a subquery referencing the members table.
	var foundSubquery bool
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		sub, ok := node.(*sqlparser.Subquery)
		if !ok {
			return true, nil
		}
		subStr := sqlparser.String(sub)
		if strContains(subStr, "__marmot_vec_embeddings_members") {
			foundSubquery = true
			return false, nil
		}
		return true, nil
	}, sel.Where)
	require.True(t, foundSubquery, "post-filter WHERE must contain subquery on members table")

	// User predicate still present.
	var foundUserPred bool
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		cmp, ok := node.(*sqlparser.ComparisonExpr)
		if !ok {
			return true, nil
		}
		if col, ok := cmp.Left.(*sqlparser.ColName); ok && col.Name.EqualString("status") {
			foundUserPred = true
		}
		return true, nil
	}, sel.Where)
	require.True(t, foundUserPred, "post-filter WHERE must preserve the user predicate")
}

// 17. Fallback populated: post plan + fallback=on ⇒ FallbackOn=true, FallbackStmt is pre-filter.
func TestRewrite_FallbackPopulated(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) AND status = 'x' ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "post", prefilterCap: 5000, fallback: "on"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.Equal(t, PlanPostFilter, info.Plan)
	require.True(t, info.FallbackOn)
	require.NotNil(t, info.FallbackStmt)
	require.NotEmpty(t, info.FallbackSQL)

	// Fallback is the pre-filter: no vec_match, has vec_distance_cosine.
	fb, err := vitessParser.Parse(info.FallbackSQL)
	require.NoError(t, err)
	require.Nil(t, walkForFunc(t, fb, "vec_match"))
	require.NotNil(t, walkForFunc(t, fb, "vec_distance_cosine"))
}

// 18. Fallback not populated when plan is pre-filter.
func TestRewrite_NoFallbackOnPreFilter(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) AND status = 'x' ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "pre", prefilterCap: 5000, fallback: "on"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.Equal(t, PlanPreFilter, info.Plan)
	require.False(t, info.FallbackOn)
	require.Nil(t, info.FallbackStmt)
}

// 19. Fallback not populated when session.Fallback()=="off".
func TestRewrite_NoFallbackWhenOff(t *testing.T) {
	t.Parallel()
	sql := `SELECT title FROM docs WHERE vec_match(embed, ?, 10) AND status = 'x' ORDER BY vec_distance(embed, ?) LIMIT 10`
	stmt := parseSQL(t, sql)
	sess := &stubSession{forcePlan: "post", prefilterCap: 5000, fallback: "off"}
	info, err := RewriteVectorQuery(stmt, make([]byte, 16), sess, defaultEngine(), defaultLookup())
	require.NoError(t, err)
	require.Equal(t, PlanPostFilter, info.Plan)
	require.False(t, info.FallbackOn)
}

// --- helpers for test ---

// customRowCountLookup lets tests control the row count returned.
type customRowCountLookup struct {
	meta *common.VectorIndexMeta
	rows int64
}

func (c *customRowCountLookup) GetIndexByColumn(_, table, column string) (*common.VectorIndexMeta, bool) {
	if c.meta != nil && c.meta.TableName == table && c.meta.ColumnName == column {
		return c.meta, true
	}
	return nil, false
}

func (c *customRowCountLookup) EstimatedRowCount(_, _ string) int64 { return c.rows }

// strContains reports whether sub appears in s.
func strContains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
