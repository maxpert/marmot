package coordinator

import "vitess.io/vitess/go/vt/sqlparser"

// RewritePlan identifies which IVF query plan was selected.
type RewritePlan string

const (
	PlanPreFilter  RewritePlan = "pre_filter"
	PlanPostFilter RewritePlan = "post_filter"
)

// RewriteInfo carries the fully-planned vector query rewrite (§7, §7.5).
// Produced by RewriteVectorQuery (P2-A). Consumed by the coordinator handler (P2-B).
type RewriteInfo struct {
	Plan       RewritePlan
	IndexName  string
	Database   string
	TableName  string
	ColumnName string
	Metric     string // "l2" | "cosine" | "dot"
	K          int    // from LIMIT clause

	// Primary statement executed by the handler. PrimarySQL still contains the
	// original `?` placeholders for vec_distance (vec_match was structurally
	// stripped from WHERE). The handler splices vec_match's placeholder out of
	// the bound-param list before dispatch — the rewriter does not own args.
	PrimaryStmt sqlparser.Statement
	PrimarySQL  string

	// Short-result fallback (§7.5). Only set when Plan==PlanPostFilter AND session.Fallback()=="on".
	FallbackStmt sqlparser.Statement
	FallbackSQL  string
	FallbackOn   bool // true → re-execute fallback when primary returns <K rows

	// Telemetry (log/metrics only, not for execution).
	EstimatedF   int64
	EstimatedI   int64
	PrefilterCap int64
	ForcePlan    string  // "auto"|"pre"|"post"
	ClusterIDs   []int64 // post_filter: already baked into PrimarySQL

	// GoRank, when non-nil, directs the handler to execute the post-filter
	// plan via the Go-side ranking path (§7.6) instead of the SQL-UDF PrimarySQL.
	// Populated only for PlanPostFilter with session.UseGoRank()==true.
	GoRank *GoRankPlan
}

// QuerySession is the read-only view of per-connection @@marmot_vec_* vars that
// the rewrite planner (P2-A) and handler (P2-B) consume. It falls back to
// index-level defaults when a var has not been set on the connection.
type QuerySession interface {
	// Nprobe returns the effective nprobe, using indexDefault when the session
	// var is unset (VecVars.Nprobe == 0).
	Nprobe(indexDefault int) int
	// ForcePlan returns "auto", "pre", or "post".
	ForcePlan() string
	// PrefilterCap returns the maximum pre-filter candidate set size.
	PrefilterCap() int64
	// Fallback returns "on" or "off".
	Fallback() string
	// UseGoRank returns true when the Go-side ranking path is enabled.
	UseGoRank() bool
	// UseCache returns true when the in-memory vector cache ranking path is
	// enabled (task #16).
	UseCache() bool
}
