//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator

import (
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// NewMockNodeProvider exposes the internal test mock to external test packages.
func NewMockNodeProvider(nodes []uint64) NodeProvider {
	return newMockNodeProvider(nodes)
}

// NewTestHandler builds a minimal CoordinatorHandler for external tests that
// only need the read path + vector rewrite hooks.
func NewTestHandler(nodeID uint64, rc *ReadCoordinator, dbMgr DatabaseManager, clock *hlc.Clock) *CoordinatorHandler {
	return &CoordinatorHandler{
		nodeID:    nodeID,
		readCoord: rc,
		dbManager: dbMgr,
		clock:     clock,
	}
}

// MaybeRewriteVectorSelect exposes the unexported rewrite entrypoint to external tests.
func (h *CoordinatorHandler) MaybeRewriteVectorSelect(
	stmt protocol.Statement,
	params []interface{},
	session *protocol.ConnectionSession,
) (*RewriteInfo, []interface{}, error) {
	return h.maybeRewriteVectorSelect(stmt, params, session)
}

// ExecuteVectorPlan exposes the unexported execute entrypoint to external tests.
func (h *CoordinatorHandler) ExecuteVectorPlan(
	stmt protocol.Statement,
	info *RewriteInfo,
	args []interface{},
	consistency protocol.ConsistencyLevel,
) (*protocol.ResultSet, error) {
	return h.executeVectorPlan(stmt, info, args, consistency)
}

// CacheRankForTest exposes the cache-ranking fast path so external tests can
// verify the epoch coherence guard (task #16). Returns (topKRowIDs, hit) —
// hit=false means cacheRank fell through to SQL (caller can assert this when
// the plan's probe epoch does not match the cache).
func (h *CoordinatorHandler) CacheRankForTest(plan *GoRankPlan) ([]int64, bool) {
	items, ok := h.cacheRank(plan)
	if !ok {
		return nil, false
	}
	rowids := make([]int64, len(items))
	for i, it := range items {
		rowids[i] = it.rowid
	}
	return rowids, true
}
