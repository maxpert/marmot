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

// CausesImplicitCommit exposes the implicit-commit classification to external
// test packages.
func CausesImplicitCommit(stmt protocol.Statement) bool {
	return causesImplicitCommit(stmt)
}

// TakeAndReleasePinnedStateForTest takes and releases the pinned transaction
// state for connID, simulating it being torn down by something other than
// that transaction's own COMMIT/ROLLBACK - e.g. a concurrent forward-session
// eviction calling CoordinatorHandler.CloseSession. Unlike CloseSession, it
// does not touch the session's transaction state, so tests can reproduce the
// exact race window handleCommit's empty-transaction fast path must guard
// against: pinned state gone, but the session's ConnectionSession still
// believes it is mid-transaction. Returns false if there was no pinned state
// to take.
func (h *CoordinatorHandler) TakeAndReleasePinnedStateForTest(connID uint64) bool {
	st := h.takePinnedState(connID)
	if st == nil {
		return false
	}
	st.releaseAll()
	return true
}
