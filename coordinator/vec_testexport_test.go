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
