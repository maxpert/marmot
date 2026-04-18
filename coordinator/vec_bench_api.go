//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator

import (
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// BenchNodeProvider is a minimal, non-clustered NodeProvider suitable for
// single-node benchmark tools (cmd/vec-bench). It reports a fixed node list
// and ignores liveness transitions.
type BenchNodeProvider struct {
	nodes []uint64
}

// NewBenchNodeProvider builds a NodeProvider that advertises the given node
// IDs as the full alive membership.
func NewBenchNodeProvider(nodes []uint64) *BenchNodeProvider {
	return &BenchNodeProvider{nodes: nodes}
}

func (p *BenchNodeProvider) GetAliveNodes() ([]uint64, error) { return p.nodes, nil }
func (p *BenchNodeProvider) GetClusterSize() int              { return len(p.nodes) }
func (p *BenchNodeProvider) GetTotalMembershipSize() int      { return len(p.nodes) }

// NewBenchHandler builds a CoordinatorHandler wired only for the read + vector
// rewrite paths — suitable for benchmark tooling (cmd/vec-bench). Writes,
// DDL, schema-version replication, and node-membership broadcasts are all
// absent; attempting a write query on the returned handler will nil-deref.
func NewBenchHandler(nodeID uint64, rc *ReadCoordinator, dbMgr DatabaseManager, clock *hlc.Clock) *CoordinatorHandler {
	return &CoordinatorHandler{
		nodeID:    nodeID,
		readCoord: rc,
		dbManager: dbMgr,
		clock:     clock,
	}
}

// BenchMaybeRewriteVectorSelect exposes the unexported rewrite entrypoint for
// benchmark tooling.
func (h *CoordinatorHandler) BenchMaybeRewriteVectorSelect(
	stmt protocol.Statement,
	params []interface{},
	session *protocol.ConnectionSession,
) (*RewriteInfo, []interface{}, error) {
	return h.maybeRewriteVectorSelect(stmt, params, session)
}

// BenchExecuteVectorPlan exposes the unexported execute entrypoint for
// benchmark tooling.
func (h *CoordinatorHandler) BenchExecuteVectorPlan(
	stmt protocol.Statement,
	info *RewriteInfo,
	args []interface{},
	consistency protocol.ConsistencyLevel,
) (*protocol.ResultSet, error) {
	return h.executeVectorPlan(stmt, info, args, consistency)
}

// BenchResetSharedScanStats clears shared-scan counters on the benchmark
// handler, if the coordinator has been initialized.
func (h *CoordinatorHandler) BenchResetSharedScanStats() {
	if c := h.sharedScanCoordinatorIfLoaded(); c != nil {
		c.ResetStats()
	}
}

// BenchSharedScanStats snapshots shared-scan counters on the benchmark
// handler, if the coordinator has been initialized.
func (h *CoordinatorHandler) BenchSharedScanStats() VecSharedScanStats {
	if c := h.sharedScanCoordinatorIfLoaded(); c != nil {
		return c.SnapshotStats()
	}
	return VecSharedScanStats{}
}

// BenchConfigureSharedScan overrides shared-scan batching knobs for the
// benchmark handler.
func (h *CoordinatorHandler) BenchConfigureSharedScan(window time.Duration, maxRequests, maxUnion int) {
	c := h.sharedScanCoordinator()
	c.Configure(vecSharedScanOptions{
		MicrobatchWindow: window,
		MaxRequests:      maxRequests,
		MaxUnionClusters: maxUnion,
	})
}
