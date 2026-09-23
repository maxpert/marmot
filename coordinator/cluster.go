package coordinator

import (
	"fmt"

	"github.com/maxpert/marmot/protocol"
)

// ClusterState represents the current state of the cluster for coordination operations.
// It encapsulates alive nodes, total membership, and required quorum for a given consistency level.
type ClusterState struct {
	// AliveNodes contains all currently alive node IDs in the cluster
	AliveNodes []uint64

	// TotalMembership is the total known cluster membership (ALIVE + SUSPECT + DEAD nodes)
	// Used for quorum calculation to prevent split-brain scenarios
	TotalMembership int

	// RequiredQuorum is the number of successful operations required for the consistency level
	RequiredQuorum int
}

// consistencyNeedsMajority reports whether a consistency level's guarantee
// depends on the total-membership denominator. QUORUM and ALL do; ONE and
// LOCAL_ONE explicitly do not, and an unknown level is treated as needing one,
// matching QuorumSize's own default-for-safety.
func consistencyNeedsMajority(level protocol.ConsistencyLevel) bool {
	switch level {
	case protocol.ConsistencyLocalOne, protocol.ConsistencyOne:
		return false
	default:
		return true
	}
}

// ErrMembershipNotLearned is returned when a node configured to join a cluster
// still knows only itself, so it cannot tell a healthy single-node deployment
// from a restart that has not yet heard from its peers.
//
// It is ErrCodeLockTimeout (1205) because the condition is transient and the
// client should retry: gossip converges within a few seconds of a peer becoming
// reachable. 1205 is the code ORM retry layers already key on, alongside 1213,
// and the AUTO_INCREMENT claim path returns it for the same reason. A
// non-retryable code would turn a few seconds of convergence into a failed
// transaction the application has to handle itself.
func ErrMembershipNotLearned() *protocol.MySQLError {
	return protocol.NewMySQLError(
		protocol.ErrCodeLockTimeout,
		protocol.SQLStateGeneral,
		"cluster membership not yet learned; refusing to act as a quorum of one")
}

// GetClusterState retrieves the current cluster state for DML operations.
// It returns a ClusterState with alive nodes, total membership, and required quorum size.
//
// CRITICAL: Uses TOTAL membership for quorum calculation, not just alive nodes.
// This prevents split-brain: in a 6-node cluster split 3x3, each partition would see
// clusterSize=3 and achieve quorum=2 if we only counted alive nodes. By using total
// membership, quorum=4 and neither partition can write/read with quorum.
func GetClusterState(nodeProvider NodeProvider, consistency protocol.ConsistencyLevel) (*ClusterState, error) {
	// Get all alive nodes for replication
	aliveNodes, err := nodeProvider.GetAliveNodes()
	if err != nil {
		return nil, fmt.Errorf("failed to get alive nodes: %w", err)
	}

	aliveCount := len(aliveNodes)
	if aliveCount == 0 {
		return nil, fmt.Errorf("no alive nodes in cluster")
	}

	// CRITICAL: Use TOTAL membership for quorum calculation, not just alive nodes
	totalMembership := nodeProvider.GetTotalMembershipSize()

	// A node configured with seed nodes that knows only itself has not learned
	// the cluster yet: it either restarted before gossip converged, or booted
	// with its seeds unreachable. Quorum of a membership of one is one, so it
	// would commit alone and diverge from peers that are up. Refuse, with a
	// retryable code, until membership is learned.
	//
	// This is a belt, independent of the registry's persisted membership: it
	// still holds when the snapshot is absent or unreadable, which is exactly
	// when the denominator would otherwise be wrong.
	//
	// It applies ONLY to levels whose guarantee depends on the denominator.
	// LOCAL_ONE and ONE ask for no quorum, so a wrong denominator cannot
	// mislead them, and refusing there would take out health checks: the
	// cluster harness's own liveness probe is a LOCAL_ONE "SELECT 1", and an
	// earlier revision of this guard made a booting node look dead to it.
	if totalMembership <= 1 && consistencyNeedsMajority(consistency) && nodeProvider.HasSeedNodes() {
		return nil, ErrMembershipNotLearned()
	}

	// Validate consistency level against total membership (not just alive)
	if err := ValidateConsistencyLevel(consistency, totalMembership); err != nil {
		return nil, fmt.Errorf("invalid consistency level: %w", err)
	}

	// Calculate required quorum based on TOTAL membership (split-brain protection)
	requiredQuorum := QuorumSize(consistency, totalMembership)

	return &ClusterState{
		AliveNodes:      aliveNodes,
		TotalMembership: totalMembership,
		RequiredQuorum:  requiredQuorum,
	}, nil
}
