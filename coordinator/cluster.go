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
