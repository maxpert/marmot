package grpc

import (
	"github.com/maxpert/marmot/coordinator"
)

// GossipNodeProvider implements coordinator.NodeProvider using the gossip protocol's
// node registry. It returns all alive nodes in the cluster for full database replication.
type GossipNodeProvider struct {
	registry *NodeRegistry
}

// NewGossipNodeProvider creates a new NodeProvider backed by the gossip node registry
func NewGossipNodeProvider(registry *NodeRegistry) coordinator.NodeProvider {
	return &GossipNodeProvider{
		registry: registry,
	}
}

// GetAliveNodes returns all nodes eligible for replication (ALIVE only, excludes JOINING)
// JOINING nodes are catching up and should not receive write replication yet
func (gnp *GossipNodeProvider) GetAliveNodes() ([]uint64, error) {
	allNodes := gnp.registry.GetReplicationEligible()
	nodeIDs := make([]uint64, 0, len(allNodes))

	for _, node := range allNodes {
		nodeIDs = append(nodeIDs, node.NodeId)
	}

	return nodeIDs, nil
}

// GetClusterSize returns the current number of alive nodes in the cluster
func (gnp *GossipNodeProvider) GetClusterSize() int {
	nodes, _ := gnp.GetAliveNodes()
	return len(nodes)
}

// GetTotalMembershipSize returns the total known cluster membership
// (ALIVE + SUSPECT + DEAD nodes). Used for quorum calculation to prevent split-brain.
func (gnp *GossipNodeProvider) GetTotalMembershipSize() int {
	return gnp.registry.Count()
}
