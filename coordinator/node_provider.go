package coordinator

// NodeProvider provides access to cluster nodes for replication.
// This interface is used for full database replication where ALL nodes
// receive ALL writes (unlike partitioned replication which uses consistent hashing).
type NodeProvider interface {
	// GetAliveNodes returns all ALIVE nodes for replication
	GetAliveNodes() ([]uint64, error)

	// GetClusterSize returns the total number of alive nodes (replication targets)
	GetClusterSize() int

	// GetTotalMembershipSize returns the total known cluster membership
	// (ALIVE + SUSPECT + DEAD nodes). This is used for quorum calculation
	// to prevent split-brain: quorum must be majority of TOTAL membership,
	// not just currently reachable nodes.
	GetTotalMembershipSize() int

	// HasSeedNodes reports whether this node was configured to join an existing
	// cluster. It is threaded through the provider rather than read from the
	// global config so the coordinator stays testable and has no config
	// dependency of its own.
	//
	// It exists for one decision: a node with seeds whose membership is itself
	// alone has not yet learned who its peers are, and must not act as a quorum
	// of one. A node without seeds is a legitimate single-node deployment.
	HasSeedNodes() bool
}
