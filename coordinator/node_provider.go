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
}
