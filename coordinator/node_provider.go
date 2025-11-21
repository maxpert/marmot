package coordinator

// NodeProvider provides access to all alive cluster nodes.
// This interface is used for full database replication where ALL nodes
// receive ALL writes (unlike partitioned replication which uses consistent hashing).
type NodeProvider interface {
	// GetAliveNodes returns all currently alive nodes in the cluster
	GetAliveNodes() ([]uint64, error)

	// GetClusterSize returns the total number of alive nodes
	GetClusterSize() int
}
