package vecindex

// VectorUDFProvider is the minimal surface a vector index engine must
// expose so that SQLite UDFs registered on a per-connection basis can
// reach back into the engine. Implementations land in P1-C.
//
// Methods must be safe for concurrent invocation from arbitrary SQLite
// connection goroutines.
type VectorUDFProvider interface {
	// AssignNearest returns the cluster id the given vector should be
	// assigned to for the named index. Implementations should error with
	// MARMOT-VEC-013 if the index is unknown.
	AssignNearest(indexName string, vec []byte) (int64, error)

	// TopNprobeClusters returns the nearest n 1-based cluster IDs for the query
	// vector against the named index's probeState. Error MARMOT-VEC-013 if the
	// index is unknown.
	TopNprobeClusters(indexName string, vec []byte, n int) ([]int64, error)
}
