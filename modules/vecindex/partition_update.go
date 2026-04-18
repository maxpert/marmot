package vecindex

// PartitionUpdate describes a row that has moved out of the resident delta
// partition into a stable cluster after a successful delta-flush commit.
type PartitionUpdate struct {
	ClusterID int64
	RowID     int64
}
