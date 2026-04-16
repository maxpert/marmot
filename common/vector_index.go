package common

// VectorIndexMeta holds metadata for a vector index.
// This type is shared between the db and coordinator packages.
type VectorIndexMeta struct {
	IndexName  string
	TableName  string
	ColumnName string
	Database   string
	Metric     string
	Dim        int
	// Nlist is the number of IVF centroids. 0 means auto-tune at CREATE time.
	Nlist int
	// Nprobe is the number of centroids searched per query. 0 means auto-tune.
	Nprobe int
	// MaxNorm is the fixed L2 norm cap for dot-product MIPS→L2 augmentation.
	MaxNorm   float32
	Status    string // "building", "ready", "reindexing"
	CreatedAt int64
}
