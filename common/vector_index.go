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
	// AutoTuneNlist records whether Nlist was user-supplied or auto-derived.
	// REINDEX recomputes auto-tuned values against the current corpus size.
	AutoTuneNlist bool
	// AutoTuneNprobe records whether Nprobe was user-supplied or auto-derived.
	AutoTuneNprobe bool
	// TargetPartitionSize is the desired average vectors/partition when
	// auto-tuning. Reserved for future tuning strategies; defaults to 100.
	TargetPartitionSize int
	// MaxNorm is the fixed L2 norm cap for dot-product MIPS→L2 augmentation.
	MaxNorm   float32
	Status    string // "building", "ready", "reindexing"
	CreatedAt int64
}
