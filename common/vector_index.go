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
	Status     string // "building", "ready", "error"
	CreatedAt  int64
}

// VectorSearchHit is a single result from a kNN vector search.
// ExternalID is an 8-byte big-endian encoded SQLite rowid.
type VectorSearchHit struct {
	ExternalID []byte
	Distance   float32
	Score      float32
}
