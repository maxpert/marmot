package db

import (
	"context"

	"github.com/maxpert/marmot/common"
)

// VectorIndex represents an open vector index.
type VectorIndex interface {
	Search(ctx context.Context, vector []float32, topK int) ([]VectorSearchHit, error)
	Upsert(ctx context.Context, externalID []byte, vector []float32, txnID, seqID uint64) error
	Delete(ctx context.Context, externalID []byte, txnID, seqID uint64) error
	Stats() VectorIndexStats
	Close() error
}

// VectorIndexEngine creates and manages vector indexes.
type VectorIndexEngine interface {
	CreateIndex(ctx context.Context, id string, dim int, metric string, vectors []VectorBulkEntry) (VectorIndex, error)
	OpenIndex(ctx context.Context, id string) (VectorIndex, error)
	DropIndex(ctx context.Context, id string) error
	Close() error
}

// VectorSearchHit is a single search result.
// Aliased from common to allow coordinator to reference it without an import cycle.
type VectorSearchHit = common.VectorSearchHit

// VectorIndexStats provides index statistics.
type VectorIndexStats struct {
	VectorCount    uint64
	WatermarkTxnID uint64
}

// VectorBulkEntry is a single vector for bulk loading.
type VectorBulkEntry struct {
	ExternalID []byte
	Vector     []float32
}
