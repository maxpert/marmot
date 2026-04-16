package db

import (
	"hash/fnv"
	"io"
	"strconv"

	"github.com/maxpert/marmot/common"
)

// StableIndexSeed derives a deterministic 64-bit seed from the immutable
// identity of a vector index: (TableName, ColumnName, Dim, Metric, Nlist).
//
// All nodes that observe the same CREATE VECTOR INDEX DDL produce byte-identical
// centroids when fed the same base-table vectors (task #17 determinism contract).
// CreatedAt is node-local (HLC-LWW decides the winning row, but both peers still
// run k-means locally), so seeding from CreatedAt would make the HLC-LWW loser's
// compute a write-off. Seeding from stable identity makes concurrent CREATE on
// two nodes converge to byte-identical centroid blobs — replication round-trip
// for the losing row is a no-op.
func StableIndexSeed(meta common.VectorIndexMeta) uint64 {
	h := fnv.New64a()
	sep := []byte{0}
	_, _ = io.WriteString(h, meta.TableName)
	_, _ = h.Write(sep)
	_, _ = io.WriteString(h, meta.ColumnName)
	_, _ = h.Write(sep)
	_, _ = io.WriteString(h, strconv.Itoa(meta.Dim))
	_, _ = h.Write(sep)
	_, _ = io.WriteString(h, meta.Metric)
	_, _ = h.Write(sep)
	_, _ = io.WriteString(h, strconv.Itoa(meta.Nlist))
	return h.Sum64()
}
