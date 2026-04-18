package db

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/modules/vecindex"
	vecmetric "github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// sqlPartitionLoader materialises vecindex partitions from the clustered
// sidecar members table. It satisfies vecindex.PartitionLoader and is owned by
// a PartitionCache: invoked only on cache misses.
//
// The loader reads from the given *sql.DB — callers should pass the read
// pool (readDB) so concurrent BulkLoads execute on the WAL-concurrent path
// rather than serialising against the single writer.
type sqlPartitionLoader struct {
	db           *sql.DB
	indexName    string
	dim          int
	membersTable string
}

func newSQLPartitionLoader(db *sql.DB, indexName, _baseTable, _embedColumn string, dim int, _ vecmetric.Metric) *sqlPartitionLoader {
	return &sqlPartitionLoader{
		db:           db,
		indexName:    indexName,
		dim:          dim,
		membersTable: vecindex.MembersTable(indexName),
	}
}

// BulkLoad reads every requested cluster_id's member vectors in a single
// statement. Returns a map with one entry per requested key — empty clusters
// map to a zero-length slice so otter caches the "known empty" state and
// avoids a repeat load on every probe.
func (l *sqlPartitionLoader) BulkLoad(ctx context.Context, clusterIDs []int64) (map[int64]vecindex.CachedPartition, error) {
	if len(clusterIDs) == 0 {
		return map[int64]vecindex.CachedPartition{}, nil
	}

	// Pre-seed result with empty slices so any clusterID the loader did not
	// return a row for still lands in the map (otter-caching contract: keys
	// omitted from the returned map are NOT cached, causing re-loads).
	out := make(map[int64]vecindex.CachedPartition, len(clusterIDs))
	for _, cid := range clusterIDs {
		out[cid] = vecindex.CachedPartition{}
	}

	query := l.buildSelect(len(clusterIDs))
	args := make([]interface{}, len(clusterIDs))
	for i, cid := range clusterIDs {
		args[i] = cid
	}

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("partition loader query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid, rid int64
		var blob []byte
		if err := rows.Scan(&cid, &rid, &blob); err != nil {
			return nil, fmt.Errorf("partition loader scan: %w", err)
		}
		vec := l.decodeBlob(blob)
		if vec == nil {
			continue
		}
		part := out[cid]
		part.RowIDs = append(part.RowIDs, rid)
		part.Vecs = append(part.Vecs, vec...)
		out[cid] = part
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("partition loader iter: %w", err)
	}

	// Ensure every key has at least a non-empty map entry so the
	// cache records "empty partition" explicitly.
	return out, nil
}

// loadDelta returns a fully-populated DeltaBuffer containing every
// cluster_id=0 row. Called once at index open; the buffer stays resident for
// the life of the cache.
func (l *sqlPartitionLoader) loadDelta(ctx context.Context) (*vecindex.DeltaBuffer, error) {
	got, err := l.BulkLoad(ctx, []int64{0})
	if err != nil {
		return nil, err
	}
	buf := vecindex.NewDeltaBuffer()
	part := got[0]
	entries := make([]vecindex.CachedVector, 0, part.Len())
	for i, rid := range part.RowIDs {
		entries = append(entries, vecindex.CachedVector{
			RowID: rid,
			Vec:   append([]float32(nil), part.Vector(i, l.dim)...),
		})
	}
	buf.AppendBatch(entries)
	return buf, nil
}

// buildSelect produces a single-statement SELECT that groups rows by
// cluster_id via the `IN (?, ?, ...)` predicate. IN-list size is bounded by
// the caller — SQLite's default variable cap is 999 and our typical probe
// count is <= nlist (a few thousand at most).
func (l *sqlPartitionLoader) buildSelect(n int) string {
	var sb strings.Builder
	sb.WriteString("SELECT m.cluster_id, m.rowid, m.vec")
	sb.WriteString(" FROM ")
	sb.WriteString(quoteIdent(l.membersTable))
	sb.WriteString(" m WHERE m.cluster_id IN (")
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
	}
	sb.WriteByte(')')
	return sb.String()
}

// decodeBlob decodes a little-endian float32 BLOB into a freshly allocated
// []float32. Returns nil for malformed or dim-mismatched blobs so the caller
// can skip the row instead of failing the load.
//
// The sidecar table already stores the internal vector representation:
// cosine rows are unit-normalized, dot rows are MIPS-augmented, and L2 rows
// are raw float32 blobs. The loader therefore performs no metric-specific
// transforms here and simply decodes the stored bytes.
func (l *sqlPartitionLoader) decodeBlob(blob []byte) []float32 {
	if len(blob) == 0 || len(blob)%4 != 0 {
		return nil
	}
	n := len(blob) / 4
	if n != l.dim {
		return nil
	}
	vec := make([]float32, n)
	for i := 0; i < n; i++ {
		bits := uint32(blob[i*4]) |
			uint32(blob[i*4+1])<<8 |
			uint32(blob[i*4+2])<<16 |
			uint32(blob[i*4+3])<<24
		vec[i] = math.Float32frombits(bits)
	}
	return vec
}

// partitionCacheBytes returns the configured byte budget for the legacy
// in-memory partition cache. Zero means disabled.
func partitionCacheBytes() uint64 {
	if cfg.Config != nil {
		return cfg.Config.VectorIndex.CacheBytes
	}
	return 0
}
