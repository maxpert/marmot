package db

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/maxpert/marmot/modules/vecindex"
	vecmetric "github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// sqlPartitionLoader reads the clustered sidecar members table.
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

// loadDelta returns a fully-populated DeltaBuffer containing every
// cluster_id=0 row.
func (l *sqlPartitionLoader) loadDelta(ctx context.Context) (*vecindex.DeltaBuffer, error) {
	rows, err := l.db.QueryContext(ctx, l.buildSelect(1), int64(0))
	if err != nil {
		return nil, fmt.Errorf("load delta query: %w", err)
	}
	defer rows.Close()

	buf := vecindex.NewDeltaBuffer()
	var entries []vecindex.CachedVector
	for rows.Next() {
		var _cid, rid int64
		var blob []byte
		if err := rows.Scan(&_cid, &rid, &blob); err != nil {
			return nil, fmt.Errorf("load delta scan: %w", err)
		}
		vec := l.decodeBlob(blob)
		if vec == nil {
			continue
		}
		entries = append(entries, vecindex.CachedVector{
			RowID: rid,
			Vec:   vec,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load delta iter: %w", err)
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
