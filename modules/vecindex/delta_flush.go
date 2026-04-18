package vecindex

import (
	"context"
	"time"

	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// DeltaFlushConfig holds configuration for the per-index delta flush worker.
type DeltaFlushConfig struct {
	// Interval between flush cycles (design §8.6: @@marmot_vec_delta_flush_interval).
	Interval time.Duration
	// MaxRows is the maximum number of delta rows to process per cycle.
	MaxRows int
	// BatchSize is the number of assignments committed per transaction.
	BatchSize int
	// OnError is called when a flush cycle encounters a SQL error (fetch or
	// commit). If nil, errors are silently dropped. The callback receives the
	// index name and the error; it must not block.
	OnError func(indexName string, err error)
}

// DefaultDeltaFlushConfig returns the default configuration per design §8.6.
func DefaultDeltaFlushConfig() DeltaFlushConfig {
	return DeltaFlushConfig{
		Interval:  time.Second,
		MaxRows:   10_000,
		BatchSize: 1_000,
	}
}

// DeltaRow is a (rowid, internal-vector-blob) pair fetched from the delta scan.
type DeltaRow struct {
	Rowid int64
	Embed []byte
}

// DeltaAssignment is a (rowid, cluster_id, vec) triple for flush commit.
type DeltaAssignment struct {
	Rowid     int64
	ClusterID int64
	Embed     []byte
}

// DeltaFlushDB provides the SQL operations needed by the delta flush worker.
// Implemented by the db package; keeps the vecindex package SQL-agnostic.
type DeltaFlushDB interface {
	// FetchDeltaEmbeddings returns up to limit (rowid, internal vector blob)
	// pairs from the sidecar delta partition (cluster_id=0).
	FetchDeltaEmbeddings(ctx context.Context, database, indexName, tableName, columnName string, limit int) ([]DeltaRow, error)

	// CommitFlushBatch atomically performs an exact-token delete of the fetched
	// delta row and only reinserts it under the assigned cluster if the delete
	// matched. This prevents stale fetched rows from overwriting a newer delta
	// row for the same rowid.
	CommitFlushBatch(ctx context.Context, database, indexName string, assignments []DeltaAssignment) error
}

// deltaFlushLoop is the per-index background goroutine that assigns delta
// rows (cluster_id=0) to their nearest centroid per design §8.6.
//
// On each tick:
//  1. Fetch up to MaxRows delta embeddings via DeltaFlushDB.
//  2. Capture cv_start = probeState.Version.
//  3. Assign each embedding against probeState in Go (NOT via UDF).
//  4. For each batch of BatchSize assignments:
//     a. Recheck probeState.Version == cv_start; abort cycle on mismatch.
//     b. Commit the batch via DeltaFlushDB.
//     c. Update MacQueen drift tracker.
//
// Exits cleanly when ctx is cancelled.
func deltaFlushLoop(
	ctx context.Context,
	cfg DeltaFlushConfig,
	state *IndexState,
	db DeltaFlushDB,
	database, indexName, tableName, columnName string,
) {
	deltaFlushCycle(ctx, cfg, state, db, database, indexName, tableName, columnName)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deltaFlushCycle(ctx, cfg, state, db, database, indexName, tableName, columnName)
		}
	}
}

// deltaFlushCycle runs one flush iteration. Separated from the loop for
// testability.
func deltaFlushCycle(
	ctx context.Context,
	cfg DeltaFlushConfig,
	state *IndexState,
	db DeltaFlushDB,
	database, indexName, tableName, columnName string,
) {
	for {
		rows, err := db.FetchDeltaEmbeddings(ctx, database, indexName, tableName, columnName, cfg.MaxRows)
		if err != nil {
			if cfg.OnError != nil {
				cfg.OnError(indexName, err)
			}
			return
		}
		if len(rows) == 0 {
			return
		}

		cvStart := state.ProbeVersion()

		// Assign all rows against probeState.
		type assignedRow struct {
			DeltaAssignment
			vec []float32 // retained for MacQueen drift update
		}
		assignments := make([]assignedRow, 0, len(rows))
		for _, r := range rows {
			clusterID, assignErr := state.AssignNearestPrepared(r.Embed)
			if assignErr != nil {
				continue // skip malformed embeddings
			}
			assignments = append(assignments, assignedRow{
				DeltaAssignment: DeltaAssignment{Rowid: r.Rowid, ClusterID: clusterID, Embed: append([]byte(nil), r.Embed...)},
				vec:             bytesToFloat32Copy(r.Embed),
			})
		}

		// Commit in batches.
		for i := 0; i < len(assignments); i += cfg.BatchSize {
			if state.ProbeVersion() != cvStart {
				return
			}

			end := i + cfg.BatchSize
			if end > len(assignments) {
				end = len(assignments)
			}
			batch := assignments[i:end]

			deltas := make([]DeltaAssignment, len(batch))
			for j, a := range batch {
				deltas[j] = a.DeltaAssignment
			}

			if err := db.CommitFlushBatch(ctx, database, indexName, deltas); err != nil {
				if cfg.OnError != nil {
					cfg.OnError(indexName, err)
				}
				return
			}

			// MacQueen drift update after successful commit.
			for _, a := range batch {
				state.DriftUpdate(a.ClusterID, a.vec)
			}

			// Cache update (task #16) — epoch-gated inside CacheInsertBatch so a
			// concurrent REINDEX that already installed a fresh cache cannot have
			// these stale entries leaked back in.
			cacheEntries := make([]CacheEntry, 0, len(batch))
			for _, a := range batch {
				cacheEntries = append(cacheEntries, CacheEntry{
					ClusterID: a.ClusterID,
					RowID:     a.Rowid,
					Vec:       a.vec,
				})
			}
			state.CacheInsertBatch(cvStart, cacheEntries)
		}

		if len(rows) < cfg.MaxRows {
			return
		}
	}
}

// bytesToFloat32Copy decodes a little-endian float32 BLOB to a new []float32.
// Unlike metric.BytesToFloat32 this allocates a copy, which is needed because
// the BLOB from SQL may be reused after the rows iterator advances.
func bytesToFloat32Copy(b []byte) []float32 {
	n := len(b) / 4
	out := make([]float32, n)
	copy(out, metric.BytesToFloat32(b))
	return out
}
