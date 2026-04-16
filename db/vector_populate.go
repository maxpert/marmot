package db

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"strings"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/rs/zerolog/log"
)

const (
	// kmeansMaxIter is the maximum number of Lloyd iterations during k-means.
	kmeansMaxIter = 25
	// sampleFloor is the minimum sample size considered for k-means input.
	sampleFloor = 200_000
)

// BulkPopulate executes design §8.1 step 10 after the DDL transaction commits.
//
// It checks whether a centroid row already exists for the index (replica path).
// If yes: loads centroids and bulk-populates members using the loaded centroids.
// If no:  samples vectors, runs k-means, writes the centroid row, then
// bulk-populates members.
//
// The engine must have the UDFs registered on every connection (via ConnectHook)
// before this is called. BulkPopulate registers the IndexState into the engine
// immediately before running the bulk SQL so the UDF can resolve assignments.
//
// tableName and columnName are SQL identifiers from the parsed DDL; they are
// quoted via quoteIdent before embedding in SQL strings.
//
// updatedAt is the HLC timestamp (Timestamp.ToTxnID()) used for the centroid
// row's updated_at column, enabling LWW conflict resolution on concurrent CREATE.
func BulkPopulate(
	ctx context.Context,
	db *sql.DB,
	engine *vecindex.Engine,
	updatedAt int64,
	tableName string,
	columnName string,
	spec vecindex.IVFSpec,
) error {
	cs, err := loadCentroidSet(ctx, db, spec.ID)
	if err != nil {
		return fmt.Errorf("bulk populate %q: check existing centroids: %w", spec.ID, err)
	}

	if cs == nil {
		cs, err = computeCentroids(ctx, db, tableName, columnName, spec)
		if err != nil {
			return fmt.Errorf("bulk populate %q: compute centroids: %w", spec.ID, err)
		}
		if cs == nil {
			// Empty base table — nothing to index yet; DDL status stays 'building'.
			log.Info().Str("index", spec.ID).Msg("BulkPopulate: base table empty, skipping centroid compute")
			return nil
		}
		if err := writeCentroidRow(ctx, db, spec, cs, updatedAt); err != nil {
			return fmt.Errorf("bulk populate %q: write centroid row: %w", spec.ID, err)
		}
	}

	// Register BEFORE the bulk SQL so the __marmot_vec_assign UDF can resolve.
	state := engine.RegisterWithCentroidSet(spec.ID, spec, cs)

	if err := populateMembers(ctx, db, tableName, columnName, spec.ID); err != nil {
		engine.Unregister(spec.ID)
		return fmt.Errorf("bulk populate %q: populate members: %w", spec.ID, err)
	}

	// Build the in-memory vector cache (task #16) from the committed members.
	// A single streaming scan decodes vectors once into owned float32 slices.
	if err := buildAndStoreCache(ctx, db, state, spec, tableName, columnName); err != nil {
		log.Warn().Err(err).Str("index", spec.ID).Msg("BulkPopulate: vector cache build failed; queries will fall back to SQL")
	}
	return nil
}

// buildAndStoreCache scans (members JOIN base) once, decodes each vector into
// an owned float32 slice, groups them by cluster_id, and atomically installs
// the result as the index's in-memory VectorCache (task #16).
//
// A cache failure is non-fatal — searches transparently fall back to the SQL
// candidate path. Callers log the error and continue.
func buildAndStoreCache(
	ctx context.Context,
	db *sql.DB,
	state *vecindex.IndexState,
	spec vecindex.IVFSpec,
	tableName, columnName string,
) error {
	mt := quoteIdent(vecindex.MembersTable(spec.ID))
	colQ := quoteIdent(columnName)
	tblQ := quoteIdent(tableName)

	rows, err := db.QueryContext(ctx,
		fmt.Sprintf(`SELECT m.cluster_id, m.rowid, b.%s FROM %s m JOIN %s b ON b.rowid = m.rowid`,
			colQ, mt, tblQ),
	)
	if err != nil {
		return fmt.Errorf("cache scan query: %w", err)
	}
	defer rows.Close()

	byCluster := make(map[int64][]vecindex.CachedVector)
	for rows.Next() {
		var cid, rid int64
		var blob []byte
		if err := rows.Scan(&cid, &rid, &blob); err != nil {
			return fmt.Errorf("cache scan row: %w", err)
		}
		if len(blob) == 0 || len(blob)%4 != 0 {
			continue
		}
		n := len(blob) / 4
		if n != spec.Dim {
			continue
		}
		vec := make([]float32, n)
		for i := 0; i < n; i++ {
			bits := uint32(blob[i*4]) |
				uint32(blob[i*4+1])<<8 |
				uint32(blob[i*4+2])<<16 |
				uint32(blob[i*4+3])<<24
			vec[i] = math.Float32frombits(bits)
		}
		byCluster[cid] = append(byCluster[cid], vecindex.CachedVector{RowID: rid, Vec: vec})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("cache scan iter: %w", err)
	}

	cache := vecindex.NewVectorCache(state.ProbeVersion(), byCluster)
	state.StoreCache(cache)
	log.Info().
		Str("index", spec.ID).
		Int("cached_vectors", cache.Len()).
		Int("clusters", cache.ClusterCount()).
		Uint64("epoch", cache.Epoch()).
		Msg("vector cache: populated from members scan")
	return nil
}

// loadCentroidSet queries the centroids table and returns the stored CentroidSet,
// or nil if no row exists yet. Handles both 'zstd' and 'none' compression.
func loadCentroidSet(ctx context.Context, db *sql.DB, indexName string) (*kmeans.CentroidSet, error) {
	table := vecindex.CentroidsTable(indexName)
	row := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT version, compression, centroids FROM %s WHERE index_id = 1",
			quoteIdent(table)))

	var version int64
	var compression string
	var blob []byte
	err := row.Scan(&version, &compression, &blob)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load centroid set: scan row: %w", err)
	}

	var cs *kmeans.CentroidSet
	switch strings.ToLower(compression) {
	case "zstd":
		cs, err = vecindex.DecodeCentroidBlob(blob)
	case "none":
		cs, err = kmeans.DecodeCentroidSet(blob)
	default:
		return nil, fmt.Errorf("load centroid set: unknown compression %q", compression)
	}
	if err != nil {
		return nil, fmt.Errorf("load centroid set: decode: %w", err)
	}
	return cs, nil
}

// computeCentroids samples vectors from the base table, augments for dot-metric
// if needed, and runs k-means. Returns nil when the table has no vectors.
// sampleCap = min(n_total, max(sampleFloor, 2*nlist)) per design §8.1.
func computeCentroids(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	columnName string,
	spec vecindex.IVFSpec,
) (*kmeans.CentroidSet, error) {
	tableQ := quoteIdent(tableName)
	colQ := quoteIdent(columnName)

	var nTotal int
	err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", tableQ, colQ)).Scan(&nTotal)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: count rows: %w", err)
	}
	if nTotal == 0 {
		return nil, nil
	}

	sampleCap := max(sampleFloor, 2*spec.Nlist)
	if sampleCap > nTotal {
		sampleCap = nTotal
	}

	// Deterministic sampling (design §8.1 L): scan in rowid order and apply
	// Algorithm-R reservoir sampling seeded from spec.Seed. Two nodes seeing
	// byte-identical base rows in rowid order (CDC replay guarantees this;
	// task #17) will select identical samples, feed them into k-means++ in
	// identical order, and produce byte-identical centroids.
	//
	// SQLite's ORDER BY RANDOM() seeds from system entropy and cannot be
	// pinned — using it would silently break the HLC-LWW convergence story
	// since two nodes would compute different centroids for the same data.
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY rowid",
			colQ, tableQ, colQ),
	)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: sample query: %w", err)
	}
	defer rows.Close()

	rng := rand.New(rand.NewSource(int64(spec.Seed)))
	samples := make([][]float32, 0, sampleCap)
	seen := 0
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return nil, fmt.Errorf("compute centroids: scan blob: %w", err)
		}

		// Roll the reservoir decision BEFORE decoding so every row advances
		// the PRNG identically on every node, independent of decode cost.
		// Rejected rows skip decode entirely.
		seen++
		slot := reservoirSlot(seen, sampleCap, rng)
		if slot < 0 {
			continue
		}

		raw, decErr := decodeVec(blob)
		if decErr != nil {
			return nil, fmt.Errorf("compute centroids: decode vector: %w", decErr)
		}
		// decodeVec returns an unsafe alias of blob; copy before retaining.
		v := make([]float32, len(raw))
		copy(v, raw)
		if spec.Metric == vecindex.MetricDot {
			augmented, augErr := metric.AugmentData(v, spec.MaxNorm, nil)
			if augErr != nil {
				return nil, fmt.Errorf("compute centroids: augment: %w", augErr)
			}
			v = augmented
		}

		if slot < len(samples) {
			samples[slot] = v
		} else {
			samples = append(samples, v)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("compute centroids: row iteration: %w", err)
	}
	if len(samples) == 0 {
		return nil, nil
	}

	actualK := spec.Nlist
	if actualK > len(samples) {
		actualK = len(samples)
	}

	centroids, err := kmeans.KMeansPlusPlus(samples, actualK, spec.Seed, kmeansMaxIter)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: k-means: %w", err)
	}

	return kmeans.NewCentroidSet(1, centroids)
}

// writeCentroidRow inserts the initial centroid row into the centroids table.
// Uses REPLACE INTO for idempotency on crash-recovery retries.
func writeCentroidRow(
	ctx context.Context,
	db *sql.DB,
	spec vecindex.IVFSpec,
	cs *kmeans.CentroidSet,
	updatedAt int64,
) error {
	blob, err := vecindex.EncodeCentroidBlob(cs)
	if err != nil {
		return fmt.Errorf("write centroid row: encode: %w", err)
	}
	table := vecindex.CentroidsTable(spec.ID)
	_, err = db.ExecContext(ctx,
		fmt.Sprintf(`REPLACE INTO %s (index_id, version, updated_at, nlist, compression, centroids, last_n)
		             VALUES (1, ?, ?, ?, 'zstd', ?, ?)`, quoteIdent(table)),
		int64(cs.Epoch()), updatedAt, cs.Len(), blob, cs.Len(),
	)
	return err
}

// populateMembers bulk-assigns every non-null vector to its nearest centroid
// via the __marmot_vec_assign UDF. Runs inside a single SQLite writer txn to
// eliminate the trigger-race window (design §8.1 step 10):
//
//  1. DELETE rows with cluster_id=0 (delta) that arrived via AFTER INSERT
//     triggers during the DDL-commit → populate gap.
//  2. INSERT all assignments atomically — no new trigger can fire until commit.
//
// After commit, fresh inserts go to cluster_id=0 via the trigger and are
// handled by the delta-flush goroutine (design §8.6).
func populateMembers(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	columnName string,
	indexName string,
) error {
	mt := quoteIdent(vecindex.MembersTable(indexName))
	colQ := quoteIdent(columnName)
	tblQ := quoteIdent(tableName)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("populate members: begin txn: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	// Clear trigger-inserted delta rows from the DDL→populate window.
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE cluster_id = 0`, mt)); err != nil {
		return fmt.Errorf("populate members: clear delta: %w", err)
	}

	// Bulk-assign all non-null vectors. No OR IGNORE needed: delta cleared above.
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (cluster_id, rowid)
		             SELECT __marmot_vec_assign(?, %s), rowid
		             FROM %s WHERE %s IS NOT NULL`,
			mt, colQ, tblQ, colQ),
		indexName,
	); err != nil {
		return fmt.Errorf("populate members: insert: %w", err)
	}

	// MEDIUM-6 fix: status='ready' flip inside the same txn as the populate
	// so a crash between populate-commit and status-flip cannot leave the
	// index in 'building' with fully populated members.
	if _, err := tx.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes SET status='ready' WHERE index_name=?`,
		indexName,
	); err != nil {
		return fmt.Errorf("populate members: set status ready: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("populate members: commit: %w", err)
	}
	tx = nil
	return nil
}

// quoteIdent wraps a SQL identifier in double quotes, escaping any embedded
// double quotes by doubling them (standard SQL identifier quoting).
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
