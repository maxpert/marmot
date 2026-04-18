package db

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/rs/zerolog/log"
)

// reindexAssignment is a (cluster_id, rowid) pair produced during
// Go-side assignment against the NEW centroid set and inserted into
// the staging table.
type reindexAssignment struct {
	clusterID int64
	rowid     int64
	vec       []byte
}

// defaultReindexChunkRows matches vecindex.DefaultVecSessionVars().ReindexChunkRows.
// Kept as a package constant so the pipeline does not depend on session-var
// wiring for non-coordinator callers (tests, future background workers).
const defaultReindexChunkRows = 10000

func retuneReindexMeta(meta common.VectorIndexMeta, spec vecindex.IVFSpec, rows int64) (common.VectorIndexMeta, vecindex.IVFSpec) {
	if meta.AutoTuneNlist {
		meta.Nlist = autoTuneNlist(rows)
		spec.Nlist = meta.Nlist
	}
	if meta.AutoTuneNprobe {
		meta.Nprobe = autoTuneNprobe(meta.Nlist)
		spec.Nprobe = meta.Nprobe
	}
	if meta.AutoTuneNlist || meta.AutoTuneNprobe {
		spec.Seed = StableIndexSeed(meta)
	}
	return meta, spec
}

// Reindex executes the design §8.3 shadow-swap pipeline for one vector index.
//
// Sequence:
//  1. Resolve the IndexState via engine.Lookup — the manager flipped status
//     to 'reindexing' before calling us, so the state must be registered.
//  2. Warm-start k-means from driftState.Snapshot() (fix G). If drift is
//     empty (rare: very-new index, or test) fall back to k-means++.
//  3. Sample base-table vectors, run Lloyd iterations, build newCentroids.
//  4. Drop any leftover staging table from a crashed prior attempt, then
//     CREATE TABLE <staging> (same shape as members).
//  5. Chunked populate — each chunk its own short txn:
//     SELECT rowid, embed FROM base WHERE rowid > :last ORDER BY rowid
//     LIMIT :chunk; assign every embed in Go against NEW centroids (NOT
//     via the __marmot_vec_assign UDF, which still sees old probeState);
//     INSERT (cluster_id, rowid) into staging.
//  6. Single short swap txn (BEGIN IMMEDIATE): delta-replay in Go, DROP
//     old members, ALTER staging RENAME TO members, CREATE rowid index,
//     REPLACE centroids row, UPDATE status='ready'.
//  7. After COMMIT, atomic in-memory probeState.Swap(newCS) and
//     driftState.Reset(newCS) (§8.5 — next MacQueen cycle forks clean).
//
// chunkRows <= 0 falls back to defaultReindexChunkRows.
// updatedAt is the HLC timestamp written to _marmot_vec_<idx>_centroids.updated_at.
func Reindex(
	ctx context.Context,
	db *sql.DB,
	engine *vecindex.Engine,
	meta common.VectorIndexMeta,
	chunkRows int,
	updatedAt int64,
) error {
	if chunkRows <= 0 {
		chunkRows = defaultReindexChunkRows
	}
	state, ok := engine.Lookup(meta.IndexName)
	if !ok {
		return fmt.Errorf("MARMOT-VEC-013: vector index %q not registered in engine", meta.IndexName)
	}
	spec := state.Spec()
	currentN, err := countIndexableRows(ctx, db, meta.TableName, meta.ColumnName, spec)
	if err != nil {
		return fmt.Errorf("reindex: count rows for tuning: %w", err)
	}
	meta, spec = retuneReindexMeta(meta, spec, currentN)

	oldCS := state.ProbeState()
	oldEpoch := uint64(0)
	if oldCS != nil {
		oldEpoch = oldCS.Epoch()
	}

	// Step 2+3: warm-start seed from MacQueen-drifted centroids (fix G).
	warmSeed := state.DriftCentroids()
	if len(warmSeed) != spec.Nlist {
		warmSeed = nil // dimension mismatch → cold start
	}

	// Sample vectors from base table.
	samples, err := sampleVectorsForReindex(ctx, db, meta.TableName, meta.ColumnName, spec)
	if err != nil {
		return fmt.Errorf("reindex: sample: %w", err)
	}
	if len(samples) == 0 {
		// Empty base table — nothing to rebuild. Treat as success: staging
		// stays absent and the swap is a no-op, but we still want the
		// status='ready' flip so the manager sees the index settle.
		if err := swapEmptyMembers(ctx, db, meta); err != nil {
			return fmt.Errorf("reindex: empty swap: %w", err)
		}
		engine.Register(meta.IndexName, vecindex.NewIndexState(spec, nil))
		log.Info().Str("index", meta.IndexName).Msg("Reindex: empty base table, nothing to rebuild")
		return nil
	}

	// Step 3: train new centroids.
	actualK := spec.Nlist
	if actualK > len(samples) {
		actualK = len(samples)
	}
	var newCentroids [][]float32
	if len(warmSeed) == actualK {
		newCentroids, err = kmeans.LloydFromInit(samples, warmSeed, spec.Seed, kmeansMaxIter)
		if err != nil {
			return fmt.Errorf("reindex: warm-start k-means: %w", err)
		}
	} else {
		// Cold fallback — fresh k-means++. Reached only when drift is empty
		// (first REINDEX on a replica that never ran MacQueen) or when nlist
		// changed (not currently supported as a REINDEX option, but guarded
		// for forward safety).
		newCentroids, err = kmeans.KMeansPlusPlus(samples, actualK, spec.Seed, kmeansMaxIter)
		if err != nil {
			return fmt.Errorf("reindex: cold k-means: %w", err)
		}
	}

	newCS, err := kmeans.NewCentroidSet(oldEpoch+1, newCentroids)
	if err != nil {
		return fmt.Errorf("reindex: build new centroid set: %w", err)
	}
	blob, err := vecindex.EncodeCentroidBlob(newCS)
	if err != nil {
		return fmt.Errorf("reindex: encode blob: %w", err)
	}

	// Step 4: (re)create staging table.
	if err := recreateStagingTable(ctx, db, meta.IndexName); err != nil {
		return err
	}

	// Step 5: chunked populate of staging table. Vectors are NOT accumulated
	// in RAM here — the primary read path is the packed snapshot plus resident
	// delta rebuilt after the swap commits.
	if err := populateStaging(ctx, db, meta, spec, newCS, chunkRows); err != nil {
		return fmt.Errorf("reindex: populate staging: %w", err)
	}

	// Step 6: atomic swap txn (delta replay + DROP + RENAME + REPLACE + status).
	if err := swapStagingIntoMembers(ctx, db, meta, spec, newCS, blob, updatedAt, currentN); err != nil {
		return fmt.Errorf("reindex: swap: %w", err)
	}

	// Step 7: in-memory swap (probe + drift + resident delta).
	newState := vecindex.NewIndexState(spec, newCS)
	if err := loadAndStoreResidentDelta(ctx, db, newState, spec, meta.TableName, meta.ColumnName); err != nil {
		log.Warn().Err(err).Str("index", meta.IndexName).
			Msg("reindex: resident delta load failed")
	}
	engine.Register(meta.IndexName, newState)

	log.Info().
		Str("index", meta.IndexName).
		Uint64("old_epoch", oldEpoch).
		Uint64("new_epoch", newCS.Epoch()).
		Int64("rows", currentN).
		Msg("Reindex: shadow-swap committed")
	return nil
}

// sampleVectorsForReindex reads up to sampleCap vectors from the base table
// for k-means training. Mirrors computeCentroids but returns the raw sample
// set so LloydFromInit / KMeansPlusPlus can be chosen upstream.
//
// Scans in rowid order and applies Algorithm-R reservoir sampling seeded from
// spec.Seed (see reservoirSlot). Two nodes seeing byte-identical base rows
// select identical samples, feeding k-means deterministically so concurrent
// REINDEX converges. ORDER BY RANDOM() would silently break this — SQLite's
// RANDOM() is seeded from system entropy, not spec.Seed.
func sampleVectorsForReindex(
	ctx context.Context,
	db *sql.DB,
	tableName, columnName string,
	spec vecindex.IVFSpec,
) ([][]float32, error) {
	tableQ := quoteIdent(tableName)
	colQ := quoteIdent(columnName)

	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY rowid",
			colQ, tableQ, colQ),
	)
	if err != nil {
		return nil, fmt.Errorf("sample query: %w", err)
	}
	defer rows.Close()

	rng := rand.New(rand.NewSource(int64(spec.Seed)))
	sampleCap := max(sampleFloor, 2*spec.Nlist)
	samples := make([][]float32, 0, sampleCap)
	seen := 0
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		mv, err := materializeVectorBlob(blob, spec.Metric, spec.Dim, spec.MaxNorm)
		if err != nil {
			return nil, fmt.Errorf("materialize: %w", err)
		}
		if mv == nil {
			continue
		}

		// Roll the reservoir decision before decoding so every row advances
		// the PRNG identically on every node.
		seen++
		slot := reservoirSlot(seen, sampleCap, rng)
		if slot < 0 {
			continue
		}

		raw, decErr := decodeVec(blob)
		if decErr != nil {
			return nil, fmt.Errorf("decode: %w", decErr)
		}
		v := make([]float32, len(raw))
		copy(v, raw)
		if spec.Metric == vecindex.MetricDot {
			augmented, augErr := metric.AugmentData(v, spec.MaxNorm, nil)
			if augErr != nil {
				return nil, fmt.Errorf("augment: %w", augErr)
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
		return nil, fmt.Errorf("iter: %w", err)
	}
	return samples, nil
}

// countIndexableRows returns the number of base rows whose vectors currently
// materialize into a live sidecar value. This excludes non-null but
// non-indexable inputs such as zero-norm cosine vectors.
func countIndexableRows(
	ctx context.Context,
	db *sql.DB,
	tableName, columnName string,
	spec vecindex.IVFSpec,
) (int64, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY rowid",
			quoteIdent(columnName), quoteIdent(tableName), quoteIdent(columnName)),
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return 0, err
		}
		mv, err := materializeVectorBlob(blob, spec.Metric, spec.Dim, spec.MaxNorm)
		if err != nil {
			return 0, err
		}
		if mv != nil {
			n++
		}
	}
	return n, rows.Err()
}

// recreateStagingTable drops any leftover staging table (from a crashed
// previous attempt) and creates a fresh one with the same shape as members.
// Runs outside the swap txn — the staging table is CDC-excluded by the
// double-underscore naming convention.
func recreateStagingTable(ctx context.Context, db *sql.DB, indexName string) error {
	staging := vecindex.StagingTable(indexName)
	stagingQ := quoteIdent(staging)
	stagingIdxQ := quoteIdent(vecindex.StagingRowidIndex(indexName))
	stagingUQ := quoteIdent(vecindex.StagingRowidUniqueIndex(indexName))

	if _, err := db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, stagingQ)); err != nil {
		return fmt.Errorf("drop staging: %w", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (
		cluster_id INTEGER NOT NULL,
		rowid      INTEGER NOT NULL,
		vec        BLOB    NOT NULL,
		PRIMARY KEY (cluster_id, rowid)
	) WITHOUT ROWID`, stagingQ)); err != nil {
		return fmt.Errorf("create staging: %w", err)
	}
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf(`CREATE INDEX %s ON %s(rowid)`, stagingIdxQ, stagingQ)); err != nil {
		return fmt.Errorf("create staging rowid idx: %w", err)
	}
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf(`CREATE UNIQUE INDEX %s ON %s(rowid)`, stagingUQ, stagingQ)); err != nil {
		return fmt.Errorf("create staging rowid unique idx: %w", err)
	}
	return nil
}

// populateStaging assigns every materializable base-row embedding to the NEW
// centroid set and inserts (cluster_id, rowid, vec) into the staging table.
//
// Assignment happens in Go — the engine's __marmot_vec_assign UDF reads
// probeState which still holds OLD centroids until the swap, so using
// that UDF here would produce stale assignments.
//
// Each chunk is its own short txn so base-table writers see writer slots
// between chunks. Iteration is rowid-ordered cursor (WHERE rowid > :last)
// so concurrent INSERTs during the populate do not shift the cursor
// backwards. Rows inserted during populate land in members with
// cluster_id=0 (via the AFTER INSERT trigger) and are picked up by the
// swap txn's delta-replay pass.
func populateStaging(
	ctx context.Context,
	db *sql.DB,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	newCS *kmeans.CentroidSet,
	chunkRows int,
) error {
	staging := quoteIdent(vecindex.StagingTable(meta.IndexName))
	tableQ := quoteIdent(meta.TableName)
	colQ := quoteIdent(meta.ColumnName)

	var lastRowid int64 = 0
	for {
		rows, err := db.QueryContext(ctx,
			fmt.Sprintf(
				`SELECT rowid, %s FROM %s WHERE rowid > ? AND %s IS NOT NULL ORDER BY rowid LIMIT ?`,
				colQ, tableQ, colQ),
			lastRowid, chunkRows,
		)
		if err != nil {
			return fmt.Errorf("chunk query at rowid>%d: %w", lastRowid, err)
		}

		batch := make([]reindexAssignment, 0, chunkRows)
		var batchLastRowid int64 = lastRowid

		for rows.Next() {
			var rid int64
			var blob []byte
			if err := rows.Scan(&rid, &blob); err != nil {
				rows.Close()
				return fmt.Errorf("scan chunk row: %w", err)
			}
			vec, matErr := materializeVectorBlob(blob, spec.Metric, spec.Dim, spec.MaxNorm)
			if matErr != nil {
				rows.Close()
				return fmt.Errorf("materialize rowid %d: %w", rid, matErr)
			}
			if vec == nil {
				batchLastRowid = rid
				continue
			}
			cid, assignErr := assignPreparedAgainstSet(vec, spec, newCS)
			if assignErr != nil {
				rows.Close()
				return fmt.Errorf("assign rowid %d: %w", rid, assignErr)
			}
			batch = append(batch, reindexAssignment{clusterID: cid, rowid: rid, vec: vec})
			batchLastRowid = rid
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("chunk iter: %w", err)
		}
		rows.Close()

		if len(batch) == 0 {
			// No more rows — cursor exhausted.
			break
		}

		if err := insertStagingBatch(ctx, db, staging, batch); err != nil {
			return fmt.Errorf("insert chunk: %w", err)
		}
		lastRowid = batchLastRowid
	}
	return nil
}

// assignPreparedAgainstSet mirrors IndexState.AssignNearestPrepared for an
// arbitrary CentroidSet (the NEW one, not the engine's live probeState).
// vecBytes must already be in the internal sidecar/search space.
func assignPreparedAgainstSet(vecBytes []byte, spec vecindex.IVFSpec, cs *kmeans.CentroidSet) (int64, error) {
	if len(vecBytes) == 0 || len(vecBytes)%4 != 0 {
		return 0, fmt.Errorf("MARMOT-VEC-014: invalid vector blob length %d", len(vecBytes))
	}
	if got := len(vecBytes) / 4; got != spec.InternalDim() {
		return 0, fmt.Errorf("MARMOT-VEC-014: internal dim mismatch: got %d want %d", got, spec.InternalDim())
	}
	clusterID, _, err := cs.AssignNearest(metric.BytesToFloat32(vecBytes), spec.InternalMetric())
	if err != nil {
		return 0, err
	}
	return int64(clusterID) + 1, nil
}

// insertStagingBatch inserts all (cluster_id, rowid, vec) triples in batch
// within a single short txn. The unique rowid index ensures later writes for a
// row replace earlier staging snapshots.
func insertStagingBatch(ctx context.Context, db *sql.DB, staging string, batch []reindexAssignment) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin chunk txn: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.PrepareContext(ctx,
		fmt.Sprintf(`INSERT OR REPLACE INTO %s (cluster_id, rowid, vec) VALUES (?, ?, ?)`, staging))
	if err != nil {
		return fmt.Errorf("prepare chunk insert: %w", err)
	}
	defer stmt.Close()

	for _, a := range batch {
		if _, err := stmt.ExecContext(ctx, a.clusterID, a.rowid, a.vec); err != nil {
			return fmt.Errorf("exec insert rowid=%d: %w", a.rowid, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit chunk: %w", err)
	}
	tx = nil
	return nil
}

// swapStagingIntoMembers is the §8.3 step-7 atomic swap. Everything here
// happens inside one SQLite write txn, bounded by the delta-replay cost
// (O(|cluster_id=0|)). Target: < 200ms @ 100K rows.
//
// Delta replay is done in Go to honour the "assign against NEW centroids"
// invariant — we read (rowid, embed) for every member with cluster_id=0,
// compute the new assignment, and INSERT OR IGNORE into staging. Then we
// DROP the live members table, RENAME the staging table into its place,
// CREATE the rowid secondary index, REPLACE the centroids blob row, and
// flip the metadata status to 'ready'.
//
// The in-memory probeState swap happens AFTER this function returns; the
// design notes it "inside the txn" but Go cannot share transactional
// visibility with an in-process atomic.Pointer — an immediate post-commit
// swap is observationally equivalent for readers using the UDF or the
// query transpiler, because both acquire the engine state independently
// of the SQL txn.
func swapStagingIntoMembers(
	ctx context.Context,
	db *sql.DB,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	newCS *kmeans.CentroidSet,
	blob []byte,
	updatedAt int64,
	currentN int64,
) error {
	members := vecindex.MembersTable(meta.IndexName)
	staging := vecindex.StagingTable(meta.IndexName)
	membersIdx := vecindex.MembersRowidIndex(meta.IndexName)
	membersUQ := vecindex.MembersRowidUniqueIndex(meta.IndexName)
	stagingIdx := vecindex.StagingRowidIndex(meta.IndexName)
	stagingUQ := vecindex.StagingRowidUniqueIndex(meta.IndexName)
	centroids := vecindex.CentroidsTable(meta.IndexName)

	membersQ := quoteIdent(members)
	stagingQ := quoteIdent(staging)
	stagingIdxQ := quoteIdent(stagingIdx)
	stagingUQQ := quoteIdent(stagingUQ)
	centroidsQ := quoteIdent(centroids)

	swapStart := time.Now()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin swap txn: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM %s AS s WHERE NOT EXISTS (SELECT 1 FROM %s AS m WHERE m.rowid = s.rowid)`,
			stagingQ, membersQ),
	); err != nil {
		return fmt.Errorf("prune stale staging rows: %w", err)
	}

	// Delta replay: pull every prepared sidecar vector currently in cluster_id=0,
	// assign against NEW centroids in Go, and overwrite any older staging row
	// for the same rowid.
	deltaRows, err := tx.QueryContext(ctx,
		fmt.Sprintf(`SELECT rowid, vec FROM %s WHERE cluster_id = 0`, membersQ),
	)
	if err != nil {
		return fmt.Errorf("delta query: %w", err)
	}
	var delta []reindexAssignment
	for deltaRows.Next() {
		var rid int64
		var vec []byte
		if err := deltaRows.Scan(&rid, &vec); err != nil {
			deltaRows.Close()
			return fmt.Errorf("delta scan: %w", err)
		}
		cid, assignErr := assignPreparedAgainstSet(vec, spec, newCS)
		if assignErr != nil {
			deltaRows.Close()
			return fmt.Errorf("delta assign rowid %d: %w", rid, assignErr)
		}
		delta = append(delta, reindexAssignment{clusterID: cid, rowid: rid, vec: append([]byte(nil), vec...)})
	}
	if err := deltaRows.Err(); err != nil {
		deltaRows.Close()
		return fmt.Errorf("delta iter: %w", err)
	}
	deltaRows.Close()

	if len(delta) > 0 {
		deltaStmt, err := tx.PrepareContext(ctx,
			fmt.Sprintf(`INSERT OR REPLACE INTO %s (cluster_id, rowid, vec) VALUES (?, ?, ?)`, stagingQ))
		if err != nil {
			return fmt.Errorf("prepare delta insert: %w", err)
		}
		for _, d := range delta {
			if _, err := deltaStmt.ExecContext(ctx, d.clusterID, d.rowid, d.vec); err != nil {
				deltaStmt.Close()
				return fmt.Errorf("delta insert rowid %d: %w", d.rowid, err)
			}
		}
		deltaStmt.Close()
	}

	// SQLite validates trigger references during ALTER TABLE RENAME TO.
	// Between DROP members and RENAME staging→members the base-table
	// triggers reference a non-existent table — PRAGMA legacy_alter_table
	// disables this validation so the rename completes without error.
	// The triggers re-bind at execution time (§8.3 "Trigger-name survival
	// across RENAME"). Scoped to this txn only.
	if _, err := tx.ExecContext(ctx, `PRAGMA legacy_alter_table = ON`); err != nil {
		return fmt.Errorf("set legacy_alter_table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DROP TABLE %s`, membersQ)); err != nil {
		return fmt.Errorf("drop members: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, stagingQ, quoteIdent(members)),
	); err != nil {
		return fmt.Errorf("rename staging: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA legacy_alter_table = OFF`); err != nil {
		return fmt.Errorf("reset legacy_alter_table: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`CREATE INDEX %s ON %s(rowid)`, quoteIdent(membersIdx), membersQ),
	); err != nil {
		return fmt.Errorf("create rowid idx: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`CREATE UNIQUE INDEX %s ON %s(rowid)`, quoteIdent(membersUQ), membersQ),
	); err != nil {
		return fmt.Errorf("create rowid unique idx: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DROP INDEX IF EXISTS %s`, stagingIdxQ)); err != nil {
		return fmt.Errorf("drop staging rowid idx: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DROP INDEX IF EXISTS %s`, stagingUQQ)); err != nil {
		return fmt.Errorf("drop staging rowid unique idx: %w", err)
	}

	// REPLACE INTO centroids row — replicated via CDC (single-underscore prefix).
	// last_n = current COUNT from the base table so replicas converge on the
	// same row count that drove this rebuild.
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT OR REPLACE INTO %s (index_id, version, updated_at, nlist, compression, centroids, last_n)
		             VALUES (1, ?, ?, ?, 'zstd', ?, ?)`, centroidsQ),
		int64(newCS.Epoch()), updatedAt, newCS.Len(), blob, currentN,
	); err != nil {
		return fmt.Errorf("replace centroids: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes
		    SET nlist=?, nprobe=?, status='ready'
		  WHERE index_name=?`,
		meta.Nlist, meta.Nprobe, meta.IndexName); err != nil {
		return fmt.Errorf("update metadata ready: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit swap: %w", err)
	}
	tx = nil

	log.Debug().
		Str("index", meta.IndexName).
		Dur("swap_duration", time.Since(swapStart)).
		Int("delta_rows", len(delta)).
		Msg("Reindex: swap txn committed")
	return nil
}

func swapEmptyMembers(ctx context.Context, db *sql.DB, meta common.VectorIndexMeta) error {
	membersQ := quoteIdent(vecindex.MembersTable(meta.IndexName))
	centroidsQ := quoteIdent(vecindex.CentroidsTable(meta.IndexName))

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin empty swap txn: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, membersQ)); err != nil {
		return fmt.Errorf("clear members: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, centroidsQ)); err != nil {
		return fmt.Errorf("clear centroids: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes
		    SET nlist=?, nprobe=?, status='ready'
		  WHERE index_name=?`,
		meta.Nlist, meta.Nprobe, meta.IndexName); err != nil {
		return fmt.Errorf("update metadata ready: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit empty swap: %w", err)
	}
	tx = nil
	return nil
}
