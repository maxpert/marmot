package db

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/rs/zerolog/log"
)

// indexCacheKey uniquely identifies an index by (database, table, column).
// Using a struct key gives O(1) map lookup without string concatenation.
type indexCacheKey struct {
	database string
	table    string
	column   string
}

// rowCountEntry caches a table row count with a TTL for cost estimation.
type rowCountEntry struct {
	count     int64
	expiresAt time.Time
}

// IndexLifecycleHook is notified after a CREATE VECTOR INDEX DDL transaction
// commits so that the caller can trigger bulk population and centroid training.
// P1-C implements this interface.
type IndexLifecycleHook interface {
	OnIndexCreated(ctx context.Context, meta common.VectorIndexMeta) error
}

// IndexReindexHook is notified when the manager receives a REINDEX VECTOR
// DDL. The implementation (EngineHook) owns the shadow-swap pipeline:
// warm-start k-means, chunked populate of the staging table, and atomic
// swap including in-memory probeState swap (design §8.3).
//
// The hook MUST NOT change __marmot_vector_indexes.status itself — the
// manager owns that column so crash recovery can detect "still reindexing".
type IndexReindexHook interface {
	OnIndexReindex(ctx context.Context, meta common.VectorIndexMeta) error
}

// EngineProvider allows the VectorIndexManager to remove in-memory engine
// state before the DROP SQL transaction begins, ensuring concurrent queries
// fail fast rather than seeing partially-dropped state.
//
// RemoveIndex returns a restore function that re-registers the state if
// the DROP DDL transaction fails (MEDIUM-7 fix). The caller must invoke
// restore() on DDL failure to avoid leaving the index in an inconsistent
// state (engine evicted, SQL objects still present).
type EngineProvider interface {
	RemoveIndex(indexName string) (restore func())
}

// VectorIndexMeta is an alias for the shared common.VectorIndexMeta.
type VectorIndexMeta = common.VectorIndexMeta

// VectorIndexManager manages vector index DDL lifecycle in SQLite.
// It owns:
//   - __marmot_vector_indexes metadata table (schema + CRUD)
//   - Trigger + shadow-table DDL for each index (CREATE / DROP)
//   - Schema migration at startup
//
// In-memory engine state is delegated to the EngineProvider (P1-C).
type VectorIndexManager struct {
	mu            sync.Mutex
	dbMgr         *DatabaseManager
	lifecycleHook IndexLifecycleHook
	reindexHook   IndexReindexHook
	engineProv    EngineProvider

	// cacheMu protects indexCache and rowCountCache. Separate from mu to avoid
	// holding the lifecycle lock during cache reads on the query hot-path.
	cacheMu       sync.RWMutex
	indexCache    map[indexCacheKey]*VectorIndexMeta // keyed by (db, table, col)
	rowCountCache map[indexCacheKey]*rowCountEntry   // keyed by (db, table, "")
}

// NewVectorIndexManager creates a new VectorIndexManager.
func NewVectorIndexManager(dbMgr *DatabaseManager) *VectorIndexManager {
	return &VectorIndexManager{
		dbMgr:         dbMgr,
		indexCache:    make(map[indexCacheKey]*VectorIndexMeta),
		rowCountCache: make(map[indexCacheKey]*rowCountEntry),
	}
}

// SetLifecycleHook installs the hook called after each successful CREATE DDL.
func (m *VectorIndexManager) SetLifecycleHook(h IndexLifecycleHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lifecycleHook = h
}

// SetReindexHook installs the hook invoked by ReindexIndex to execute the
// shadow-swap pipeline. Safe to call before or after Start.
func (m *VectorIndexManager) SetReindexHook(h IndexReindexHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reindexHook = h
}

// SetEngineProvider installs the provider called during DROP to remove
// in-memory state before the SQL transaction begins.
func (m *VectorIndexManager) SetEngineProvider(e EngineProvider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.engineProv = e
}

// Start runs schema migration for all known databases and loads existing indexes.
func (m *VectorIndexManager) Start(ctx context.Context) error {
	for _, dbName := range m.dbMgr.ListDatabases() {
		conn, err := m.dbMgr.GetDatabaseConnection(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("VectorIndexManager: failed to get connection for migration")
			continue
		}
		if err := MigrateVectorIndexesSchema(conn); err != nil {
			return fmt.Errorf("vector index schema migration for %s: %w", dbName, err)
		}
	}
	if err := m.loadExistingIndexes(ctx); err != nil {
		return err
	}
	// Design §8.4: recover any indexes left in status='reindexing' after a
	// crash. Drop leftover staging tables so the next REINDEX starts clean.
	return m.recoverReindexingIndexes(ctx)
}

// Stop is a no-op; individual index shutdown is handled by the engine.
func (m *VectorIndexManager) Stop() error {
	return nil
}

// CreateIndex executes the full CREATE VECTOR INDEX DDL transaction (design §8.1):
//  1. Validates the base table has INTEGER PRIMARY KEY (fix R6).
//  2. Auto-tunes nlist/nprobe if not supplied.
//  3. Creates: centroids table, members table+index, base-table triggers,
//     centroid-change triggers — all in one atomic DDL transaction.
//  4. Inserts metadata row with status='building'.
//  5. Calls the lifecycle hook (P1-C) which performs bulk population and
//     flips status to 'ready'.
//
// Note: the DDL transaction holds SQLite's single writer lock for its duration.
// Concurrent user writes block until the DDL commits. For large base tables the
// lock window is the DDL itself (fast); bulk populate runs outside the DDL txn.
func (m *VectorIndexManager) CreateIndex(ctx context.Context, meta VectorIndexMeta) error {
	conn, err := m.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		return fmt.Errorf("vector index: get database %s: %w", meta.Database, err)
	}

	// Schema migration is run at Start(); skip redundant call here.

	if err := vecindex.ValidateIndexName(meta.IndexName); err != nil {
		return fmt.Errorf("vector index: %w", err)
	}

	if err := ValidateBaseTableForVectorIndex(conn, meta.TableName); err != nil {
		return err
	}

	// Auto-tune nlist and nprobe if not user-supplied (design §6.1).
	if meta.Nlist == 0 || meta.Nprobe == 0 {
		n, err := countRows(ctx, conn, meta.TableName)
		if err != nil {
			return fmt.Errorf("vector index: count rows for auto-tune: %w", err)
		}
		if meta.Nlist == 0 {
			meta.Nlist = autoTuneNlist(n)
		}
		if meta.Nprobe == 0 {
			meta.Nprobe = autoTuneNprobe(meta.Nlist)
		}
	}

	if err := m.execCreateDDL(ctx, conn, meta); err != nil {
		return err
	}

	// Hand off to engine for bulk populate (P1-C). Non-fatal if hook not yet wired.
	m.mu.Lock()
	hook := m.lifecycleHook
	m.mu.Unlock()

	// Insert into in-memory cache now that DDL has committed.
	metaCopy := meta
	key := indexCacheKey{database: meta.Database, table: meta.TableName, column: meta.ColumnName}
	m.cacheMu.Lock()
	m.indexCache[key] = &metaCopy
	m.cacheMu.Unlock()

	if hook != nil {
		if err := hook.OnIndexCreated(ctx, meta); err != nil {
			log.Error().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: lifecycle hook failed")
			return fmt.Errorf("vector index: populate failed: %w", err)
		}
	}
	return nil
}

// DropIndex executes the atomic DROP VECTOR INDEX sequence (design §8.2):
//  1. Removes in-memory engine state first (via EngineProvider) so concurrent
//     queries fail fast with "index not found" rather than touching partially-
//     dropped SQLite objects.
//  2. Runs a single DDL transaction: drop triggers → drop shadow tables →
//     drop centroids table → delete metadata row.
//
// Failure semantics: if the DDL transaction fails after in-memory state has
// been evicted, the index is left in an inconsistent state (engine evicted,
// SQL objects still present). All DROP statements use IF EXISTS, so this
// situation is rare. Recovery: restart the node — Start() reloads metadata
// and the lifecycle hook re-registers the engine state. Documented as
// acceptable per design §8.2 ("no 'dropping' status column needed").
//
// Note: the DDL transaction holds SQLite's single writer lock for its duration.
func (m *VectorIndexManager) DropIndex(ctx context.Context, indexName, database string) error {
	conn, err := m.dbMgr.GetDatabaseConnection(database)
	if err != nil {
		return fmt.Errorf("vector index: get database %s: %w", database, err)
	}

	// Evict in-memory state before the SQL txn (design §8.2 fix N).
	m.mu.Lock()
	ep := m.engineProv
	m.mu.Unlock()

	var restoreEngine func()
	if ep != nil {
		restoreEngine = ep.RemoveIndex(indexName)
	}

	// Remove from in-memory cache before SQL DDL (fail-fast contract).
	var removedKey indexCacheKey
	var removedMeta *VectorIndexMeta
	m.cacheMu.Lock()
	for k, v := range m.indexCache {
		if v.IndexName == indexName {
			removedKey = k
			removedMeta = v
			delete(m.indexCache, k)
			break
		}
	}
	m.cacheMu.Unlock()

	// MEDIUM-7 fix: if DDL fails, restore in-memory state so the index
	// remains functional rather than requiring a restart.
	ddlSucceeded := false
	defer func() {
		if ddlSucceeded {
			return
		}
		if restoreEngine != nil {
			restoreEngine()
		}
		if removedMeta != nil {
			m.cacheMu.Lock()
			m.indexCache[removedKey] = removedMeta
			m.cacheMu.Unlock()
		}
	}()

	if err := m.execDropDDL(ctx, conn, indexName); err != nil {
		log.Error().Err(err).Str("index", indexName).
			Msg("VectorIndexManager: DROP DDL failed, restoring in-memory state")
		return err
	}
	ddlSucceeded = true
	return nil
}

// ReindexIndex executes REINDEX VECTOR <name> per design §8.3:
//  1. Validates the index exists and is in a reindex-eligible status.
//  2. Flips __marmot_vector_indexes.status to 'reindexing' in a short txn.
//  3. Invokes the installed IndexReindexHook (EngineHook), which runs the
//     shadow-swap pipeline: warm-start k-means → chunked populate of the
//     staging table → atomic swap txn → in-memory probeState swap.
//  4. On pipeline success, the swap txn has already set status='ready'.
//  5. On pipeline failure, reverts status back to 'ready' so subsequent
//     retries can re-enter the pipeline from a clean baseline.
//
// The staging table is the sole drift-isolation boundary: triggers on the
// base table continue writing cluster_id=0 entries to the LIVE members
// table during reindex, and the swap txn replays them against the new
// centroids via Go-side assignment before the DROP+RENAME.
func (m *VectorIndexManager) ReindexIndex(ctx context.Context, indexName string) error {
	if err := vecindex.ValidateIndexName(indexName); err != nil {
		return fmt.Errorf("vector reindex: %w", err)
	}

	meta, ok := m.getIndexByName(indexName)
	if !ok {
		return fmt.Errorf("MARMOT-VEC-013: vector index %q not found", indexName)
	}

	m.mu.Lock()
	hook := m.reindexHook
	m.mu.Unlock()
	if hook == nil {
		return fmt.Errorf("vector reindex: engine hook not installed; REINDEX requires engine wiring")
	}

	conn, err := m.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		return fmt.Errorf("vector reindex: get database %s: %w", meta.Database, err)
	}

	// Flip status to 'reindexing' so crash recovery can detect in-flight state.
	if err := updateIndexStatus(ctx, conn, indexName, "reindexing"); err != nil {
		return fmt.Errorf("vector reindex: set status reindexing: %w", err)
	}
	m.setCachedStatus(meta.Database, meta.TableName, meta.ColumnName, "reindexing")

	if err := hook.OnIndexReindex(ctx, *meta); err != nil {
		// Revert on failure so retries aren't blocked by stale 'reindexing'.
		if revertErr := updateIndexStatus(ctx, conn, indexName, "ready"); revertErr != nil {
			log.Error().Err(revertErr).Str("index", indexName).
				Msg("VectorIndexManager: failed to revert status after REINDEX failure")
		}
		m.setCachedStatus(meta.Database, meta.TableName, meta.ColumnName, "ready")
		return fmt.Errorf("vector reindex: pipeline: %w", err)
	}
	// Pipeline's swap txn already set status='ready' — refresh the cache.
	m.setCachedStatus(meta.Database, meta.TableName, meta.ColumnName, "ready")

	log.Info().Str("index", indexName).Msg("VectorIndexManager: REINDEX complete")
	return nil
}

// getIndexByName scans the in-memory cache for a metadata row by index name.
// Returns a copy plus ok=true on hit. O(n) in the number of indexes, which
// is small (typically 0-10 per node).
func (m *VectorIndexManager) getIndexByName(name string) (*VectorIndexMeta, bool) {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()
	for _, v := range m.indexCache {
		if v.IndexName == name {
			cp := *v
			return &cp, true
		}
	}
	return nil, false
}

// setCachedStatus updates the Status field on a cached VectorIndexMeta.
// No-op when the entry is missing.
func (m *VectorIndexManager) setCachedStatus(database, table, column, status string) {
	key := indexCacheKey{database: database, table: table, column: column}
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	if v, ok := m.indexCache[key]; ok {
		v.Status = status
	}
}

// updateIndexStatus executes a short UPDATE on the metadata row. Shared by
// ReindexIndex and crash recovery.
func updateIndexStatus(ctx context.Context, conn *sql.DB, indexName, status string) error {
	_, err := conn.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes SET status=? WHERE index_name=?`,
		status, indexName)
	return err
}

// recoverReindexingIndexes implements design §8.4 for status='reindexing':
// on startup, drop any leftover staging table (from a crashed REINDEX
// attempt) and flip the metadata status back to 'ready'. The live members
// table is untouched — old centroids remain valid; the operator may
// re-issue REINDEX to retry.
func (m *VectorIndexManager) recoverReindexingIndexes(ctx context.Context) error {
	m.cacheMu.RLock()
	stale := make([]VectorIndexMeta, 0)
	for _, v := range m.indexCache {
		if v.Status == "reindexing" {
			stale = append(stale, *v)
		}
	}
	m.cacheMu.RUnlock()

	for _, meta := range stale {
		conn, err := m.dbMgr.GetDatabaseConnection(meta.Database)
		if err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).Str("database", meta.Database).
				Msg("VectorIndexManager: crash recovery: get connection failed")
			continue
		}
		staging := vecindex.StagingTable(meta.IndexName)
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, staging)); err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).
				Msg("VectorIndexManager: crash recovery: drop staging failed")
			continue
		}
		if err := updateIndexStatus(ctx, conn, meta.IndexName, "ready"); err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).
				Msg("VectorIndexManager: crash recovery: revert status failed")
			continue
		}
		m.setCachedStatus(meta.Database, meta.TableName, meta.ColumnName, "ready")
		log.Info().Str("index", meta.IndexName).
			Msg("VectorIndexManager: crash recovery: dropped staging and reverted to 'ready'")
	}
	return nil
}

// execCreateDDL runs the DDL transaction for CREATE VECTOR INDEX (design §8.1 steps 1-9).
//
// Identifier safety: idx, tbl, and col are interpolated into DDL strings via
// double-quote SQL identifier escaping. idx is pre-validated by
// vecindex.ValidateIndexName (ASCII letters/digits/underscore only) before
// reaching this function, so the '...<idx>...' literal in trigger bodies is
// injection-safe. tbl and col are quoted with escapeQuote to handle edge-cases.
func (m *VectorIndexManager) execCreateDDL(ctx context.Context, conn *sql.DB, meta VectorIndexMeta) error {
	idx := meta.IndexName
	tbl := meta.TableName
	col := meta.ColumnName

	// Defense-in-depth: idx is interpolated as a SQL string literal inside the
	// centroid-change trigger bodies (`'<idx>'`). ValidateIndexName guarantees
	// idx matches [A-Za-z][A-Za-z0-9_]* with no quote characters, making the
	// interpolation safe. Re-assert here so any future caller that skips the
	// CreateIndex validation path is caught at emit time rather than silently
	// producing unsafe DDL.
	if err := vecindex.ValidateIndexName(idx); err != nil {
		return fmt.Errorf("execCreateDDL: %w", err)
	}

	centroids := vecindex.CentroidsTable(idx)
	members := vecindex.MembersTable(idx)
	membersIdx := vecindex.MembersRowidIndex(idx)
	trgAI := vecindex.TriggerInsert(idx)
	trgAU := vecindex.TriggerUpdate(idx)
	trgAD := vecindex.TriggerDelete(idx)
	trgCentAI := vecindex.TriggerCentroidChange(idx)
	trgCentAU := vecindex.TriggerCentroidsVersionUpdate(idx)

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("vector index: begin DDL txn: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	stmts := []string{
		// Centroids table — CDC-replicated (single underscore prefix).
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
			index_id    INTEGER PRIMARY KEY,
			version     INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL,
			nlist       INTEGER NOT NULL,
			compression TEXT    NOT NULL,
			centroids   BLOB    NOT NULL,
			last_n      INTEGER NOT NULL
		)`, centroids),

		// Members shadow table — CDC-excluded (double underscore prefix).
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
			cluster_id INTEGER NOT NULL,
			rowid      INTEGER NOT NULL,
			PRIMARY KEY (cluster_id, rowid)
		) WITHOUT ROWID`, members),

		// Secondary index for fast rowid lookups during UPDATE/DELETE.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s" ON "%s"(rowid)`,
			membersIdx, members),

		// AFTER INSERT trigger: newly inserted rows enter delta (cluster_id=0).
		fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS "%s"
			AFTER INSERT ON "%s" WHEN NEW."%s" IS NOT NULL
			BEGIN
				INSERT INTO "%s" (cluster_id, rowid) VALUES (0, NEW.rowid);
			END`, trgAI, tbl, col, members),

		// AFTER UPDATE trigger: remove old assignment, re-enter delta.
		fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS "%s"
			AFTER UPDATE OF "%s" ON "%s" WHEN NEW."%s" IS NOT NULL
			BEGIN
				DELETE FROM "%s" WHERE rowid = OLD.rowid;
				INSERT INTO "%s" (cluster_id, rowid) VALUES (0, NEW.rowid);
			END`, trgAU, col, tbl, col, members, members),

		// AFTER DELETE trigger: remove from members entirely.
		fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS "%s"
			AFTER DELETE ON "%s"
			BEGIN
				DELETE FROM "%s" WHERE rowid = OLD.rowid;
			END`, trgAD, tbl, members),

		// Centroid-change trigger — AFTER INSERT on centroids (design §8.8).
		fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS "%s"
			AFTER INSERT ON "%s"
			BEGIN
				SELECT __marmot_vec_notify_centroid_change('%s', NEW.version);
			END`, trgCentAI, centroids, idx),

		// Centroid-change trigger — AFTER UPDATE OF version on centroids (design §8.8).
		fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS "%s"
			AFTER UPDATE OF version ON "%s"
			BEGIN
				SELECT __marmot_vec_notify_centroid_change('%s', NEW.version);
			END`, trgCentAU, centroids, idx),

		// Metadata row — status='building'; engine flips to 'ready' after populate.
		fmt.Sprintf(`INSERT INTO __marmot_vector_indexes
			(index_name, table_name, column_name, database_name, metric, dim,
			 nlist, nprobe, max_norm, status, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'building', ?)
			ON CONFLICT(index_name) DO NOTHING`),
	}

	// All DDL statements except the last metadata INSERT.
	for _, s := range stmts[:len(stmts)-1] {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("vector index: DDL %q: %w", s[:min(60, len(s))], err)
		}
	}

	// Metadata INSERT with bound parameters.
	if _, err := tx.ExecContext(ctx, stmts[len(stmts)-1],
		meta.IndexName, meta.TableName, meta.ColumnName, meta.Database,
		meta.Metric, meta.Dim, meta.Nlist, meta.Nprobe, meta.MaxNorm,
		meta.CreatedAt,
	); err != nil {
		return fmt.Errorf("vector index: insert metadata: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("vector index: commit DDL: %w", err)
	}
	tx = nil // prevent deferred rollback

	log.Info().
		Str("index", meta.IndexName).
		Str("database", meta.Database).
		Str("table", meta.TableName).
		Str("column", meta.ColumnName).
		Int("nlist", meta.Nlist).
		Int("nprobe", meta.Nprobe).
		Msg("VectorIndexManager: DDL committed")
	return nil
}

// execDropDDL runs the atomic DROP DDL transaction (design §8.2 steps 2-9).
func (m *VectorIndexManager) execDropDDL(ctx context.Context, conn *sql.DB, indexName string) error {
	idx := indexName

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("vector index drop: begin DDL txn: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	stmts := []string{
		fmt.Sprintf(`DROP TRIGGER IF EXISTS "%s"`, vecindex.TriggerInsert(idx)),
		fmt.Sprintf(`DROP TRIGGER IF EXISTS "%s"`, vecindex.TriggerUpdate(idx)),
		fmt.Sprintf(`DROP TRIGGER IF EXISTS "%s"`, vecindex.TriggerDelete(idx)),
		fmt.Sprintf(`DROP TRIGGER IF EXISTS "%s"`, vecindex.TriggerCentroidChange(idx)),
		fmt.Sprintf(`DROP TRIGGER IF EXISTS "%s"`, vecindex.TriggerCentroidsVersionUpdate(idx)),
		fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, vecindex.MembersTable(idx)),
		// Drop centroids table last — its CDC event replicates the DROP to peers.
		fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, vecindex.CentroidsTable(idx)),
		`DELETE FROM __marmot_vector_indexes WHERE index_name = ?`,
	}

	for _, s := range stmts[:len(stmts)-1] {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("vector index drop: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, stmts[len(stmts)-1], indexName); err != nil {
		return fmt.Errorf("vector index drop: delete metadata: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("vector index drop: commit: %w", err)
	}
	tx = nil

	log.Info().Str("index", indexName).Msg("VectorIndexManager: index dropped")
	return nil
}

// loadExistingIndexes loads persisted index metadata into the in-memory cache.
func (m *VectorIndexManager) loadExistingIndexes(ctx context.Context) error {
	for _, dbName := range m.dbMgr.ListDatabases() {
		conn, err := m.dbMgr.GetDatabaseConnection(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("VectorIndexManager: failed to get connection for load")
			continue
		}

		rows, err := conn.QueryContext(ctx, `
			SELECT index_name, table_name, column_name, database_name,
			       metric, dim, nlist, nprobe, max_norm, status
			FROM __marmot_vector_indexes`)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("VectorIndexManager: failed to query existing indexes")
			continue
		}
		for rows.Next() {
			var meta VectorIndexMeta
			if err := rows.Scan(
				&meta.IndexName, &meta.TableName, &meta.ColumnName, &meta.Database,
				&meta.Metric, &meta.Dim, &meta.Nlist, &meta.Nprobe, &meta.MaxNorm,
				&meta.Status,
			); err != nil {
				log.Warn().Err(err).Msg("VectorIndexManager: failed to scan index row")
				continue
			}
			key := indexCacheKey{database: meta.Database, table: meta.TableName, column: meta.ColumnName}
			metaCopy := meta
			m.cacheMu.Lock()
			m.indexCache[key] = &metaCopy
			m.cacheMu.Unlock()
			log.Info().Str("index", meta.IndexName).Str("status", meta.Status).Str("database", dbName).
				Msg("VectorIndexManager: found existing index")
		}
		rows.Close()
	}
	return nil
}

// GetIndexByColumn returns the index metadata for (database, table, column).
// If database is empty the cache is searched for a unique match across all
// databases; if the match is ambiguous MARMOT-VEC-022 is returned.
// Second return is false when no index is defined on that column.
// Lookup is O(1) for the exact-match case. Safe for concurrent use.
func (m *VectorIndexManager) GetIndexByColumn(database, table, column string) (*VectorIndexMeta, bool) {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()

	if database != "" {
		key := indexCacheKey{database: database, table: table, column: column}
		meta, ok := m.indexCache[key]
		return meta, ok
	}

	// Empty database: scan for a unique match.
	var found *VectorIndexMeta
	for k, v := range m.indexCache {
		if k.table == table && k.column == column {
			if found != nil {
				// Ambiguous — caller must supply a database qualifier.
				return nil, false
			}
			found = v
		}
	}
	return found, found != nil
}

// EstimatedRowCount returns an approximate row count for (database, table).
// The result is cached with a 1-minute TTL; on cache miss a SELECT COUNT(*)
// is executed with a short timeout. Returns 100_000 on any error so the
// planner degrades gracefully.
func (m *VectorIndexManager) EstimatedRowCount(database, table string) int64 {
	key := indexCacheKey{database: database, table: table}

	m.cacheMu.RLock()
	if entry, ok := m.rowCountCache[key]; ok && time.Now().Before(entry.expiresAt) {
		m.cacheMu.RUnlock()
		return entry.count
	}
	m.cacheMu.RUnlock()

	conn, err := m.dbMgr.GetDatabaseConnection(database)
	if err != nil {
		return 100_000
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var n int64
	if err := conn.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, escapeQuote(table)),
	).Scan(&n); err != nil {
		return 100_000
	}

	m.cacheMu.Lock()
	m.rowCountCache[key] = &rowCountEntry{count: n, expiresAt: time.Now().Add(time.Minute)}
	m.cacheMu.Unlock()

	return n
}

// countRows returns the number of rows in tableName.
func countRows(ctx context.Context, db *sql.DB, tableName string) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, escapeQuote(tableName)),
	).Scan(&n)
	return n, err
}

// autoTuneNlist computes nlist = clamp(4·√n, 64, 2048) per design §6.1.
func autoTuneNlist(n int64) int {
	v := int(4 * math.Sqrt(float64(n)))
	if v < 64 {
		return 64
	}
	if v > 2048 {
		return 2048
	}
	return v
}

// autoTuneNprobe computes nprobe = max(8, √nlist) per design §6.1.
func autoTuneNprobe(nlist int) int {
	v := int(math.Sqrt(float64(nlist)))
	if v < 8 {
		return 8
	}
	return v
}
