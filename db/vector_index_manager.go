package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
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

type IndexLocalChangeHook interface {
	OnIndexLocalChanges(ctx context.Context, meta common.VectorIndexMeta, entries []common.CDCEntry) error
}

type IndexOpenHook interface {
	OnIndexLoaded(ctx context.Context, meta common.VectorIndexMeta) error
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

const defaultTargetPartitionSize = 512

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
	// Recover any indexes left in status='reindexing' after a crash, then open
	// local file-backed state for all known indexes.
	if err := m.recoverReindexingIndexes(ctx); err != nil {
		return err
	}
	return m.openExistingIndexes(ctx)
}

// Stop is a no-op; individual index shutdown is handled by the engine.
func (m *VectorIndexManager) Stop() error {
	return nil
}

// CreateIndex records the vector-index metadata in SQLite and then lets the
// engine build or bootstrap the local file-backed serving state.
func (m *VectorIndexManager) CreateIndex(ctx context.Context, meta VectorIndexMeta) error {
	meta, err := m.ResolveCreateIndexMeta(ctx, meta)
	if err != nil {
		return err
	}
	conn, err := m.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		return fmt.Errorf("vector index: get database %s: %w", meta.Database, err)
	}
	if err := MigrateVectorIndexesSchema(conn); err != nil {
		return fmt.Errorf("vector index schema migration for %s: %w", meta.Database, err)
	}

	if err := m.execCreateDDL(ctx, conn, meta); err != nil {
		return err
	}

	// Hand off to engine for bulk populate (P1-C). Non-fatal if hook not yet wired.
	m.mu.Lock()
	hook := m.lifecycleHook
	ep := m.engineProv
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
			if ep != nil {
				ep.RemoveIndex(meta.IndexName)
			}
			m.removeCachedIndex(meta.Database, meta.TableName, meta.ColumnName)
			rollbackErr := m.execDropDDL(ctx, conn, meta.IndexName)
			if m.dbMgr != nil {
				if dbPath, pathErr := m.dbMgr.GetDatabasePath(meta.Database); pathErr == nil {
					rollbackErr = errors.Join(rollbackErr, os.RemoveAll(vecindex.SegmentStoreDir(dbPath, meta.IndexName)))
				}
			}
			if rollbackErr != nil {
				log.Error().Err(rollbackErr).Str("index", meta.IndexName).
					Msg("VectorIndexManager: failed to roll back metadata after lifecycle hook failure")
				return fmt.Errorf("vector index: populate failed: %w", errors.Join(err, fmt.Errorf("rollback state: %w", rollbackErr)))
			}
			return fmt.Errorf("vector index: populate failed: %w", err)
		}
	}
	return nil
}

// ResolveCreateIndexMeta validates create metadata and resolves auto-tuned
// parameters once at the coordinator before the vector-control payload is
// replicated. Replicas apply the resolved metadata without re-deciding nlist.
func (m *VectorIndexManager) ResolveCreateIndexMeta(ctx context.Context, meta VectorIndexMeta) (VectorIndexMeta, error) {
	conn, err := m.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		return meta, fmt.Errorf("vector index: get database %s: %w", meta.Database, err)
	}
	if err := vecindex.ValidateIndexName(meta.IndexName); err != nil {
		return meta, fmt.Errorf("vector index: %w", err)
	}
	if err := ValidateBaseTableForVectorIndex(conn, meta.TableName); err != nil {
		return meta, err
	}
	resolveNlist := meta.Nlist == 0
	resolveNprobe := meta.Nprobe == 0
	if resolveNlist {
		meta.AutoTuneNlist = true
	}
	if resolveNprobe {
		meta.AutoTuneNprobe = true
	}
	if meta.TargetPartitionSize <= 0 {
		meta.TargetPartitionSize = defaultTargetPartitionSize
	}
	if resolveNlist || resolveNprobe {
		metric, err := metricFromString(meta.Metric)
		if err != nil {
			return meta, fmt.Errorf("vector index: parse metric for auto-tune: %w", err)
		}
		n, err := countIndexableRows(ctx, conn, meta.TableName, meta.ColumnName, vecindex.IVFSpec{
			ID:      meta.IndexName,
			Dim:     meta.Dim,
			Metric:  metric,
			MaxNorm: meta.MaxNorm,
		})
		if err != nil {
			return meta, fmt.Errorf("vector index: count indexable rows for auto-tune: %w", err)
		}
		if resolveNlist {
			meta.Nlist = autoTuneNlistForTarget(n, meta.TargetPartitionSize)
		}
		if resolveNprobe {
			meta.Nprobe = autoTuneNprobeForTarget(meta.Nlist, meta.TargetPartitionSize)
		}
		if n > 0 {
			if resolveNlist {
				meta.AutoTuneNlist = false
			}
			if resolveNprobe {
				meta.AutoTuneNprobe = false
			}
		}
	}
	return meta, nil
}

// DropIndex removes in-memory engine state first so concurrent queries fail
// fast, then deletes the metadata row from SQLite.
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
		if v.Database == database && v.IndexName == indexName {
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
	if dbPath, err := m.dbMgr.GetDatabasePath(database); err == nil {
		_ = os.RemoveAll(vecindex.SegmentStoreDir(dbPath, indexName))
	}
	return nil
}

// ReindexIndex executes REINDEX VECTOR <name> by flipping metadata status to
// 'reindexing', delegating the rebuild to the engine hook, then restoring the
// cached metadata.
func (m *VectorIndexManager) ReindexIndex(ctx context.Context, indexName string) error {
	if err := vecindex.ValidateIndexName(indexName); err != nil {
		return fmt.Errorf("vector reindex: %w", err)
	}

	meta, ok := m.getIndexByNameAny(indexName)
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
	// Pipeline's swap txn already updated metadata; refresh the cache.
	if refreshed, err := loadIndexMetaByName(ctx, conn, indexName); err != nil {
		log.Warn().Err(err).Str("index", indexName).
			Msg("VectorIndexManager: failed to refresh metadata after REINDEX; retaining cached values")
		m.setCachedStatus(meta.Database, meta.TableName, meta.ColumnName, "ready")
	} else {
		key := indexCacheKey{database: refreshed.Database, table: refreshed.TableName, column: refreshed.ColumnName}
		m.cacheMu.Lock()
		m.indexCache[key] = refreshed
		m.cacheMu.Unlock()
	}

	log.Info().Str("index", indexName).Msg("VectorIndexManager: REINDEX complete")
	return nil
}

// getIndexByName scans the in-memory cache for a metadata row by database and index name.
// Returns a copy plus ok=true on hit. O(n) in the number of indexes, which
// is small (typically 0-10 per node).
func (m *VectorIndexManager) getIndexByName(database, name string) (*VectorIndexMeta, bool) {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()
	for _, v := range m.indexCache {
		if v.Database == database && v.IndexName == name {
			cp := *v
			return &cp, true
		}
	}
	return nil, false
}

func (m *VectorIndexManager) getIndexByNameAny(name string) (*VectorIndexMeta, bool) {
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

func (m *VectorIndexManager) removeCachedIndex(database, table, column string) {
	key := indexCacheKey{database: database, table: table, column: column}
	m.cacheMu.Lock()
	delete(m.indexCache, key)
	m.cacheMu.Unlock()
}

func (m *VectorIndexManager) storeCachedIndexMeta(meta *VectorIndexMeta) {
	if m == nil || meta == nil {
		return
	}
	key := indexCacheKey{database: meta.Database, table: meta.TableName, column: meta.ColumnName}
	metaCopy := *meta
	m.cacheMu.Lock()
	m.indexCache[key] = &metaCopy
	m.cacheMu.Unlock()
}

// ApplyLocalCDC updates local vector serving state after a transaction has
// committed successfully on this node. It never touches replication payload;
// it only forwards relevant CDC entries to the local file-backed vector path.
func (m *VectorIndexManager) ApplyLocalCDC(ctx context.Context, database string, entries []common.CDCEntry) error {
	return m.ApplyCommittedVectorCDC(ctx, database, 0, 0, entries)
}

func (m *VectorIndexManager) ApplyCommittedVectorCDC(ctx context.Context, database string, txnID, seqNum uint64, entries []common.CDCEntry) error {
	if len(entries) == 0 {
		return nil
	}
	for i := range entries {
		if entries[i].CommitTxnID == 0 {
			entries[i].CommitTxnID = txnID
		}
		if entries[i].CommitSeqNum == 0 {
			entries[i].CommitSeqNum = seqNum
		}
	}

	m.mu.Lock()
	hook, _ := m.lifecycleHook.(IndexLocalChangeHook)
	m.mu.Unlock()
	if hook == nil {
		return nil
	}

	byTable := make(map[string][]common.CDCEntry)
	for _, entry := range entries {
		if entry.Table == "" {
			continue
		}
		byTable[entry.Table] = append(byTable[entry.Table], entry)
	}

	for tableName, tableEntries := range byTable {
		for _, meta := range m.indexesForTable(database, tableName) {
			if err := hook.OnIndexLocalChanges(ctx, meta, tableEntries); err != nil {
				m.markIndexDirty(ctx, meta, err)
				return err
			}
		}
	}
	return nil
}

func (m *VectorIndexManager) ApplyVectorControl(ctx context.Context, change common.VectorIndexChange) error {
	if change.Database == "" {
		return fmt.Errorf("vector index control: database is required")
	}
	if change.IndexName == "" {
		return fmt.Errorf("vector index control: index name is required")
	}
	switch change.Action {
	case common.VectorIndexActionCreate:
		if existing, ok := m.getIndexByName(change.Database, change.IndexName); ok {
			m.storeCachedIndexMeta(existing)
			return nil
		}
		return m.CreateIndex(ctx, change.Meta())
	case common.VectorIndexActionDrop:
		return m.DropIndex(ctx, change.IndexName, change.Database)
	case common.VectorIndexActionReindex:
		return m.ReindexIndex(ctx, change.IndexName)
	case common.VectorIndexActionCheckpoint:
		if existing, ok := m.getIndexByName(change.Database, change.IndexName); ok {
			m.mu.Lock()
			maintenanceHook, _ := m.lifecycleHook.(interface {
				StartMaintenanceForIndex(common.VectorIndexMeta)
			})
			m.mu.Unlock()
			if maintenanceHook != nil {
				maintenanceHook.StartMaintenanceForIndex(*existing)
			}
			return nil
		}
		return fmt.Errorf("vector index control: checkpoint index %q not found", change.IndexName)
	default:
		return fmt.Errorf("vector index control: unsupported action %d", change.Action)
	}
}

func (m *VectorIndexManager) markIndexDirty(ctx context.Context, meta common.VectorIndexMeta, cause error) {
	conn, err := m.dbMgr.GetDatabaseConnection(meta.Database)
	if err == nil {
		if _, updateErr := conn.ExecContext(ctx,
			`UPDATE __marmot_vector_indexes SET status='dirty' WHERE index_name=?`,
			meta.IndexName,
		); updateErr != nil {
			log.Warn().Err(updateErr).Str("index", meta.IndexName).Msg("VectorIndexManager: failed to mark dirty")
		}
	}
	m.setCachedStatus(meta.Database, meta.TableName, meta.ColumnName, "dirty")
	log.Error().Err(cause).Str("index", meta.IndexName).Msg("VectorIndexManager: local vector index marked dirty")
}

func (m *VectorIndexManager) indexesForTable(database, table string) []common.VectorIndexMeta {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()

	var metas []common.VectorIndexMeta
	for key, meta := range m.indexCache {
		if key.database != database || key.table != table {
			continue
		}
		metas = append(metas, *meta)
	}
	return metas
}

func (m *VectorIndexManager) openExistingIndexes(ctx context.Context) error {
	m.mu.Lock()
	hook, _ := m.lifecycleHook.(IndexOpenHook)
	m.mu.Unlock()
	if hook == nil {
		return nil
	}

	m.cacheMu.RLock()
	metas := make([]common.VectorIndexMeta, 0, len(m.indexCache))
	for _, meta := range m.indexCache {
		metas = append(metas, *meta)
	}
	m.cacheMu.RUnlock()

	for _, meta := range metas {
		if err := hook.OnIndexLoaded(ctx, meta); err != nil {
			return err
		}
	}
	return nil
}

func loadIndexMetaByName(ctx context.Context, conn *sql.DB, indexName string) (*VectorIndexMeta, error) {
	row := conn.QueryRowContext(ctx, `
		SELECT index_name, table_name, column_name, database_name,
		       metric, dim, nlist, nprobe, auto_nlist, auto_nprobe,
		       target_partition_size, max_norm, status, created_at
		  FROM __marmot_vector_indexes
		 WHERE index_name = ?`, indexName)
	var (
		meta       VectorIndexMeta
		autoNlist  int64
		autoNprobe int64
	)
	if err := row.Scan(
		&meta.IndexName, &meta.TableName, &meta.ColumnName, &meta.Database,
		&meta.Metric, &meta.Dim, &meta.Nlist, &meta.Nprobe,
		&autoNlist, &autoNprobe, &meta.TargetPartitionSize,
		&meta.MaxNorm, &meta.Status, &meta.CreatedAt,
	); err != nil {
		return nil, err
	}
	meta.AutoTuneNlist = autoNlist != 0
	meta.AutoTuneNprobe = autoNprobe != 0
	return &meta, nil
}

// updateIndexStatus executes a short UPDATE on the metadata row. Shared by
// ReindexIndex and crash recovery.
func updateIndexStatus(ctx context.Context, conn *sql.DB, indexName, status string) error {
	_, err := conn.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes SET status=? WHERE index_name=?`,
		status, indexName)
	return err
}

// recoverReindexingIndexes flips any index left in status='reindexing' back to
// 'ready' on startup so a local rebuild can be retried cleanly.
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
		if err := updateIndexStatus(ctx, conn, meta.IndexName, "ready"); err != nil {
			log.Warn().Err(err).Str("index", meta.IndexName).
				Msg("VectorIndexManager: crash recovery: revert status failed")
			continue
		}
		m.setCachedStatus(meta.Database, meta.TableName, meta.ColumnName, "ready")
		log.Info().Str("index", meta.IndexName).
			Msg("VectorIndexManager: crash recovery: reverted to 'ready'")
	}
	return nil
}

// execCreateDDL records vector-index metadata only. Local serving state lives
// in file-backed segment/overlay data outside SQLite.
func (m *VectorIndexManager) execCreateDDL(ctx context.Context, conn *sql.DB, meta VectorIndexMeta) error {
	if err := vecindex.ValidateIndexName(meta.IndexName); err != nil {
		return fmt.Errorf("execCreateDDL: %w", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("vector index: begin DDL txn: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, `INSERT INTO __marmot_vector_indexes
		(index_name, table_name, column_name, database_name, metric, dim,
		 nlist, nprobe, auto_nlist, auto_nprobe, target_partition_size,
		 max_norm, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'building', ?)
		ON CONFLICT(index_name) DO NOTHING`,
		meta.IndexName, meta.TableName, meta.ColumnName, meta.Database,
		meta.Metric, meta.Dim, meta.Nlist, meta.Nprobe,
		boolToInt(meta.AutoTuneNlist), boolToInt(meta.AutoTuneNprobe),
		meta.TargetPartitionSize, meta.MaxNorm,
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

// execDropDDL removes the metadata row for the vector index. Local files are
// managed outside SQLite.
func (m *VectorIndexManager) execDropDDL(ctx context.Context, conn *sql.DB, indexName string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("vector index drop: begin DDL txn: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, `DELETE FROM __marmot_vector_indexes WHERE index_name = ?`, indexName); err != nil {
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
			       metric, dim, nlist, nprobe, auto_nlist, auto_nprobe,
			       target_partition_size, max_norm, status, created_at
			FROM __marmot_vector_indexes`)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("VectorIndexManager: failed to query existing indexes")
			continue
		}
		for rows.Next() {
			var (
				meta       VectorIndexMeta
				autoNlist  int64
				autoNprobe int64
			)
			if err := rows.Scan(
				&meta.IndexName, &meta.TableName, &meta.ColumnName, &meta.Database,
				&meta.Metric, &meta.Dim, &meta.Nlist, &meta.Nprobe,
				&autoNlist, &autoNprobe, &meta.TargetPartitionSize,
				&meta.MaxNorm, &meta.Status, &meta.CreatedAt,
			); err != nil {
				log.Warn().Err(err).Msg("VectorIndexManager: failed to scan index row")
				continue
			}
			meta.AutoTuneNlist = autoNlist != 0
			meta.AutoTuneNprobe = autoNprobe != 0
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
		if !ok || !isQueryableVectorIndex(meta) {
			return nil, false
		}
		return meta, true
	}

	// Empty database: scan for a unique match.
	var found *VectorIndexMeta
	for k, v := range m.indexCache {
		if k.table == table && k.column == column {
			if !isQueryableVectorIndex(v) {
				continue
			}
			if found != nil {
				// Ambiguous — caller must supply a database qualifier.
				return nil, false
			}
			found = v
		}
	}
	return found, found != nil
}

func isQueryableVectorIndex(meta *VectorIndexMeta) bool {
	if meta == nil {
		return false
	}
	return meta.Status != "dirty"
}

// EstimatedRowCount returns an approximate row count for (database, table).
// The result is cached with a 1-minute TTL; on cache miss MAX(rowid) is
// queried against the read pool. Returns 100_000 on any error so the
// planner degrades gracefully.
//
// We use MAX(rowid), not COUNT(*). On a table with INTEGER PRIMARY KEY
// rowid SQLite answers MAX(rowid) via a single right-descent of the PK
// btree — O(log n) page reads, typically sub-millisecond. COUNT(*) has to
// walk every leaf page and on a 1M-row × 6KB-blob table can easily exceed
// 1 second (observed ~1.2 s uncached on DBpedia-1M). The planner only needs
// an estimate for cardinality comparisons (pre- vs post-filter), so this
// upper-bound approximation is strictly better — it avoids a pathological
// 2 s stall on every vec_match rewrite when the cached row-count has
// expired or could not be populated within the query timeout.
//
// Edge cases:
//   - Empty table: MAX(rowid) → NULL. We surface 0 via sql.NullInt64.
//   - Rowids with gaps from DELETEs: estimate is an upper bound, which is
//     safe for selectivity comparisons.
//   - Tables without INTEGER PRIMARY KEY: rowid still exists as an alias
//     and MAX(rowid) still returns the largest assigned rowid.
func (m *VectorIndexManager) EstimatedRowCount(database, table string) int64 {
	key := indexCacheKey{database: database, table: table}

	m.cacheMu.RLock()
	if entry, ok := m.rowCountCache[key]; ok && time.Now().Before(entry.expiresAt) {
		m.cacheMu.RUnlock()
		return entry.count
	}
	m.cacheMu.RUnlock()

	conn, err := m.dbMgr.GetDatabaseReadConnection(database)
	if err != nil {
		return 100_000
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var maxRowID sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT MAX(rowid) FROM "%s"`, escapeQuote(table)),
	).Scan(&maxRowID); err != nil {
		return 100_000
	}
	n := int64(0)
	if maxRowID.Valid {
		n = maxRowID.Int64
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

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// autoTuneNlist computes nlist from the default target partition size.
func autoTuneNlist(n int64) int {
	return autoTuneNlistForTarget(n, defaultTargetPartitionSize)
}

// autoTuneNlistForTarget computes nlist so average cluster size stays near the
// requested target partition size, clamped to the supported IVF range.
func autoTuneNlistForTarget(n int64, targetPartitionSize int) int {
	if targetPartitionSize <= 0 {
		targetPartitionSize = defaultTargetPartitionSize
	}
	v := int((n + int64(targetPartitionSize) - 1) / int64(targetPartitionSize))
	if v < 64 {
		return 64
	}
	if v > 2048 {
		return 2048
	}
	return v
}

const defaultScanBudgetRows = 8192

func defaultScanBudgetRowsForTarget(targetPartitionSize int) int {
	if targetPartitionSize <= 0 {
		targetPartitionSize = defaultTargetPartitionSize
	}
	budget := defaultScanBudgetRows
	if widened := 16 * targetPartitionSize; widened > budget {
		budget = widened
	}
	return budget
}

// autoTuneNprobe computes the derived/default probe count from the current
// target partition size policy. It is introspection/default metadata only; the
// query engine may further adapt the live probe prefix using row-budget logic.
func autoTuneNprobe(nlist int) int {
	return autoTuneNprobeForTarget(nlist, defaultTargetPartitionSize)
}

func autoTuneNprobeForTarget(nlist int, targetPartitionSize int) int {
	if nlist <= 0 {
		return 1
	}
	if targetPartitionSize <= 0 {
		targetPartitionSize = defaultTargetPartitionSize
	}
	probe := int(math.Ceil(float64(defaultScanBudgetRowsForTarget(targetPartitionSize)) / float64(targetPartitionSize)))
	if probe < 1 {
		probe = 1
	}
	if probe > nlist {
		probe = nlist
	}
	return probe
}
