package db

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/encoding"
	"github.com/rs/zerolog/log"
)

const vectorIndexMetaTable = `
CREATE TABLE IF NOT EXISTS __marmot_vector_indexes (
    index_name  TEXT PRIMARY KEY,
    table_name  TEXT NOT NULL,
    column_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    metric      TEXT NOT NULL,
    dim         INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'building',
    created_at  INTEGER NOT NULL,
    UNIQUE(table_name, column_name)
)`

// VectorIndexMeta is an alias for the shared common.VectorIndexMeta.
type VectorIndexMeta = common.VectorIndexMeta

// VectorIndexManager manages vector index lifecycle and CDC-driven mutations.
type VectorIndexManager struct {
	mu                sync.RWMutex
	engine            VectorIndexEngine
	indexes           map[string]VectorIndex      // indexName → VectorIndex
	tableMeta         map[string]*VectorIndexMeta // "database.table" → meta (for CDC routing)
	dbMgr             *DatabaseManager
	cdcCancel         func() // cancel CDC subscription
	bgCtx             context.Context
	bgCancel          context.CancelFunc
	reconcileInterval time.Duration
}

// NewVectorIndexManager creates a new VectorIndexManager.
// reconcileInterval controls how often the manager scans for CDC signal gaps.
// Pass 0 to disable periodic reconciliation.
func NewVectorIndexManager(engine VectorIndexEngine, dbMgr *DatabaseManager, reconcileInterval time.Duration) *VectorIndexManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &VectorIndexManager{
		engine:            engine,
		indexes:           make(map[string]VectorIndex),
		tableMeta:         make(map[string]*VectorIndexMeta),
		dbMgr:             dbMgr,
		bgCtx:             ctx,
		bgCancel:          cancel,
		reconcileInterval: reconcileInterval,
	}
}

// Start subscribes to CDC signals and loads existing index metadata from all databases.
func (m *VectorIndexManager) Start(ctx context.Context) error {
	if err := m.loadExistingIndexes(ctx); err != nil {
		return fmt.Errorf("vector index manager: load existing indexes: %w", err)
	}

	if m.reconcileInterval > 0 {
		go m.runReconcileLoop(ctx)
	}

	hub := m.dbMgr.GetCDCHub()
	if hub == nil {
		log.Warn().Msg("VectorIndexManager: no CDC hub available, CDC routing disabled")
		return nil
	}

	signals, cancel := hub.Subscribe(CDCFilter{})
	m.mu.Lock()
	m.cdcCancel = cancel
	m.mu.Unlock()

	go m.runCDCLoop(ctx, signals)
	return nil
}

// Stop cancels CDC subscription, background goroutines, and closes all indexes.
func (m *VectorIndexManager) Stop() error {
	m.bgCancel()

	m.mu.Lock()
	cancel := m.cdcCancel
	m.cdcCancel = nil
	idxCopy := make(map[string]VectorIndex, len(m.indexes))
	for k, v := range m.indexes {
		idxCopy[k] = v
	}
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var lastErr error
	for name, idx := range idxCopy {
		if err := idx.Close(); err != nil {
			log.Error().Err(err).Str("index", name).Msg("VectorIndexManager: failed to close index")
			lastErr = err
		}
	}
	return lastErr
}

// CreateIndex creates a new vector index, stores metadata in SQLite, and builds the index.
func (m *VectorIndexManager) CreateIndex(ctx context.Context, meta VectorIndexMeta) error {
	conn, err := m.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		return fmt.Errorf("vector index: get database %s: %w", meta.Database, err)
	}

	if _, err := conn.ExecContext(ctx, vectorIndexMetaTable); err != nil {
		return fmt.Errorf("vector index: ensure meta table: %w", err)
	}

	// Read all existing vectors for bulk load
	// #nosec G202 -- tableName and columnName come from DDL statements, not user input
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT rowid, %s FROM %s", meta.ColumnName, meta.TableName))
	if err != nil {
		return fmt.Errorf("vector index: scan source table %s: %w", meta.TableName, err)
	}

	var bulk []VectorBulkEntry
	for rows.Next() {
		var rowID int64
		var rawBlob []byte
		if err := rows.Scan(&rowID, &rawBlob); err != nil {
			rows.Close()
			return fmt.Errorf("vector index: scan row: %w", err)
		}
		if len(rawBlob)%4 != 0 {
			log.Warn().Int64("rowid", rowID).Str("table", meta.TableName).Msg("VectorIndexManager: skipping row with invalid vector blob length")
			continue
		}
		vec := encoding.DecodeFloat32Slice(rawBlob)
		bulk = append(bulk, VectorBulkEntry{
			ExternalID: rowidToBytes(rowID),
			Vector:     vec,
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("vector index: iterate source table: %w", err)
	}

	if meta.CreatedAt == 0 {
		meta.CreatedAt = time.Now().UnixNano()
	}

	// Persist metadata (status=building) before creating index so a crash is recoverable.
	_, err = conn.ExecContext(ctx,
		`INSERT INTO __marmot_vector_indexes
         (index_name, table_name, column_name, database_name, metric, dim, status, created_at)
         VALUES (?, ?, ?, ?, ?, ?, 'building', ?)`,
		meta.IndexName, meta.TableName, meta.ColumnName, meta.Database,
		meta.Metric, meta.Dim, meta.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("vector index: insert metadata: %w", err)
	}

	idx, err := m.engine.CreateIndex(ctx, meta.IndexName, meta.Dim, meta.Metric, bulk)
	if err != nil {
		// Roll back metadata row on engine failure.
		_, _ = conn.ExecContext(ctx, "DELETE FROM __marmot_vector_indexes WHERE index_name = ?", meta.IndexName)
		return fmt.Errorf("vector index: engine create: %w", err)
	}

	_, err = conn.ExecContext(ctx,
		"UPDATE __marmot_vector_indexes SET status = 'ready' WHERE index_name = ?",
		meta.IndexName,
	)
	if err != nil {
		_ = idx.Close()
		return fmt.Errorf("vector index: update status to ready: %w", err)
	}

	meta.Status = "ready"
	tableKey := tableMetaKey(meta.Database, meta.TableName)

	m.mu.Lock()
	m.indexes[meta.IndexName] = idx
	m.tableMeta[tableKey] = &meta
	m.mu.Unlock()

	log.Info().
		Str("index", meta.IndexName).
		Str("database", meta.Database).
		Str("table", meta.TableName).
		Str("column", meta.ColumnName).
		Int("vectors", len(bulk)).
		Msg("VectorIndexManager: index created")
	return nil
}

// DropIndex drops a vector index and removes metadata.
func (m *VectorIndexManager) DropIndex(ctx context.Context, indexName, database string) error {
	m.mu.Lock()
	idx, exists := m.indexes[indexName]
	if exists {
		delete(m.indexes, indexName)
	}
	// Remove table meta entry for this index
	for key, meta := range m.tableMeta {
		if meta.IndexName == indexName {
			delete(m.tableMeta, key)
			break
		}
	}
	m.mu.Unlock()

	if exists {
		if err := idx.Close(); err != nil {
			log.Error().Err(err).Str("index", indexName).Msg("VectorIndexManager: failed to close index on drop")
		}
	}

	if err := m.engine.DropIndex(ctx, indexName); err != nil {
		return fmt.Errorf("vector index: engine drop %s: %w", indexName, err)
	}

	conn, err := m.dbMgr.GetDatabaseConnection(database)
	if err != nil {
		return fmt.Errorf("vector index: get database %s: %w", database, err)
	}

	_, err = conn.ExecContext(ctx,
		"DELETE FROM __marmot_vector_indexes WHERE index_name = ?", indexName,
	)
	if err != nil {
		return fmt.Errorf("vector index: delete metadata: %w", err)
	}

	log.Info().Str("index", indexName).Msg("VectorIndexManager: index dropped")
	return nil
}

// Search performs a kNN search on a named index.
// Returns an error if the index is not found or if the source table no longer exists.
func (m *VectorIndexManager) Search(ctx context.Context, indexName string, vector []float32, topK int) ([]VectorSearchHit, error) {
	m.mu.RLock()
	idx, ok := m.indexes[indexName]
	var meta *VectorIndexMeta
	for _, v := range m.tableMeta {
		if v.IndexName == indexName {
			meta = v
			break
		}
	}
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("vector index %q not found", indexName)
	}

	if meta != nil {
		conn, err := m.dbMgr.GetDatabaseConnection(meta.Database)
		if err != nil {
			return nil, fmt.Errorf("vector index %q: source database %q unavailable: %w", indexName, meta.Database, err)
		}
		if !tableExists(ctx, conn, meta.TableName) {
			return nil, fmt.Errorf("vector index %q: source table %q no longer exists", indexName, meta.TableName)
		}
	}

	return idx.Search(ctx, vector, topK)
}

// GetIndexMeta returns metadata for an index by name.
func (m *VectorIndexManager) GetIndexMeta(indexName string) (*VectorIndexMeta, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, meta := range m.tableMeta {
		if meta.IndexName == indexName {
			cp := *meta
			return &cp, true
		}
	}
	return nil, false
}

// loadExistingIndexes scans all databases for __marmot_vector_indexes and opens ready indexes.
// Indexes stuck in 'building' are marked as 'error' (crash recovery).
// Indexes whose Pebble directory is missing trigger an async rebuild.
func (m *VectorIndexManager) loadExistingIndexes(ctx context.Context) error {
	names := m.dbMgr.ListDatabases()
	for _, dbName := range names {
		conn, err := m.dbMgr.GetDatabaseConnection(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("VectorIndexManager: failed to get connection for load")
			continue
		}

		// Fetch both 'ready' and 'building' rows so we can handle crash recovery.
		// Collect all rows first, then close the cursor before issuing any writes
		// (SQLite allows only one active statement per connection at a time).
		rows, err := conn.QueryContext(ctx,
			`SELECT index_name, table_name, column_name, database_name, metric, dim, status, created_at
             FROM __marmot_vector_indexes WHERE status IN ('ready', 'building')`)
		if err != nil {
			// Table does not exist yet; skip silently.
			continue
		}

		var metas []VectorIndexMeta
		for rows.Next() {
			var meta VectorIndexMeta
			if err := rows.Scan(
				&meta.IndexName, &meta.TableName, &meta.ColumnName, &meta.Database,
				&meta.Metric, &meta.Dim, &meta.Status, &meta.CreatedAt,
			); err != nil {
				log.Error().Err(err).Str("database", dbName).Msg("VectorIndexManager: failed to scan index metadata")
				continue
			}
			metas = append(metas, meta)
		}
		rows.Close()

		// Process after cursor is closed so writes don't contend with the open read.
		var toRebuild []VectorIndexMeta
		for _, meta := range metas {
			// Crash recovery: index was mid-build when the process died.
			if meta.Status == "building" {
				log.Warn().Str("index", meta.IndexName).Msg("VectorIndexManager: found interrupted index build, marking as error")
				if _, uerr := conn.ExecContext(ctx,
					"UPDATE __marmot_vector_indexes SET status = 'error' WHERE index_name = ?",
					meta.IndexName,
				); uerr != nil {
					log.Error().Err(uerr).Str("index", meta.IndexName).Msg("VectorIndexManager: failed to update interrupted index to error")
				}
				continue
			}

			idx, err := m.engine.OpenIndex(ctx, meta.IndexName)
			if err != nil {
				log.Warn().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: index missing or corrupt, scheduling rebuild")
				if _, uerr := conn.ExecContext(ctx,
					"UPDATE __marmot_vector_indexes SET status = 'error' WHERE index_name = ?",
					meta.IndexName,
				); uerr != nil {
					log.Error().Err(uerr).Str("index", meta.IndexName).Msg("VectorIndexManager: failed to update missing index to error")
				}
				toRebuild = append(toRebuild, meta)
				continue
			}

			tableKey := tableMetaKey(meta.Database, meta.TableName)
			m.mu.Lock()
			m.indexes[meta.IndexName] = idx
			m.tableMeta[tableKey] = &meta
			m.mu.Unlock()

			log.Info().Str("index", meta.IndexName).Str("database", dbName).Msg("VectorIndexManager: loaded existing index")
		}

		for _, meta := range toRebuild {
			go m.rebuildIndex(m.bgCtx, meta)
		}
	}
	return nil
}

// rebuildIndex drops any stale index data and recreates it by scanning the source table.
// It updates the metadata row status to 'ready' on success or 'error' on failure.
func (m *VectorIndexManager) rebuildIndex(ctx context.Context, meta VectorIndexMeta) {
	log.Info().Str("index", meta.IndexName).Str("database", meta.Database).Msg("VectorIndexManager: rebuilding index from source table")

	conn, err := m.dbMgr.GetDatabaseConnection(meta.Database)
	if err != nil {
		log.Error().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: rebuild — cannot get database connection")
		return
	}

	// Drop stale Pebble data if any (best effort; ignore error).
	_ = m.engine.DropIndex(ctx, meta.IndexName)

	// Check source table still exists before attempting scan.
	if !tableExists(ctx, conn, meta.TableName) {
		log.Warn().Str("index", meta.IndexName).Str("table", meta.TableName).Msg("VectorIndexManager: rebuild skipped — source table no longer exists")
		if _, uerr := conn.ExecContext(ctx,
			"UPDATE __marmot_vector_indexes SET status = 'error' WHERE index_name = ?",
			meta.IndexName,
		); uerr != nil {
			log.Error().Err(uerr).Str("index", meta.IndexName).Msg("VectorIndexManager: failed to mark missing-table index as error")
		}
		return
	}

	// #nosec G202 -- tableName and columnName originate from DDL, not user input
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT rowid, %s FROM %s", meta.ColumnName, meta.TableName))
	if err != nil {
		log.Error().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: rebuild — failed to scan source table")
		return
	}

	var bulk []VectorBulkEntry
	for rows.Next() {
		var rowID int64
		var rawBlob []byte
		if err := rows.Scan(&rowID, &rawBlob); err != nil {
			rows.Close()
			log.Error().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: rebuild — failed to scan row")
			return
		}
		if len(rawBlob)%4 != 0 {
			log.Warn().Int64("rowid", rowID).Str("index", meta.IndexName).Msg("VectorIndexManager: rebuild — skipping row with invalid vector blob length")
			continue
		}
		bulk = append(bulk, VectorBulkEntry{
			ExternalID: rowidToBytes(rowID),
			Vector:     encoding.DecodeFloat32Slice(rawBlob),
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Error().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: rebuild — source table iteration failed")
		return
	}

	// Mark as building before creating index so crashes are detectable.
	if _, err := conn.ExecContext(ctx,
		"UPDATE __marmot_vector_indexes SET status = 'building' WHERE index_name = ?",
		meta.IndexName,
	); err != nil {
		log.Error().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: rebuild — failed to set building status")
		return
	}

	idx, err := m.engine.CreateIndex(ctx, meta.IndexName, meta.Dim, meta.Metric, bulk)
	if err != nil {
		log.Error().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: rebuild — engine CreateIndex failed")
		_, _ = conn.ExecContext(ctx,
			"UPDATE __marmot_vector_indexes SET status = 'error' WHERE index_name = ?",
			meta.IndexName,
		)
		return
	}

	if _, err := conn.ExecContext(ctx,
		"UPDATE __marmot_vector_indexes SET status = 'ready' WHERE index_name = ?",
		meta.IndexName,
	); err != nil {
		_ = idx.Close()
		log.Error().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: rebuild — failed to set ready status")
		return
	}

	meta.Status = "ready"
	tableKey := tableMetaKey(meta.Database, meta.TableName)

	m.mu.Lock()
	m.indexes[meta.IndexName] = idx
	m.tableMeta[tableKey] = &meta
	m.mu.Unlock()

	log.Info().
		Str("index", meta.IndexName).
		Str("database", meta.Database).
		Int("vectors", len(bulk)).
		Msg("VectorIndexManager: index rebuilt successfully")
}

// tableExists reports whether the named table is present in the SQLite database.
func tableExists(ctx context.Context, conn *sql.DB, tableName string) bool {
	var count int
	err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
		tableName,
	).Scan(&count)
	return err == nil && count > 0
}

// runReconcileLoop periodically checks each vector index watermark against the
// latest committed txnID in its source database and logs any gaps detected.
func (m *VectorIndexManager) runReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(m.reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcileAll()
		}
	}
}

// reconcileAll iterates every registered index and checks for watermark gaps.
func (m *VectorIndexManager) reconcileAll() {
	m.mu.RLock()
	metas := make([]*VectorIndexMeta, 0, len(m.tableMeta))
	for _, meta := range m.tableMeta {
		metas = append(metas, meta)
	}
	m.mu.RUnlock()

	for _, meta := range metas {
		if err := m.reconcileIndex(meta); err != nil {
			log.Warn().Err(err).
				Str("index", meta.IndexName).
				Str("database", meta.Database).
				Msg("VectorIndexManager: reconciliation failed")
		}
	}
}

// reconcileIndex compares the index watermark against the max committed txnID
// for the database and logs a warning when a gap is detected.
func (m *VectorIndexManager) reconcileIndex(meta *VectorIndexMeta) error {
	m.mu.RLock()
	idx, exists := m.indexes[meta.IndexName]
	m.mu.RUnlock()
	if !exists {
		return nil
	}

	indexWatermark := idx.Stats().WatermarkTxnID

	rdb, err := m.dbMgr.GetDatabase(meta.Database)
	if err != nil {
		return fmt.Errorf("get database: %w", err)
	}

	metaStore := rdb.GetMetaStore()
	if metaStore == nil {
		return nil
	}

	maxTxnID, err := metaStore.GetMaxCommittedTxnID()
	if err != nil {
		return fmt.Errorf("get max committed txn id: %w", err)
	}

	if indexWatermark >= maxTxnID {
		return nil
	}

	gap := maxTxnID - indexWatermark
	log.Warn().
		Str("index", meta.IndexName).
		Str("database", meta.Database).
		Uint64("index_watermark", indexWatermark).
		Uint64("db_max_txn_id", maxTxnID).
		Uint64("gap", gap).
		Msg("VectorIndexManager: watermark gap detected — CDC signals may have been dropped")
	return nil
}

// runCDCLoop reads CDC signals and routes changes to vector indexes.
func (m *VectorIndexManager) runCDCLoop(ctx context.Context, signals <-chan CDCSignal) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig, ok := <-signals:
			if !ok {
				return
			}
			m.handleCDCSignal(sig)
		}
	}
}

// handleCDCSignal processes a CDC signal by reading committed entries and routing to indexes.
func (m *VectorIndexManager) handleCDCSignal(signal CDCSignal) {
	m.mu.RLock()
	// Fast check: does any index exist for this database?
	hasIndex := false
	for _, meta := range m.tableMeta {
		if meta.Database == signal.Database {
			hasIndex = true
			break
		}
	}
	m.mu.RUnlock()

	if !hasIndex {
		return
	}

	mdb, err := m.dbMgr.GetDatabase(signal.Database)
	if err != nil {
		log.Warn().Err(err).Str("database", signal.Database).Msg("VectorIndexManager: CDC signal for dropped database, skipping")
		return
	}

	metaStore := mdb.GetMetaStore()
	if metaStore == nil {
		return
	}

	entries, err := metaStore.GetIntentEntries(signal.TxnID)
	if err != nil {
		log.Error().Err(err).
			Str("database", signal.Database).
			Uint64("txn_id", signal.TxnID).
			Msg("VectorIndexManager: failed to read intent entries")
		return
	}

	ctx := context.Background()
	for _, entry := range entries {
		m.routeEntry(ctx, signal.Database, signal.TxnID, entry)
	}
}

// routeEntry applies a single CDC entry to any matching vector index.
func (m *VectorIndexManager) routeEntry(ctx context.Context, database string, txnID uint64, entry *IntentEntry) {
	tableKey := tableMetaKey(database, entry.Table)

	m.mu.RLock()
	meta, ok := m.tableMeta[tableKey]
	if !ok {
		m.mu.RUnlock()
		return
	}
	// Copy to avoid holding the lock during I/O.
	metaCopy := *meta
	idx := m.indexes[metaCopy.IndexName]
	m.mu.RUnlock()

	if idx == nil {
		return
	}

	op := OpType(entry.Operation)
	switch op {
	case OpTypeInsert, OpTypeReplace, OpTypeUpdate:
		rawVec, found := entry.NewValues[metaCopy.ColumnName]
		if !found {
			return
		}
		if len(rawVec)%4 != 0 {
			log.Warn().
				Str("table", entry.Table).
				Str("column", metaCopy.ColumnName).
				Msg("VectorIndexManager: invalid vector blob length in CDC entry, skipping")
			return
		}
		vec := encoding.DecodeFloat32Slice(rawVec)
		externalID := intentKeyToExternalID(entry.IntentKey)
		if err := idx.Upsert(ctx, externalID, vec, txnID, entry.Seq); err != nil {
			log.Error().Err(err).
				Str("index", metaCopy.IndexName).
				Msg("VectorIndexManager: upsert failed")
		}

	case OpTypeDelete:
		externalID := intentKeyToExternalID(entry.IntentKey)
		if err := idx.Delete(ctx, externalID, txnID, entry.Seq); err != nil {
			log.Error().Err(err).
				Str("index", metaCopy.IndexName).
				Msg("VectorIndexManager: delete failed")
		}
	}
}

// tableMetaKey returns the lookup key for the tableMeta map.
func tableMetaKey(database, table string) string {
	return database + "." + table
}

// rowidToBytes encodes a SQLite rowid as an 8-byte big-endian slice.
func rowidToBytes(rowID int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(rowID))
	return b
}

// intentKeyToExternalID extracts the externalID from an intent key.
// Intent keys are opaque bytes; we use them directly as the external ID.
func intentKeyToExternalID(intentKey []byte) []byte {
	cp := make([]byte, len(intentKey))
	copy(cp, intentKey)
	return cp
}
