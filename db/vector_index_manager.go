package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

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

// VectorIndexMeta holds metadata for a vector index stored in SQLite.
type VectorIndexMeta struct {
	IndexName  string
	TableName  string
	ColumnName string
	Database   string
	Metric     string
	Dim        int
	Status     string // "building", "ready", "error"
	CreatedAt  int64
}

// VectorIndexManager manages vector index lifecycle and CDC-driven mutations.
type VectorIndexManager struct {
	mu        sync.RWMutex
	engine    VectorIndexEngine
	indexes   map[string]VectorIndex      // indexName → VectorIndex
	tableMeta map[string]*VectorIndexMeta // "database.table" → meta (for CDC routing)
	dbMgr     *DatabaseManager
	cdcCancel func() // cancel CDC subscription
}

// NewVectorIndexManager creates a new VectorIndexManager.
func NewVectorIndexManager(engine VectorIndexEngine, dbMgr *DatabaseManager) *VectorIndexManager {
	return &VectorIndexManager{
		engine:    engine,
		indexes:   make(map[string]VectorIndex),
		tableMeta: make(map[string]*VectorIndexMeta),
		dbMgr:     dbMgr,
	}
}

// Start subscribes to CDC signals and loads existing index metadata from all databases.
func (m *VectorIndexManager) Start(ctx context.Context) error {
	if err := m.loadExistingIndexes(ctx); err != nil {
		return fmt.Errorf("vector index manager: load existing indexes: %w", err)
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

// Stop cancels CDC subscription and closes all indexes.
func (m *VectorIndexManager) Stop() error {
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
		vec, err := decodeFloat32Slice(rawBlob)
		if err != nil {
			// Skip rows with unreadable vector data rather than aborting the build.
			log.Warn().Err(err).Int64("rowid", rowID).Str("table", meta.TableName).Msg("VectorIndexManager: skipping row with invalid vector")
			continue
		}
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
func (m *VectorIndexManager) Search(ctx context.Context, indexName string, vector []float32, topK int) ([]VectorSearchHit, error) {
	m.mu.RLock()
	idx, ok := m.indexes[indexName]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("vector index %q not found", indexName)
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
func (m *VectorIndexManager) loadExistingIndexes(ctx context.Context) error {
	names := m.dbMgr.ListDatabases()
	for _, dbName := range names {
		conn, err := m.dbMgr.GetDatabaseConnection(dbName)
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("VectorIndexManager: failed to get connection for load")
			continue
		}

		// Table might not exist yet — that is fine.
		rows, err := conn.QueryContext(ctx,
			`SELECT index_name, table_name, column_name, database_name, metric, dim, status, created_at
             FROM __marmot_vector_indexes WHERE status = 'ready'`)
		if err != nil {
			// Table does not exist yet; skip silently.
			continue
		}

		for rows.Next() {
			var meta VectorIndexMeta
			if err := rows.Scan(
				&meta.IndexName, &meta.TableName, &meta.ColumnName, &meta.Database,
				&meta.Metric, &meta.Dim, &meta.Status, &meta.CreatedAt,
			); err != nil {
				log.Error().Err(err).Str("database", dbName).Msg("VectorIndexManager: failed to scan index metadata")
				continue
			}

			idx, err := m.engine.OpenIndex(ctx, meta.IndexName)
			if err != nil {
				log.Error().Err(err).Str("index", meta.IndexName).Msg("VectorIndexManager: failed to open index")
				continue
			}

			tableKey := tableMetaKey(meta.Database, meta.TableName)
			m.mu.Lock()
			m.indexes[meta.IndexName] = idx
			m.tableMeta[tableKey] = &meta
			m.mu.Unlock()

			log.Info().Str("index", meta.IndexName).Str("database", dbName).Msg("VectorIndexManager: loaded existing index")
		}
		rows.Close()
	}
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
		vec, err := decodeFloat32Slice(rawVec)
		if err != nil {
			log.Warn().Err(err).
				Str("table", entry.Table).
				Str("column", metaCopy.ColumnName).
				Msg("VectorIndexManager: invalid vector in CDC entry, skipping")
			return
		}
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

// decodeFloat32Slice decodes a raw BLOB into a []float32.
// The BLOB must be a sequence of IEEE-754 little-endian 32-bit floats.
func decodeFloat32Slice(data []byte) ([]float32, error) {
	if len(data)%4 != 0 {
		return nil, fmt.Errorf("vector blob length %d is not a multiple of 4", len(data))
	}
	n := len(data) / 4
	vec := make([]float32, n)
	for i := range vec {
		bits := binary.LittleEndian.Uint32(data[i*4:])
		vec[i] = math.Float32frombits(bits)
	}
	return vec, nil
}
