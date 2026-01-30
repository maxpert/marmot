package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// Ensure PendingLocalExecution implements coordinator.PendingExecution
var _ coordinator.PendingExecution = (*PendingLocalExecution)(nil)

// ReplicatedDatabase wraps a SQL database with distributed transaction support
// This is the main integration point between application layer and transactional storage
//
// SQLite WAL mode allows ONE writer + MANY concurrent readers.
// We maintain separate connection pools:
// - writeDB: Single connection for all writes (SQLite limitation)
// - hookDB: Single connection for CDC hook capture (separate from writeDB to avoid deadlock)
// - readDB: Multiple connections for concurrent reads (pool_size from config)
// - metaStore: Separate MetaStore for transaction metadata (separate file)
type ReplicatedDatabase struct {
	writeDB        *sql.DB   // Write connection (pool size=1, _txlock=immediate)
	hookDB         *sql.DB   // Hook connection for CDC capture (pool size=1, released before 2PC)
	readDB         *sql.DB   // Read connection pool (pool size from config)
	metaStore      MetaStore // Separate metadata storage (transaction records, intents, etc.)
	txnMgr         *TransactionManager
	clock          *hlc.Clock
	nodeID         uint64
	replicationFn  ReplicationFunc
	batchCommitter *SQLiteBatchCommitter
	schemaCache    *SchemaCache // Shared schema cache for preupdate hooks
}

// ReplicationFunc is called to replicate transactions to other nodes
// This is injected from the coordinator layer
type ReplicationFunc func(ctx context.Context, txn *Transaction) error

// NewReplicatedDatabase creates a new transaction-enabled database
// metaStore is the MetaStore for storing transaction metadata (intent entries, txn records, etc.)
func NewReplicatedDatabase(dbPath string, nodeID uint64, clock *hlc.Clock, metaStore MetaStore) (*ReplicatedDatabase, error) {
	// Get timeout from config (LockWaitTimeoutSeconds is in seconds, SQLite needs milliseconds)
	busyTimeoutMS := cfg.Config.Transaction.LockWaitTimeoutSeconds * 1000
	poolCfg := cfg.Config.ConnectionPool
	isMemoryDB := strings.Contains(dbPath, ":memory:")

	var writeDB, hookDB, readDB *sql.DB

	// Helper to close all opened connections on error
	closeAll := func() {
		if writeDB != nil {
			writeDB.Close()
		}
		if hookDB != nil {
			hookDB.Close()
		}
		if readDB != nil {
			readDB.Close()
		}
	}

	// === WRITE CONNECTION ===
	// Single connection for all writes (SQLite allows only one writer at a time)
	// Uses _txlock=immediate to acquire write lock at BEGIN, avoiding deadlocks
	writeDSN := dbPath
	if !isMemoryDB {
		if strings.Contains(writeDSN, "?") {
			writeDSN += fmt.Sprintf("&_journal_mode=WAL&_busy_timeout=%d&_txlock=immediate", busyTimeoutMS)
		} else {
			writeDSN += fmt.Sprintf("?_journal_mode=WAL&_busy_timeout=%d&_txlock=immediate", busyTimeoutMS)
		}
	}

	var err error
	writeDB, err = sql.Open(SQLiteDriverName, writeDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open write database: %w", err)
	}

	// Write connection: exactly 1 connection (SQLite limitation)
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0) // Keep connection alive forever

	// === HOOK CONNECTION (for CDC capture) ===
	// Separate connection for preupdate hooks to capture CDC data.
	// This connection is acquired during ExecuteLocalWithHooks, captures CDC,
	// then releases BEFORE 2PC broadcast - avoiding deadlock with incoming commits.
	hookDSN := writeDSN // Same settings as write connection
	hookDB, err = sql.Open(SQLiteDriverName, hookDSN)
	if err != nil {
		closeAll()
		return nil, fmt.Errorf("failed to open hook database: %w", err)
	}
	hookDB.SetMaxOpenConns(1)
	hookDB.SetMaxIdleConns(1)
	hookDB.SetConnMaxLifetime(0)

	// === READ CONNECTION POOL ===
	// Multiple connections for concurrent reads (WAL mode supports this)
	// No _txlock needed for reads - they don't acquire write locks
	// Note: Don't use mode=ro as it can interfere with WAL checkpointing
	readDSN := dbPath
	if !isMemoryDB {
		if strings.Contains(readDSN, "?") {
			readDSN += fmt.Sprintf("&_journal_mode=WAL&_busy_timeout=%d", busyTimeoutMS)
		} else {
			readDSN += fmt.Sprintf("?_journal_mode=WAL&_busy_timeout=%d", busyTimeoutMS)
		}
	}

	readDB, err = sql.Open(SQLiteDriverName, readDSN)
	if err != nil {
		closeAll()
		return nil, fmt.Errorf("failed to open read database: %w", err)
	}

	// Read connections: pool size from config (default 4)
	readDB.SetMaxOpenConns(poolCfg.PoolSize)
	readDB.SetMaxIdleConns(poolCfg.PoolSize)
	if poolCfg.MaxLifetimeSeconds > 0 {
		readDB.SetConnMaxLifetime(time.Duration(poolCfg.MaxLifetimeSeconds) * time.Second)
	}
	if poolCfg.MaxIdleTimeSeconds > 0 {
		readDB.SetConnMaxIdleTime(time.Duration(poolCfg.MaxIdleTimeSeconds) * time.Second)
	}

	// Configure all connections with optimal SQLite settings
	for _, db := range []*sql.DB{writeDB, hookDB, readDB} {
		if !isMemoryDB {
			if _, err = db.Exec("PRAGMA journal_mode=WAL"); err != nil {
				closeAll()
				return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
			}
			if _, err = db.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMS)); err != nil {
				closeAll()
				return nil, fmt.Errorf("failed to set busy timeout: %w", err)
			}
			if _, err = db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
				closeAll()
				return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
			}
			if _, err = db.Exec("PRAGMA cache_size=-64000"); err != nil {
				closeAll()
				return nil, fmt.Errorf("failed to set cache size: %w", err)
			}
			if _, err = db.Exec("PRAGMA temp_store=MEMORY"); err != nil {
				closeAll()
				return nil, fmt.Errorf("failed to set temp store: %w", err)
			}
		}
	}

	// Enable incremental auto-vacuum on write connection if configured
	// Note: auto_vacuum mode can only be changed on an empty database or after VACUUM
	// For existing databases, this sets the mode for future use
	if !isMemoryDB && cfg.Config.BatchCommit.IncrementalVacuumEnabled {
		if _, err = writeDB.Exec("PRAGMA auto_vacuum=INCREMENTAL"); err != nil {
			log.Debug().Err(err).Msg("Failed to set auto_vacuum mode (may require VACUUM for existing database)")
		}
	}

	// Create schema cache (shared by TransactionManager and preupdate hooks)
	schemaCache := NewSchemaCache()

	// Create transaction manager (uses write connection + MetaStore + schema cache)
	txnMgr := NewTransactionManager(writeDB, metaStore, clock, schemaCache)

	// Create batch committer for SQLite-level batching (opens its own optimized connection)
	var batchCommitter *SQLiteBatchCommitter
	if cfg.Config.BatchCommit.Enabled {
		batchCommitter = NewSQLiteBatchCommitter(
			dbPath,
			cfg.Config.BatchCommit.MaxBatchSize,
			time.Duration(cfg.Config.BatchCommit.MaxWaitMS)*time.Millisecond,
			cfg.Config.BatchCommit.CheckpointEnabled,
			cfg.Config.BatchCommit.CheckpointPassiveThreshMB,
			cfg.Config.BatchCommit.CheckpointRestartThreshMB,
			cfg.Config.BatchCommit.AllowDynamicBatchSize,
			cfg.Config.BatchCommit.IncrementalVacuumEnabled,
			cfg.Config.BatchCommit.IncrementalVacuumPages,
			cfg.Config.BatchCommit.IncrementalVacuumTimeLimitMS,
		)
		if err := batchCommitter.Start(); err != nil {
			closeAll()
			return nil, fmt.Errorf("failed to start batch committer: %w", err)
		}
	}

	mdb := &ReplicatedDatabase{
		writeDB:        writeDB,
		hookDB:         hookDB,
		readDB:         readDB,
		metaStore:      metaStore,
		txnMgr:         txnMgr,
		clock:          clock,
		nodeID:         nodeID,
		batchCommitter: batchCommitter,
		schemaCache:    schemaCache,
	}

	// Wire batch committer to transaction manager
	if batchCommitter != nil {
		txnMgr.SetBatchCommitter(batchCommitter)
	}

	// Load schema cache with existing tables (required for CDC preupdate hooks)
	if err := mdb.ReloadSchema(); err != nil {
		closeAll()
		if batchCommitter != nil {
			batchCommitter.Stop()
		}
		return nil, fmt.Errorf("failed to reload schema cache: %w", err)
	}

	return mdb, nil
}

// SetReplicationFunc sets the replication function
func (mdb *ReplicatedDatabase) SetReplicationFunc(fn ReplicationFunc) {
	mdb.replicationFn = fn
}

// GetDB returns the write database handle (for backwards compatibility)
// Prefer GetWriteDB() or GetReadDB() for explicit connection selection
func (mdb *ReplicatedDatabase) GetDB() *sql.DB {
	return mdb.writeDB
}

// GetWriteDB returns the dedicated write connection (pool size=1)
func (mdb *ReplicatedDatabase) GetWriteDB() *sql.DB {
	return mdb.writeDB
}

// GetReadDB returns the read connection pool (pool size from config)
func (mdb *ReplicatedDatabase) GetReadDB() *sql.DB {
	return mdb.readDB
}

// RefreshReadPool forces read connections to refresh their schema cache.
// SQLite caches schema per-connection. After DDL on writeDB, read connections
// need to be refreshed to see the new schema.
// This runs a WAL checkpoint to flush changes and closes idle connections.
func (mdb *ReplicatedDatabase) RefreshReadPool() {
	if mdb.readDB == nil || mdb.writeDB == nil {
		return
	}

	// Force WAL checkpoint to ensure DDL changes are written to main database file
	// PRAGMA wal_checkpoint(TRUNCATE) checkpoints and removes the WAL file
	if _, err := mdb.writeDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		log.Warn().Err(err).Msg("WAL checkpoint failed after DDL")
	}

	// Get current pool size from config
	poolSize := 4 // Default
	if cfg.Config != nil && cfg.Config.ConnectionPool.PoolSize > 0 {
		poolSize = cfg.Config.ConnectionPool.PoolSize
	}

	// Close ALL read connections (not just idle) by setting max to 0
	// then immediately restore to force fresh connections
	mdb.readDB.SetMaxOpenConns(0)
	mdb.readDB.SetMaxIdleConns(0)

	// Restore pool settings - new connections will have fresh schema
	mdb.readDB.SetMaxOpenConns(poolSize)
	mdb.readDB.SetMaxIdleConns(poolSize)

	log.Debug().Int("pool_size", poolSize).Msg("Read connection pool refreshed after DDL")
}

// GetTransactionManager returns the transaction manager
func (mdb *ReplicatedDatabase) GetTransactionManager() *TransactionManager {
	return mdb.txnMgr
}

// GetClock returns the HLC clock
func (mdb *ReplicatedDatabase) GetClock() *hlc.Clock {
	return mdb.clock
}

// Close closes the database connections, MetaStore, and stops GC.
// Order is important: stop GC first, then close connections.
func (mdb *ReplicatedDatabase) Close() error {
	// Stop GC goroutine first to prevent it from accessing closed connections
	if mdb.txnMgr != nil {
		mdb.txnMgr.StopGarbageCollection()
	}

	// Stop batch committer (flushes pending)
	if mdb.batchCommitter != nil {
		mdb.batchCommitter.Stop()
	}

	var errs []error

	if mdb.writeDB != nil {
		if err := mdb.writeDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if mdb.hookDB != nil {
		if err := mdb.hookDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if mdb.readDB != nil {
		if err := mdb.readDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if mdb.metaStore != nil {
		if err := mdb.metaStore.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// CloseSQLiteConnections closes all SQLite connections synchronously.
// This MUST be called BEFORE replacing database files during snapshot apply.
// After file replacement, call OpenSQLiteConnections to create new connections.
// Note: This does NOT touch MetaStore (PebbleDB) - only SQLite connections.
func (mdb *ReplicatedDatabase) CloseSQLiteConnections() {
	if mdb.writeDB != nil {
		mdb.writeDB.Close()
		mdb.writeDB = nil
	}
	if mdb.hookDB != nil {
		mdb.hookDB.Close()
		mdb.hookDB = nil
	}
	if mdb.readDB != nil {
		mdb.readDB.Close()
		mdb.readDB = nil
	}
}

// OpenSQLiteConnections opens new SQLite connections to the database file.
// This MUST be called AFTER database files have been replaced during snapshot apply.
// Note: This does NOT touch MetaStore (PebbleDB) - only SQLite connections.
func (mdb *ReplicatedDatabase) OpenSQLiteConnections(dbPath string) error {
	busyTimeoutMS := cfg.Config.Transaction.LockWaitTimeoutSeconds * 1000
	poolCfg := cfg.Config.ConnectionPool

	// Open write connection
	writeDSN := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d&_txlock=immediate", dbPath, busyTimeoutMS)
	writeDB, err := sql.Open(SQLiteDriverName, writeDSN)
	if err != nil {
		return fmt.Errorf("failed to open write connection: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0)

	// Verify connection works
	if err := writeDB.Ping(); err != nil {
		writeDB.Close()
		return fmt.Errorf("failed to ping write connection: %w", err)
	}

	// Open hook connection
	hookDB, err := sql.Open(SQLiteDriverName, writeDSN)
	if err != nil {
		writeDB.Close()
		return fmt.Errorf("failed to open hook connection: %w", err)
	}
	hookDB.SetMaxOpenConns(1)
	hookDB.SetMaxIdleConns(1)
	hookDB.SetConnMaxLifetime(0)

	// Open read connection pool
	readDSN := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d", dbPath, busyTimeoutMS)
	readDB, err := sql.Open(SQLiteDriverName, readDSN)
	if err != nil {
		writeDB.Close()
		hookDB.Close()
		return fmt.Errorf("failed to open read connection: %w", err)
	}
	readDB.SetMaxOpenConns(poolCfg.PoolSize)
	readDB.SetMaxIdleConns(poolCfg.PoolSize)
	if poolCfg.MaxLifetimeSeconds > 0 {
		readDB.SetConnMaxLifetime(time.Duration(poolCfg.MaxLifetimeSeconds) * time.Second)
	}
	if poolCfg.MaxIdleTimeSeconds > 0 {
		readDB.SetConnMaxIdleTime(time.Duration(poolCfg.MaxIdleTimeSeconds) * time.Second)
	}

	// Apply SQLite pragmas
	for _, db := range []*sql.DB{writeDB, hookDB, readDB} {
		if _, err = db.Exec("PRAGMA journal_mode=WAL"); err != nil {
			writeDB.Close()
			hookDB.Close()
			readDB.Close()
			return fmt.Errorf("failed to enable WAL mode: %w", err)
		}
		if _, err = db.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMS)); err != nil {
			writeDB.Close()
			hookDB.Close()
			readDB.Close()
			return fmt.Errorf("failed to set busy timeout: %w", err)
		}
		if _, err = db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
			writeDB.Close()
			hookDB.Close()
			readDB.Close()
			return fmt.Errorf("failed to set synchronous mode: %w", err)
		}
		if _, err = db.Exec("PRAGMA cache_size=-64000"); err != nil {
			writeDB.Close()
			hookDB.Close()
			readDB.Close()
			return fmt.Errorf("failed to set cache size: %w", err)
		}
		if _, err = db.Exec("PRAGMA temp_store=MEMORY"); err != nil {
			writeDB.Close()
			hookDB.Close()
			readDB.Close()
			return fmt.Errorf("failed to set temp store: %w", err)
		}
	}

	// Assign connections
	mdb.writeDB = writeDB
	mdb.hookDB = hookDB
	mdb.readDB = readDB

	// Reload schema after opening new connections (replaces Clear + manual reload)
	// This ensures schema cache is populated before any CDC operations
	if mdb.schemaCache != nil {
		if err := mdb.ReloadSchema(); err != nil {
			// On reload failure, clean up and return error
			writeDB.Close()
			hookDB.Close()
			readDB.Close()
			mdb.writeDB = nil
			mdb.hookDB = nil
			mdb.readDB = nil
			return fmt.Errorf("failed to reload schema: %w", err)
		}
	}

	return nil
}

// GetMetaStore returns the MetaStore for transaction metadata
func (mdb *ReplicatedDatabase) GetMetaStore() MetaStore {
	return mdb.metaStore
}

// GetCachedTableSchema returns the cached schema for a table.
// This uses the in-memory schema cache and does NOT query SQLite.
func (mdb *ReplicatedDatabase) GetCachedTableSchema(tableName string) (*TableSchema, error) {
	if mdb.schemaCache == nil {
		return nil, fmt.Errorf("schema cache not initialized")
	}
	return mdb.schemaCache.GetSchemaFor(tableName)
}

// GetSchemaCache returns the schema cache for building determinism schemas.
// Used by coordinator to check if DML statements are deterministic.
// Returns interface{} to avoid import cycles with coordinator package.
func (mdb *ReplicatedDatabase) GetSchemaCache() interface{} {
	return mdb.schemaCache
}

// ApplyCDCEntries applies CDC data entries to SQLite.
// Used by CompletedLocalExecution.Commit() to persist data captured during hooks.
func (mdb *ReplicatedDatabase) ApplyCDCEntries(entries []*IntentEntry) error {
	if mdb.txnMgr == nil {
		return fmt.Errorf("transaction manager not initialized")
	}
	return mdb.txnMgr.applyCDCEntries(0, entries)
}

// PendingLocalExecution represents a locally executed transaction waiting for quorum
// The SQLite transaction is held open until Commit or Rollback is called
type PendingLocalExecution struct {
	session *EphemeralHookSession // Ephemeral session (owns its connection)
}

// CompletedLocalExecution represents a CDC capture that's already been rolled back.
// Used by the new hookDB flow where we capture CDC then release the connection
// BEFORE 2PC broadcast. Commit/Rollback are no-ops.
type CompletedLocalExecution struct {
	cdcEntries   []*IntentEntry
	lastInsertId int64
	db           *ReplicatedDatabase
}

// Ensure CompletedLocalExecution implements coordinator.PendingExecution
var _ coordinator.PendingExecution = (*CompletedLocalExecution)(nil)

// GetTotalRowCount returns count of CDC entries.
func (c *CompletedLocalExecution) GetTotalRowCount() int64 {
	return int64(len(c.cdcEntries))
}

// Commit applies CDC entries to persist data captured during hooks
func (c *CompletedLocalExecution) Commit() error {
	if c.db == nil || len(c.cdcEntries) == 0 {
		return nil
	}
	return c.db.ApplyCDCEntries(c.cdcEntries)
}

// Rollback is a no-op - hookDB was already rolled back
func (c *CompletedLocalExecution) Rollback() error {
	return nil
}

// GetIntentEntries returns CDC entries captured from hooks
func (c *CompletedLocalExecution) GetIntentEntries() ([]*IntentEntry, error) {
	return c.cdcEntries, nil
}

// GetCDCEntries returns CDC data for replication
func (c *CompletedLocalExecution) GetCDCEntries() []common.CDCEntry {
	if len(c.cdcEntries) == 0 {
		return nil
	}
	result := make([]common.CDCEntry, len(c.cdcEntries))
	for i, e := range c.cdcEntries {
		result[i] = common.CDCEntry{
			Table:     e.Table,
			IntentKey: e.IntentKey,
			OldValues: e.OldValues,
			NewValues: e.NewValues,
		}
	}
	return result
}

// GetLastInsertId returns the last insert ID from the most recent insert
func (c *CompletedLocalExecution) GetLastInsertId() int64 {
	return c.lastInsertId
}

// GetTotalRowCount returns count from CDC entries.
func (p *PendingLocalExecution) GetTotalRowCount() int64 {
	entries := p.GetCDCEntries()
	return int64(len(entries))
}

// Commit finalizes the local transaction
func (p *PendingLocalExecution) Commit() error {
	if p.session != nil {
		return p.session.Commit()
	}
	return nil
}

// Rollback aborts the local transaction
func (p *PendingLocalExecution) Rollback() error {
	if p.session != nil {
		return p.session.Rollback()
	}
	return nil
}

// GetIntentEntries returns CDC entries from the system database
func (p *PendingLocalExecution) GetIntentEntries() ([]*IntentEntry, error) {
	if p.session == nil {
		return nil, nil
	}
	return p.session.GetIntentEntries()
}

// GetCDCEntries returns CDC data captured by hooks for replication
func (p *PendingLocalExecution) GetCDCEntries() []common.CDCEntry {
	if p.session == nil {
		return nil
	}
	entries, err := p.session.GetIntentEntries()
	if err != nil || len(entries) == 0 {
		return nil
	}
	result := make([]common.CDCEntry, len(entries))
	for i, e := range entries {
		result[i] = common.CDCEntry{
			Table:     e.Table,
			IntentKey: e.IntentKey,
			OldValues: e.OldValues,
			NewValues: e.NewValues,
		}
	}
	return result
}

// GetLastInsertId returns the last insert ID from the most recent insert
func (p *PendingLocalExecution) GetLastInsertId() int64 {
	if p.session == nil {
		return 0
	}
	return p.session.GetLastInsertId()
}

// ExecuteLocalWithHooks executes SQL locally with preupdate hooks capturing CDC data.
// Returns a PendingExecution with captured CDC entries. The hookDB transaction is
// ALREADY ROLLED BACK - no Commit/Rollback needed from caller.
//
// This implements the coordinator flow:
// 1. Create ephemeral session with hookDB (separate from writeDB)
// 2. Register hooks and preload schemas
// 3. BEGIN TRANSACTION on hookDB
// 4. Execute mutation commands (hooks capture affected rows to MetaStore)
// 5. ROLLBACK hookDB immediately (release connection BEFORE 2PC)
// 6. Return CDC entries - actual commit happens via uniform CDC replay path
//
// This design avoids deadlock: hookDB is released before 2PC broadcast,
// so incoming COMMIT from other coordinators can acquire writeDB.
func (mdb *ReplicatedDatabase) ExecuteLocalWithHooks(ctx context.Context, txnID uint64, requests []coordinator.ExecutionRequest) (coordinator.PendingExecution, error) {
	// Create ephemeral session with hookDB (NOT writeDB - avoids deadlock)
	// SchemaCache must be pre-populated via ReloadSchema() before calling this
	session, err := StartEphemeralSession(ctx, mdb.hookDB, mdb.metaStore, mdb.schemaCache, txnID)
	if err != nil {
		return nil, fmt.Errorf("failed to start session: %w", err)
	}

	// Begin transaction on hookDB
	if err := session.BeginTx(ctx); err != nil {
		_ = session.Rollback()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute each statement - hooks capture raw CDC data to Pebble
	for _, req := range requests {
		if err := session.ExecContext(ctx, req.SQL, req.Params...); err != nil {
			_ = session.Rollback()
			return nil, fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	// Get last insert ID BEFORE rollback (available immediately)
	lastInsertId := session.GetLastInsertId()

	// ROLLBACK hookDB - this also calls ProcessCapturedRows which converts
	// raw captured data to IntentEntries
	if err := session.Rollback(); err != nil {
		return nil, fmt.Errorf("failed to rollback hook session: %w", err)
	}

	// Get CDC entries AFTER rollback - ProcessCapturedRows has now created IntentEntries
	cdcEntries, _ := mdb.metaStore.GetIntentEntries(txnID)

	// Return completed execution with captured CDC data
	return &CompletedLocalExecution{
		cdcEntries:   cdcEntries,
		lastInsertId: lastInsertId,
		db:           mdb,
	}, nil
}

// ExecuteTransaction executes a transaction with distributed transaction semantics
// This is the main entry point for application-level transactions
func (mdb *ReplicatedDatabase) ExecuteTransaction(ctx context.Context, statements []protocol.Statement) error {
	// Begin transaction
	txn, err := mdb.txnMgr.BeginTransaction(mdb.nodeID)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Add all statements
	for _, stmt := range statements {
		if err := mdb.txnMgr.AddStatement(txn, stmt); err != nil {
			_ = mdb.txnMgr.AbortTransaction(txn)
			return fmt.Errorf("failed to add statement: %w", err)
		}

		// Create write intent for each statement
		// Extract intent key (simplified - would need proper SQL parsing)
		intentKey := extractIntentKeyFromStatement(stmt)
		dataSnapshot, serErr := SerializeData(map[string]interface{}{
			"sql":       stmt.SQL,
			"type":      stmt.Type,
			"timestamp": txn.StartTS.WallTime,
		})
		if serErr != nil {
			_ = mdb.txnMgr.AbortTransaction(txn)
			return fmt.Errorf("failed to serialize data: %w", serErr)
		}

		err := mdb.txnMgr.WriteIntent(txn, IntentTypeDML, stmt.TableName, intentKey, stmt, dataSnapshot)
		if err != nil {
			_ = mdb.txnMgr.AbortTransaction(txn)
			return fmt.Errorf("write conflict: %w", err)
		}
	}

	// Replicate to other nodes if replication is configured
	if mdb.replicationFn != nil {
		if err := mdb.replicationFn(ctx, txn); err != nil {
			_ = mdb.txnMgr.AbortTransaction(txn)
			return fmt.Errorf("replication failed: %w", err)
		}
	}

	// Commit transaction
	if err := mdb.txnMgr.CommitTransaction(txn); err != nil {
		_ = mdb.txnMgr.AbortTransaction(txn)
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

// ExecuteQuery executes a read query with snapshot isolation
// Uses the read connection pool for concurrent read access
func (mdb *ReplicatedDatabase) ExecuteQuery(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	// Get current snapshot timestamp
	snapshotTS := mdb.clock.Now()

	// Execute on read connection pool for concurrent reads
	// SQLite's WAL mode provides snapshot isolation at the connection level
	rows, err := mdb.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	// Note: snapshotTS is captured but SQLite's WAL mode provides snapshot isolation
	// at the transaction level. For full transaction support with write intents, use ExecuteSnapshotRead.
	_ = snapshotTS
	return rows, nil
}

// ExecuteSnapshotRead executes a read query with full transactional support
// Uses rqlite/sql AST parser for proper SQL analysis
// Uses the read connection pool for concurrent read access
func (mdb *ReplicatedDatabase) ExecuteSnapshotRead(ctx context.Context, query string, args ...interface{}) ([]string, []map[string]interface{}, error) {
	// SQLite WAL mode provides snapshot isolation at connection level
	rows, err := mdb.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, err
		}
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}
		results = append(results, rowMap)
	}
	return columns, results, nil
}

// ExecuteQueryRow executes a single-row query with snapshot isolation
// Uses the read connection pool for concurrent read access
func (mdb *ReplicatedDatabase) ExecuteQueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	// Get current snapshot timestamp
	// Note: SQLite's WAL mode provides snapshot isolation at transaction level.
	// For structured queries requiring full transaction support with write intent checking,
	// use ExecuteSnapshotRead instead.
	snapshotTS := mdb.clock.Now()
	_ = snapshotTS

	return mdb.readDB.QueryRowContext(ctx, query, args...)
}

// Exec executes a statement (for DDL and non-transactional operations)
// Uses the write connection for data modifications
func (mdb *ReplicatedDatabase) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return mdb.writeDB.ExecContext(ctx, query, args...)
}

// extractIntentKeyFromStatement extracts intent key from statement
// The intent key is extracted during parsing from the original MySQL AST,
// so we just use the pre-extracted value from the Statement struct
func extractIntentKeyFromStatement(stmt protocol.Statement) string {
	// If IntentKey was extracted during parsing, use it
	if len(stmt.IntentKey) > 0 {
		return string(stmt.IntentKey)
	}

	// Fallback: use hash of SQL (this should rarely happen)
	return fmt.Sprintf("%x", []byte(stmt.SQL)[:min(16, len(stmt.SQL))])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
