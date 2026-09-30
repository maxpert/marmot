package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

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
	gate           *writeGate   // refuses every commit once the database leaves service

	dbName        string        // database name, "" for the system database
	schemaVersion atomic.Uint64 // cached __marmot_schema_version value; unused for the system database
}

// ReplicationFunc is called to replicate transactions to other nodes
// This is injected from the coordinator layer
type ReplicationFunc func(ctx context.Context, txn *Transaction) error

// ReplicatedDatabaseOption configures NewReplicatedDatabase.
type ReplicatedDatabaseOption func(*replicatedDatabaseOptions)

type replicatedDatabaseOptions struct {
	driverName  string
	synchronous string
	batchCommit bool

	dbName              string                             // "" when unnamed (the system database): no legacy schema version to migrate
	legacySchemaVersion func(dbName string) (int64, error) // migration read source, nil if not applicable
}

// WithDatabaseName names the database being opened: the name its
// __marmot_schema_version table migrates a legacy value under, and the
// name its applied-transaction repair records. The system database is opened
// without it; its table stays at 0, since no replicated DDL targets it.
func WithDatabaseName(name string) ReplicatedDatabaseOption {
	return func(o *replicatedDatabaseOptions) {
		o.dbName = name
	}
}

// WithLegacySchemaVersionSource supplies the retiring pebble-stored schema
// version counter, read once at open to migrate a pre-existing database's
// version into __marmot_schema_version. Only consulted when the
// SQLite file already existed before this open; see ensureSchemaVersionTable.
func WithLegacySchemaVersionSource(read func(dbName string) (int64, error)) ReplicatedDatabaseOption {
	return func(o *replicatedDatabaseOptions) {
		o.legacySchemaVersion = read
	}
}

// WithDurableCommits makes every commit on the database survive an OS crash
// or power loss, not only a process crash. Every connection it opens runs
// synchronous=FULL, carried in the DSN because the driver resets the mode on
// each new connection, through SQLiteDurableDriverName. It gets no batch
// committer, whose connection would commit and checkpoint the same file at
// synchronous=NORMAL.
func WithDurableCommits() ReplicatedDatabaseOption {
	return func(o *replicatedDatabaseOptions) {
		o.driverName = SQLiteDurableDriverName
		o.synchronous = "FULL"
		o.batchCommit = false
	}
}

// NewReplicatedDatabase creates a new transaction-enabled database
// metaStore is the MetaStore for storing transaction metadata (intent entries, txn records, etc.)
func NewReplicatedDatabase(dbPath string, nodeID uint64, clock *hlc.Clock, metaStore MetaStore, opts ...ReplicatedDatabaseOption) (*ReplicatedDatabase, error) {
	o := replicatedDatabaseOptions{
		driverName:  SQLiteDriverName,
		synchronous: "NORMAL",
		batchCommit: cfg.Config.BatchCommit.Enabled,
	}
	for _, opt := range opts {
		opt(&o)
	}

	// Get timeout from config (LockWaitTimeoutSeconds is in seconds, SQLite needs milliseconds)
	busyTimeoutMS := cfg.Config.Transaction.LockWaitTimeoutSeconds * 1000
	poolCfg := cfg.Config.ConnectionPool
	isMemoryDB := strings.Contains(dbPath, ":memory:")

	var writeDB, hookDB, readDB *sql.DB
	gate := &writeGate{}

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
	// Uses cache=shared so all local connections share a single page cache.
	writeDSN := dbPath
	if !isMemoryDB {
		if strings.Contains(writeDSN, "?") {
			writeDSN += fmt.Sprintf("&_journal_mode=WAL&_busy_timeout=%d&_txlock=immediate&_sync=%s&cache=shared", busyTimeoutMS, o.synchronous)
		} else {
			writeDSN += fmt.Sprintf("?_journal_mode=WAL&_busy_timeout=%d&_txlock=immediate&_sync=%s&cache=shared", busyTimeoutMS, o.synchronous)
		}
	}

	var err error
	writeDB, err = gate.openDB(o.driverName, writeDSN)
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
	hookDB, err = gate.openDB(o.driverName, hookDSN)
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
	// Uses cache=shared so all local connections share a single page cache.
	readDSN := dbPath
	if !isMemoryDB {
		if strings.Contains(readDSN, "?") {
			readDSN += fmt.Sprintf("&_journal_mode=WAL&_busy_timeout=%d&_sync=%s&cache=shared", busyTimeoutMS, o.synchronous)
		} else {
			readDSN += fmt.Sprintf("?_journal_mode=WAL&_busy_timeout=%d&_sync=%s&cache=shared", busyTimeoutMS, o.synchronous)
		}
	}

	readDB, err = gate.openDB(o.driverName, readDSN)
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

	// The migration rule for __marmot_schema_version turns on whether
	// __marmot_applied_txn already existed before this open, so it must be
	// checked before ensureAppliedTxnTable creates it.
	filePreexisted := false
	if !isMemoryDB {
		if existed, err := sqliteTableExists(writeDB, "__marmot_applied_txn"); err != nil {
			closeAll()
			return nil, err
		} else {
			filePreexisted = existed
		}
	}

	if err := ensureAppliedTxnTable(writeDB); err != nil {
		closeAll()
		return nil, err
	}
	if err := ensureRowVersionTable(writeDB); err != nil {
		closeAll()
		return nil, err
	}
	if err := repairAppliedTxnMetadata(writeDB, metaStore, o.dbName); err != nil {
		closeAll()
		return nil, err
	}

	// Every database file carries __marmot_schema_version, whichever path
	// opened it: a commit that bumps it must never find it missing. Only a
	// named user database migrates a legacy value into it.
	schemaVersion, err := ensureSchemaVersionTable(writeDB, o.dbName, filePreexisted, o.legacySchemaVersion)
	if err != nil {
		closeAll()
		return nil, fmt.Errorf("failed to prepare schema version table: %w", err)
	}

	// Create schema cache (shared by TransactionManager and preupdate hooks)
	schemaCache := NewSchemaCache()

	// Create transaction manager (uses write connection + MetaStore + schema cache)
	txnMgr := NewTransactionManager(writeDB, metaStore, clock, schemaCache)
	if err := seedClockFromLog(clock, metaStore); err != nil {
		closeAll()
		return nil, err
	}

	// Create batch committer for SQLite-level batching (opens its own optimized connection)
	var batchCommitter *SQLiteBatchCommitter
	if o.batchCommit {
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
		batchCommitter.gate = gate
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
		gate:           gate,
		dbName:         o.dbName,
	}
	mdb.schemaVersion.Store(schemaVersion)
	if o.dbName != "" {
		txnMgr.SetSchemaVersionBumped(func(v uint64) { mdb.advanceSchemaVersion(v) })
	}
	txnMgr.SetAppliedMarkerCheck(mdb.appliedMarkerExists)

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

// appliedMarkerExists reports whether __marmot_applied_txn holds txnID,
// read through the read pool so it never waits on the single writer.
func (mdb *ReplicatedDatabase) appliedMarkerExists(txnID uint64) (bool, error) {
	readDB := mdb.readDB
	if readDB == nil {
		return false, errors.New("database connections are closed")
	}
	return markerExists(readDB, txnID)
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

// Close closes the database connections and the MetaStore and stops GC
// (closeSQLite). It does not wait for a write transaction in flight: that
// transaction's commit is refused (see writeGate).
func (mdb *ReplicatedDatabase) Close() error {
	err := mdb.closeSQLite()
	if mdb.metaStore != nil {
		if msErr := mdb.metaStore.Close(); err == nil {
			err = msErr
		}
	}
	return err
}

// closeSQLite closes the database's SQLite file for good, leaving the meta
// store open: the batch committer stops after committing what is queued, no
// later commit on any of the database's connections succeeds (writeGate), the
// GC stops, and the pools close. The pool fields keep their closed *sql.DB, so
// a caller still holding the database gets "sql: database is closed", never a
// nil pool.
//
// The caller must not hold DatabaseManager.mu: stopping the GC waits for a
// pass that may itself be waiting on that lock.
func (mdb *ReplicatedDatabase) closeSQLite() error {
	if mdb.batchCommitter != nil {
		mdb.batchCommitter.Stop()
	}
	mdb.gate.close()
	return mdb.closePools()
}

// drainSQLite takes the database's SQLite file out of service for a restore
// that will replace it, leaving the meta store open. Unlike closeSQLite it
// refuses first: the gate closes before the batch committer stops, so the
// commits it has queued are refused rather than written into the file being
// replaced (their transactions stay prepared). It then waits, bounded by ctx,
// for the write transaction in flight on writeDB to finish: when it returns
// nil, every commit that passed the gate before it closed has completed, and
// none can complete later. If ctx ends first it returns that error, still
// tearing everything down; a commit already past the gate may then complete
// later. A failure to close a pool is logged, not returned: it says nothing
// about what the database's file holds.
//
// The caller must not hold DatabaseManager.mu (see closeSQLite).
func (mdb *ReplicatedDatabase) drainSQLite(ctx context.Context) error {
	mdb.gate.close()
	if mdb.batchCommitter != nil {
		mdb.batchCommitter.Stop()
	}
	// writeDB has a single connection: getting it means the transaction that
	// held it has committed or rolled back. Holding it through the pool's
	// Close keeps every other caller off it.
	held, err := mdb.writeDB.Conn(ctx)
	if closeErr := mdb.closePools(); closeErr != nil {
		log.Warn().Err(closeErr).Msg("Error closing a drained database's SQLite pools")
	}
	if err != nil {
		return fmt.Errorf("drain in-flight writes: %w", err)
	}
	_ = held.Close()
	return nil
}

// closePools stops the GC and closes the SQLite pools.
func (mdb *ReplicatedDatabase) closePools() error {
	if mdb.txnMgr != nil {
		mdb.txnMgr.StopGarbageCollection()
	}
	var errs []error
	for _, pool := range []*sql.DB{mdb.writeDB, mdb.hookDB, mdb.readDB} {
		if pool == nil {
			continue
		}
		if err := pool.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
// Like the pools NewReplicatedDatabase opens, they are gated: once the
// database leaves service, their commits are refused.
// Note: This does NOT touch MetaStore (PebbleDB) - only SQLite connections.
func (mdb *ReplicatedDatabase) OpenSQLiteConnections(dbPath string) error {
	busyTimeoutMS := cfg.Config.Transaction.LockWaitTimeoutSeconds * 1000
	poolCfg := cfg.Config.ConnectionPool

	// Open write connection
	writeDSN := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d&_txlock=immediate&cache=shared", dbPath, busyTimeoutMS)
	writeDB, err := mdb.gate.openDB(SQLiteDriverName, writeDSN)
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
	hookDB, err := mdb.gate.openDB(SQLiteDriverName, writeDSN)
	if err != nil {
		writeDB.Close()
		return fmt.Errorf("failed to open hook connection: %w", err)
	}
	hookDB.SetMaxOpenConns(1)
	hookDB.SetMaxIdleConns(1)
	hookDB.SetConnMaxLifetime(0)

	// Open read connection pool
	readDSN := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d&cache=shared", dbPath, busyTimeoutMS)
	readDB, err := mdb.gate.openDB(SQLiteDriverName, readDSN)
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

// DatabaseName returns the name this database was opened under ("" for the
// system database).
func (mdb *ReplicatedDatabase) DatabaseName() string {
	return mdb.dbName
}

// SchemaVersion returns the cached __marmot_schema_version value. It
// is always 0 for the system database, which has no such table.
func (mdb *ReplicatedDatabase) SchemaVersion() uint64 {
	return mdb.schemaVersion.Load()
}

// advanceSchemaVersion raises the cached __marmot_schema_version value to v,
// never lowering it: every post-commit store of a value read
// from the database itself - the replay path and the 2PC non-DML commit
// callback - goes through this instead of a plain Store, because two such
// commits can finish in either order (replay's anti-entropy goroutine versus
// this node's own DDL committer) and an out-of-order Store would regress the
// cache below a value SQLite itself already has committed, wrongly declining
// the PREPARE gate until the next DDL. The initial Store at open (a restore
// reopen legitimately resets the cache) is exempt and stays a plain Store.
func (mdb *ReplicatedDatabase) advanceSchemaVersion(v uint64) {
	for {
		cur := mdb.schemaVersion.Load()
		if v <= cur {
			return
		}
		if mdb.schemaVersion.CompareAndSwap(cur, v) {
			return
		}
	}
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
	return mdb.txnMgr.applyCDCEntries(0, mdb.clock.Now(), entries)
}

// CompletedLocalExecution represents a CDC capture that's already been rolled back.
// Used by the new hookDB flow where we capture CDC then release the connection
// BEFORE 2PC broadcast. Commit/Rollback are no-ops.
type CompletedLocalExecution struct {
	cdcEntries   []*IntentEntry
	lastInsertId int64
	db           *ReplicatedDatabase
	rowCount     int64
}

// Ensure CompletedLocalExecution implements coordinator.PendingExecution
var _ coordinator.PendingExecution = (*CompletedLocalExecution)(nil)

// GetTotalRowCount returns count of CDC entries.
func (c *CompletedLocalExecution) GetTotalRowCount() int64 {
	if c.rowCount > 0 {
		return c.rowCount
	}
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
			Table:        e.Table,
			IntentKey:    e.IntentKey,
			Operation:    e.Operation,
			OldValues:    e.OldValues,
			NewValues:    e.NewValues,
			EncodedRow:   e.EncodedRow,
			EncodedCodec: e.EncodedCodec,
		}
	}
	return result
}

// GetLastInsertId returns the OK packet's insert id for this execution: the
// first AUTO_INCREMENT value the statement generated, or 0 when it generated
// none. The name mirrors MySQL's own OK-packet field and the coordinator
// interface; the value is deliberately the FIRST id, not the last, which is
// what SQLite's connection-wide last-rowid register would have given.
func (c *CompletedLocalExecution) GetLastInsertId() int64 {
	return c.lastInsertId
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
func (mdb *ReplicatedDatabase) ExecuteLocalWithHooks(ctx context.Context, txnID uint64, req coordinator.ExecutionRequest) (coordinator.PendingExecution, error) {
	// Create ephemeral session with hookDB (NOT writeDB - avoids deadlock)
	// SchemaCache must be pre-populated via ReloadSchema() before calling this
	session, err := StartEphemeralSession(ctx, mdb.hookDB, mdb.metaStore, mdb.schemaCache, txnID)
	if err != nil {
		// Non-hook builds: fall back to statement-based 2PC without CDC row capture.
		// This keeps DML functional in single-node/testing environments.
		if strings.Contains(err.Error(), "preupdate hook requires build tag") {
			log.Warn().
				Uint64("txn_id", txnID).
				Msg("Preupdate hook not enabled; falling back to statement-based execution")
			return &CompletedLocalExecution{
				cdcEntries:   nil,
				lastInsertId: 0,
				db:           mdb,
				rowCount:     1,
			}, nil
		}
		return nil, fmt.Errorf("failed to start session: %w", err)
	}

	// Begin transaction on hookDB
	if err := session.BeginTx(ctx); err != nil {
		_ = session.Rollback()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute the statement - hooks capture raw CDC data to Pebble.
	// rows-affected is not used here: this autocommit path computes it from
	// the captured CDC entries via CompletedLocalExecution.GetTotalRowCount.
	if _, err := session.ExecContext(ctx, req.SQL, req.Params...); err != nil {
		_ = session.Rollback()
		return nil, fmt.Errorf("failed to execute statement: %w", err)
	}

	// ROLLBACK hookDB - this also calls ProcessCapturedRows which converts
	// raw captured data to IntentEntries
	if err := session.Rollback(); err != nil {
		session.cleanup()
		return nil, fmt.Errorf("failed to rollback hook session: %w", err)
	}

	// Get CDC entries cached by session processing. Capture errors must be
	// propagated so DML never falls back to statement replication.
	cdcEntries, err := session.GetIntentEntries()
	if err != nil {
		session.cleanup()
		return nil, fmt.Errorf("failed to collect CDC entries: %w", err)
	}
	session.cleanup()

	// The insert id comes from this statement's own CDC entries, not from
	// SQLite's connection-wide last-rowid register: hookDB is capped at one
	// connection, so that register is shared by every client in turn. The
	// signature takes one request, so cdcEntries cannot span two statements.
	lastInsertId := statementInsertID(mdb.schemaCache, cdcEntries)

	// Return completed execution with captured CDC data
	return &CompletedLocalExecution{
		cdcEntries:   cdcEntries,
		lastInsertId: lastInsertId,
		db:           mdb,
		rowCount:     int64(len(cdcEntries)),
	}, nil
}

// pinnedHookSession implements coordinator.PinnedSession over an
// EphemeralHookSession whose SQLite transaction is held open across multiple
// statements (BEGIN...COMMIT/ROLLBACK), instead of the one-shot
// execute-then-rollback flow ExecuteLocalWithHooks uses for autocommit
// statements.
type pinnedHookSession struct {
	session *EphemeralHookSession
}

var _ coordinator.PinnedSession = (*pinnedHookSession)(nil)

// ExecuteStatement runs one DML statement on the pinned transaction, then
// captures and row-locks any newly captured CDC rows immediately so a
// conflicting transaction sees the lock from statement time, not just from
// Release.
func (p *pinnedHookSession) ExecuteStatement(ctx context.Context, sql string, params []interface{}) (int64, int64, error) {
	rowsAffected, err := p.session.ExecContext(ctx, sql, params...)
	if err != nil {
		return 0, 0, err
	}
	if err := p.session.captureAndLockNewRows(); err != nil {
		return 0, 0, err
	}
	return rowsAffected, p.session.StatementInsertID(), nil
}

func (p *pinnedHookSession) Query(ctx context.Context, sqlText string, params []interface{}) ([]string, []map[string]interface{}, error) {
	rows, err := p.session.QueryContext(ctx, sqlText, params...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

func (p *pinnedHookSession) CDCEntries() []common.CDCEntry {
	entries := p.session.CapturedIntentEntries()
	if len(entries) == 0 {
		return nil
	}
	result := make([]common.CDCEntry, len(entries))
	for i, e := range entries {
		result[i] = common.CDCEntry{
			Table:        e.Table,
			IntentKey:    e.IntentKey,
			Operation:    e.Operation,
			OldValues:    e.OldValues,
			NewValues:    e.NewValues,
			EncodedRow:   e.EncodedRow,
			EncodedCodec: e.EncodedCodec,
		}
	}
	return result
}

func (p *pinnedHookSession) Release() error {
	return p.session.Rollback()
}

// BeginPinnedSession starts a new PinnedSession: an EphemeralHookSession on
// hookDB (never writeDB - same deadlock-avoidance reasoning as
// ExecuteLocalWithHooks) with its SQLite transaction opened and left open.
// ctx must stay alive (not be cancelled) until the caller calls Release - see
// PinnedSession's doc comment in coordinator/pinned_txn.go.
//
// Unlike ExecuteLocalWithHooks, this does not fall back to statement-based
// execution when the preupdate hook build tag is absent: eager execution
// requires the hook build tag, consistent with how coordinator/pinned_txn.go
// and its tests are gated.
func (mdb *ReplicatedDatabase) BeginPinnedSession(ctx context.Context, txnID uint64) (coordinator.PinnedSession, error) {
	session, err := StartEphemeralSession(ctx, mdb.hookDB, mdb.metaStore, mdb.schemaCache, txnID)
	if err != nil {
		return nil, fmt.Errorf("failed to start pinned session: %w", err)
	}
	if err := session.BeginTx(ctx); err != nil {
		session.cleanup()
		return nil, fmt.Errorf("failed to begin pinned transaction: %w", err)
	}
	return &pinnedHookSession{session: session}, nil
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

// ExecuteQuery executes a read query with snapshot isolation.
// Uses the read connection pool for concurrent read access.
func (mdb *ReplicatedDatabase) ExecuteQuery(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	// Get current snapshot timestamp
	snapshotTS := mdb.clock.Now()

	rows, err := mdb.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	// Note: snapshotTS is captured but SQLite's WAL mode provides snapshot isolation
	// at the transaction level. For full transaction support with write intents, use ExecuteSnapshotRead.
	_ = snapshotTS
	return rows, nil
}

// ExecuteSnapshotRead executes a read query with full transactional support.
// Uses the read connection pool for concurrent read access.
func (mdb *ReplicatedDatabase) ExecuteSnapshotRead(ctx context.Context, query string, args ...interface{}) ([]string, []map[string]interface{}, error) {
	rows, err := mdb.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	return scanRowsToMaps(rows)
}

// scanRowsToMaps drains rows into a (columns, row-maps) pair. []byte values
// are converted to string, matching SQLite's TEXT/BLOB ambiguity handling
// used elsewhere for query results. The caller owns closing rows.
func scanRowsToMaps(rows *sql.Rows) ([]string, []map[string]interface{}, error) {
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

// ExecuteQueryRow executes a single-row query with snapshot isolation.
// Uses the read connection pool for concurrent read access.
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
