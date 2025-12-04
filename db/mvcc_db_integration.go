package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// Ensure PendingLocalExecution implements coordinator.PendingExecution
var _ coordinator.PendingExecution = (*PendingLocalExecution)(nil)

// MVCCDatabase wraps a SQL database with MVCC transaction support
// This is the main integration point between application layer and MVCC storage
//
// SQLite WAL mode allows ONE writer + MANY concurrent readers.
// We maintain separate connection pools:
// - writeDB: Single connection for all writes (SQLite limitation)
// - hookDB: Single connection for CDC hook capture (separate from writeDB to avoid deadlock)
// - readDB: Multiple connections for concurrent reads (pool_size from config)
// - metaStore: Separate MetaStore for transaction metadata (separate file)
type MVCCDatabase struct {
	writeDB       *sql.DB   // Write connection (pool size=1, _txlock=immediate)
	hookDB        *sql.DB   // Hook connection for CDC capture (pool size=1, released before 2PC)
	readDB        *sql.DB   // Read connection pool (pool size from config)
	metaStore     MetaStore // Separate metadata storage (transaction records, intents, etc.)
	txnMgr        *MVCCTransactionManager
	clock         *hlc.Clock
	nodeID        uint64
	replicationFn ReplicationFunc
	commitBatcher *CommitBatcher
	schemaCache   *SchemaCache // Shared schema cache for preupdate hooks
}

// ReplicationFunc is called to replicate transactions to other nodes
// This is injected from the coordinator layer
type ReplicationFunc func(ctx context.Context, txn *MVCCTransaction) error

// NewMVCCDatabase creates a new MVCC-enabled database
// metaStore is the MetaStore for storing transaction metadata (intent entries, txn records, etc.)
func NewMVCCDatabase(dbPath string, nodeID uint64, clock *hlc.Clock, metaStore MetaStore) (*MVCCDatabase, error) {
	// Get timeout from config (LockWaitTimeoutSeconds is in seconds, SQLite needs milliseconds)
	busyTimeoutMS := cfg.Config.MVCC.LockWaitTimeoutSeconds * 1000
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
	writeDB, err = sql.Open("sqlite3", writeDSN)
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
	hookDB, err = sql.Open("sqlite3", hookDSN)
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

	readDB, err = sql.Open("sqlite3", readDSN)
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

	// Create transaction manager (uses write connection + MetaStore)
	txnMgr := NewMVCCTransactionManager(writeDB, metaStore, clock)

	// Create commit batcher for SQLite-level batching (uses write connection)
	commitBatcher := NewCommitBatcher(writeDB, 20, 2*time.Millisecond)
	commitBatcher.Start()

	return &MVCCDatabase{
		writeDB:       writeDB,
		hookDB:        hookDB,
		readDB:        readDB,
		metaStore:     metaStore,
		txnMgr:        txnMgr,
		clock:         clock,
		nodeID:        nodeID,
		commitBatcher: commitBatcher,
		schemaCache:   NewSchemaCache(),
	}, nil
}

// SetReplicationFunc sets the replication function
func (mdb *MVCCDatabase) SetReplicationFunc(fn ReplicationFunc) {
	mdb.replicationFn = fn
}

// GetDB returns the write database handle (for backwards compatibility)
// Prefer GetWriteDB() or GetReadDB() for explicit connection selection
func (mdb *MVCCDatabase) GetDB() *sql.DB {
	return mdb.writeDB
}

// GetWriteDB returns the dedicated write connection (pool size=1)
func (mdb *MVCCDatabase) GetWriteDB() *sql.DB {
	return mdb.writeDB
}

// GetReadDB returns the read connection pool (pool size from config)
func (mdb *MVCCDatabase) GetReadDB() *sql.DB {
	return mdb.readDB
}

// GetTransactionManager returns the MVCC transaction manager
func (mdb *MVCCDatabase) GetTransactionManager() *MVCCTransactionManager {
	return mdb.txnMgr
}

// GetClock returns the HLC clock
func (mdb *MVCCDatabase) GetClock() *hlc.Clock {
	return mdb.clock
}

// Close closes the database connections, MetaStore, and stops GC.
// Order is important: stop GC first, then close connections.
func (mdb *MVCCDatabase) Close() error {
	// Stop GC goroutine first to prevent it from accessing closed connections
	if mdb.txnMgr != nil {
		mdb.txnMgr.StopGarbageCollection()
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

// GetSchemaCache returns the shared schema cache for preupdate hooks
func (mdb *MVCCDatabase) GetSchemaCache() *SchemaCache {
	return mdb.schemaCache
}

// CloseSQLiteConnections closes all SQLite connections synchronously.
// This MUST be called BEFORE replacing database files during snapshot apply.
// After file replacement, call OpenSQLiteConnections to create new connections.
// Note: This does NOT touch MetaStore (PebbleDB) - only SQLite connections.
func (mdb *MVCCDatabase) CloseSQLiteConnections() {
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
func (mdb *MVCCDatabase) OpenSQLiteConnections(dbPath string) error {
	busyTimeoutMS := cfg.Config.MVCC.LockWaitTimeoutSeconds * 1000
	poolCfg := cfg.Config.ConnectionPool

	// Open write connection
	writeDSN := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d&_txlock=immediate", dbPath, busyTimeoutMS)
	writeDB, err := sql.Open("sqlite3", writeDSN)
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
	hookDB, err := sql.Open("sqlite3", writeDSN)
	if err != nil {
		writeDB.Close()
		return fmt.Errorf("failed to open hook connection: %w", err)
	}
	hookDB.SetMaxOpenConns(1)
	hookDB.SetMaxIdleConns(1)
	hookDB.SetConnMaxLifetime(0)

	// Open read connection pool
	readDSN := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=%d", dbPath, busyTimeoutMS)
	readDB, err := sql.Open("sqlite3", readDSN)
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

	// Invalidate schema cache since tables may have changed
	if mdb.schemaCache != nil {
		mdb.schemaCache.InvalidateAll()
	}

	return nil
}

// GetMetaStore returns the MetaStore for transaction metadata
func (mdb *MVCCDatabase) GetMetaStore() MetaStore {
	return mdb.metaStore
}

// ApplyCDCEntries applies CDC data entries to SQLite.
// Used by CompletedLocalExecution.Commit() to persist data captured during hooks.
func (mdb *MVCCDatabase) ApplyCDCEntries(entries []*IntentEntry) error {
	if mdb.txnMgr == nil {
		return fmt.Errorf("transaction manager not initialized")
	}
	return mdb.txnMgr.applyCDCEntries(0, entries)
}

// PendingLocalExecution represents a locally executed transaction waiting for quorum
// The SQLite transaction is held open until Commit or Rollback is called
type PendingLocalExecution struct {
	session *EphemeralHookSession // Ephemeral session (owns its connection)
	db      *MVCCDatabase
}

// CompletedLocalExecution represents a CDC capture that's already been rolled back.
// Used by the new hookDB flow where we capture CDC then release the connection
// BEFORE 2PC broadcast. Commit/Rollback are no-ops.
type CompletedLocalExecution struct {
	cdcEntries []*IntentEntry
	rowCounts  map[string]int64
	keyHashes  map[string][]uint64
	db         *MVCCDatabase
}

// Ensure CompletedLocalExecution implements coordinator.PendingExecution
var _ coordinator.PendingExecution = (*CompletedLocalExecution)(nil)

// GetRowCounts returns the number of affected rows per table
func (c *CompletedLocalExecution) GetRowCounts() map[string]int64 {
	return c.rowCounts
}

// GetTotalRowCount returns total rows affected across all tables
func (c *CompletedLocalExecution) GetTotalRowCount() int64 {
	var total int64
	for _, count := range c.rowCounts {
		total += count
	}
	return total
}

// GetKeyHashes returns XXH64 hashes of affected row keys per table
// Returns nil for tables that exceed maxRows (signals MVCC fallback)
func (c *CompletedLocalExecution) GetKeyHashes(maxRows int) map[string][]uint64 {
	if maxRows <= 0 {
		return c.keyHashes
	}
	result := make(map[string][]uint64, len(c.keyHashes))
	for table, hashes := range c.keyHashes {
		rowCount := c.rowCounts[table]
		if rowCount > int64(maxRows) {
			result[table] = nil
			continue
		}
		result[table] = hashes
	}
	return result
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
func (c *CompletedLocalExecution) GetCDCEntries() []coordinator.CDCEntry {
	if len(c.cdcEntries) == 0 {
		return nil
	}
	result := make([]coordinator.CDCEntry, len(c.cdcEntries))
	for i, e := range c.cdcEntries {
		result[i] = coordinator.CDCEntry{
			RowKey:    e.RowKey,
			OldValues: e.OldValues,
			NewValues: e.NewValues,
		}
	}
	return result
}

// FlushIntentLog is a no-op - CDC data already persisted in MetaStore
func (c *CompletedLocalExecution) FlushIntentLog() error {
	return nil
}

// GetRowCounts returns the number of affected rows per table
func (p *PendingLocalExecution) GetRowCounts() map[string]int64 {
	if p.session == nil {
		return nil
	}
	return p.session.GetRowCounts()
}

// GetTotalRowCount returns total rows affected across all tables
func (p *PendingLocalExecution) GetTotalRowCount() int64 {
	counts := p.GetRowCounts()
	var total int64
	for _, count := range counts {
		total += count
	}
	return total
}

// GetKeyHashes returns XXH64 hashes of affected row keys per table.
// Returns nil for tables exceeding maxRows.
func (p *PendingLocalExecution) GetKeyHashes(maxRows int) map[string][]uint64 {
	if p.session == nil {
		return nil
	}
	return p.session.GetKeyHashes(maxRows)
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
func (p *PendingLocalExecution) GetCDCEntries() []coordinator.CDCEntry {
	if p.session == nil {
		return nil
	}
	entries, err := p.session.GetIntentEntries()
	if err != nil || len(entries) == 0 {
		return nil
	}
	result := make([]coordinator.CDCEntry, len(entries))
	for i, e := range entries {
		result[i] = coordinator.CDCEntry{
			RowKey:    e.RowKey,
			OldValues: e.OldValues,
			NewValues: e.NewValues,
		}
	}
	return result
}

// FlushIntentLog is a no-op with SQLite-backed intent storage.
// SQLite WAL mode handles durability automatically.
// Kept for API compatibility.
func (p *PendingLocalExecution) FlushIntentLog() error {
	if p.session == nil {
		return nil
	}
	return p.session.FlushIntentLog()
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
func (mdb *MVCCDatabase) ExecuteLocalWithHooks(ctx context.Context, txnID uint64, statements []protocol.Statement) (coordinator.PendingExecution, error) {
	// Extract table names from statements
	tables := make([]string, 0)
	seen := make(map[string]struct{})
	for _, stmt := range statements {
		if stmt.TableName != "" {
			if _, ok := seen[stmt.TableName]; !ok {
				tables = append(tables, stmt.TableName)
				seen[stmt.TableName] = struct{}{}
			}
		}
	}

	// Create ephemeral session with hookDB (NOT writeDB - avoids deadlock)
	session, err := StartEphemeralSession(ctx, mdb.hookDB, mdb.metaStore, mdb.schemaCache, txnID, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to start session: %w", err)
	}

	// Begin transaction on hookDB
	if err := session.BeginTx(ctx); err != nil {
		session.Rollback()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute each statement - hooks capture CDC to MetaStore
	for _, stmt := range statements {
		if err := session.ExecContext(ctx, stmt.SQL); err != nil {
			session.Rollback()
			return nil, fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	// Get CDC entries and row counts BEFORE rollback
	cdcEntries, _ := session.GetIntentEntries()
	rowCounts := session.GetRowCounts()
	keyHashes := session.GetKeyHashes(1000) // For interface compatibility

	// ROLLBACK hookDB immediately - release connection BEFORE 2PC
	// CDC data is already persisted in MetaStore, so we don't lose anything
	if err := session.Rollback(); err != nil {
		return nil, fmt.Errorf("failed to rollback hook session: %w", err)
	}

	// Return completed execution with captured CDC data
	// Commit/Rollback are no-ops since session is already closed
	return &CompletedLocalExecution{
		cdcEntries: cdcEntries,
		rowCounts:  rowCounts,
		keyHashes:  keyHashes,
		db:         mdb,
	}, nil
}

// ExecuteTransaction executes a transaction with MVCC semantics
// This is the main entry point for application-level transactions
func (mdb *MVCCDatabase) ExecuteTransaction(ctx context.Context, statements []protocol.Statement) error {
	// Begin transaction
	txn, err := mdb.txnMgr.BeginTransaction(mdb.nodeID)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Add all statements
	for _, stmt := range statements {
		if err := mdb.txnMgr.AddStatement(txn, stmt); err != nil {
			mdb.txnMgr.AbortTransaction(txn)
			return fmt.Errorf("failed to add statement: %w", err)
		}

		// Create write intent for each statement
		// Extract row key (simplified - would need proper SQL parsing)
		rowKey := extractRowKeyFromStatement(stmt)
		dataSnapshot, serErr := SerializeData(map[string]interface{}{
			"sql":       stmt.SQL,
			"type":      stmt.Type,
			"timestamp": txn.StartTS.WallTime,
		})
		if serErr != nil {
			mdb.txnMgr.AbortTransaction(txn)
			return fmt.Errorf("failed to serialize data: %w", serErr)
		}

		err := mdb.txnMgr.WriteIntent(txn, IntentTypeDML, stmt.TableName, rowKey, stmt, dataSnapshot)
		if err != nil {
			mdb.txnMgr.AbortTransaction(txn)
			return fmt.Errorf("write conflict: %w", err)
		}
	}

	// Replicate to other nodes if replication is configured
	if mdb.replicationFn != nil {
		if err := mdb.replicationFn(ctx, txn); err != nil {
			mdb.txnMgr.AbortTransaction(txn)
			return fmt.Errorf("replication failed: %w", err)
		}
	}

	// Commit transaction
	if err := mdb.txnMgr.CommitTransaction(txn); err != nil {
		mdb.txnMgr.AbortTransaction(txn)
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

// ExecuteQuery executes a read query with MVCC snapshot isolation
// Uses the read connection pool for concurrent read access
func (mdb *MVCCDatabase) ExecuteQuery(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	// Get current snapshot timestamp
	snapshotTS := mdb.clock.Now()

	// Execute on read connection pool for concurrent reads
	// SQLite's WAL mode provides snapshot isolation at the connection level
	rows, err := mdb.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	// Note: snapshotTS is captured but SQLite's WAL mode provides snapshot isolation
	// at the transaction level. For full MVCC with write intents, use ExecuteMVCCRead.
	_ = snapshotTS
	return rows, nil
}

// ExecuteMVCCRead executes a read query with full MVCC support
// Uses rqlite/sql AST parser for proper SQL analysis
// Uses the read connection pool for concurrent read access
func (mdb *MVCCDatabase) ExecuteMVCCRead(ctx context.Context, query string, args ...interface{}) ([]string, []map[string]interface{}, error) {
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

// ExecuteQueryRow executes a single-row query with MVCC snapshot isolation
// Uses the read connection pool for concurrent read access
func (mdb *MVCCDatabase) ExecuteQueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	// Get current snapshot timestamp
	// Note: SQLite's WAL mode provides snapshot isolation at transaction level.
	// For structured queries requiring full MVCC with write intent checking,
	// use ExecuteMVCCRead instead which handles version resolution.
	snapshotTS := mdb.clock.Now()
	_ = snapshotTS

	return mdb.readDB.QueryRowContext(ctx, query, args...)
}

// Exec executes a statement (for DDL and non-transactional operations)
// Uses the write connection for data modifications
func (mdb *MVCCDatabase) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return mdb.writeDB.ExecContext(ctx, query, args...)
}

// extractRowKeyFromStatement extracts row key from statement
// The row key is extracted during parsing from the original MySQL AST,
// so we just use the pre-extracted value from the Statement struct
func extractRowKeyFromStatement(stmt protocol.Statement) string {
	// If RowKey was extracted during parsing, use it
	if stmt.RowKey != "" {
		return stmt.RowKey
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
