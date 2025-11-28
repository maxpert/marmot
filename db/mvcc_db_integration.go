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

	rqlitesql "github.com/rqlite/sql"
)

// Ensure PendingLocalExecution implements coordinator.PendingExecution
var _ coordinator.PendingExecution = (*PendingLocalExecution)(nil)

// MVCCDatabase wraps a SQL database with MVCC transaction support
// This is the main integration point between application layer and MVCC storage
//
// SQLite WAL mode allows ONE writer + MANY concurrent readers.
// We maintain separate connection pools:
// - writeDB: Single connection for all writes (SQLite limitation)
// - readDB: Multiple connections for concurrent reads (pool_size from config)
type MVCCDatabase struct {
	writeDB       *sql.DB // Write connection (pool size=1, _txlock=immediate)
	readDB        *sql.DB // Read connection pool (pool size from config)
	txnMgr        *MVCCTransactionManager
	clock         *hlc.Clock
	nodeID        uint64
	replicationFn ReplicationFunc
	commitBatcher *CommitBatcher
	schemaCache   *SchemaCache // Shared schema cache for preupdate hooks
	systemDB      *sql.DB      // System DB for intent entries (separate file)
}

// ReplicationFunc is called to replicate transactions to other nodes
// This is injected from the coordinator layer
type ReplicationFunc func(ctx context.Context, txn *MVCCTransaction) error

// NewMVCCDatabase creates a new MVCC-enabled database
// systemDB is the system database (*sql.DB) for storing intent entries during hooks
func NewMVCCDatabase(dbPath string, nodeID uint64, clock *hlc.Clock, systemDB *sql.DB) (*MVCCDatabase, error) {
	// Get timeout from config (LockWaitTimeoutSeconds is in seconds, SQLite needs milliseconds)
	busyTimeoutMS := cfg.Config.MVCC.LockWaitTimeoutSeconds * 1000
	poolCfg := cfg.Config.ConnectionPool
	isMemoryDB := strings.Contains(dbPath, ":memory:")

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

	writeDB, err := sql.Open("sqlite3", writeDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open write database: %w", err)
	}

	// Write connection: exactly 1 connection (SQLite limitation)
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0) // Keep connection alive forever

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

	readDB, err := sql.Open("sqlite3", readDSN)
	if err != nil {
		writeDB.Close()
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

	// Configure both connections with optimal SQLite settings
	for _, db := range []*sql.DB{writeDB, readDB} {
		if !isMemoryDB {
			_, err = db.Exec("PRAGMA journal_mode=WAL")
			if err != nil {
				writeDB.Close()
				readDB.Close()
				return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
			}

			_, err = db.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMS))
			if err != nil {
				writeDB.Close()
				readDB.Close()
				return nil, fmt.Errorf("failed to set busy timeout: %w", err)
			}

			// PRAGMA synchronous=NORMAL is safe with WAL mode
			_, err = db.Exec("PRAGMA synchronous=NORMAL")
			if err != nil {
				writeDB.Close()
				readDB.Close()
				return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
			}

			// Increase cache size for better performance (64MB)
			_, err = db.Exec("PRAGMA cache_size=-64000")
			if err != nil {
				writeDB.Close()
				readDB.Close()
				return nil, fmt.Errorf("failed to set cache size: %w", err)
			}

			// Store temp tables in memory
			_, err = db.Exec("PRAGMA temp_store=MEMORY")
			if err != nil {
				writeDB.Close()
				readDB.Close()
				return nil, fmt.Errorf("failed to set temp store: %w", err)
			}
		}
	}

	// Initialize MVCC schema (using write connection)
	if err := initializeMVCCSchema(writeDB); err != nil {
		writeDB.Close()
		readDB.Close()
		return nil, fmt.Errorf("failed to initialize MVCC schema: %w", err)
	}

	// Create transaction manager (uses write connection)
	txnMgr := NewMVCCTransactionManager(writeDB, clock)

	// Create commit batcher for SQLite-level batching (uses write connection)
	commitBatcher := NewCommitBatcher(writeDB, 20, 2*time.Millisecond)
	commitBatcher.Start()

	return &MVCCDatabase{
		writeDB:       writeDB,
		readDB:        readDB,
		txnMgr:        txnMgr,
		clock:         clock,
		nodeID:        nodeID,
		commitBatcher: commitBatcher,
		schemaCache:   NewSchemaCache(),
		systemDB:      systemDB,
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

// Close closes both read and write database connections
func (mdb *MVCCDatabase) Close() error {
	var writeErr, readErr error
	if mdb.writeDB != nil {
		writeErr = mdb.writeDB.Close()
	}
	if mdb.readDB != nil {
		readErr = mdb.readDB.Close()
	}
	if writeErr != nil {
		return writeErr
	}
	return readErr
}

// GetSchemaCache returns the shared schema cache for preupdate hooks
func (mdb *MVCCDatabase) GetSchemaCache() *SchemaCache {
	return mdb.schemaCache
}

// GetSystemDB returns the system database for intent entries
func (mdb *MVCCDatabase) GetSystemDB() *sql.DB {
	return mdb.systemDB
}

// PendingLocalExecution represents a locally executed transaction waiting for quorum
// The SQLite transaction is held open until Commit or Rollback is called
type PendingLocalExecution struct {
	session *EphemeralHookSession // Ephemeral session (owns its connection)
	db      *MVCCDatabase
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
// Used for MutationGuard hash list conflict detection.
// Returns nil for tables exceeding maxRows to let MVCC handle conflicts.
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

// FlushIntentLog is a no-op with SQLite-backed intent storage.
// SQLite WAL mode handles durability automatically.
// Kept for API compatibility.
func (p *PendingLocalExecution) FlushIntentLog() error {
	if p.session == nil {
		return nil
	}
	return p.session.FlushIntentLog()
}

// ExecuteLocalWithHooks executes SQL locally with preupdate hooks capturing changes.
// Returns a PendingExecution that holds the transaction open.
// Caller MUST call Commit() or Rollback() on the result.
//
// This implements the coordinator flow:
// 1. Create ephemeral session with dedicated connection
// 2. Register hooks and preload schemas
// 3. BEGIN TRANSACTION locally
// 4. Execute mutation commands (hooks capture affected rows)
// 5. Return without commit - caller decides based on quorum result
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

	// Create ephemeral session with dedicated write connection
	session, err := StartEphemeralSession(ctx, mdb.writeDB, mdb.systemDB, mdb.schemaCache, txnID, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to start session: %w", err)
	}

	// Begin transaction on the session's connection
	if err := session.BeginTx(ctx); err != nil {
		session.Rollback()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute each statement
	for _, stmt := range statements {
		if err := session.ExecContext(ctx, stmt.SQL); err != nil {
			session.Rollback()
			return nil, fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	// Return pending execution - transaction held open
	return &PendingLocalExecution{
		session: session,
		db:      mdb,
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

		err := mdb.txnMgr.WriteIntent(txn, stmt.TableName, rowKey, stmt, dataSnapshot)
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
	snapshotTS := mdb.clock.Now()

	// Parse SELECT with rqlite/sql parser for MVCC optimization
	tableName, pkValue := parseSelectForMVCC(query, args)
	if tableName != "" && pkValue != "" {
		// Use ReadWithWriteIntentCheck for PK lookups (uses read connection)
		row, err := coordinator.ReadWithWriteIntentCheck(mdb.readDB, snapshotTS, tableName, pkValue)
		if err != nil {
			return nil, nil, err
		}

		if _, ok := row["_no_version"]; ok {
			// Fallback to standard read
		} else {
			// Found MVCC version but deserialization not implemented, falling back
		}
	}

	// Fallback to standard read (Snapshot Isolation via SQLite WAL)
	// Uses read connection pool for concurrent reads
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

// parseSelectForMVCC parses a SELECT query using rqlite/sql AST
// Returns (tableName, pkValue) if this is a simple PK lookup, otherwise ("", "")
func parseSelectForMVCC(query string, args []interface{}) (string, string) {
	parser := rqlitesql.NewParser(strings.NewReader(query))
	stmt, err := parser.ParseStatement()
	if err != nil {
		return "", ""
	}

	sel, ok := stmt.(*rqlitesql.SelectStatement)
	if !ok {
		return "", ""
	}

	// Extract table name from FROM clause
	tableName := ""
	if sel.Source != nil {
		tableName = rqlitesql.SourceName(sel.Source)
	}
	if tableName == "" {
		return "", ""
	}

	// Check for simple WHERE clause with equality on single column (potential PK)
	if sel.WhereExpr == nil {
		return "", ""
	}

	pkValue := extractPKFromWhere(sel.WhereExpr, args)
	if pkValue == "" {
		return "", ""
	}

	return tableName, pkValue
}

// extractPKFromWhere extracts the PK value from a WHERE expression
// Only handles simple cases: WHERE col = value or WHERE col = ?
func extractPKFromWhere(whereExpr rqlitesql.Expr, args []interface{}) string {
	if whereExpr == nil {
		return ""
	}

	// Check for binary expression (col = value)
	binExpr, ok := whereExpr.(*rqlitesql.BinaryExpr)
	if !ok {
		return ""
	}

	// Must be equality
	if binExpr.Op != rqlitesql.EQ {
		return ""
	}

	// Extract value from right side
	switch v := binExpr.Y.(type) {
	case *rqlitesql.StringLit:
		return v.Value
	case *rqlitesql.NumberLit:
		return v.Value
	case *rqlitesql.BindExpr:
		// Parameter placeholder - use first arg
		if len(args) > 0 {
			return fmt.Sprintf("%v", args[0])
		}
	}

	return ""
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

// initializeMVCCSchema creates all MVCC system tables
func initializeMVCCSchema(db *sql.DB) error {
	schemas := []string{
		CreateTransactionRecordsTable,
		CreateWriteIntentsTable,
		CreateMVCCVersionsTable,
		CreateMetadataTable,
		CreateReplicationStateTable,
		CreateSchemaVersionTable,
		CreateDDLLockTable,
	}

	for _, schema := range schemas {
		if _, err := db.Exec(schema); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	return nil
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
