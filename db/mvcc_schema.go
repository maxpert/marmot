package db

// MVCC Schema Design for Marmot v2.0
//
// Based on Percolator (TiDB) and CockroachDB transaction protocols:
// - Write intents act as distributed locks
// - Transaction records track commit state
// - MVCC provides snapshot isolation
// - Write-write conflicts are DETECTED and cause ABORT (never silent data loss)

const (
	// CreateTransactionRecordsTable stores transaction metadata
	// Similar to CockroachDB's transaction record and TiDB's transaction table
	CreateTransactionRecordsTable = `
	CREATE TABLE IF NOT EXISTS __marmot__txn_records (
		txn_id INTEGER PRIMARY KEY,
		node_id INTEGER NOT NULL,
		status TEXT NOT NULL, -- PENDING, COMMITTED, ABORTED
		start_ts_wall INTEGER NOT NULL,
		start_ts_logical INTEGER NOT NULL,
		commit_ts_wall INTEGER,
		commit_ts_logical INTEGER,
		created_at INTEGER NOT NULL,
		committed_at INTEGER,
		-- Heartbeat for long-running transactions
		last_heartbeat INTEGER,
		-- List of tables involved (for cleanup)
		tables_involved TEXT,
		-- JSON array of SQL statements for delta sync replication
		statements_json TEXT,
		-- Database name for routing
		database_name TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_txn_status ON __marmot__txn_records(status);
	CREATE INDEX IF NOT EXISTS idx_txn_heartbeat ON __marmot__txn_records(last_heartbeat);
	CREATE INDEX IF NOT EXISTS idx_txn_commit_ts ON __marmot__txn_records(commit_ts_wall, commit_ts_logical);
	`

	// CreateWriteIntentsTable stores provisional writes (intents)
	// These act as both provisional values AND distributed locks
	CreateWriteIntentsTable = `
	CREATE TABLE IF NOT EXISTS __marmot__write_intents (
		-- Intent identification
		table_name TEXT NOT NULL,
		row_key TEXT NOT NULL, -- Serialized primary key

		-- Transaction that created this intent
		txn_id INTEGER NOT NULL,

		-- Timestamp of the write
		ts_wall INTEGER NOT NULL,
		ts_logical INTEGER NOT NULL,
		node_id INTEGER NOT NULL,

		-- The actual SQL operation
		operation TEXT NOT NULL, -- INSERT, UPDATE, DELETE
		sql_statement TEXT NOT NULL,

		-- Provisional data (for reads during txn)
		data_snapshot BLOB,

		created_at INTEGER NOT NULL,

		PRIMARY KEY (table_name, row_key),
		FOREIGN KEY (txn_id) REFERENCES __marmot__txn_records(txn_id)
	);

	CREATE INDEX IF NOT EXISTS idx_intent_txn ON __marmot__write_intents(txn_id);
	CREATE INDEX IF NOT EXISTS idx_intent_ts ON __marmot__write_intents(ts_wall, ts_logical);
	`

	// CreateMVCCVersionsTable stores all versions of data
	// Enables time-travel queries and snapshot reads
	CreateMVCCVersionsTable = `
	CREATE TABLE IF NOT EXISTS __marmot__mvcc_versions (
		-- Version identification
		table_name TEXT NOT NULL,
		row_key TEXT NOT NULL,

		-- Timestamp this version was written
		ts_wall INTEGER NOT NULL,
		ts_logical INTEGER NOT NULL,
		node_id INTEGER NOT NULL,

		-- Transaction that created this version
		txn_id INTEGER NOT NULL,

		-- The data
		operation TEXT NOT NULL, -- INSERT, UPDATE, DELETE
		data_snapshot BLOB,

		created_at INTEGER NOT NULL,

		PRIMARY KEY (table_name, row_key, ts_wall, ts_logical, node_id),
		FOREIGN KEY (txn_id) REFERENCES __marmot__txn_records(txn_id)
	);

	CREATE INDEX IF NOT EXISTS idx_mvcc_ts ON __marmot__mvcc_versions(ts_wall, ts_logical);
	CREATE INDEX IF NOT EXISTS idx_mvcc_table_key ON __marmot__mvcc_versions(table_name, row_key);
	`

	// CreateMetadataTable stores cluster metadata
	CreateMetadataTable = `
	CREATE TABLE IF NOT EXISTS __marmot__metadata (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,
		updated_at INTEGER NOT NULL
	);
	`

	// CreateReplicationStateTable tracks replication progress per peer per database
	// Used for delta sync catch-up after partition heals
	// Multi-database support: tracks each database independently
	CreateReplicationStateTable = `
	CREATE TABLE IF NOT EXISTS __marmot__replication_state (
		peer_node_id INTEGER NOT NULL,
		database_name TEXT NOT NULL,
		last_applied_txn_id INTEGER NOT NULL DEFAULT 0,
		last_applied_ts_wall INTEGER NOT NULL DEFAULT 0,
		last_applied_ts_logical INTEGER NOT NULL DEFAULT 0,
		last_sync_time INTEGER NOT NULL,
		sync_status TEXT NOT NULL DEFAULT 'SYNCED', -- SYNCED, CATCHING_UP, FAILED
		PRIMARY KEY (peer_node_id, database_name)
	);
	`

	// CreateSchemaVersionTable tracks schema evolution per database
	// Used to ensure nodes apply DDL in order and detect schema drift
	// Enables QUORUM-based DDL replication with automatic catch-up
	CreateSchemaVersionTable = `
	CREATE TABLE IF NOT EXISTS __marmot__schema_versions (
		database_name TEXT PRIMARY KEY,
		schema_version INTEGER NOT NULL DEFAULT 0,
		last_ddl_sql TEXT,
		last_ddl_txn_id INTEGER,
		updated_at INTEGER NOT NULL
	);
	`

	// CreateDDLLockTable provides cluster-wide DDL serialization
	// Prevents concurrent DDL statements on the same database from conflicting
	// Uses HLC timestamps and lease-based locking
	CreateDDLLockTable = `
	CREATE TABLE IF NOT EXISTS __marmot__ddl_locks (
		database_name TEXT PRIMARY KEY,
		locked_by_node_id INTEGER NOT NULL,
		locked_at INTEGER NOT NULL,
		lease_expires_at INTEGER NOT NULL,
		txn_id INTEGER  -- Transaction ID that holds the lock
	);
	`

	// CreateIntentEntriesTable stores CDC entries captured during preupdate_hook
	// This replaces the file-based intent log with SQLite-based storage
	// Stored in system database (__marmot_system.db) to allow writes during hooks
	CreateIntentEntriesTable = `
	CREATE TABLE IF NOT EXISTS __marmot__intent_entries (
		txn_id INTEGER NOT NULL,
		seq INTEGER NOT NULL,
		operation INTEGER NOT NULL,  -- 0=INSERT, 1=UPDATE, 2=DELETE
		table_name TEXT NOT NULL,
		row_key TEXT NOT NULL,
		old_values BLOB,  -- JSON, NULL for INSERT
		new_values BLOB,  -- JSON, NULL for DELETE
		created_at INTEGER NOT NULL,
		PRIMARY KEY (txn_id, seq)
	);

	CREATE INDEX IF NOT EXISTS idx_intent_entries_txn ON __marmot__intent_entries(txn_id);
	`
)

// Transaction status constants
const (
	TxnStatusPending   = "PENDING"
	TxnStatusCommitted = "COMMITTED"
	TxnStatusAborted   = "ABORTED"
)

// Operation type constants
const (
	OpInsert = "INSERT"
	OpUpdate = "UPDATE"
	OpDelete = "DELETE"
)
