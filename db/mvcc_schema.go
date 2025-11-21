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
		tables_involved TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_txn_status ON __marmot__txn_records(status);
	CREATE INDEX IF NOT EXISTS idx_txn_heartbeat ON __marmot__txn_records(last_heartbeat);
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
