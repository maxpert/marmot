package db

// Operation constants for intent entries
// These are used across multiple files and must be available regardless of build tags
const (
	OpInsertInt uint8 = 0
	OpUpdateInt uint8 = 1
	OpDeleteInt uint8 = 2
)

// Internal table names for special operations
const (
	TableDatabaseOperations = "__marmot__database_operations"
	TableDDLOps             = "__marmot__ddl_ops"
)

// Database operation names for CREATE/DROP DATABASE
const (
	OpNameCreateDatabase = "CREATE_DATABASE"
	OpNameDropDatabase   = "DROP_DATABASE"
)

// DDL row key prefix for DDL operations in write intents
const DDLRowKeyPrefix = "__ddl__"

// StatementTypeToOpCode converts protocol.StatementType to uint8 operation code
// This is the canonical implementation used across packages
// StatementType values: Insert=0, Replace=1, Update=2, Delete=3
func StatementTypeToOpCode(stmtType int) uint8 {
	switch stmtType {
	case 0, 1: // StatementInsert, StatementReplace
		return OpInsertInt
	case 2: // StatementUpdate
		return OpUpdateInt
	case 3: // StatementDelete
		return OpDeleteInt
	default:
		return OpInsertInt
	}
}

// MetaStore Schema Design for Marmot v2.0
//
// All metadata is stored in a separate BadgerDB instance (user_db_meta.badger/)
// to avoid writer contention with user data writes.
//
// SQLite allows only ONE writer per database file. By separating metadata:
// - User data writes and metadata writes happen in PARALLEL
// - Transaction records don't block user writes
// - Write intent creation doesn't block user writes

const (
	// MetaCreateTransactionRecordsTable stores transaction metadata
	MetaCreateTransactionRecordsTable = `
	CREATE TABLE IF NOT EXISTS __marmot__txn_records (
		txn_id INTEGER PRIMARY KEY,
		node_id INTEGER NOT NULL,
		seq_num INTEGER, -- Monotonic sequence number for gap detection
		status TEXT NOT NULL, -- PENDING, COMMITTED, ABORTED
		start_ts_wall INTEGER NOT NULL,
		start_ts_logical INTEGER NOT NULL,
		commit_ts_wall INTEGER,
		commit_ts_logical INTEGER,
		created_at INTEGER NOT NULL,
		committed_at INTEGER,
		last_heartbeat INTEGER,
		tables_involved TEXT,
		statements_json TEXT,
		database_name TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_txn_status ON __marmot__txn_records(status);
	CREATE INDEX IF NOT EXISTS idx_txn_heartbeat ON __marmot__txn_records(last_heartbeat);
	CREATE INDEX IF NOT EXISTS idx_txn_commit_ts ON __marmot__txn_records(commit_ts_wall, commit_ts_logical);
	CREATE INDEX IF NOT EXISTS idx_txn_seq_num ON __marmot__txn_records(seq_num);
	`

	// MetaCreateNodeSequencesTable tracks per-node sequence counters for gap-free replication
	MetaCreateNodeSequencesTable = `
	CREATE TABLE IF NOT EXISTS __marmot__node_sequences (
		node_id INTEGER PRIMARY KEY,
		last_seq_num INTEGER NOT NULL DEFAULT 0
	);
	`

	// MetaCreateWriteIntentsTable stores provisional writes (intents)
	// These act as both provisional values AND distributed locks
	MetaCreateWriteIntentsTable = `
	CREATE TABLE IF NOT EXISTS __marmot__write_intents (
		table_name TEXT NOT NULL,
		row_key TEXT NOT NULL,
		txn_id INTEGER NOT NULL,
		ts_wall INTEGER NOT NULL,
		ts_logical INTEGER NOT NULL,
		node_id INTEGER NOT NULL,
		operation TEXT NOT NULL,
		sql_statement TEXT NOT NULL,
		data_snapshot BLOB,
		created_at INTEGER NOT NULL,
		PRIMARY KEY (table_name, row_key),
		FOREIGN KEY (txn_id) REFERENCES __marmot__txn_records(txn_id)
	);

	CREATE INDEX IF NOT EXISTS idx_intent_txn ON __marmot__write_intents(txn_id);
	CREATE INDEX IF NOT EXISTS idx_intent_ts ON __marmot__write_intents(ts_wall, ts_logical);
	`

	// MetaCreateMVCCVersionsTable stores all versions of data
	MetaCreateMVCCVersionsTable = `
	CREATE TABLE IF NOT EXISTS __marmot__mvcc_versions (
		table_name TEXT NOT NULL,
		row_key TEXT NOT NULL,
		ts_wall INTEGER NOT NULL,
		ts_logical INTEGER NOT NULL,
		node_id INTEGER NOT NULL,
		txn_id INTEGER NOT NULL,
		operation TEXT NOT NULL,
		data_snapshot BLOB,
		created_at INTEGER NOT NULL,
		PRIMARY KEY (table_name, row_key, ts_wall, ts_logical, node_id),
		FOREIGN KEY (txn_id) REFERENCES __marmot__txn_records(txn_id)
	);

	CREATE INDEX IF NOT EXISTS idx_mvcc_ts ON __marmot__mvcc_versions(ts_wall, ts_logical);
	CREATE INDEX IF NOT EXISTS idx_mvcc_table_key ON __marmot__mvcc_versions(table_name, row_key);
	`

	// MetaCreateMetadataTable stores cluster metadata
	MetaCreateMetadataTable = `
	CREATE TABLE IF NOT EXISTS __marmot__metadata (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,
		updated_at INTEGER NOT NULL
	);
	`

	// MetaCreateReplicationStateTable tracks replication progress per peer per database
	MetaCreateReplicationStateTable = `
	CREATE TABLE IF NOT EXISTS __marmot__replication_state (
		peer_node_id INTEGER NOT NULL,
		database_name TEXT NOT NULL,
		last_applied_txn_id INTEGER NOT NULL DEFAULT 0,
		last_applied_ts_wall INTEGER NOT NULL DEFAULT 0,
		last_applied_ts_logical INTEGER NOT NULL DEFAULT 0,
		last_sync_time INTEGER NOT NULL,
		sync_status TEXT NOT NULL DEFAULT 'SYNCED',
		PRIMARY KEY (peer_node_id, database_name)
	);
	`

	// MetaCreateSchemaVersionTable tracks schema evolution per database
	MetaCreateSchemaVersionTable = `
	CREATE TABLE IF NOT EXISTS __marmot__schema_versions (
		database_name TEXT PRIMARY KEY,
		schema_version INTEGER NOT NULL DEFAULT 0,
		last_ddl_sql TEXT,
		last_ddl_txn_id INTEGER,
		updated_at INTEGER NOT NULL
	);
	`

	// MetaCreateDDLLockTable provides cluster-wide DDL serialization
	MetaCreateDDLLockTable = `
	CREATE TABLE IF NOT EXISTS __marmot__ddl_locks (
		database_name TEXT PRIMARY KEY,
		locked_by_node_id INTEGER NOT NULL,
		locked_at INTEGER NOT NULL,
		lease_expires_at INTEGER NOT NULL,
		txn_id INTEGER
	);
	`

	// MetaCreateIntentEntriesTable stores CDC entries captured during preupdate_hook
	// Previously in system database, now in per-user meta database
	MetaCreateIntentEntriesTable = `
	CREATE TABLE IF NOT EXISTS __marmot__intent_entries (
		txn_id INTEGER NOT NULL,
		seq INTEGER NOT NULL,
		operation INTEGER NOT NULL,
		table_name TEXT NOT NULL,
		row_key TEXT NOT NULL,
		old_values BLOB,
		new_values BLOB,
		created_at INTEGER NOT NULL,
		PRIMARY KEY (txn_id, seq)
	);

	CREATE INDEX IF NOT EXISTS idx_intent_entries_txn ON __marmot__intent_entries(txn_id);
	`
)

// MetaSchemas returns all schema statements for meta database initialization
func MetaSchemas() []string {
	return []string{
		MetaCreateTransactionRecordsTable,
		MetaCreateNodeSequencesTable,
		MetaCreateWriteIntentsTable,
		MetaCreateMVCCVersionsTable,
		MetaCreateMetadataTable,
		MetaCreateReplicationStateTable,
		MetaCreateSchemaVersionTable,
		MetaCreateDDLLockTable,
		MetaCreateIntentEntriesTable,
	}
}

// MetaMigrations returns migration statements for existing databases
// These should be run when opening an existing meta database
func MetaMigrations() []string {
	return []string{
		// Add seq_num column if it doesn't exist
		`ALTER TABLE __marmot__txn_records ADD COLUMN seq_num INTEGER`,
		// Create node_sequences table if it doesn't exist
		MetaCreateNodeSequencesTable,
		// Create index on seq_num if it doesn't exist
		`CREATE INDEX IF NOT EXISTS idx_txn_seq_num ON __marmot__txn_records(seq_num)`,
	}
}
