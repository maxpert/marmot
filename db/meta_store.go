package db

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
)

// MetaStore provides transactional metadata storage separate from user data.
// Each user database has its own MetaStore backed by PebbleDB.
// This separation allows user data writes and metadata writes to happen in parallel.
type MetaStore interface {
	// Transaction lifecycle
	BeginTransaction(txnID, nodeID uint64, startTS hlc.Timestamp) error
	CommitTransaction(txnID uint64, commitTS hlc.Timestamp, statements []byte, dbName, tablesInvolved string) error
	AbortTransaction(txnID uint64) error
	GetTransaction(txnID uint64) (*TransactionRecord, error)
	GetPendingTransactions() ([]*TransactionRecord, error)
	Heartbeat(txnID uint64) error

	// StoreReplayedTransaction inserts a fully-committed transaction record directly.
	// Used by delta sync to record transactions that were replayed from other nodes.
	// Unlike CommitTransaction, this doesn't require a prior BeginTransaction call.
	StoreReplayedTransaction(txnID, nodeID uint64, commitTS hlc.Timestamp, statements []byte, dbName string) error

	// Write intents (distributed locks)
	WriteIntent(txnID uint64, intentType IntentType, tableName, rowKey string, op OpType, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error
	ValidateIntent(tableName, rowKey string, expectedTxnID uint64) (bool, error)
	DeleteIntent(tableName, rowKey string, txnID uint64) error
	DeleteIntentsByTxn(txnID uint64) error
	MarkIntentsForCleanup(txnID uint64) error // Fast path: mark intents as ready for overwrite
	GetIntentsByTxn(txnID uint64) ([]*WriteIntentRecord, error)
	GetIntent(tableName, rowKey string) (*WriteIntentRecord, error)
	GetIntentFilter() *IntentFilter // Cuckoo filter for fast-path conflict detection

	// MVCC versions
	CreateMVCCVersion(tableName, rowKey string, ts hlc.Timestamp, nodeID, txnID uint64, op OpType, data []byte) error
	GetLatestVersion(tableName, rowKey string) (*MVCCVersionRecord, error)
	GetMVCCVersionCount(tableName, rowKey string) (int, error)

	// Replication state
	GetReplicationState(peerNodeID uint64, dbName string) (*ReplicationStateRecord, error)
	UpdateReplicationState(peerNodeID uint64, dbName string, lastTxnID uint64, lastTS hlc.Timestamp) error
	GetMinAppliedTxnID(dbName string) (uint64, error)
	GetAllReplicationStates() ([]*ReplicationStateRecord, error)

	// Sequence numbers for gap-free replication
	GetNextSeqNum(nodeID uint64) (uint64, error)
	GetMaxSeqNum() (uint64, error)
	GetMinAppliedSeqNum(dbName string) (uint64, error)

	// Schema/DDL
	GetSchemaVersion(dbName string) (int64, error)
	UpdateSchemaVersion(dbName string, version int64, ddlSQL string, txnID uint64) error
	GetAllSchemaVersions() (map[string]int64, error)
	TryAcquireDDLLock(dbName string, nodeID uint64, leaseDuration time.Duration) (bool, error)
	ReleaseDDLLock(dbName string, nodeID uint64) error

	// CDC intent entries
	WriteIntentEntry(txnID, seq uint64, op uint8, table, rowKey string, oldVals, newVals []byte) error
	GetIntentEntries(txnID uint64) ([]*IntentEntry, error)
	DeleteIntentEntries(txnID uint64) error

	// GC
	CleanupStaleTransactions(timeout time.Duration) (int, error)
	CleanupOldTransactionRecords(minRetention, maxRetention time.Duration, minAppliedTxnID, minAppliedSeqNum uint64) (int, error)
	CleanupOldMVCCVersions(keepVersions int) (int, error)

	// Aggregation queries for anti-entropy
	GetMaxCommittedTxnID() (uint64, error)
	GetCommittedTxnCount() (int64, error)

	// Streaming for anti-entropy delta sync
	StreamCommittedTransactions(fromTxnID uint64, callback func(*TransactionRecord) error) error

	// Lifecycle
	Close() error
	Checkpoint() error // Triggers PebbleDB checkpoint for consistency
}

// TransactionRecord represents a transaction record in meta store
type TransactionRecord struct {
	TxnID                uint64
	NodeID               uint64
	SeqNum               uint64 // Monotonic sequence for gap detection
	Status               TxnStatus
	StartTSWall          int64
	StartTSLogical       int32
	CommitTSWall         int64
	CommitTSLogical      int32
	CreatedAt            int64
	CommittedAt          int64
	LastHeartbeat        int64
	TablesInvolved       string
	SerializedStatements []byte
	DatabaseName         string
}

// WriteIntentRecord represents a write intent in meta store
type WriteIntentRecord struct {
	IntentType       IntentType // Type discriminator: DML, DDL, or DatabaseOp
	TableName        string
	RowKey           string
	TxnID            uint64
	TSWall           int64
	TSLogical        int32
	NodeID           uint64
	Operation        OpType
	SQLStatement     string
	DataSnapshot     []byte
	CreatedAt        int64
	MarkedForCleanup bool // Set to true when txn commits/aborts - allows immediate overwrite
}

// MVCCVersionRecord represents an MVCC version in meta store
type MVCCVersionRecord struct {
	TableName    string
	RowKey       string
	TSWall       int64
	TSLogical    int32
	NodeID       uint64
	TxnID        uint64
	Operation    OpType
	DataSnapshot []byte
	CreatedAt    int64
}

// ReplicationStateRecord represents replication state for a peer
type ReplicationStateRecord struct {
	PeerNodeID           uint64
	DatabaseName         string
	LastAppliedTxnID     uint64
	LastAppliedTSWall    int64
	LastAppliedTSLogical int32
	LastSyncTime         int64
	SyncStatus           SyncStatus
}

// NewMetaStore creates a PebbleDB-backed MetaStore.
// basePath is the path to the user database (e.g., "/data/mydb.db").
// Creates {basePath}_meta.pebble/ directory
func NewMetaStore(basePath string) (MetaStore, error) {
	metaPath := strings.TrimSuffix(basePath, ".db") + "_meta.pebble"

	if cfg.Config != nil && !filepath.IsAbs(metaPath) {
		metaPath = filepath.Join(cfg.Config.DataDir, metaPath)
	}

	// Use DefaultPebbleOptions() which reads from cfg.Config
	return NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
}
