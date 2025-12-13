package db

import (
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
)

// ErrStopIteration signals scan callbacks to stop iteration without error
var ErrStopIteration = errors.New("stop iteration")

// MetaStore provides transactional metadata storage separate from user data.
// Each user database has its own MetaStore backed by PebbleDB.
// This separation allows user data writes and metadata writes to happen in parallel.
type MetaStore interface {
	// Transaction lifecycle
	BeginTransaction(txnID, nodeID uint64, startTS hlc.Timestamp) error
	CommitTransaction(txnID uint64, commitTS hlc.Timestamp, statements []byte, dbName, tablesInvolved string, requiredSchemaVersion uint64) error
	AbortTransaction(txnID uint64) error
	GetTransaction(txnID uint64) (*TransactionRecord, error)
	GetPendingTransactions() ([]*TransactionRecord, error)
	Heartbeat(txnID uint64) error

	// StoreReplayedTransaction inserts a fully-committed transaction record directly.
	// Used by delta sync to record transactions that were replayed from other nodes.
	// Unlike CommitTransaction, this doesn't require a prior BeginTransaction call.
	StoreReplayedTransaction(txnID, nodeID uint64, commitTS hlc.Timestamp, statements []byte, dbName string) error

	// Write intents (distributed locks)
	WriteIntent(txnID uint64, intentType IntentType, tableName, intentKey string, op OpType, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error
	ValidateIntent(tableName, intentKey string, expectedTxnID uint64) (bool, error)
	DeleteIntent(tableName, intentKey string, txnID uint64) error
	DeleteIntentsByTxn(txnID uint64) error
	MarkIntentsForCleanup(txnID uint64) error // Fast path: mark intents as ready for overwrite
	GetIntentsByTxn(txnID uint64) ([]*WriteIntentRecord, error)
	GetIntent(tableName, intentKey string) (*WriteIntentRecord, error)
	GetIntentFilter() *IntentFilter // Cuckoo filter for fast-path conflict detection

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
	WriteIntentEntry(txnID, seq uint64, op uint8, table, intentKey string, oldVals, newVals []byte) error
	GetIntentEntries(txnID uint64) ([]*IntentEntry, error)
	DeleteIntentEntries(txnID uint64) error

	// CDC active locks for conflict detection
	AcquireCDCRowLock(txnID uint64, tableName, intentKey string) error
	ReleaseCDCRowLock(tableName, intentKey string, txnID uint64) error
	ReleaseCDCRowLocksByTxn(txnID uint64) error
	GetCDCRowLock(tableName, intentKey string) (uint64, error) // Returns txnID or 0 if no lock

	AcquireCDCTableDDLLock(txnID uint64, tableName string) error
	ReleaseCDCTableDDLLock(tableName string, txnID uint64) error
	HasCDCRowLocksForTable(tableName string) (bool, error) // For DDL to check if DML in progress
	GetCDCTableDDLLock(tableName string) (uint64, error)   // Returns txnID or 0 if no lock

	// GC
	CleanupStaleTransactions(timeout time.Duration) (int, error)
	CleanupOldTransactionRecords(minRetention, maxRetention time.Duration, minAppliedTxnID, minAppliedSeqNum uint64) (int, error)

	// Aggregation queries for anti-entropy
	GetMaxCommittedTxnID() (uint64, error)
	GetCommittedTxnCount() (int64, error)

	// Streaming for anti-entropy delta sync
	StreamCommittedTransactions(fromTxnID uint64, callback func(*TransactionRecord) error) error

	// ScanTransactions iterates transactions from fromTxnID.
	// If descending is true, scans from newest to oldest.
	// Callback returns nil to continue, ErrStopIteration to stop, or other error to abort.
	ScanTransactions(fromTxnID uint64, descending bool, callback func(*TransactionRecord) error) error

	// Lifecycle
	Close() error
	Checkpoint() error // Triggers PebbleDB checkpoint for consistency
}

// TransactionRecord represents a transaction record in meta store
type TransactionRecord struct {
	TxnID                 uint64
	NodeID                uint64
	SeqNum                uint64 // Monotonic sequence for gap detection
	Status                TxnStatus
	StartTSWall           int64
	StartTSLogical        int32
	CommitTSWall          int64
	CommitTSLogical       int32
	CreatedAt             int64
	CommittedAt           int64
	LastHeartbeat         int64
	TablesInvolved        string
	SerializedStatements  []byte
	DatabaseName          string
	RequiredSchemaVersion uint64 // Minimum schema version required for this transaction
}

// WriteIntentRecord represents a write intent in meta store
type WriteIntentRecord struct {
	IntentType       IntentType // Type discriminator: DML, DDL, or DatabaseOp
	TableName        string
	IntentKey        string
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
