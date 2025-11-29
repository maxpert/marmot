package db

import (
	"database/sql"
	"time"

	"github.com/maxpert/marmot/hlc"
)

// MetaStore provides transactional metadata storage separate from user data.
// Each user database has its own MetaStore backed by a separate SQLite file.
// This separation allows user data writes and metadata writes to happen in parallel,
// avoiding the single-writer bottleneck in SQLite.
type MetaStore interface {
	// Transaction lifecycle
	BeginTransaction(txnID, nodeID uint64, startTS hlc.Timestamp) error
	CommitTransaction(txnID uint64, commitTS hlc.Timestamp, statements []byte, dbName string) error
	AbortTransaction(txnID uint64) error
	GetTransaction(txnID uint64) (*TransactionRecord, error)
	GetPendingTransactions() ([]*TransactionRecord, error)
	Heartbeat(txnID uint64) error

	// Write intents (distributed locks)
	WriteIntent(txnID uint64, tableName, rowKey, op, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error
	ValidateIntent(tableName, rowKey string, expectedTxnID uint64) (bool, error)
	DeleteIntent(tableName, rowKey string, txnID uint64) error
	DeleteIntentsByTxn(txnID uint64) error
	GetIntentsByTxn(txnID uint64) ([]*WriteIntentRecord, error)
	GetIntent(tableName, rowKey string) (*WriteIntentRecord, error)

	// MVCC versions
	CreateMVCCVersion(tableName, rowKey string, ts hlc.Timestamp, nodeID, txnID uint64, op string, data []byte) error
	GetLatestVersion(tableName, rowKey string) (*MVCCVersionRecord, error)

	// Replication state
	GetReplicationState(peerNodeID uint64, dbName string) (*ReplicationStateRecord, error)
	UpdateReplicationState(peerNodeID uint64, dbName string, lastTxnID uint64, lastTS hlc.Timestamp) error
	GetMinAppliedTxnID(dbName string) (uint64, error)

	// Sequence numbers for gap-free replication
	GetNextSeqNum(nodeID uint64) (uint64, error)
	GetMaxSeqNum() (uint64, error)
	GetMinAppliedSeqNum(dbName string) (uint64, error)

	// Schema/DDL
	GetSchemaVersion(dbName string) (int64, error)
	UpdateSchemaVersion(dbName string, version int64, ddlSQL string, txnID uint64) error
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

	// Lifecycle
	Close() error
	WriteDB() *sql.DB
	ReadDB() *sql.DB
}

// TransactionRecord represents a transaction record in meta store
type TransactionRecord struct {
	TxnID           uint64
	NodeID          uint64
	SeqNum          uint64 // Monotonic sequence for gap detection
	Status          string
	StartTSWall     int64
	StartTSLogical  int32
	CommitTSWall    int64
	CommitTSLogical int32
	CreatedAt       int64
	CommittedAt     int64
	LastHeartbeat   int64
	TablesInvolved  string
	StatementsJSON  []byte
	DatabaseName    string
}

// WriteIntentRecord represents a write intent in meta store
type WriteIntentRecord struct {
	TableName    string
	RowKey       string
	TxnID        uint64
	TSWall       int64
	TSLogical    int32
	NodeID       uint64
	Operation    string
	SQLStatement string
	DataSnapshot []byte
	CreatedAt    int64
}

// MVCCVersionRecord represents an MVCC version in meta store
type MVCCVersionRecord struct {
	TableName    string
	RowKey       string
	TSWall       int64
	TSLogical    int32
	NodeID       uint64
	TxnID        uint64
	Operation    string
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
	SyncStatus           string
}

// MetaStore status constants
const (
	MetaTxnStatusPending   = "PENDING"
	MetaTxnStatusCommitted = "COMMITTED"
	MetaTxnStatusAborted   = "ABORTED"

	MetaSyncStatusSynced     = "SYNCED"
	MetaSyncStatusCatchingUp = "CATCHING_UP"
	MetaSyncStatusFailed     = "FAILED"
)
