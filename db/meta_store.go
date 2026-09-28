package db

import (
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

// ErrStopIteration signals scan callbacks to stop iteration without error
var ErrStopIteration = errors.New("stop iteration")

// ErrAbortCommitted is returned by AbortTransaction when the transaction is
// already COMMITTED: the local log is append-only, so only GC may ever remove
// a committed entry.
var ErrAbortCommitted = errors.New("cannot abort a committed transaction")

// ErrNotAbandonedBegin is returned by DiscardAbandonedBegin for a transaction
// whose record is not PendingBegunAbandoned.
var ErrNotAbandonedBegin = errors.New("transaction record is not an abandoned begin")

// PreparedPayload is what a transaction's durable prepare
// (DurablyPrepareTransaction) recorded it holds: its captured row count and
// its persisted (non-DML) intent count. A commit compares what it finds
// against it, so a transaction whose payload something deleted after the
// prepare is refused rather than committed empty.
type PreparedPayload struct {
	Rows    uint64 `msgpack:"r"`
	Intents uint64 `msgpack:"i"`
}

// PendingKind classifies a transaction's local PENDING record by how far its
// PREPARE got. A transaction some peer's committed log holds was decided
// COMMITTED, and the log puller resolves this node's PENDING record for it
// by kind: commit a prepared one through the local commit path, discard an
// abandoned begin and replay, and leave a live begin to finish first.
type PendingKind uint8

const (
	// PendingNone: no PENDING record (absent, COMMITTED or ABORTED).
	PendingNone PendingKind = iota
	// PendingBegunLive: begun but not durably prepared, and still tracked by
	// this process - a PREPARE executing right now.
	PendingBegunLive
	// PendingBegunAbandoned: begun but never durably prepared, and not
	// tracked by this process - left by a PREPARE that died with the
	// process (kill -9) before its durable prepare.
	PendingBegunAbandoned
	// PendingPrepared: durably prepared (DurablyPrepareTransaction).
	PendingPrepared
)

// CapturedRowCursor iterates over raw captured rows for a transaction.
// Must call Close() when done to release resources.
type CapturedRowCursor interface {
	// Next advances to the next row. Returns false when iteration is complete.
	Next() bool
	// Row returns the current row's sequence number and data.
	// Only valid after Next() returns true.
	Row() (seq uint64, data []byte)
	// Err returns any error encountered during iteration.
	Err() error
	// Close releases resources held by the cursor.
	Close() error
}

// MetaStore provides transactional metadata storage separate from user data.
// Each user database has its own MetaStore backed by PebbleDB.
// This separation allows user data writes and metadata writes to happen in parallel.
type MetaStore interface {
	// Transaction lifecycle
	BeginTransaction(txnID, nodeID uint64, startTS hlc.Timestamp) error
	DurablyPrepareTransaction(txnID uint64) error
	CommitTransaction(txnID uint64, commitTS hlc.Timestamp, statements []byte, dbName, tablesInvolved string, requiredSchemaVersion uint64, rowCount uint32) error
	AbortTransaction(txnID uint64) error
	GetTransaction(txnID uint64) (*TransactionRecord, error)
	GetPendingTransactions() ([]*TransactionRecord, error)
	Heartbeat(txnID uint64) error

	// PreparedPayload returns what txnID's durable prepare recorded it holds
	// (PreparedPayload); found is false for a transaction with no such record
	// (never durably prepared, or prepared before the record existed).
	PreparedPayload(txnID uint64) (payload PreparedPayload, found bool, err error)

	// ClassifyPending reports how far txnID's local PENDING record got (see
	// PendingKind); PendingNone when there is no PENDING record.
	ClassifyPending(txnID uint64) (PendingKind, error)

	// DiscardAbandonedBegin deletes txnID's PendingBegunAbandoned record with
	// every row lock, intent and captured row it holds. It refuses, with
	// ErrNotAbandonedBegin, a record in any other state.
	DiscardAbandonedBegin(txnID uint64) error

	// StoreReplayedTransaction inserts a fully-committed transaction record directly.
	// Used to record transactions that were replayed (pulled) from another
	// node's log. Unlike CommitTransaction, this doesn't require a prior
	// BeginTransaction call. originNodeID is the transaction's coordinator
	// (persisted as the immutable NodeID), not the replaying node.
	StoreReplayedTransaction(txnID, originNodeID uint64, commitTS hlc.Timestamp, dbName string, rowCount uint32, requiredSchemaVersion uint64) error

	// Write intents (distributed locks)
	WriteIntent(txnID uint64, intentType IntentType, tableName, intentKey string, op OpType, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error
	ValidateIntent(tableName, intentKey string, expectedTxnID uint64) (bool, error)
	DeleteIntent(tableName, intentKey string, txnID uint64) error
	DeleteIntentsByTxn(txnID uint64) error
	GetIntentsByTxn(txnID uint64) ([]*WriteIntentRecord, error)
	GetIntent(tableName, intentKey string) (*WriteIntentRecord, error)

	// GetMaxSeqNum returns the highest seq ever recorded in this store's
	// seq index (LogPosition.Seq), including entries from before this
	// process's own allocations.
	GetMaxSeqNum() (uint64, error)

	// StableSeq returns the highest seq s such that every seq <= s this
	// process has allocated from the store-wide local commit sequence has
	// finished, successfully or not (see logSeqTracker's doc comment in
	// log_position.go for the proof this is safe to read before listing).
	StableSeq() uint64

	// ListCommittedLog returns this store's local log entries strictly
	// after `after`, in position order, restricted to COMMITTED
	// transactions with Seq <= the stable point read at the start of the
	// call, up to limit. more is true when further stable entries exist
	// beyond the returned page.
	ListCommittedLog(after LogPosition, limit int) (entries []LogPosition, stable uint64, more bool, err error)

	// GetPullCursor and SetPullCursor persist C[self,peer,d]: this node's
	// pull position in peerNodeID's log for this database.
	GetPullCursor(peerNodeID uint64) (LogPosition, error)
	SetPullCursor(peerNodeID uint64, pos LogPosition) error

	// SetConsumedPosition stores R[self,requester,d]: the `after` position
	// requesterNodeID last sent when listing this store's log, as is, not
	// as a max. ConsumedPositions returns every requester's last
	// reported position; DeleteConsumedPosition removes a departed member's
	// so it stops pinning GC.
	SetConsumedPosition(requesterNodeID uint64, pos LogPosition) error
	ConsumedPositions() (map[uint64]LogPosition, error)
	DeleteConsumedPosition(nodeID uint64) error

	// TruncatedThrough returns T[self,d]: the highest LogPosition this
	// store's GC has deleted through. It never decreases.
	TruncatedThrough() (LogPosition, error)

	// SetReapplyPending durably records whether this database's local log
	// must be re-applied to its SQLite file (set before a snapshot restore
	// replaces the file, cleared once ReapplyLocalLog succeeds), so a crash
	// between the two never loses a transaction only this node held.
	SetReapplyPending(pending bool) error
	ReapplyPending() (bool, error)

	// GetSchemaVersion is kept only as the migration read for the
	// __marmot_schema_version table now living in each user database's own
	// SQLite file: NewReplicatedDatabase consults it once, through the
	// system database's store, the first time it opens a pre-existing
	// database file. There is no write path any more - UpdateSchemaVersion is
	// removed - and no other production caller should read it.
	GetSchemaVersion(dbName string) (int64, error)
	TryAcquireDDLLock(dbName string, nodeID uint64, leaseDuration time.Duration) (bool, error)
	ReleaseDDLLock(dbName string, nodeID uint64) error

	// CDC intent entries (final processed format)
	GetIntentEntries(txnID uint64) ([]*IntentEntry, error)
	DeleteIntentEntries(txnID uint64) error
	CleanupAfterCommit(txnID uint64) error

	// CDC raw capture (fast path during hook - stores raw values without per-value encoding)
	WriteCapturedRow(txnID, seq uint64, data []byte) error
	SealCapturedRows(txnID uint64) error
	HasCapturedRows(txnID uint64) bool
	IterateCapturedRows(txnID uint64) (CapturedRowCursor, error)
	DeleteCapturedRow(txnID, seq uint64) error
	DeleteCapturedRows(txnID uint64) error

	// CDC active locks for conflict detection
	AcquireCDCRowLock(txnID uint64, tableName, intentKey string) error
	ReleaseCDCRowLock(tableName, intentKey string, txnID uint64) error
	ReleaseCDCRowLocksByTxn(txnID uint64) error
	GetCDCRowLock(tableName, intentKey string) (uint64, error) // Returns txnID or 0 if no lock

	AcquireCDCTableDDLLock(txnID uint64, tableName string) error
	ReleaseCDCTableDDLLock(tableName string, txnID uint64) error
	HasCDCRowLocksForTable(tableName string) (bool, error) // For DDL to check if DML in progress
	GetCDCTableDDLLock(tableName string) (uint64, error)   // Returns txnID or 0 if no lock

	// Stale-transaction GC. StaleTransactionIDs lists the PENDING
	// transactions whose heartbeat is older than timeout.
	// AbortStaleTransaction re-checks one of them and, if it is still PENDING
	// and stale, deletes its intents and captured rows and aborts it,
	// reporting whether it did. The caller must hold the transaction's commit
	// guard (TransactionManager.acquireCommit) across the call, so the abort
	// can never interleave with a commit of the same transaction.
	StaleTransactionIDs(timeout time.Duration) ([]uint64, error)
	AbortStaleTransaction(txnID uint64, timeout time.Duration) (bool, error)

	// CleanupOldTransactionRecords deletes committed log entries whose
	// position is <= safe and whose CommittedAt is older than minRetention,
	// or whose CommittedAt is older than maxRetention regardless of safe.
	// It only ever deletes a prefix of the log in position order (see the
	// implementation's doc comment) and advances TruncatedThrough to the
	// highest position it deleted.
	CleanupOldTransactionRecords(minRetention, maxRetention time.Duration, safe LogPosition) (int, error)

	// Aggregation queries for anti-entropy
	GetMaxCommittedTxnID() (uint64, error)
	GetCommittedTxnCount() (int64, error)

	// Streaming for anti-entropy delta sync
	StreamCommittedTransactions(fromTxnID uint64, callback func(*TransactionRecord) error) error

	// ScanTransactions iterates transactions from fromTxnID.
	// If descending is true, scans from newest to oldest.
	// Callback returns nil to continue, ErrStopIteration to stop, or other error to abort.
	ScanTransactions(fromTxnID uint64, descending bool, callback func(*TransactionRecord) error) error

	// Stats methods for telemetry
	GetRowLockStats() (activeLocks, activeTransactions, tablesWithLocks int)
	IntentStats() (pendingIntents int, err error)

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
	DatabaseName          string
	RequiredSchemaVersion uint64 // Minimum schema version required for this transaction
	RowCount              uint32 // Number of captured rows for this transaction (0 until committed)
}

// TxnImmutableRecord contains fields set once at transaction start (never modified)
type TxnImmutableRecord struct {
	TxnID          uint64
	NodeID         uint64
	StartTSWall    int64
	StartTSLogical int32
	CreatedAt      int64
}

// TxnCommitRecord contains commit-time data (written once at commit, never modified)
type TxnCommitRecord struct {
	SeqNum                uint64
	CommitTSWall          int64
	CommitTSLogical       int32
	CommittedAt           int64
	TablesInvolved        string
	DatabaseName          string
	RequiredSchemaVersion uint64
	RowCount              uint32 // Number of captured rows for this transaction
}

// WriteIntentRecord represents a write intent in meta store
type WriteIntentRecord struct {
	IntentType   IntentType // Type discriminator: DML, DDL, or DatabaseOp
	TableName    string
	IntentKey    []byte
	TxnID        uint64
	TSWall       int64
	TSLogical    int32
	NodeID       uint64
	Operation    OpType
	SQLStatement string
	DataSnapshot []byte
	CreatedAt    int64
}

// IntentLock is a lightweight lock stored at /intent/{table}/{key}.
// Contains only fields needed for lock validation - full data is in /intent_by_txn/.
type IntentLock struct {
	TxnID uint64
}

// NewMetaStore creates a MemoryMetaStore (which wraps PebbleMetaStore).
// basePath is the path to the user database (e.g., "/data/mydb.db").
// Creates {basePath}_meta.pebble/ directory
func NewMetaStore(basePath string) (MetaStore, error) {
	metaPath := strings.TrimSuffix(basePath, ".db") + "_meta.pebble"

	if cfg.Config != nil && !filepath.IsAbs(metaPath) {
		metaPath = filepath.Join(cfg.Config.DataDir, metaPath)
	}

	// Create PebbleMetaStore
	pebble, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	if err != nil {
		return nil, err
	}

	// Wrap with MemoryMetaStore for transitionary state optimization
	memStore := NewMemoryMetaStore(pebble)

	// Register recovered prepared transactions and clean up orphaned CDC data
	// from crashed ones. The registration cannot fail; only the cleanup can.
	if err := memStore.ReconstructFromPebble(); err != nil {
		// Log warning but don't fail startup - orphaned data is just wasted space
		log.Warn().Err(err).Msg("Failed to reconstruct from Pebble at startup")
	}

	return memStore, nil
}
