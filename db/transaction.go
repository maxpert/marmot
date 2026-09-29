package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// Transaction represents a distributed transaction
// Implements Percolator-style distributed transactions with write intents
// Note: Write intents are stored ONLY in MetaStore (not in memory) for durability
// and to ensure cleanup happens correctly even after crashes or partial failures.
type Transaction struct {
	ID                    uint64
	NodeID                uint64
	StartTS               hlc.Timestamp
	CommitTS              hlc.Timestamp
	Status                TxnStatus
	Statements            []protocol.Statement
	RequiredSchemaVersion uint64 // Minimum schema version required for this transaction
	mu                    sync.RWMutex
}

// GCSafePositionFunc returns the highest LogPosition below which GC may
// delete committed log entries (the min of every current member's
// consumed position, R[self,m,d], in this database's log), and whether
// that position is known yet. Absent, or returning false, GC treats
// nothing as safe except entries past max retention.
type GCSafePositionFunc func() (LogPosition, bool)

type VectorCDCNotifier interface {
	ApplyCommittedVectorCDC(ctx context.Context, database string, txnID, seqNum uint64, entries []common.CDCEntry) error
}

// TransactionManager manages distributed transactions
// All transaction state is stored in MetaStore (PebbleDB) - no in-memory caching
type TransactionManager struct {
	db                  *sql.DB   // User database for data operations
	metaStore           MetaStore // MetaStore for transaction metadata
	clock               *hlc.Clock
	schemaCache         *SchemaCache // Schema cache for table metadata
	mu                  sync.RWMutex
	gcInterval          time.Duration
	gcThreshold         time.Duration
	gcMinRetention      time.Duration // Minimum retention for replication
	gcMaxRetention      time.Duration // Force GC after this duration
	heartbeatTimeout    time.Duration
	stopGC              chan struct{}
	gcDone              chan struct{}
	gcRunning           bool
	databaseName        string                     // Name of database this manager manages
	gcSafePosition      GCSafePositionFunc         // Callback for GC's safe deletion position
	memberCount         func() int                 // Current member count, for tombstone GC (SetMemberCountFunc)
	batchCommitter      *SQLiteBatchCommitter      // SQLite write batcher (nil if disabled)
	notifier            CDCNotifier                // Injected, can be nil
	vectorCDCNotifier   VectorCDCNotifier          // Injected, can be nil
	autoIncClaimStore   *AutoIncClaimStore         // AUTO_INCREMENT claim store; always backed by the system database
	schemaVersionBumped func(uint64)               // Injected; told the new value whenever a non-DML commit bumps __marmot_schema_version
	appliedMarker       func(uint64) (bool, error) // Injected; reports whether __marmot_applied_txn holds a txn id (SetAppliedMarkerCheck)

	// commitInProgress serializes CommitTransaction across *Transaction
	// objects that name the same txn id: txn id -> a channel closed when
	// that id's commit attempt ends. A locally PREPARED transaction the log
	// puller commits through the normal 2PC path can then never race a COMMIT
	// RPC for the same id into a double apply. See CommitTransaction.
	commitInProgress sync.Map

	// txnIDLocks gates BeginTransactionWithID and
	// ReplicatedDatabase.LogReplayedTxn against each other for the same txn
	// id, so a local PREPARE's begin and a replay of the same txn id
	// can never interleave. See lockTxnID's doc comment.
	txnIDLocks [txnIDLockStripes]sync.Mutex

	// failBeforeFinalizeForTest stops CommitTransaction after the data and
	// marker are committed and before the commit record: the crash window a
	// test drives. Only tests set it.
	failBeforeFinalizeForTest bool
}

// errInjectedCrashBeforeFinalize is what CommitTransaction returns when a
// test injects a crash before the commit record (failBeforeFinalizeForTest).
var errInjectedCrashBeforeFinalize = errors.New("injected crash before the commit record")

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(db *sql.DB, metaStore MetaStore, clock *hlc.Clock, schemaCache *SchemaCache) *TransactionManager {
	// Import config values (with fallback to defaults if config not loaded)
	gcInterval := 60 * time.Second // Default: 60s (MUST be >= anti-entropy interval for fresh watermarks)
	gcThreshold := 1 * time.Hour
	gcMinRetention := 1 * time.Hour
	gcMaxRetention := 4 * time.Hour
	heartbeatTimeout := 10 * time.Second

	// Try to use config values if available
	if cfg.Config != nil {
		heartbeatTimeout = time.Duration(cfg.Config.Transaction.HeartbeatTimeoutSeconds) * time.Second
		gcMinRetention = time.Duration(cfg.Config.Replication.GCMinRetentionHours) * time.Hour
		gcMaxRetention = time.Duration(cfg.Config.Replication.GCMaxRetentionHours) * time.Hour
		gcInterval = time.Duration(cfg.Config.Replication.GCIntervalS) * time.Second
	}

	tm := &TransactionManager{
		db:               db,
		metaStore:        metaStore,
		clock:            clock,
		schemaCache:      schemaCache,
		gcInterval:       gcInterval,
		gcThreshold:      gcThreshold,
		gcMinRetention:   gcMinRetention,
		gcMaxRetention:   gcMaxRetention,
		heartbeatTimeout: heartbeatTimeout,
		stopGC:           make(chan struct{}),
		gcRunning:        false,
		databaseName:     "", // Set later via SetDatabaseName()
	}

	// Start background garbage collection
	tm.StartGarbageCollection()

	return tm
}

// SetDatabaseName sets the database name for this transaction manager
// Used for GC coordination across peers
func (tm *TransactionManager) SetDatabaseName(name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.databaseName = name
}

// SetGCSafePositionFunc sets the callback GC uses to learn the safe
// deletion position. It replaces SetMinAppliedTxnIDFunc,
// SetClusterMinWatermarkFunc and SetRefreshReplicationStatesFunc, whose
// txn-id/seq-num watermarks could not tell GC which log positions every
// member had pulled (see CleanupOldTransactionRecords).
func (tm *TransactionManager) SetGCSafePositionFunc(fn GCSafePositionFunc) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.gcSafePosition = fn
}

// SetMemberCountFunc sets the callback tombstone GC (purgeTombstones) uses
// to learn how many members the cluster has.
func (tm *TransactionManager) SetMemberCountFunc(fn func() int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.memberCount = fn
}

// SetNotifier sets the CDC notifier for signaling after commits
func (tm *TransactionManager) SetNotifier(n CDCNotifier) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.notifier = n
}

// batchCommitEnabled returns true if batch committing is enabled and configured
func (tm *TransactionManager) batchCommitEnabled() bool {
	return tm.batchCommitter != nil
}

// SetBatchCommitter sets the batch committer (called by ReplicatedDatabase)
func (tm *TransactionManager) SetBatchCommitter(bc *SQLiteBatchCommitter) {
	if bc != nil {
		bc.SetSealCapturedRows(tm.metaStore.SealCapturedRows)
	}
	tm.batchCommitter = bc
}

func (tm *TransactionManager) SetVectorCDCNotifier(notifier VectorCDCNotifier) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.vectorCDCNotifier = notifier
}

// VectorCDCNotifier returns the injected vector CDC notifier, or nil if none
// is configured. Used by db/replay_apply.go to apply vector-control rows and
// committed vector CDC for a replayed transaction.
func (tm *TransactionManager) VectorCDCNotifier() VectorCDCNotifier {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.vectorCDCNotifier
}

// SetSchemaVersionBumped injects the callback applyNonDMLIntents notifies,
// with the new value, whenever it bumps this database's
// __marmot_schema_version. Wired by NewReplicatedDatabase to update
// its own cached atomic; nil is fine and just means nothing is told.
func (tm *TransactionManager) SetSchemaVersionBumped(fn func(uint64)) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.schemaVersionBumped = fn
}

// SetAutoIncClaimStore injects the AUTO_INCREMENT claim store used by
// seedAutoIncBasesForDDL (db/autoinc_seed.go). The store always backs onto
// the system database, never onto tm's own db; DatabaseManager.wireGCCoordination
// wires the same store instance into every TransactionManager, user and
// system alike (db/database_manager.go).
func (tm *TransactionManager) SetAutoIncClaimStore(s *AutoIncClaimStore) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.autoIncClaimStore = s
}

// autoIncClaimStoreAndDatabaseName returns the injected claim store and this
// manager's database name under a single read lock, for callers (currently
// only seedAutoIncBasesForDDL) that need both together.
func (tm *TransactionManager) autoIncClaimStoreAndDatabaseName() (*AutoIncClaimStore, string) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.autoIncClaimStore, tm.databaseName
}

// autoIncIncarnationEnded tells the claim store's listener that a DDL
// statement ended the incarnation of table in this manager's database
// (AutoIncIncarnationListener).
func (tm *TransactionManager) autoIncIncarnationEnded(table string) {
	if claimStore, dbName := tm.autoIncClaimStoreAndDatabaseName(); claimStore != nil {
		claimStore.tableIncarnationEnded(dbName, table)
	}
}

// BeginTransaction starts a new distributed transaction with auto-generated ID
func (tm *TransactionManager) BeginTransaction(nodeID uint64) (*Transaction, error) {
	startTS := tm.clock.Now()

	// Generate unique transaction ID using Percolator/TiDB pattern: (physical_ms << 18) | logical
	// This guarantees uniqueness by keeping physical and logical in separate bit ranges
	txnID := startTS.ToTxnID()

	return tm.BeginTransactionWithID(txnID, nodeID, startTS)
}

// txnIDLockStripes is the stripe count for TransactionManager.txnIDLocks: a
// power of two so a stripe index is a cheap mask, sized well above the
// expected number of transactions racing this window concurrently.
const txnIDLockStripes = 64

// lockTxnID acquires the stripe of txnIDLocks that guards txnID and returns
// a function that releases it. Held by BeginTransactionWithID around
// tm.metaStore.BeginTransaction, and by ReplicatedDatabase.LogReplayedTxn
// across its whole body, so a local PREPARE's begin can never interleave
// with a replay of the same txn id: either the replay logs the txn
// completely first - LogReplayedTxn's own COMMITTED check then makes the
// later begin observe it - or the begin runs first and the replay's status
// check (or its caller's PENDING refusal) resolves the race instead.
func (tm *TransactionManager) lockTxnID(txnID uint64) (unlock func()) {
	stripe := &tm.txnIDLocks[txnID%txnIDLockStripes]
	stripe.Lock()
	return stripe.Unlock
}

// ErrTxnAlreadyCommitted refuses to begin a transaction id that this node
// already holds COMMITTED or already applied (see BeginTransactionWithID).
var ErrTxnAlreadyCommitted = errors.New("transaction already committed on this node")

// SetAppliedMarkerCheck installs the read-only lookup of the database's
// applied-txn marker that BeginTransactionWithID consults. The owning
// ReplicatedDatabase installs one reading through its read pool, so the check
// never waits on the single SQLite writer.
func (tm *TransactionManager) SetAppliedMarkerCheck(check func(txnID uint64) (bool, error)) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.appliedMarker = check
}

// refuseCommittedTxnID returns ErrTxnAlreadyCommitted when txnID's local
// record is COMMITTED or, lacking a record, its applied-txn marker exists.
func (tm *TransactionManager) refuseCommittedTxnID(txnID uint64) error {
	rec, err := tm.metaStore.GetTransaction(txnID)
	if err != nil {
		return fmt.Errorf("check transaction %d status: %w", txnID, err)
	}
	if rec != nil {
		if rec.Status == TxnStatusCommitted {
			return fmt.Errorf("transaction %d: %w", txnID, ErrTxnAlreadyCommitted)
		}
		return nil
	}
	tm.mu.RLock()
	check := tm.appliedMarker
	tm.mu.RUnlock()
	if check == nil {
		return nil
	}
	applied, err := check(txnID)
	if err != nil {
		return fmt.Errorf("check transaction %d applied marker: %w", txnID, err)
	}
	if applied {
		return fmt.Errorf("transaction %d: %w", txnID, ErrTxnAlreadyCommitted)
	}
	return nil
}

// BeginTransactionWithID starts a distributed transaction with a specific ID
// Used by coordinator replication to ensure consistent txn_id across cluster
// Transaction state is persisted to MetaStore only - no in-memory caching
//
// A txn id this node already holds COMMITTED, or whose applied-txn marker its
// database already carries, is refused with ErrTxnAlreadyCommitted: it was
// pulled from a peer's log (or arrived in a restored snapshot) before this
// late PREPARE, and beginning it again would overwrite the committed record
// with a PENDING one whose abort or commit would corrupt the log entry.
func (tm *TransactionManager) BeginTransactionWithID(txnID, nodeID uint64, startTS hlc.Timestamp) (*Transaction, error) {
	unlock := tm.lockTxnID(txnID)
	err := tm.refuseCommittedTxnID(txnID)
	if err == nil {
		if err = tm.metaStore.BeginTransaction(txnID, nodeID, startTS); err != nil {
			err = fmt.Errorf("failed to create transaction record: %w", err)
		}
	}
	unlock()
	if err != nil {
		return nil, err
	}

	// Return a transient object for use during this request
	// Actual state is in MetaStore
	txn := &Transaction{
		ID:         txnID,
		NodeID:     nodeID,
		StartTS:    startTS,
		Status:     TxnStatusPending,
		Statements: make([]protocol.Statement, 0),
	}

	return txn, nil
}

// ValidateDDL checks that the given DDL statements can be applied to this node's
// database without leaving any side effects. Called during PREPARE so a node
// never promises to commit DDL that SQLite would reject at COMMIT time.
func (tm *TransactionManager) ValidateDDL(ctx context.Context, statements []string) error {
	return ValidateDDLStatements(ctx, tm.db, statements)
}

// AddStatement adds a statement to the transaction buffer
func (tm *TransactionManager) AddStatement(txn *Transaction, stmt protocol.Statement) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending (status: %s)", txn.ID, txn.Status)
	}

	txn.Statements = append(txn.Statements, stmt)
	return nil
}

// WriteIntent creates a write intent for a row
// This is the CRITICAL part: write intents act as distributed locks
// Intents are stored ONLY in MetaStore (not in memory) for durability
func (tm *TransactionManager) WriteIntent(txn *Transaction, intentType IntentType, tableName, intentKey string,
	stmt protocol.Statement, dataSnapshot []byte) error {

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	// Only DML intents need OpType conversion - DDL uses SQL statement directly
	var op OpType
	if intentType == IntentTypeDML {
		op = StatementTypeToOpType(stmt.Type)
	} else {
		op = OpTypeInsert // Placeholder for non-DML intents (DDL uses SQLStatement field)
	}

	// Persist the intent directly to MetaStore (durable storage)
	err := tm.metaStore.WriteIntent(txn.ID, intentType, tableName, intentKey,
		op, stmt.SQL, dataSnapshot, txn.StartTS, txn.NodeID)

	if err != nil {
		return fmt.Errorf("failed to persist write intent: %w", err)
	}

	return nil
}

// CommitTransaction commits the transaction.
// DML: Get CDC entries → apply via batch committer → cleanup
// DDL: Flush pending DML → get intents → apply DDL → cleanup
//
// Concurrent-commit guard: commitInProgress serializes commits of the same
// txn id across distinct *Transaction objects (txn.mu only guards one
// object, not the id) - the log puller commits a locally prepared
// transaction through this path (DatabaseManager.CommitLocallyPrepared),
// which can race the COMMIT RPC, or the coordinator's own local commit, for
// the same id. A second caller waits for the first to finish, then re-reads
// the authoritative status from MetaStore - not just txn.Status, which an
// independently reconstructed *Transaction object still shows as PENDING -
// and gets ErrTxnAlreadyCommitted, without redoing any of the apply, if the
// first committed it (MarkSQLiteTxnApplied is INSERT OR IGNORE, so its own
// idempotence would otherwise mask a duplicate finalizeCommit writing a
// second log entry).
func (tm *TransactionManager) CommitTransaction(txn *Transaction) error {
	return tm.CommitTransactionAfter(txn, hlc.Timestamp{}, nil)
}

// CommitTransactionAfter is CommitTransaction with before run first, under
// the concurrent-commit guard and after the status check, so work that must
// happen exactly once before the commit (applying an AUTO_INCREMENT claim)
// never runs for a transaction another caller already committed. An error
// from before aborts the commit and is returned as is.
//
// commitTS is the commit timestamp the transaction's coordinator decided,
// the same on every node; every row the commit writes is versioned with it.
// A zero commitTS (a coordinator or peer too old to send one) falls back to
// a timestamp computed here, as every commit did before.
func (tm *TransactionManager) CommitTransactionAfter(txn *Transaction, commitTS hlc.Timestamp, before func() error) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	release, err := tm.acquireCommit(txn.ID)
	if err != nil {
		return err
	}
	defer release()

	if before != nil {
		if err := before(); err != nil {
			return err
		}
	}

	// Get CDC entries - determines DML vs DDL path
	cdcEntries, err := tm.metaStore.GetIntentEntries(txn.ID)
	if err != nil {
		return fmt.Errorf("failed to load CDC entries: %w", err)
	}
	if err := tm.verifyPreparedPayload(txn.ID, len(cdcEntries)); err != nil {
		return err
	}

	txn.CommitTS = tm.resolveCommitTS(txn, commitTS)

	if len(cdcEntries) > 0 {
		// DML path: apply CDC entries
		if tm.batchCommitEnabled() {
			txn.Statements = tm.rebuildStatementsFromCDC(cdcEntries, nil)
			fut := tm.batchCommitter.Enqueue(txn.ID, txn.CommitTS, cdcEntries, txn.Statements)
			if _, err := fut.Get(); err != nil {
				return fmt.Errorf("batch commit failed: %w", err)
			}
		} else {
			if err := tm.metaStore.SealCapturedRows(txn.ID); err != nil {
				return fmt.Errorf("failed to seal CDC rows: %w", err)
			}
			if err := tm.applyCDCEntries(txn.ID, txn.CommitTS, cdcEntries); err != nil {
				return err
			}
			txn.Statements = tm.rebuildStatementsFromCDC(cdcEntries, nil)
		}
	} else {
		if statementsNamePreparedRows(txn.Statements) {
			return fmt.Errorf("transaction %d: %w", txn.ID, ErrPreparedRowsMissing)
		}

		intents, err := tm.metaStore.GetIntentsByTxn(txn.ID)
		if err != nil {
			return fmt.Errorf("failed to fetch write intents: %w", err)
		}

		// Statement/DDL path: flush pending DML first to ensure isolation. A
		// claim-only commit writes nothing to this database
		// (applyNonDMLIntents), so it must not wait for queued DML: that DML
		// may be waiting for the writer an explicit transaction holds while
		// that transaction waits for this very claim.
		if tm.batchCommitEnabled() && len(classifyNonDMLIntents(intents)) > 0 {
			tm.batchCommitter.Flush()
		}

		// The captured rows are written before the SQLite commit that writes
		// the marker: a crash after that commit then leaves rows for
		// repairAppliedTxnMetadata to finish the commit record from at the
		// next open, so the applied DDL still enters this node's log.
		if err := tm.writeNonDMLToCDC(txn.ID, intents); err != nil {
			return err
		}
		if err := tm.metaStore.SealCapturedRows(txn.ID); err != nil {
			return fmt.Errorf("failed to seal CDC rows: %w", err)
		}
		if err := tm.applyNonDMLIntents(txn.ID, txn.CommitTS, intents); err != nil {
			return err
		}
		txn.Statements = tm.rebuildStatementsFromCDC(nil, intents)
	}

	if tm.failBeforeFinalizeForTest {
		return errInjectedCrashBeforeFinalize
	}

	// Finalize commit in MetaStore
	if err := tm.finalizeCommit(txn); err != nil {
		return err
	}

	if len(cdcEntries) > 0 {
		tm.notifyVectorCDC(txn, cdcEntries)
	}

	// Signal CDC subscribers that new data is available
	if tm.notifier != nil {
		tm.notifier.Signal(tm.databaseName, txn.ID)
	}

	// Cleanup
	tm.cleanupAfterCommit(txn)

	return nil
}

// acquireCommit takes txnID's commitInProgress entry, waiting for any
// commit of the same id already under way to finish first, and re-reads its
// authoritative status (see CommitTransaction's concurrent-commit guard):
// ErrTxnAlreadyCommitted if it is COMMITTED, an error if it is otherwise not
// PENDING. The caller runs release once its commit attempt ends.
func (tm *TransactionManager) acquireCommit(txnID uint64) (release func(), err error) {
	done := make(chan struct{})
	for {
		prev, busy := tm.commitInProgress.LoadOrStore(txnID, done)
		if !busy {
			break
		}
		<-prev.(chan struct{})
	}
	release = func() {
		tm.commitInProgress.Delete(txnID)
		close(done)
	}
	rec, err := tm.metaStore.GetTransaction(txnID)
	switch {
	case err != nil:
		err = fmt.Errorf("read transaction %d status: %w", txnID, err)
	case rec != nil && rec.Status == TxnStatusCommitted:
		err = fmt.Errorf("transaction %d: %w", txnID, ErrTxnAlreadyCommitted)
	case rec == nil || rec.Status != TxnStatusPending:
		err = fmt.Errorf("transaction %d is not pending", txnID)
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// ErrPreparedRowsMissing refuses the COMMIT of a transaction whose prepared
// payload is gone: DML rows its statements name, or fewer captured rows or
// persisted intents than its durable prepare recorded (PreparedPayload).
// Every such row and intent was written at PREPARE, so finding fewer means
// something discarded them - a stale-transaction abort, or a recovery that
// dropped them - and committing would ACK a write this node never applied.
var ErrPreparedRowsMissing = errors.New("prepared DML rows are gone; refusing to commit nothing")

// verifyPreparedPayload refuses, with ErrPreparedRowsMissing, a commit of
// txnID when what it finds - rows captured rows and its persisted intents -
// differs from what its durable prepare recorded. A transaction with no such
// record (never durably prepared) is not checked. The check reads the
// durable record, not the Statements of the *Transaction being committed,
// which a commit that reconstructed the transaction (the log puller, a
// COMMIT RPC) never has.
func (tm *TransactionManager) verifyPreparedPayload(txnID uint64, rows int) error {
	payload, found, err := tm.metaStore.PreparedPayload(txnID)
	if err != nil {
		return fmt.Errorf("read prepared payload of transaction %d: %w", txnID, err)
	}
	if !found {
		return nil
	}
	if uint64(rows) != payload.Rows {
		return fmt.Errorf("transaction %d: %w: prepared %d captured rows, found %d",
			txnID, ErrPreparedRowsMissing, payload.Rows, rows)
	}
	if payload.Intents == 0 {
		return nil
	}
	intents, err := tm.metaStore.GetIntentsByTxn(txnID)
	if err != nil {
		return fmt.Errorf("failed to fetch write intents: %w", err)
	}
	var persisted uint64
	for _, intent := range intents {
		if intent.IntentType != IntentTypeDML {
			persisted++
		}
	}
	if persisted != payload.Intents {
		return fmt.Errorf("transaction %d: %w: prepared %d intents, found %d",
			txnID, ErrPreparedRowsMissing, payload.Intents, persisted)
	}
	return nil
}

// statementsNamePreparedRows reports whether statements carry a DML row that
// PREPARE captured: a row change with an intent key. An INSERT without one
// (an auto-increment key the CDC hook assigns) and an AUTO_INCREMENT claim
// capture nothing at PREPARE.
func statementsNamePreparedRows(statements []protocol.Statement) bool {
	for _, stmt := range statements {
		if protocol.IsDML(stmt) && len(stmt.IntentKey) > 0 && !stmt.AutoIDClaim {
			return true
		}
	}
	return false
}

func (tm *TransactionManager) notifyVectorCDC(txn *Transaction, entries []*IntentEntry) {
	tm.mu.RLock()
	notifier := tm.vectorCDCNotifier
	dbName := tm.databaseName
	tm.mu.RUnlock()
	if notifier == nil || len(entries) == 0 {
		return
	}
	if dbName == "" && len(txn.Statements) > 0 {
		dbName = txn.Statements[0].Database
	}
	rec, err := tm.metaStore.GetTransaction(txn.ID)
	var seqNum uint64
	if err == nil && rec != nil {
		seqNum = rec.SeqNum
	}
	cdcEntries := make([]common.CDCEntry, 0, len(entries))
	for _, entry := range entries {
		cdcEntries = append(cdcEntries, common.CDCEntry{
			Table:        entry.Table,
			IntentKey:    entry.IntentKey,
			Operation:    entry.Operation,
			OldValues:    entry.OldValues,
			NewValues:    entry.NewValues,
			EncodedRow:   entry.EncodedRow,
			EncodedCodec: entry.EncodedCodec,
			CommitTxnID:  txn.ID,
			CommitSeqNum: seqNum,
		})
	}
	if err := notifier.ApplyCommittedVectorCDC(context.Background(), dbName, txn.ID, seqNum, cdcEntries); err != nil {
		log.Error().Err(err).Uint64("txn_id", txn.ID).Str("database", dbName).Msg("Failed to apply committed vector CDC")
	}
}

// resolveCommitTS returns txn's commit timestamp: decided, the
// coordinator's, merged into this node's clock so any transaction this node
// prepares later is ordered after it, or, when decided is zero,
// calculateCommitTS. Its NodeID is always txn's origin (its coordinator),
// the tie-break every node applying txn uses for its row versions.
func (tm *TransactionManager) resolveCommitTS(txn *Transaction, decided hlc.Timestamp) hlc.Timestamp {
	commitTS := decided
	if decided.IsZero() {
		commitTS = tm.calculateCommitTS(txn.StartTS)
	} else {
		tm.clock.Update(decided)
	}
	commitTS.NodeID = txn.NodeID
	return commitTS
}

// calculateCommitTS determines the commit timestamp (must be > start_ts).
func (tm *TransactionManager) calculateCommitTS(startTS hlc.Timestamp) hlc.Timestamp {
	commitTS := tm.clock.Now()
	if hlc.Compare(commitTS, startTS) <= 0 {
		commitTS = tm.clock.Update(startTS)
		commitTS.Logical++
	}
	return commitTS
}

// rebuildStatementsFromCDC reconstructs protocol.Statement slice from CDC entries
// and non-DML intents for in-memory transaction tracking.
func (tm *TransactionManager) rebuildStatementsFromCDC(cdcEntries []*IntentEntry, intents []*WriteIntentRecord) []protocol.Statement {
	statements := make([]protocol.Statement, 0, len(cdcEntries)+len(intents))

	// Add DML statements from CDC entries
	for _, entry := range cdcEntries {
		stmt := protocol.Statement{
			TableName:    entry.Table,
			IntentKey:    entry.IntentKey,
			OldValues:    entry.OldValues,
			NewValues:    entry.NewValues,
			Operation:    entry.Operation,
			EncodedRow:   entry.EncodedRow,
			EncodedCodec: entry.EncodedCodec,
			Type:         OpTypeToStatementType(OpType(entry.Operation)),
		}
		statements = append(statements, stmt)
	}

	// Add non-DML statements from intents (only if no CDC entries)
	if len(cdcEntries) == 0 {
		// Filter and sort non-DML intents by CreatedAt to preserve execution order.
		nonDMLIntents := make([]*WriteIntentRecord, 0, len(intents))
		for _, intent := range intents {
			if intent.IntentType == IntentTypeDDL && (intent.SQLStatement != "" || len(intent.DataSnapshot) > 0) {
				nonDMLIntents = append(nonDMLIntents, intent)
			}
		}

		// Sort by CreatedAt to ensure deterministic execution order.
		sort.Slice(nonDMLIntents, func(i, j int) bool {
			return nonDMLIntents[i].CreatedAt < nonDMLIntents[j].CreatedAt
		})

		for _, intent := range nonDMLIntents {
			var vectorChange common.VectorIndexChange
			if err := DeserializeData(intent.DataSnapshot, &vectorChange); err == nil && vectorChange.Action != 0 {
				statements = append(statements, vectorIndexStatementFromChange(vectorChange))
				continue
			}
			stmt := protocol.Statement{
				TableName: intent.TableName,
				SQL:       intent.SQLStatement,
				Type:      protocol.StatementDDL,
			}
			var loadSnap LoadDataSnapshot
			if err := DeserializeData(intent.DataSnapshot, &loadSnap); err == nil && loadSnap.Type == int(protocol.StatementLoadData) {
				stmt.Type = protocol.StatementLoadData
				stmt.LoadDataPayload = loadSnap.Data
			}
			statements = append(statements, stmt)
		}
	}

	return statements
}

// applyCDCEntries applies CDC data entries to SQLite within a single transaction.
func (tm *TransactionManager) applyCDCEntries(txnID uint64, commitTS hlc.Timestamp, entries []*IntentEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Begin SQLite transaction to batch all DML writes
	tx, err := tm.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin SQLite transaction: %w", err)
	}
	defer tx.Rollback()

	applier, err := newVersionedApplier(tx, &schemaCacheAdapter{cache: tm.schemaCache})
	if err != nil {
		return err
	}
	defer applier.Close()

	for _, entry := range entries {
		if err := applier.applyEntry(entry, commitTS); err != nil {
			return fmt.Errorf("failed to write CDC data for %s: %w", entry.Table, err)
		}
	}
	if err := MarkSQLiteTxnApplied(tx, txnID, commitTS); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit CDC transaction: %w", err)
	}

	return nil
}

// nonDMLIntentKind classifies one non-DML write intent by what its data
// snapshot decodes as: vector-control metadata, a LOAD DATA payload, or (the
// default) a raw DDL statement.
type nonDMLIntentKind struct {
	intent *WriteIntentRecord
	vector *common.VectorIndexChange
	load   *LoadDataSnapshot
}

// classifyNonDMLIntents filters intents down to the DDL-carrying ones (the
// same filter applyNonDMLIntents has always used) and decodes each one's
// kind, in CreatedAt order.
func classifyNonDMLIntents(intents []*WriteIntentRecord) []nonDMLIntentKind {
	filtered := make([]*WriteIntentRecord, 0, len(intents))
	for _, intent := range intents {
		if intent.IntentType == IntentTypeDDL && (intent.SQLStatement != "" || len(intent.DataSnapshot) > 0) {
			filtered = append(filtered, intent)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt < filtered[j].CreatedAt
	})

	kinds := make([]nonDMLIntentKind, 0, len(filtered))
	for _, intent := range filtered {
		var vectorChange common.VectorIndexChange
		if err := DeserializeData(intent.DataSnapshot, &vectorChange); err == nil && vectorChange.Action != 0 {
			kinds = append(kinds, nonDMLIntentKind{intent: intent, vector: &vectorChange})
			continue
		}
		var loadSnap LoadDataSnapshot
		if err := DeserializeData(intent.DataSnapshot, &loadSnap); err == nil && loadSnap.Type == int(protocol.StatementLoadData) {
			kinds = append(kinds, nonDMLIntentKind{intent: intent, load: &loadSnap})
			continue
		}
		kinds = append(kinds, nonDMLIntentKind{intent: intent})
	}
	return kinds
}

// applyNonDMLIntents applies a non-DML transaction's DDL, LOAD DATA and
// vector-control intents, atomically with the applied-txn marker and (when
// the txn carries real DDL) the schema-version bump.
//
// Exactly-once / idempotence, per effect:
//   - DDL, LOAD DATA, the schema-version bump and the marker all commit in one
//     SQLite transaction: a crash anywhere before commit leaves none of them
//     applied, and replay (db/replay_apply.go) sees no marker and reapplies
//     the whole txn from scratch. A crash after commit is a normal completed
//     commit.
//   - Vector control (CREATE/DROP/CHECKPOINT/REINDEX on __marmot_vector_indexes)
//     is not SQLite and cannot share that transaction, so it is applied first,
//     outside it. CREATE, DROP and CHECKPOINT are no-ops on repeat and REINDEX
//     is a repeatable rebuild (VectorIndexManager.ApplyVectorControl), so a
//     crash between applying the control and committing the marker tx just
//     re-runs it once on the next apply - replay is gated on the marker
//     before the control runs (db/replay_apply.go).
//   - A claim-only txn (an AUTO_INCREMENT range claim, which has no DDL/LOAD
//     DATA/vector intents at all) writes NO marker here and opens no SQLite
//     tx at all, as explained below.
//
// A claim-only commit writes no marker because it cannot take the user
// database's writer: ReplicationEngine.Commit calls this after
// AutoIncClaimStore.ApplyClaims for every claim-carrying txn, including the
// LLDAP shape where a pinned session already holds the user database's one
// SQLite writer (_txlock=immediate) for the whole surrounding transaction.
// __marmot_applied_txn lives in that same user database file, so writing it
// needs that same writer - which would self-deadlock the claim behind the
// pinned session that is waiting on it, defeating the entire reason
// AutoIncClaimStore lives in the system database (a separate file with its
// own writer) rather than the user database
// (TestClaimCompletesWhilePinnedSessionOpen pins this).
//
// Skipping it is safe: a claim-only txn still gets a local log entry
// (finalizeCommit writes its commit record), so a peer's anti-entropy, and
// this node's own restore re-apply, will find it without a marker and replay
// it. That replay is a zero-row ReplayTxn: ApplyReplayedTxn only writes the
// marker, off this commit path, and never re-applies the claim - claim
// payloads never travel in CDC, and a bystander's floor comes from the claim
// protocol's merged base (AutoIncHoldTable, grpc.RunAutoIncBaseMerge), not
// from replay. The marker is therefore bookkeeping for the pull diff only,
// and writing it later, once, is equivalent to writing it here.
func (tm *TransactionManager) applyNonDMLIntents(txnID uint64, commitTS hlc.Timestamp, intents []*WriteIntentRecord) error {
	kinds := classifyNonDMLIntents(intents)
	if len(kinds) == 0 {
		// Claim-only (or otherwise empty): no marker, no SQLite tx. See the
		// doc comment for why.
		return nil
	}

	if err := tm.applyNonDMLVectorControl(kinds); err != nil {
		return err
	}

	ctx := context.Background()
	tx, err := tm.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin non-DML transaction: %w", err)
	}
	defer tx.Rollback()

	change, hasDDL, err := tm.execNonDMLIntentsInTx(ctx, tx, txnID, commitTS, kinds)
	if err != nil {
		return err
	}

	var newSchemaVersion uint64
	if hasDDL {
		newSchemaVersion, err = bumpSchemaVersionInTx(tx)
		if err != nil {
			return fmt.Errorf("failed to bump schema version (txn %d): %w", txnID, err)
		}
	}

	if err := MarkSQLiteTxnApplied(tx, txnID, commitTS); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit non-DML transaction: %w", err)
	}

	return tm.afterNonDMLCommit(txnID, hasDDL, newSchemaVersion, &change)
}

// applyNonDMLVectorControl applies every vector-index control intent in
// kinds. It runs first, outside SQLite - vector control is not SQLite and
// cannot share the tx applyNonDMLIntents opens afterward - matching the order
// applyNonDMLIntents's doc comment describes and depends on.
func (tm *TransactionManager) applyNonDMLVectorControl(kinds []nonDMLIntentKind) error {
	for _, k := range kinds {
		if k.vector == nil {
			continue
		}
		controlApplier, ok := tm.vectorCDCNotifier.(interface {
			ApplyVectorControl(context.Context, common.VectorIndexChange) error
		})
		if !ok {
			return fmt.Errorf("vector index control %s: vector manager not configured", k.vector.IndexName)
		}
		if err := controlApplier.ApplyVectorControl(context.Background(), *k.vector); err != nil {
			return fmt.Errorf("failed to apply vector index control: %w", err)
		}
	}
	return nil
}

// execNonDMLIntentsInTx runs every non-vector intent in kinds inside tx
// (LOAD DATA and DDL, vector control having already run outside it), seeds
// AUTO_INCREMENT bases for any DDL this txn ran, and reports the accumulated
// schema change and whether any DDL ran.
func (tm *TransactionManager) execNonDMLIntentsInTx(ctx context.Context, tx *sql.Tx, txnID uint64, commitTS hlc.Timestamp, kinds []nonDMLIntentKind) (SchemaChange, bool, error) {
	var change SchemaChange
	hasDDL := false
	var applier *versionedApplier
	defer func() {
		if applier != nil {
			applier.Close()
		}
	}()
	for _, k := range kinds {
		switch {
		case k.vector != nil:
			continue // already applied above
		case k.load != nil:
			if applier == nil {
				var err error
				if applier, err = newVersionedApplier(tx, &schemaCacheAdapter{cache: tm.schemaCache}); err != nil {
					return change, false, err
				}
			}
			if err := applier.applyLoadData(k.load.SQL, k.load.Data, commitTS); err != nil {
				return change, false, fmt.Errorf("failed to execute LOAD DATA statement: %w", err)
			}
			log.Debug().Uint64("txn_id", txnID).Msg("LOAD DATA statement executed")
		default:
			if err := change.Exec(ctx, tx, k.intent.SQLStatement, k.intent.TableName, k.intent.NodeID); err != nil {
				return change, false, fmt.Errorf("failed to execute DDL statement: %w", err)
			}
			hasDDL = true
			log.Debug().Uint64("txn_id", txnID).Str("sql", k.intent.SQLStatement).Msg("DDL statement executed")
		}
	}

	if !change.Empty() {
		// Reported before the schema cache reload below makes the new
		// definitions visible to queries, so no insert into a new incarnation
		// can take an id from a range claimed for an old one.
		tm.endIncarnations(&change)

		// Seed __marmot__autoinc for every table this DDL tagged
		// AUTO_INCREMENT, reading floors through tx (same as
		// FinishReplayedSchemaChange does for replay) so the seed sees
		// exactly the rows this transaction's own DDL left behind.
		if err := tm.seedSchemaChange(tx, &change); err != nil {
			return change, false, fmt.Errorf("failed to seed auto-increment base after DDL (txn %d): %w", txnID, err)
		}
	}

	return change, hasDDL, nil
}

// afterNonDMLCommit runs applyNonDMLIntents's post-commit refresh: notifying
// the injected schema-version-bumped callback (which advances the owning
// ReplicatedDatabase's cached __marmot_schema_version, monotonically) and
// reloading the schema cache when DDL ran. A failed reload leaves the cache
// stale, so subsequent preupdate hooks would silently drop CDC data for any
// column the DDL added/changed - fail the commit instead of swallowing the
// error, even though the DDL itself already committed (the same shape as
// handleReplay's post-commit ReloadSchema, but propagated instead of only
// logged, matching this path's existing contract).
func (tm *TransactionManager) afterNonDMLCommit(txnID uint64, hasDDL bool, newSchemaVersion uint64, change *SchemaChange) error {
	if hasDDL {
		tm.mu.RLock()
		bumped := tm.schemaVersionBumped
		tm.mu.RUnlock()
		if bumped != nil {
			bumped(newSchemaVersion)
		}
	}

	if !change.Empty() && tm.schemaCache != nil {
		if err := tm.reloadSchemaCache(); err != nil {
			return fmt.Errorf("failed to reload schema cache after DDL (txn %d): %w", txnID, err)
		}
	}

	return nil
}

// writeNonDMLToCDC writes DDL/LOAD DATA statements to CDC storage for streaming replication.
func (tm *TransactionManager) writeNonDMLToCDC(txnID uint64, intents []*WriteIntentRecord) error {
	var seq uint64

	for _, intent := range intents {
		if intent.IntentType != IntentTypeDDL {
			continue
		}

		var vectorChange common.VectorIndexChange
		isVectorControl := DeserializeData(intent.DataSnapshot, &vectorChange) == nil && vectorChange.Action != 0
		var loadSnap LoadDataSnapshot
		isLoadData := DeserializeData(intent.DataSnapshot, &loadSnap) == nil && loadSnap.Type == int(protocol.StatementLoadData)

		row := &EncodedCapturedRow{
			Table: intent.TableName,
		}
		if isVectorControl {
			row.Op = uint8(OpTypeVectorIndex)
			row.VectorIndexChange = &vectorChange
		} else if isLoadData {
			row.Op = uint8(OpTypeLoadData)
			row.LoadSQL = loadSnap.SQL
			row.LoadData = loadSnap.Data
		} else if intent.SQLStatement == "" {
			continue
		} else {
			row.Op = uint8(OpTypeDDL)
			row.DDLSQL = intent.SQLStatement
		}

		data, err := EncodeRow(row)
		if err != nil {
			return fmt.Errorf("failed to encode non-DML row: %w", err)
		}

		if err := tm.metaStore.WriteCapturedRow(txnID, seq, data); err != nil {
			return fmt.Errorf("failed to write DDL to CDC: %w", err)
		}

		seq++
		if isVectorControl {
			log.Debug().Uint64("txn_id", txnID).Str("index", vectorChange.IndexName).Str("action", vectorChange.Action.String()).Msg("Vector index control written to CDC for streaming")
		} else if isLoadData {
			log.Debug().Uint64("txn_id", txnID).Msg("LOAD DATA written to CDC for streaming")
		} else {
			log.Debug().Uint64("txn_id", txnID).Str("ddl", intent.SQLStatement).Msg("DDL written to CDC for streaming")
		}
	}

	return nil
}

// finalizeCommit marks the transaction as committed in MetaStore.
func (tm *TransactionManager) finalizeCommit(txn *Transaction) error {
	// Count rows instead of serializing statements
	rowCount := uint32(len(txn.Statements))

	// Get database name from statement or fall back to transaction manager's database
	dbName := ""
	if len(txn.Statements) > 0 && txn.Statements[0].Database != "" {
		dbName = txn.Statements[0].Database
	}
	if dbName == "" {
		tm.mu.RLock()
		dbName = tm.databaseName
		tm.mu.RUnlock()
	}

	// Collect unique table names from statements
	tableSet := make(map[string]struct{})
	for _, stmt := range txn.Statements {
		if stmt.TableName != "" {
			tableSet[stmt.TableName] = struct{}{}
		}
	}
	tables := make([]string, 0, len(tableSet))
	for t := range tableSet {
		tables = append(tables, t)
	}
	tablesInvolved := strings.Join(tables, ",")

	// Pass empty statements and rowCount to CommitTransaction
	if err := tm.metaStore.CommitTransaction(txn.ID, txn.CommitTS, nil, dbName, tablesInvolved, txn.RequiredSchemaVersion, rowCount); err != nil {
		return fmt.Errorf("failed to mark transaction as committed: %w", err)
	}

	txn.Status = TxnStatusCommitted
	return nil
}

// cleanupAfterCommit performs synchronous cleanup to prevent goroutine explosion.
func (tm *TransactionManager) cleanupAfterCommit(txn *Transaction) {
	if err := tm.metaStore.CleanupAfterCommit(txn.ID); err != nil {
		log.Warn().Err(err).Uint64("txn_id", txn.ID).Msg("Failed to cleanup after commit")
	}
}

// AbortTransaction aborts the transaction and cleans up write intents.
//
// A late PREPARE's abort can hit a txn id that was already replayed and
// COMMITTED in the log: MetaStore.AbortTransaction then refuses with
// ErrAbortCommitted instead of deleting the commit record. This still
// releases the PREPARE's own write intents (DeleteIntentsByTxn) - they are
// this abort's to clean up regardless - but it must NOT delete the CDC
// intent entries or captured rows (DeleteIntentEntries, DeleteCapturedRows):
// those now belong to the committed log entry, and deleting them would
// destroy rows a peer's next pull, or this node's own restore re-apply,
// still needs to read back. The error is returned so the caller knows the
// abort did not take effect.
func (tm *TransactionManager) AbortTransaction(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("transaction %d is not pending", txn.ID)
	}

	abortErr := tm.metaStore.AbortTransaction(txn.ID)
	if abortErr != nil && !errors.Is(abortErr, ErrAbortCommitted) {
		return fmt.Errorf("failed to abort transaction: %w", abortErr)
	}

	// Clean up this PREPARE's own write intents unconditionally: they are
	// never the committed log entry's data.
	_ = tm.metaStore.DeleteIntentsByTxn(txn.ID)

	if abortErr != nil {
		// ErrAbortCommitted: leave the CDC intent entries and captured rows
		// alone, they belong to the committed log entry now.
		return abortErr
	}

	txn.Status = TxnStatusAborted

	// Clean up CDC intent entries
	_ = tm.metaStore.DeleteIntentEntries(txn.ID)

	// Clean up raw captured rows (from hook callback)
	_ = tm.metaStore.DeleteCapturedRows(txn.ID)

	return nil
}

// GetTransaction retrieves a transaction by ID from MetaStore
// Returns nil if transaction doesn't exist or is not PENDING
func (tm *TransactionManager) GetTransaction(txnID uint64) *Transaction {
	rec, err := tm.metaStore.GetTransaction(txnID)
	if err != nil || rec == nil {
		return nil
	}

	// Only return PENDING transactions (COMMITTED/ABORTED are done)
	if rec.Status != TxnStatusPending {
		return nil
	}

	// Reconstruct Transaction from MetaStore record
	txn := &Transaction{
		ID:     rec.TxnID,
		NodeID: rec.NodeID,
		StartTS: hlc.Timestamp{
			WallTime: rec.StartTSWall,
			Logical:  rec.StartTSLogical,
			NodeID:   rec.NodeID,
		},
		Status:     rec.Status,
		Statements: make([]protocol.Statement, 0),
	}

	return txn
}

// Heartbeat updates the last_heartbeat timestamp for a transaction
// This keeps long-running transactions alive and prevents them from being garbage collected
func (tm *TransactionManager) Heartbeat(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TxnStatusPending {
		return fmt.Errorf("cannot heartbeat non-pending transaction %d (status: %s)", txn.ID, txn.Status)
	}

	return tm.metaStore.Heartbeat(txn.ID)
}

// StartGarbageCollection starts the background garbage collection goroutine
func (tm *TransactionManager) StartGarbageCollection() {
	tm.mu.Lock()
	if tm.gcRunning {
		tm.mu.Unlock()
		return
	}
	tm.gcRunning = true
	tm.stopGC = make(chan struct{}) // Fresh channel for this GC cycle
	tm.gcDone = make(chan struct{})
	stopGC := tm.stopGC
	gcDone := tm.gcDone
	tm.mu.Unlock()

	go func() {
		defer close(gcDone)
		tm.gcLoop(stopGC)
	}()
}

// StopGarbageCollection stops the background garbage collection.
// Safe to call multiple times.
func (tm *TransactionManager) StopGarbageCollection() {
	tm.mu.Lock()
	if !tm.gcRunning {
		done := tm.gcDone
		tm.mu.Unlock()
		if done != nil {
			<-done
		}
		return
	}
	tm.gcRunning = false
	stopGC := tm.stopGC
	done := tm.gcDone
	tm.mu.Unlock()

	// Safe close - only close if not already closed
	select {
	case <-stopGC:
		// Already closed
	default:
		close(stopGC)
	}
	if done != nil {
		<-done
	}
}

// gcLoop runs the garbage collection loop
func (tm *TransactionManager) gcLoop(stopGC <-chan struct{}) {
	ticker := time.NewTicker(tm.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.runGarbageCollection()
		case <-stopGC:
			return
		}
	}
}

// runGarbageCollection performs garbage collection in two phases:
// Phase 1 (Critical): Cleanup stale transactions and orphaned intents - always runs
// Phase 2 (Background): Cleanup old transaction records - skipped under load
func (tm *TransactionManager) runGarbageCollection() {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Warn().Interface("panic", recovered).Msg("transaction GC recovered")
		}
	}()
	// ====================
	// PHASE 1: CRITICAL - Always runs
	// ====================
	// Cleanup stale transactions and orphaned intents.
	// This is critical for preventing intent leaks that block new transactions.
	staleCount, err := tm.cleanupStaleTransactions()
	if err != nil {
		log.Error().Err(err).Msg("GC Phase 1: Failed to cleanup stale transactions")
	}

	if staleCount > 0 {
		log.Info().Int("stale_txns", staleCount).Msg("GC Phase 1: Cleaned up stale transactions")
	}

	// ====================
	// PHASE 2: BACKGROUND
	// ====================
	// Clean up old committed/aborted transaction records
	oldTxnCount, err := tm.cleanupOldTransactionRecords()
	if err != nil {
		log.Error().Err(err).Msg("GC Phase 2: Failed to cleanup old transaction records")
	}

	if oldTxnCount > 0 {
		log.Info().
			Int("old_txn_records", oldTxnCount).
			Msg("GC Phase 2: Cleaned up old data")
	}

	tombstones, err := tm.purgeTombstones()
	if err != nil {
		log.Error().Err(err).Msg("GC Phase 2: Failed to purge row version tombstones")
	}
	if tombstones > 0 {
		log.Info().Int64("tombstones", tombstones).Msg("GC Phase 2: Purged row version tombstones")
	}
}

// DiscardPrepared aborts txnID's local prepare under its commit guard
// (acquireCommit), releasing its intents. It is for a prepare that can never
// be committed here - its captured rows are gone (ErrPreparedRowsMissing) -
// of a transaction whose committed copy a peer's log serves instead. A
// transaction that a commit ended while this waited is left alone and
// reported by acquireCommit's error.
func (tm *TransactionManager) DiscardPrepared(txnID uint64) error {
	release, err := tm.acquireCommit(txnID)
	if err != nil {
		return err
	}
	defer release()
	txn := tm.GetTransaction(txnID)
	if txn == nil {
		return fmt.Errorf("transaction %d is not pending", txnID)
	}
	return tm.AbortTransaction(txn)
}

// cleanupStaleTransactions aborts transactions that haven't had a heartbeat within the timeout
//
// Each abort runs under the transaction's commit guard (acquireCommit), so it
// never interleaves with a commit of the same transaction - the log puller's
// CommitLocallyPrepared or a COMMIT RPC: a commit that wins commits the
// transaction's whole prepared payload, and one that loses finds it no longer
// PENDING. A transaction a commit ended while this pass waited is skipped.
func (tm *TransactionManager) cleanupStaleTransactions() (int, error) {
	stale, err := tm.metaStore.StaleTransactionIDs(tm.heartbeatTimeout)
	if err != nil {
		return 0, err
	}
	cleaned := 0
	for _, txnID := range stale {
		release, err := tm.acquireCommit(txnID)
		if err != nil {
			continue // committed, aborted or gone since it was listed
		}
		aborted, err := tm.metaStore.AbortStaleTransaction(txnID, tm.heartbeatTimeout)
		release()
		if err != nil {
			log.Warn().Err(err).Uint64("txn_id", txnID).Msg("GC: failed to abort stale transaction")
			continue
		}
		if aborted {
			cleaned++
		}
	}
	if cleaned > 0 {
		log.Info().Int("stale_txns", cleaned).Msg("GC: aborted stale transactions")
	}
	return cleaned, nil
}

// cleanupOldTransactionRecords deletes committed log entries at or below
// the GC-safe position (see GCSafePositionFunc) once they are older than
// gcMinRetention, or unconditionally once they are older than gcMaxRetention.
func (tm *TransactionManager) cleanupOldTransactionRecords() (int, error) {
	// Get callback under lock
	tm.mu.RLock()
	safeFn := tm.gcSafePosition
	dbName := tm.databaseName
	tm.mu.RUnlock()

	var safe LogPosition

	// Skip replication tracking for system database (it's not replicated)
	if dbName != "" && dbName != SystemDatabaseName && safeFn != nil {
		if pos, ok := safeFn(); ok {
			safe = pos
		}
	}

	count, err := tm.metaStore.CleanupOldTransactionRecords(tm.gcMinRetention, tm.gcMaxRetention, safe)
	if err != nil {
		return 0, err
	}

	if count > 0 {
		log.Info().
			Str("database", dbName).
			Int("deleted_records", count).
			Uint64("safe_seq", safe.Seq).
			Uint64("safe_txn_id", safe.TxnID).
			Msg("GC: Cleaned up old transaction records")
	}

	return count, nil
}

// SerializeData helper for data snapshots
func SerializeData(data interface{}) ([]byte, error) {
	return encoding.Marshal(data)
}

// DeserializeData helper for data snapshots
func DeserializeData(data []byte, v interface{}) error {
	return encoding.Unmarshal(data, v)
}

// schemaCacheAdapter adapts SchemaCache to CDCSchemaProvider interface
type schemaCacheAdapter struct {
	cache *SchemaCache
}

func (s *schemaCacheAdapter) GetPrimaryKeys(tableName string) ([]string, error) {
	schema, err := s.cache.GetSchemaFor(tableName)
	if err != nil {
		return nil, err
	}
	return schema.PrimaryKeys, nil
}

// unmarshalCDCValue deserializes a msgpack-encoded CDC column value.
//
// SQLite's preupdate hook hands back TEXT and BLOB storage classes identically
// as Go []byte, so distinguishing them can only happen where the schema is
// known: capture (encodeValuesWithSchema in preupdate_hook.go) converts
// TEXT-affinity columns to string before encoding, so they are written as
// msgpack Str, while BLOB-affinity columns stay []byte and are written as
// msgpack Bin. UnmarshalStrict preserves that distinction on the way back out
// (Bin -> []byte, Str -> string) so BLOB columns bind via sqlite3_bind_blob
// instead of being coerced to text. Unmarshal's loose decoding must NOT be
// used here: it collapses Bin to string, silently corrupting BLOB columns.
func unmarshalCDCValue(data []byte) (interface{}, error) {
	var value interface{}
	if err := encoding.UnmarshalStrict(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}
