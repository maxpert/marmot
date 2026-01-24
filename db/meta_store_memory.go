package db

import (
	"fmt"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

// MemoryMetaStore wraps PebbleMetaStore to use memory for transitionary state
// while delegating durable operations to Pebble.
//
// Memory stores eliminate Pebble writes for:
// - Transaction status/heartbeat (/txn_status/, /txn_heartbeat/, /txn_idx/pend/)
// - CDC row locks (/cdc/active/, /cdc/txn_locks/)
//
// Durable operations remain in Pebble:
// - Immutable transaction records (/txn/{txnID})
// - Commit records (/txn_commit/{txnID}, /txn_idx/seq/)
// - Write intents (not migrated yet - future optimization)
type MemoryMetaStore struct {
	pebble    *PebbleMetaStore
	txnStore  TransactionStore
	lockStore CDCLockStore
}

// Ensure MemoryMetaStore implements MetaStore
var _ MetaStore = (*MemoryMetaStore)(nil)

// NewMemoryMetaStore creates a new memory-backed meta store wrapper.
func NewMemoryMetaStore(pebble *PebbleMetaStore) *MemoryMetaStore {
	m := &MemoryMetaStore{
		pebble:    pebble,
		txnStore:  NewXsyncTransactionStore(),
		lockStore: NewXsyncCDCLockStore(),
	}

	// Set transaction getter so conflict resolution uses memory-tier transaction state
	pebble.SetTransactionGetter(m.GetTransaction)

	return m
}

// BeginTransaction writes immutable record to Pebble and stores status/heartbeat in memory.
func (m *MemoryMetaStore) BeginTransaction(txnID, nodeID uint64, startTS hlc.Timestamp) error {
	// Write immutable record to Pebble /txn/{txnID}
	if err := m.pebble.writeImmutableTxnRecord(txnID, nodeID, startTS); err != nil {
		return err
	}

	// Store status/heartbeat in memory
	now := time.Now().UnixNano()
	m.txnStore.Begin(txnID, &TxnState{
		NodeID:         nodeID,
		Status:         TxnStatusPending,
		StartTSWall:    startTS.WallTime,
		StartTSLogical: startTS.Logical,
		LastHeartbeat:  now,
		RowCount:       0,
	})

	return nil
}

// GetTransaction reconstructs TransactionRecord from Pebble and memory state.
func (m *MemoryMetaStore) GetTransaction(txnID uint64) (*TransactionRecord, error) {
	// Read immutable from Pebble
	immutable, err := m.pebble.readImmutableTxnRecord(txnID)
	if err != nil {
		return nil, err
	}
	if immutable == nil {
		return nil, nil
	}

	// Read status/heartbeat from memory
	state, found := m.txnStore.Get(txnID)
	if !found {
		// Transaction exists in Pebble but not in memory - must be committed/aborted
		// Read from Pebble for backward compatibility
		return m.pebble.GetTransaction(txnID)
	}

	rec := &TransactionRecord{
		TxnID:          immutable.TxnID,
		NodeID:         immutable.NodeID,
		Status:         state.Status,
		StartTSWall:    immutable.StartTSWall,
		StartTSLogical: immutable.StartTSLogical,
		CreatedAt:      immutable.CreatedAt,
		LastHeartbeat:  state.LastHeartbeat,
	}

	// If committed, read commit record from Pebble
	if state.Status == TxnStatusCommitted {
		commit, err := m.pebble.readCommitRecord(txnID)
		if err != nil {
			return nil, err
		}
		if commit != nil {
			rec.SeqNum = commit.SeqNum
			rec.CommitTSWall = commit.CommitTSWall
			rec.CommitTSLogical = commit.CommitTSLogical
			rec.CommittedAt = commit.CommittedAt
			rec.TablesInvolved = commit.TablesInvolved
			rec.DatabaseName = commit.DatabaseName
			rec.RequiredSchemaVersion = commit.RequiredSchemaVersion
		}
	}

	return rec, nil
}

// GetPendingTransactions uses memory txnStore instead of scanning Pebble.
func (m *MemoryMetaStore) GetPendingTransactions() ([]*TransactionRecord, error) {
	var records []*TransactionRecord

	m.txnStore.RangePending(func(txnID uint64) bool {
		rec, err := m.GetTransaction(txnID)
		if err == nil && rec != nil && rec.Status == TxnStatusPending {
			records = append(records, rec)
		}
		return true
	})

	return records, nil
}

// Heartbeat updates the last heartbeat timestamp in memory only.
func (m *MemoryMetaStore) Heartbeat(txnID uint64) error {
	m.txnStore.UpdateHeartbeat(txnID, time.Now().UnixNano())
	return nil
}

// CommitTransaction writes commit record to Pebble and updates memory status.
func (m *MemoryMetaStore) CommitTransaction(txnID uint64, commitTS hlc.Timestamp, statements []byte, dbName, tablesInvolved string, requiredSchemaVersion uint64, rowCount uint32) error {
	// Write commit record to Pebble (/txn_commit/, /txn_idx/seq/)
	if err := m.pebble.CommitTransaction(txnID, commitTS, statements, dbName, tablesInvolved, requiredSchemaVersion, rowCount); err != nil {
		return err
	}

	// Update memory status to committed and remove from memory
	// (committed transactions are stored in Pebble for catch-up, not needed in memory)
	m.txnStore.UpdateStatus(txnID, TxnStatusCommitted)
	m.txnStore.Remove(txnID)

	return nil
}

// AbortTransaction updates memory status and cleans up Pebble records.
func (m *MemoryMetaStore) AbortTransaction(txnID uint64) error {
	// Check if transaction exists in memory
	state, found := m.txnStore.Get(txnID)
	if !found {
		// Not in memory - might be a committed/replayed transaction, delegate to Pebble
		return m.pebble.AbortTransaction(txnID)
	}

	// Update memory status to aborted
	m.txnStore.UpdateStatus(txnID, TxnStatusAborted)

	// Manually clean up Pebble keys (can't call pebble.AbortTransaction because it expects status in Pebble)
	if err := m.pebble.deleteTransactionKeys(txnID, state.Status == TxnStatusCommitted); err != nil {
		return err
	}

	// Remove from memory
	m.txnStore.Remove(txnID)

	return nil
}

// StoreReplayedTransaction delegates to Pebble for replayed transactions.
func (m *MemoryMetaStore) StoreReplayedTransaction(txnID, nodeID uint64, commitTS hlc.Timestamp, dbName string, rowCount uint32) error {
	return m.pebble.StoreReplayedTransaction(txnID, nodeID, commitTS, dbName, rowCount)
}

// WriteIntent delegates to Pebble (not migrated to memory yet).
func (m *MemoryMetaStore) WriteIntent(txnID uint64, intentType IntentType, tableName, intentKey string, op OpType, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error {
	return m.pebble.WriteIntent(txnID, intentType, tableName, intentKey, op, sqlStmt, data, ts, nodeID)
}

// ValidateIntent delegates to Pebble.
func (m *MemoryMetaStore) ValidateIntent(tableName, intentKey string, expectedTxnID uint64) (bool, error) {
	return m.pebble.ValidateIntent(tableName, intentKey, expectedTxnID)
}

// DeleteIntent delegates to Pebble.
func (m *MemoryMetaStore) DeleteIntent(tableName, intentKey string, txnID uint64) error {
	return m.pebble.DeleteIntent(tableName, intentKey, txnID)
}

// DeleteIntentsByTxn delegates to Pebble.
func (m *MemoryMetaStore) DeleteIntentsByTxn(txnID uint64) error {
	return m.pebble.DeleteIntentsByTxn(txnID)
}

// MarkIntentsForCleanup delegates to Pebble.
func (m *MemoryMetaStore) MarkIntentsForCleanup(txnID uint64) error {
	return m.pebble.MarkIntentsForCleanup(txnID)
}

// GetIntentsByTxn delegates to Pebble.
func (m *MemoryMetaStore) GetIntentsByTxn(txnID uint64) ([]*WriteIntentRecord, error) {
	return m.pebble.GetIntentsByTxn(txnID)
}

// GetIntent delegates to Pebble.
func (m *MemoryMetaStore) GetIntent(tableName, intentKey string) (*WriteIntentRecord, error) {
	return m.pebble.GetIntent(tableName, intentKey)
}


// GetReplicationState delegates to Pebble.
func (m *MemoryMetaStore) GetReplicationState(peerNodeID uint64, dbName string) (*ReplicationStateRecord, error) {
	return m.pebble.GetReplicationState(peerNodeID, dbName)
}

// UpdateReplicationState delegates to Pebble.
func (m *MemoryMetaStore) UpdateReplicationState(peerNodeID uint64, dbName string, lastTxnID uint64, lastTS hlc.Timestamp) error {
	return m.pebble.UpdateReplicationState(peerNodeID, dbName, lastTxnID, lastTS)
}

// GetMinAppliedTxnID delegates to Pebble.
func (m *MemoryMetaStore) GetMinAppliedTxnID(dbName string) (uint64, error) {
	return m.pebble.GetMinAppliedTxnID(dbName)
}

// GetAllReplicationStates delegates to Pebble.
func (m *MemoryMetaStore) GetAllReplicationStates() ([]*ReplicationStateRecord, error) {
	return m.pebble.GetAllReplicationStates()
}

// GetNextSeqNum delegates to Pebble.
func (m *MemoryMetaStore) GetNextSeqNum(nodeID uint64) (uint64, error) {
	return m.pebble.GetNextSeqNum(nodeID)
}

// GetMaxSeqNum delegates to Pebble.
func (m *MemoryMetaStore) GetMaxSeqNum() (uint64, error) {
	return m.pebble.GetMaxSeqNum()
}

// GetMinAppliedSeqNum delegates to Pebble.
func (m *MemoryMetaStore) GetMinAppliedSeqNum(dbName string) (uint64, error) {
	return m.pebble.GetMinAppliedSeqNum(dbName)
}

// GetSchemaVersion delegates to Pebble.
func (m *MemoryMetaStore) GetSchemaVersion(dbName string) (int64, error) {
	return m.pebble.GetSchemaVersion(dbName)
}

// UpdateSchemaVersion delegates to Pebble.
func (m *MemoryMetaStore) UpdateSchemaVersion(dbName string, version int64, ddlSQL string, txnID uint64) error {
	return m.pebble.UpdateSchemaVersion(dbName, version, ddlSQL, txnID)
}

// GetAllSchemaVersions delegates to Pebble.
func (m *MemoryMetaStore) GetAllSchemaVersions() (map[string]int64, error) {
	return m.pebble.GetAllSchemaVersions()
}

// TryAcquireDDLLock delegates to Pebble.
func (m *MemoryMetaStore) TryAcquireDDLLock(dbName string, nodeID uint64, leaseDuration time.Duration) (bool, error) {
	return m.pebble.TryAcquireDDLLock(dbName, nodeID, leaseDuration)
}

// ReleaseDDLLock delegates to Pebble.
func (m *MemoryMetaStore) ReleaseDDLLock(dbName string, nodeID uint64) error {
	return m.pebble.ReleaseDDLLock(dbName, nodeID)
}

// WriteIntentEntry delegates to Pebble.
func (m *MemoryMetaStore) WriteIntentEntry(txnID, seq uint64, op uint8, table, intentKey string, oldVals, newVals map[string][]byte) error {
	return m.pebble.WriteIntentEntry(txnID, seq, op, table, intentKey, oldVals, newVals)
}

// GetIntentEntries delegates to Pebble.
func (m *MemoryMetaStore) GetIntentEntries(txnID uint64) ([]*IntentEntry, error) {
	return m.pebble.GetIntentEntries(txnID)
}

// DeleteIntentEntries delegates to Pebble.
func (m *MemoryMetaStore) DeleteIntentEntries(txnID uint64) error {
	return m.pebble.DeleteIntentEntries(txnID)
}

// CleanupAfterCommit delegates to Pebble.
func (m *MemoryMetaStore) CleanupAfterCommit(txnID uint64) error {
	return m.pebble.CleanupAfterCommit(txnID)
}

// WriteCapturedRow delegates to Pebble.
func (m *MemoryMetaStore) WriteCapturedRow(txnID, seq uint64, data []byte) error {
	return m.pebble.WriteCapturedRow(txnID, seq, data)
}

// IterateCapturedRows delegates to Pebble.
func (m *MemoryMetaStore) IterateCapturedRows(txnID uint64) (CapturedRowCursor, error) {
	return m.pebble.IterateCapturedRows(txnID)
}

// DeleteCapturedRow delegates to Pebble.
func (m *MemoryMetaStore) DeleteCapturedRow(txnID, seq uint64) error {
	return m.pebble.DeleteCapturedRow(txnID, seq)
}

// DeleteCapturedRows delegates to Pebble.
func (m *MemoryMetaStore) DeleteCapturedRows(txnID uint64) error {
	return m.pebble.DeleteCapturedRows(txnID)
}

// AcquireCDCRowLock uses memory-only lock store.
func (m *MemoryMetaStore) AcquireCDCRowLock(txnID uint64, tableName, intentKey string) error {
	return m.lockStore.Acquire(txnID, tableName, intentKey)
}

// ReleaseCDCRowLock uses memory-only lock store.
func (m *MemoryMetaStore) ReleaseCDCRowLock(tableName, intentKey string, txnID uint64) error {
	m.lockStore.Release(tableName, intentKey, txnID)
	return nil
}

// ReleaseCDCRowLocksByTxn uses memory-only lock store.
func (m *MemoryMetaStore) ReleaseCDCRowLocksByTxn(txnID uint64) error {
	m.lockStore.ReleaseByTxn(txnID)
	return nil
}

// GetCDCRowLock uses memory-only lock store.
func (m *MemoryMetaStore) GetCDCRowLock(tableName, intentKey string) (uint64, error) {
	if txnID, held := m.lockStore.GetHolder(tableName, intentKey); held {
		return txnID, nil
	}
	return 0, nil
}

// AcquireCDCTableDDLLock delegates to Pebble.
func (m *MemoryMetaStore) AcquireCDCTableDDLLock(txnID uint64, tableName string) error {
	return m.pebble.AcquireCDCTableDDLLock(txnID, tableName)
}

// ReleaseCDCTableDDLLock delegates to Pebble.
func (m *MemoryMetaStore) ReleaseCDCTableDDLLock(tableName string, txnID uint64) error {
	return m.pebble.ReleaseCDCTableDDLLock(tableName, txnID)
}

// HasCDCRowLocksForTable delegates to Pebble.
func (m *MemoryMetaStore) HasCDCRowLocksForTable(tableName string) (bool, error) {
	return m.pebble.HasCDCRowLocksForTable(tableName)
}

// GetCDCTableDDLLock delegates to Pebble.
func (m *MemoryMetaStore) GetCDCTableDDLLock(tableName string) (uint64, error) {
	return m.pebble.GetCDCTableDDLLock(tableName)
}

// CleanupStaleTransactions iterates memory txnStore instead of Pebble /txn_idx/pend/.
func (m *MemoryMetaStore) CleanupStaleTransactions(timeout time.Duration) (int, error) {
	cutoff := time.Now().Add(-timeout).UnixNano()
	cleaned := 0

	var staleTxnIDs []uint64

	// Iterate memory txnStore for pending transactions
	m.txnStore.RangePending(func(txnID uint64) bool {
		state, found := m.txnStore.Get(txnID)
		if !found {
			return true
		}

		if state.LastHeartbeat < cutoff {
			staleTxnIDs = append(staleTxnIDs, txnID)
		}
		return true
	})

	// Abort each stale transaction
	for _, txnID := range staleTxnIDs {
		if err := m.AbortTransaction(txnID); err == nil {
			cleaned++
		}
	}

	return cleaned, nil
}

// CleanupOldTransactionRecords delegates to Pebble.
func (m *MemoryMetaStore) CleanupOldTransactionRecords(minRetention, maxRetention time.Duration, minAppliedTxnID, minAppliedSeqNum uint64) (int, error) {
	return m.pebble.CleanupOldTransactionRecords(minRetention, maxRetention, minAppliedTxnID, minAppliedSeqNum)
}

// GetMaxCommittedTxnID delegates to Pebble.
func (m *MemoryMetaStore) GetMaxCommittedTxnID() (uint64, error) {
	return m.pebble.GetMaxCommittedTxnID()
}

// GetCommittedTxnCount delegates to Pebble.
func (m *MemoryMetaStore) GetCommittedTxnCount() (int64, error) {
	return m.pebble.GetCommittedTxnCount()
}

// StreamCommittedTransactions delegates to Pebble.
func (m *MemoryMetaStore) StreamCommittedTransactions(fromTxnID uint64, callback func(*TransactionRecord) error) error {
	return m.pebble.StreamCommittedTransactions(fromTxnID, callback)
}

// ScanTransactions delegates to Pebble.
func (m *MemoryMetaStore) ScanTransactions(fromTxnID uint64, descending bool, callback func(*TransactionRecord) error) error {
	return m.pebble.ScanTransactions(fromTxnID, descending, callback)
}

// Close delegates to Pebble.
func (m *MemoryMetaStore) Close() error {
	return m.pebble.Close()
}

// Checkpoint delegates to Pebble.
func (m *MemoryMetaStore) Checkpoint() error {
	return m.pebble.Checkpoint()
}

// ReconstructFromPebble cleans up orphaned CDC data from crashed transactions.
// Called at startup to ensure consistency.
// Memory stores start empty - no pending transaction state survives crash.
func (m *MemoryMetaStore) ReconstructFromPebble() error {
	orphanedTxnIDs, err := m.pebble.findOrphanedCDCRawTxnIDs()
	if err != nil {
		return fmt.Errorf("failed to find orphaned CDC txnIDs: %w", err)
	}

	if len(orphanedTxnIDs) == 0 {
		return nil
	}

	log.Info().Int("count", len(orphanedTxnIDs)).Msg("Cleaning up orphaned CDC raw data from crashed transactions")

	for _, txnID := range orphanedTxnIDs {
		if err := m.pebble.DeleteCapturedRows(txnID); err != nil {
			log.Warn().Err(err).Uint64("txn_id", txnID).Msg("Failed to delete orphaned CDC raw rows")
		}
	}

	return nil
}
