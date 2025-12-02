package db

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

// Group commit configuration
const (
	batchMaxSize     = 100                  // Max operations per batch
	batchMaxWait     = 2 * time.Millisecond // Max time to wait for batch
	batchChannelSize = 1000                 // Channel buffer size
)

// Key prefixes for BadgerDB (sorted for efficient iteration)
const (
	prefixTxn         = "/txn/"          // /txn/{txnID:016x}
	prefixTxnPending  = "/txn_idx/pend/" // /txn_idx/pend/{txnID:016x}
	prefixTxnSeq      = "/txn_idx/seq/"  // /txn_idx/seq/{seqNum:016x}/{txnID:016x}
	prefixIntent      = "/intent/"       // /intent/{tableName}/{rowKey}
	prefixMVCC        = "/mvcc/"         // /mvcc/{tableName}/{rowKey}/{wallTime:016x}{logical:08x}{nodeID:016x}
	prefixCDC         = "/cdc/"          // /cdc/{txnID:016x}/{seq:08x}
	prefixRepl        = "/repl/"         // /repl/{peerNodeID:016x}/{dbName}
	prefixSchema      = "/schema/"       // /schema/{dbName}
	prefixDDLLock     = "/ddl/"          // /ddl/{dbName}
	prefixSeq         = "/seq/"          // /seq/{nodeID:016x}
	prefixIntentByTxn = "/intent_txn/"   // /intent_txn/{txnID:016x}/{tableName}/{rowKey}
)

// batchOp represents a batched write operation
type batchOp struct {
	fn     func(txn *badger.Txn) error
	result chan error
}

// BadgerMetaStore implements MetaStore using BadgerDB
type BadgerMetaStore struct {
	db          *badger.DB
	path        string
	batchCh     chan *batchOp
	stopBatch   chan struct{}
	batchWg     sync.WaitGroup
	batchClosed bool
	batchMu     sync.RWMutex

	// Sequence generators for per-node sequence numbers (contention-free)
	sequences map[uint64]*badger.Sequence
	seqMu     sync.Mutex

	// Persistent counters for O(1) lookups (avoids full table scans)
	counters *PersistentCounter
}

// Ensure BadgerMetaStore implements MetaStore
var _ MetaStore = (*BadgerMetaStore)(nil)

// BadgerMetaStoreOptions configures BadgerDB
type BadgerMetaStoreOptions struct {
	SyncWrites     bool
	NumCompactors  int
	ValueLogGC     bool
	BlockCacheMB   int64 // Block cache size in MB (0 = use default 64MB)
	MemTableSizeMB int64 // MemTable size in MB (0 = use default 32MB)
	NumMemTables   int   // Number of MemTables (0 = use default 2)
}

// DefaultBadgerOptions returns default BadgerDB options tuned for sidecar usage
func DefaultBadgerOptions() BadgerMetaStoreOptions {
	return BadgerMetaStoreOptions{
		SyncWrites:     true,
		NumCompactors:  2,
		ValueLogGC:     true,
		BlockCacheMB:   64, // 64MB (BadgerDB default: 256MB)
		MemTableSizeMB: 32, // 32MB (BadgerDB default: 64MB)
		NumMemTables:   2,  // 2 (BadgerDB default: 5)
	}
}

// NewBadgerMetaStore creates a new BadgerDB-backed MetaStore
func NewBadgerMetaStore(path string, opts BadgerMetaStoreOptions) (*BadgerMetaStore, error) {
	badgerOpts := badger.DefaultOptions(path)
	badgerOpts.SyncWrites = opts.SyncWrites
	badgerOpts.NumCompactors = opts.NumCompactors
	badgerOpts.Logger = nil // Disable badger's default logging

	// Memory tuning for sidecar usage (defaults save ~400MB vs BadgerDB defaults)
	blockCache := int64(64 << 20) // 64MB default
	if opts.BlockCacheMB > 0 {
		blockCache = opts.BlockCacheMB << 20
	}
	badgerOpts.BlockCacheSize = blockCache

	memTableSize := int64(32 << 20) // 32MB default
	if opts.MemTableSizeMB > 0 {
		memTableSize = opts.MemTableSizeMB << 20
	}
	badgerOpts.MemTableSize = memTableSize

	numMemTables := 2 // 2 default
	if opts.NumMemTables > 0 {
		numMemTables = opts.NumMemTables
	}
	badgerOpts.NumMemtables = numMemTables

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	store := &BadgerMetaStore{
		db:        db,
		path:      path,
		batchCh:   make(chan *batchOp, batchChannelSize),
		stopBatch: make(chan struct{}),
		sequences: make(map[uint64]*badger.Sequence),
		counters:  NewPersistentCounter(db, "/meta/", 10), // Small cache for counter keys
	}

	// Start batch writer goroutine
	store.batchWg.Add(1)
	go store.batchWriter()

	// Start value log GC goroutine if enabled
	if opts.ValueLogGC {
		go store.runValueLogGC()
	}

	return store, nil
}

// batchWriter runs in a goroutine and batches write operations
func (s *BadgerMetaStore) batchWriter() {
	defer s.batchWg.Done()

	ops := make([]*batchOp, 0, batchMaxSize)
	timer := time.NewTimer(batchMaxWait)
	timer.Stop()
	timerRunning := false

	flush := func() {
		if len(ops) == 0 {
			return
		}

		// Execute all ops in a single transaction
		err := s.db.Update(func(txn *badger.Txn) error {
			for _, op := range ops {
				if opErr := op.fn(txn); opErr != nil {
					// Mark this op as failed, continue with others
					op.result <- opErr
				}
			}
			return nil
		})

		// Notify all ops of completion
		for _, op := range ops {
			select {
			case <-op.result:
				// Already sent error above
			default:
				op.result <- err
			}
		}

		ops = ops[:0]
		if timerRunning {
			timer.Stop()
			timerRunning = false
		}
	}

	for {
		select {
		case op := <-s.batchCh:
			ops = append(ops, op)

			if len(ops) >= batchMaxSize {
				flush()
			} else if !timerRunning {
				timer.Reset(batchMaxWait)
				timerRunning = true
			}

		case <-timer.C:
			timerRunning = false
			flush()

		case <-s.stopBatch:
			// Drain remaining ops
			for {
				select {
				case op := <-s.batchCh:
					ops = append(ops, op)
				default:
					flush()
					return
				}
			}
		}
	}
}

// submitBatchOp submits an operation to the batch writer and waits for result
func (s *BadgerMetaStore) submitBatchOp(fn func(txn *badger.Txn) error) error {
	s.batchMu.RLock()
	if s.batchClosed {
		s.batchMu.RUnlock()
		// Fallback to direct execution if batch writer is closed
		return s.db.Update(fn)
	}
	s.batchMu.RUnlock()

	op := &batchOp{
		fn:     fn,
		result: make(chan error, 1),
	}

	select {
	case s.batchCh <- op:
		return <-op.result
	case <-s.stopBatch:
		// Batch writer stopped, execute directly
		return s.db.Update(fn)
	}
}

// runValueLogGC periodically runs value log garbage collection
func (s *BadgerMetaStore) runValueLogGC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if s.db.IsClosed() {
			return
		}
		for {
			err := s.db.RunValueLogGC(0.5)
			if err == badger.ErrNoRewrite {
				break
			}
			if err != nil {
				break
			}
		}
	}
}

// Close closes the BadgerDB
func (s *BadgerMetaStore) Close() error {
	// Stop batch writer
	s.batchMu.Lock()
	if !s.batchClosed {
		s.batchClosed = true
		close(s.stopBatch)
	}
	s.batchMu.Unlock()

	// Wait for batch writer to finish
	s.batchWg.Wait()

	// Release all sequence generators
	s.seqMu.Lock()
	for _, seq := range s.sequences {
		seq.Release()
	}
	s.sequences = nil
	s.seqMu.Unlock()

	return s.db.Close()
}

// Checkpoint is a no-op for BadgerDB (WAL checkpoint is SQLite-specific)
func (s *BadgerMetaStore) Checkpoint() error {
	return nil
}

// IsBusy returns true if the batch channel is over 50% full
// Used by GC to skip non-critical Phase 2 work under load
func (s *BadgerMetaStore) IsBusy() bool {
	s.batchMu.RLock()
	defer s.batchMu.RUnlock()
	if s.batchClosed {
		return false
	}
	// Consider busy if channel is over 50% full
	return len(s.batchCh) > batchChannelSize/2
}

// txnKey returns the key for a transaction record
func txnKey(txnID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x", prefixTxn, txnID))
}

// txnPendingKey returns the pending index key
func txnPendingKey(txnID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x", prefixTxnPending, txnID))
}

// txnSeqKey returns the sequence index key
func txnSeqKey(seqNum, txnID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x/%016x", prefixTxnSeq, seqNum, txnID))
}

// intentKey returns the key for a write intent
func intentKey(tableName, rowKey string) []byte {
	return []byte(fmt.Sprintf("%s%s/%s", prefixIntent, tableName, rowKey))
}

// intentByTxnKey returns the secondary index key for intent by txn
func intentByTxnKey(txnID uint64, tableName, rowKey string) []byte {
	return []byte(fmt.Sprintf("%s%016x/%s/%s", prefixIntentByTxn, txnID, tableName, rowKey))
}

// mvccKey returns the key for an MVCC version
func mvccKey(tableName, rowKey string, ts hlc.Timestamp, nodeID uint64) []byte {
	return []byte(fmt.Sprintf("%s%s/%s/%016x%08x%016x", prefixMVCC, tableName, rowKey, ts.WallTime, ts.Logical, nodeID))
}

// mvccPrefix returns the prefix for all MVCC versions of a row
func mvccPrefix(tableName, rowKey string) []byte {
	return []byte(fmt.Sprintf("%s%s/%s/", prefixMVCC, tableName, rowKey))
}

// cdcKey returns the key for a CDC intent entry
func cdcKey(txnID, seq uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x/%08x", prefixCDC, txnID, seq))
}

// cdcPrefix returns the prefix for all CDC entries of a transaction
func cdcPrefix(txnID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x/", prefixCDC, txnID))
}

// replKey returns the key for replication state
func replKey(peerNodeID uint64, dbName string) []byte {
	return []byte(fmt.Sprintf("%s%016x/%s", prefixRepl, peerNodeID, dbName))
}

// schemaKey returns the key for schema version
func schemaKey(dbName string) []byte {
	return []byte(fmt.Sprintf("%s%s", prefixSchema, dbName))
}

// ddlLockKey returns the key for DDL lock
func ddlLockKey(dbName string) []byte {
	return []byte(fmt.Sprintf("%s%s", prefixDDLLock, dbName))
}

// seqKey returns the key for node sequence
func seqKey(nodeID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x", prefixSeq, nodeID))
}

// BeginTransaction creates a new transaction record with PENDING status
func (s *BadgerMetaStore) BeginTransaction(txnID, nodeID uint64, startTS hlc.Timestamp) error {
	log.Debug().
		Uint64("txn_id", txnID).
		Uint64("node_id", nodeID).
		Int64("start_ts", startTS.WallTime).
		Msg("CDC: BeginTransaction")

	rec := &TransactionRecord{
		TxnID:          txnID,
		NodeID:         nodeID,
		Status:         MetaTxnStatusPending,
		StartTSWall:    startTS.WallTime,
		StartTSLogical: startTS.Logical,
		CreatedAt:      time.Now().UnixNano(),
		LastHeartbeat:  time.Now().UnixNano(),
	}

	data, err := msgpack.Marshal(rec)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction record: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		// Insert transaction record
		if err := txn.Set(txnKey(txnID), data); err != nil {
			return err
		}
		// Add to pending index
		return txn.Set(txnPendingKey(txnID), nil)
	})
}

// CommitTransaction marks a transaction as COMMITTED
// Uses contention-free Sequence API for seq_num allocation
func (s *BadgerMetaStore) CommitTransaction(txnID uint64, commitTS hlc.Timestamp, statements []byte, dbName string) error {
	log.Debug().
		Uint64("txn_id", txnID).
		Int64("commit_ts", commitTS.WallTime).
		Str("database", dbName).
		Int("statements_len", len(statements)).
		Msg("CDC: CommitTransaction")

	// Step 1: Read nodeID from transaction record (read-only, no contention)
	var nodeID uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(txnKey(txnID))
		if err != nil {
			return fmt.Errorf("transaction %d not found: %w", txnID, err)
		}
		return item.Value(func(val []byte) error {
			var rec TransactionRecord
			if err := msgpack.Unmarshal(val, &rec); err != nil {
				return err
			}
			nodeID = rec.NodeID
			return nil
		})
	})
	if err != nil {
		return err
	}

	// Step 2: Get sequence number using contention-free Sequence API
	seqNum, err := s.GetNextSeqNum(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get next seq_num: %w", err)
	}

	// Step 3: Update transaction record (simple update, no seq contention)
	return s.db.Update(func(txn *badger.Txn) error {
		// Get existing record
		item, err := txn.Get(txnKey(txnID))
		if err != nil {
			return fmt.Errorf("transaction %d not found: %w", txnID, err)
		}

		var rec TransactionRecord
		err = item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &rec)
		})
		if err != nil {
			return err
		}

		// Update record with pre-allocated seq number
		rec.Status = MetaTxnStatusCommitted
		rec.CommitTSWall = commitTS.WallTime
		rec.CommitTSLogical = commitTS.Logical
		rec.CommittedAt = time.Now().UnixNano()
		rec.SerializedStatements = statements
		rec.DatabaseName = dbName
		rec.SeqNum = seqNum

		data, err := msgpack.Marshal(&rec)
		if err != nil {
			return err
		}

		// Update transaction record
		if err := txn.Set(txnKey(txnID), data); err != nil {
			return err
		}

		// Remove from pending index
		if err := txn.Delete(txnPendingKey(txnID)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		// Add to sequence index
		if err := txn.Set(txnSeqKey(seqNum, txnID), nil); err != nil {
			return err
		}

		// Update commit counters (O(1) lookups via PersistentCounter)
		if _, err := s.counters.UpdateMaxInTxn(txn, "max_committed_txn_id", int64(txnID)); err != nil {
			return err
		}
		_, err = s.counters.IncInTxn(txn, "committed_txn_count", 1)
		return err
	})
}

// StoreReplayedTransaction inserts a fully-committed transaction record directly.
// Used by delta sync to record transactions that were replayed from other nodes.
// Unlike CommitTransaction, this doesn't require a prior BeginTransaction call.
func (s *BadgerMetaStore) StoreReplayedTransaction(txnID, nodeID uint64, commitTS hlc.Timestamp, statements []byte, dbName string) error {
	log.Debug().
		Uint64("txn_id", txnID).
		Uint64("node_id", nodeID).
		Int64("commit_ts", commitTS.WallTime).
		Str("database", dbName).
		Int("statements_len", len(statements)).
		Msg("StoreReplayedTransaction: storing replayed transaction")

	// Get sequence number using contention-free Sequence API
	seqNum, err := s.GetNextSeqNum(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get next seq_num: %w", err)
	}

	now := time.Now().UnixNano()
	rec := TransactionRecord{
		TxnID:                txnID,
		NodeID:               nodeID,
		SeqNum:               seqNum,
		Status:               MetaTxnStatusCommitted,
		StartTSWall:          commitTS.WallTime, // Use commit TS as start for replayed txns
		StartTSLogical:       commitTS.Logical,
		CommitTSWall:         commitTS.WallTime,
		CommitTSLogical:      commitTS.Logical,
		CreatedAt:            now,
		CommittedAt:          now,
		LastHeartbeat:        now,
		SerializedStatements: statements,
		DatabaseName:         dbName,
	}

	data, err := msgpack.Marshal(rec)
	if err != nil {
		return fmt.Errorf("failed to serialize transaction record: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		// Store transaction record (use SetEntry with TTL for eventual cleanup)
		if err := txn.Set(txnKey(txnID), data); err != nil {
			return err
		}
		// Add to sequence index
		if err := txn.Set(txnSeqKey(seqNum, txnID), nil); err != nil {
			return err
		}
		// Update commit counters (O(1) lookups via PersistentCounter)
		if _, err := s.counters.UpdateMaxInTxn(txn, "max_committed_txn_id", int64(txnID)); err != nil {
			return err
		}
		_, err = s.counters.IncInTxn(txn, "committed_txn_count", 1)
		return err
	})
}

// AbortTransaction deletes a transaction record
func (s *BadgerMetaStore) AbortTransaction(txnID uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Get existing record to find seq_num if committed
		item, err := txn.Get(txnKey(txnID))
		if err == badger.ErrKeyNotFound {
			return nil // Already deleted
		}
		if err != nil {
			return err
		}

		var rec TransactionRecord
		err = item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &rec)
		})
		if err != nil {
			return err
		}

		// Delete transaction record
		if err := txn.Delete(txnKey(txnID)); err != nil {
			return err
		}

		// Remove from pending index
		txn.Delete(txnPendingKey(txnID))

		// Remove from sequence index if it had one
		if rec.SeqNum > 0 {
			txn.Delete(txnSeqKey(rec.SeqNum, txnID))
		}

		return nil
	})
}

// GetTransaction retrieves a transaction record by ID
func (s *BadgerMetaStore) GetTransaction(txnID uint64) (*TransactionRecord, error) {
	var rec *TransactionRecord

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(txnKey(txnID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		rec = &TransactionRecord{}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, rec)
		})
	})

	return rec, err
}

// GetPendingTransactions retrieves all PENDING transactions
func (s *BadgerMetaStore) GetPendingTransactions() ([]*TransactionRecord, error) {
	var records []*TransactionRecord
	prefix := []byte(prefixTxnPending)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keyStr := string(it.Item().Key())
			var txnID uint64
			fmt.Sscanf(keyStr[len(prefixTxnPending):], "%016x", &txnID)

			rec, err := s.getTransactionInTxn(txn, txnID)
			if err == nil && rec != nil && rec.Status == MetaTxnStatusPending {
				records = append(records, rec)
			}
		}
		return nil
	})

	return records, err
}

// getTransactionInTxn retrieves a transaction within an existing badger transaction
func (s *BadgerMetaStore) getTransactionInTxn(txn *badger.Txn, txnID uint64) (*TransactionRecord, error) {
	item, err := txn.Get(txnKey(txnID))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rec := &TransactionRecord{}
	err = item.Value(func(val []byte) error {
		return msgpack.Unmarshal(val, rec)
	})
	return rec, err
}

// Heartbeat updates the last_heartbeat timestamp
func (s *BadgerMetaStore) Heartbeat(txnID uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(txnKey(txnID))
		if err != nil {
			return err
		}

		var rec TransactionRecord
		err = item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &rec)
		})
		if err != nil {
			return err
		}

		rec.LastHeartbeat = time.Now().UnixNano()
		data, err := msgpack.Marshal(&rec)
		if err != nil {
			return err
		}

		return txn.Set(txnKey(txnID), data)
	})
}

// WriteIntent creates a write intent (distributed lock)
func (s *BadgerMetaStore) WriteIntent(txnID uint64, tableName, rowKey, op, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error {
	key := intentKey(tableName, rowKey)

	return s.db.Update(func(txn *badger.Txn) error {
		// Check if intent already exists and handle conflicts
		if err := s.handleExistingIntent(txn, key, txnID, tableName, rowKey, op, sqlStmt, data); err != nil {
			if err == errIntentUpdated {
				return nil // Same-txn update handled
			}
			return err
		}

		// Create new intent
		return s.createIntent(txn, key, txnID, tableName, rowKey, op, sqlStmt, data, ts, nodeID)
	})
}

// errIntentUpdated is a sentinel error indicating same-txn intent was updated
var errIntentUpdated = fmt.Errorf("intent updated")

// handleExistingIntent checks for existing intents and resolves conflicts.
// Returns nil if we can create a new intent, errIntentUpdated if same-txn update was done,
// or an error for conflicts.
func (s *BadgerMetaStore) handleExistingIntent(txn *badger.Txn, key []byte, txnID uint64, tableName, rowKey, op, sqlStmt string, data []byte) error {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil // No existing intent, proceed with creation
	}
	if err != nil {
		return err
	}

	var existing WriteIntentRecord
	if err := item.Value(func(val []byte) error {
		return msgpack.Unmarshal(val, &existing)
	}); err != nil {
		return err
	}

	// Same transaction - update intent in place
	if existing.TxnID == txnID {
		existing.Operation = op
		existing.SQLStatement = sqlStmt
		existing.DataSnapshot = data
		existing.CreatedAt = time.Now().UnixNano()
		newData, err := msgpack.Marshal(&existing)
		if err != nil {
			return err
		}
		if err := txn.Set(key, newData); err != nil {
			return err
		}
		return errIntentUpdated
	}

	// Different transaction - check if we can overwrite
	if err := s.resolveIntentConflict(txn, &existing, txnID, tableName, rowKey); err != nil {
		return err
	}

	return nil // Conflict resolved, proceed with new intent creation
}

// resolveIntentConflict handles conflict with existing intent from different transaction.
// Returns nil if conflict resolved (can overwrite), or error if active conflict.
func (s *BadgerMetaStore) resolveIntentConflict(txn *badger.Txn, existing *WriteIntentRecord, txnID uint64, tableName, rowKey string) error {
	// Marked for cleanup - safe to overwrite
	if existing.MarkedForCleanup {
		txn.Delete(intentByTxnKey(existing.TxnID, tableName, rowKey))
		return nil
	}

	// Check conflicting transaction status
	conflictTxnRec, _ := s.getTransactionInTxn(txn, existing.TxnID)

	canOverwrite := false
	switch {
	case conflictTxnRec == nil:
		// Transaction record doesn't exist - orphaned intent
		log.Debug().
			Uint64("orphan_txn_id", existing.TxnID).
			Str("table", tableName).
			Str("row_key", rowKey).
			Msg("Cleaning up orphaned intent (no transaction record)")
		canOverwrite = true

	case conflictTxnRec.Status == MetaTxnStatusCommitted:
		canOverwrite = true

	case conflictTxnRec.Status == MetaTxnStatusAborted:
		log.Debug().
			Uint64("aborted_txn_id", existing.TxnID).
			Str("table", tableName).
			Str("row_key", rowKey).
			Msg("Cleaning up intent from aborted transaction")
		canOverwrite = true

	default:
		// Check heartbeat timeout
		canOverwrite = s.isIntentStale(conflictTxnRec, existing.TxnID, tableName, rowKey)
	}

	if !canOverwrite {
		return fmt.Errorf("write-write conflict: row %s:%s locked by transaction %d (current txn: %d)",
			tableName, rowKey, existing.TxnID, txnID)
	}

	txn.Delete(intentByTxnKey(existing.TxnID, tableName, rowKey))
	return nil
}

// isIntentStale checks if a transaction's intent is stale due to heartbeat timeout.
func (s *BadgerMetaStore) isIntentStale(txnRec *TransactionRecord, txnID uint64, tableName, rowKey string) bool {
	heartbeatTimeout := int64(10 * time.Second)
	if cfg.Config != nil && cfg.Config.MVCC.HeartbeatTimeoutSeconds > 0 {
		heartbeatTimeout = int64(time.Duration(cfg.Config.MVCC.HeartbeatTimeoutSeconds) * time.Second)
	}

	timeSinceHeartbeat := time.Now().UnixNano() - txnRec.LastHeartbeat
	if timeSinceHeartbeat > heartbeatTimeout {
		log.Debug().
			Uint64("stale_txn_id", txnID).
			Str("table", tableName).
			Str("row_key", rowKey).
			Int64("heartbeat_age_ms", timeSinceHeartbeat/1e6).
			Msg("Cleaning up stale intent (heartbeat timeout)")
		return true
	}
	return false
}

// createIntent creates a new write intent with its secondary index.
func (s *BadgerMetaStore) createIntent(txn *badger.Txn, key []byte, txnID uint64, tableName, rowKey, op, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error {
	rec := &WriteIntentRecord{
		TableName:    tableName,
		RowKey:       rowKey,
		TxnID:        txnID,
		TSWall:       ts.WallTime,
		TSLogical:    ts.Logical,
		NodeID:       nodeID,
		Operation:    op,
		SQLStatement: sqlStmt,
		DataSnapshot: data,
		CreatedAt:    time.Now().UnixNano(),
	}

	recData, err := msgpack.Marshal(rec)
	if err != nil {
		return err
	}

	if err := txn.Set(key, recData); err != nil {
		return err
	}

	return txn.Set(intentByTxnKey(txnID, tableName, rowKey), nil)
}

// ValidateIntent checks if the intent is still held by the expected transaction
func (s *BadgerMetaStore) ValidateIntent(tableName, rowKey string, expectedTxnID uint64) (bool, error) {
	var valid bool

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(intentKey(tableName, rowKey))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		var rec WriteIntentRecord
		err = item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &rec)
		})
		if err != nil {
			return err
		}

		valid = rec.TxnID == expectedTxnID
		return nil
	})

	return valid, err
}

// DeleteIntent removes a specific write intent
func (s *BadgerMetaStore) DeleteIntent(tableName, rowKey string, txnID uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		key := intentKey(tableName, rowKey)

		// Verify the intent belongs to this transaction
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		var rec WriteIntentRecord
		err = item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &rec)
		})
		if err != nil {
			return err
		}

		if rec.TxnID != txnID {
			return nil // Intent belongs to different transaction
		}

		// Delete primary intent
		if err := txn.Delete(key); err != nil {
			return err
		}

		// Delete secondary index
		return txn.Delete(intentByTxnKey(txnID, tableName, rowKey))
	})
}

// DeleteIntentsByTxn removes all write intents for a transaction using WriteBatch
// WriteBatch is 7x faster than serial db.Update() for bulk deletes
func (s *BadgerMetaStore) DeleteIntentsByTxn(txnID uint64) error {
	prefix := []byte(fmt.Sprintf("%s%016x/", prefixIntentByTxn, txnID))

	// Collect keys to delete
	var primaryKeys [][]byte
	var indexKeys [][]byte

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			indexKey := append([]byte{}, it.Item().Key()...)
			indexKeys = append(indexKeys, indexKey)

			// Parse table/rowKey from index key
			keyStr := string(indexKey)
			suffix := keyStr[len(fmt.Sprintf("%s%016x/", prefixIntentByTxn, txnID)):]
			parts := strings.SplitN(suffix, "/", 2)
			if len(parts) == 2 {
				primaryKeys = append(primaryKeys, intentKey(parts[0], parts[1]))
			}
		}
		return nil
	})

	if err != nil || len(primaryKeys) == 0 {
		return err
	}

	// Use WriteBatch for bulk deletes - 7x faster than db.Update()
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, key := range primaryKeys {
		if err := wb.Delete(key); err != nil {
			return err
		}
	}
	for _, key := range indexKeys {
		if err := wb.Delete(key); err != nil {
			return err
		}
	}

	return wb.Flush()
}

// MarkIntentsForCleanup marks all intents for a transaction as ready for overwrite
// This is faster than deleting and allows immediate reuse of the row keys
func (s *BadgerMetaStore) MarkIntentsForCleanup(txnID uint64) error {
	prefix := []byte(fmt.Sprintf("%s%016x/", prefixIntentByTxn, txnID))

	// Collect primary intent keys
	var primaryKeys [][]byte

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keyStr := string(it.Item().Key())
			suffix := keyStr[len(fmt.Sprintf("%s%016x/", prefixIntentByTxn, txnID)):]
			parts := strings.SplitN(suffix, "/", 2)
			if len(parts) == 2 {
				primaryKeys = append(primaryKeys, intentKey(parts[0], parts[1]))
			}
		}
		return nil
	})

	if err != nil || len(primaryKeys) == 0 {
		return err
	}

	// Mark each intent for cleanup
	return s.db.Update(func(txn *badger.Txn) error {
		for _, key := range primaryKeys {
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}

			var rec WriteIntentRecord
			err = item.Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &rec)
			})
			if err != nil {
				return err
			}

			// Mark for cleanup
			rec.MarkedForCleanup = true
			data, err := msgpack.Marshal(&rec)
			if err != nil {
				return err
			}
			if err := txn.Set(key, data); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetIntentsByTxn retrieves all write intents for a transaction
func (s *BadgerMetaStore) GetIntentsByTxn(txnID uint64) ([]*WriteIntentRecord, error) {
	var intents []*WriteIntentRecord
	prefix := []byte(fmt.Sprintf("%s%016x/", prefixIntentByTxn, txnID))

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keyStr := string(it.Item().Key())
			suffix := keyStr[len(fmt.Sprintf("%s%016x/", prefixIntentByTxn, txnID)):]
			parts := strings.SplitN(suffix, "/", 2)
			if len(parts) != 2 {
				continue
			}

			// Fetch primary intent
			primaryKey := intentKey(parts[0], parts[1])
			item, err := txn.Get(primaryKey)
			if err != nil {
				continue
			}

			intent := &WriteIntentRecord{}
			err = item.Value(func(val []byte) error {
				return msgpack.Unmarshal(val, intent)
			})
			if err == nil && intent.TxnID == txnID {
				intents = append(intents, intent)
			}
		}
		return nil
	})

	return intents, err
}

// GetIntent retrieves a specific write intent
func (s *BadgerMetaStore) GetIntent(tableName, rowKey string) (*WriteIntentRecord, error) {
	var intent *WriteIntentRecord

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(intentKey(tableName, rowKey))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		intent = &WriteIntentRecord{}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, intent)
		})
	})

	return intent, err
}

// CreateMVCCVersion creates an MVCC version record
func (s *BadgerMetaStore) CreateMVCCVersion(tableName, rowKey string, ts hlc.Timestamp, nodeID, txnID uint64, op string, data []byte) error {
	rec := &MVCCVersionRecord{
		TableName:    tableName,
		RowKey:       rowKey,
		TSWall:       ts.WallTime,
		TSLogical:    ts.Logical,
		NodeID:       nodeID,
		TxnID:        txnID,
		Operation:    op,
		DataSnapshot: data,
		CreatedAt:    time.Now().UnixNano(),
	}

	recData, err := msgpack.Marshal(rec)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(mvccKey(tableName, rowKey, ts, nodeID), recData)
	})
}

// GetLatestVersion retrieves the latest MVCC version for a row
func (s *BadgerMetaStore) GetLatestVersion(tableName, rowKey string) (*MVCCVersionRecord, error) {
	var latest *MVCCVersionRecord
	prefix := mvccPrefix(tableName, rowKey)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to end of prefix range
		endKey := append(prefix, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
		it.Seek(endKey)

		if it.ValidForPrefix(prefix) {
			latest = &MVCCVersionRecord{}
			return it.Item().Value(func(val []byte) error {
				return msgpack.Unmarshal(val, latest)
			})
		}
		return nil
	})

	return latest, err
}

// GetMVCCVersionCount returns the number of MVCC versions for a row
func (s *BadgerMetaStore) GetMVCCVersionCount(tableName, rowKey string) (int, error) {
	count := 0
	prefix := mvccPrefix(tableName, rowKey)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})

	return count, err
}

// GetReplicationState retrieves replication state for a peer
func (s *BadgerMetaStore) GetReplicationState(peerNodeID uint64, dbName string) (*ReplicationStateRecord, error) {
	var state *ReplicationStateRecord

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(replKey(peerNodeID, dbName))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		state = &ReplicationStateRecord{}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, state)
		})
	})

	return state, err
}

// UpdateReplicationState updates replication state for a peer
func (s *BadgerMetaStore) UpdateReplicationState(peerNodeID uint64, dbName string, lastTxnID uint64, lastTS hlc.Timestamp) error {
	state := &ReplicationStateRecord{
		PeerNodeID:           peerNodeID,
		DatabaseName:         dbName,
		LastAppliedTxnID:     lastTxnID,
		LastAppliedTSWall:    lastTS.WallTime,
		LastAppliedTSLogical: lastTS.Logical,
		LastSyncTime:         time.Now().UnixNano(),
		SyncStatus:           MetaSyncStatusSynced,
	}

	data, err := msgpack.Marshal(state)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(replKey(peerNodeID, dbName), data)
	})
}

// GetMinAppliedTxnID returns the minimum applied txn_id across all peers for a database
func (s *BadgerMetaStore) GetMinAppliedTxnID(dbName string) (uint64, error) {
	var minTxnID uint64 = ^uint64(0) // Max uint64
	found := false
	prefix := []byte(prefixRepl)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var state ReplicationStateRecord
			err := it.Item().Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &state)
			})
			if err != nil {
				continue
			}

			if state.DatabaseName == dbName {
				found = true
				if state.LastAppliedTxnID < minTxnID {
					minTxnID = state.LastAppliedTxnID
				}
			}
		}
		return nil
	})

	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	return minTxnID, nil
}

// GetAllReplicationStates returns all replication state records
func (s *BadgerMetaStore) GetAllReplicationStates() ([]*ReplicationStateRecord, error) {
	var states []*ReplicationStateRecord
	prefix := []byte(prefixRepl)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			state := &ReplicationStateRecord{}
			err := it.Item().Value(func(val []byte) error {
				return msgpack.Unmarshal(val, state)
			})
			if err != nil {
				continue
			}
			states = append(states, state)
		}
		return nil
	})

	return states, err
}

// schemaVersionRecord is internal storage for schema
type schemaVersionRecord struct {
	Version   int64
	LastDDL   string
	TxnID     uint64
	UpdatedAt int64
}

// GetSchemaVersion retrieves the schema version for a database
func (s *BadgerMetaStore) GetSchemaVersion(dbName string) (int64, error) {
	var version int64

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(schemaKey(dbName))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		var rec schemaVersionRecord
		err = item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &rec)
		})
		if err != nil {
			return err
		}
		version = rec.Version
		return nil
	})

	return version, err
}

// UpdateSchemaVersion updates the schema version for a database
func (s *BadgerMetaStore) UpdateSchemaVersion(dbName string, version int64, ddlSQL string, txnID uint64) error {
	rec := &schemaVersionRecord{
		Version:   version,
		LastDDL:   ddlSQL,
		TxnID:     txnID,
		UpdatedAt: time.Now().UnixNano(),
	}

	data, err := msgpack.Marshal(rec)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(schemaKey(dbName), data)
	})
}

// GetAllSchemaVersions returns all schema versions indexed by database name
func (s *BadgerMetaStore) GetAllSchemaVersions() (map[string]int64, error) {
	versions := make(map[string]int64)
	prefix := []byte(prefixSchema)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// Extract dbName from key: "/schema/{dbName}"
			dbName := string(key[len(prefix):])

			var rec schemaVersionRecord
			err := item.Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &rec)
			})
			if err != nil {
				continue // Skip corrupted entries
			}
			versions[dbName] = rec.Version
		}
		return nil
	})

	return versions, err
}

// ddlLockRecord is internal storage for DDL locks
type ddlLockRecord struct {
	NodeID    uint64
	LockedAt  int64
	ExpiresAt int64
}

// TryAcquireDDLLock attempts to acquire a DDL lock for a database
func (s *BadgerMetaStore) TryAcquireDDLLock(dbName string, nodeID uint64, leaseDuration time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	expiresAt := now + leaseDuration.Nanoseconds()
	acquired := false

	err := s.db.Update(func(txn *badger.Txn) error {
		key := ddlLockKey(dbName)
		item, err := txn.Get(key)

		if err == badger.ErrKeyNotFound {
			// No lock exists - acquire it
			rec := &ddlLockRecord{
				NodeID:    nodeID,
				LockedAt:  now,
				ExpiresAt: expiresAt,
			}
			data, err := msgpack.Marshal(rec)
			if err != nil {
				return err
			}
			acquired = true
			return txn.Set(key, data)
		}

		if err != nil {
			return err
		}

		// Lock exists - check if expired
		var rec ddlLockRecord
		err = item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &rec)
		})
		if err != nil {
			return err
		}

		if rec.ExpiresAt < now {
			// Lock expired - acquire it
			newRec := &ddlLockRecord{
				NodeID:    nodeID,
				LockedAt:  now,
				ExpiresAt: expiresAt,
			}
			data, err := msgpack.Marshal(newRec)
			if err != nil {
				return err
			}
			acquired = true
			return txn.Set(key, data)
		}

		// Lock still held by another node
		return nil
	})

	return acquired, err
}

// ReleaseDDLLock releases a DDL lock
func (s *BadgerMetaStore) ReleaseDDLLock(dbName string, nodeID uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		key := ddlLockKey(dbName)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		var rec ddlLockRecord
		err = item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &rec)
		})
		if err != nil {
			return err
		}

		if rec.NodeID != nodeID {
			return nil // Lock held by different node
		}

		return txn.Delete(key)
	})
}

// WriteIntentEntry writes a CDC intent entry
func (s *BadgerMetaStore) WriteIntentEntry(txnID, seq uint64, op uint8, table, rowKey string, oldVals, newVals []byte) error {
	entry := &IntentEntry{
		TxnID:     txnID,
		Seq:       seq,
		Operation: op,
		Table:     table,
		RowKey:    rowKey,
		CreatedAt: time.Now().UnixNano(),
	}

	// Store oldVals and newVals as msgpack
	if oldVals != nil {
		if err := msgpack.Unmarshal(oldVals, &entry.OldValues); err != nil {
			log.Error().Err(err).Uint64("txn_id", txnID).Str("table", table).Msg("Failed to unmarshal OldValues")
			return fmt.Errorf("failed to unmarshal old values: %w", err)
		}
	}
	if newVals != nil {
		if err := msgpack.Unmarshal(newVals, &entry.NewValues); err != nil {
			log.Error().Err(err).Uint64("txn_id", txnID).Str("table", table).Msg("Failed to unmarshal NewValues")
			return fmt.Errorf("failed to unmarshal new values: %w", err)
		}
	}

	log.Debug().
		Uint64("txn_id", txnID).
		Uint64("seq", seq).
		Uint8("op", op).
		Str("table", table).
		Str("row_key", rowKey).
		Int("old_vals_len", len(oldVals)).
		Int("new_vals_len", len(newVals)).
		Int("old_values_count", len(entry.OldValues)).
		Int("new_values_count", len(entry.NewValues)).
		Msg("CDC: WriteIntentEntry")

	data, err := msgpack.Marshal(entry)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(cdcKey(txnID, seq), data)
	})
}

// GetIntentEntries retrieves CDC intent entries for a transaction
func (s *BadgerMetaStore) GetIntentEntries(txnID uint64) ([]*IntentEntry, error) {
	var entries []*IntentEntry
	prefix := cdcPrefix(txnID)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			entry := &IntentEntry{}
			err := it.Item().Value(func(val []byte) error {
				return msgpack.Unmarshal(val, entry)
			})
			if err != nil {
				continue
			}
			entries = append(entries, entry)
		}
		return nil
	})

	// Sort by sequence number
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Seq < entries[j].Seq
	})

	// Log what we found
	totalOldVals := 0
	totalNewVals := 0
	for _, e := range entries {
		totalOldVals += len(e.OldValues)
		totalNewVals += len(e.NewValues)
	}
	log.Debug().
		Uint64("txn_id", txnID).
		Int("entries_count", len(entries)).
		Int("total_old_values", totalOldVals).
		Int("total_new_values", totalNewVals).
		Msg("CDC: GetIntentEntries")

	return entries, err
}

// DeleteIntentEntries deletes CDC intent entries for a transaction using WriteBatch
func (s *BadgerMetaStore) DeleteIntentEntries(txnID uint64) error {
	prefix := cdcPrefix(txnID)
	var keys [][]byte

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, append([]byte{}, it.Item().Key()...))
		}
		return nil
	})

	if err != nil || len(keys) == 0 {
		return err
	}

	// Use WriteBatch for bulk deletes
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, key := range keys {
		if err := wb.Delete(key); err != nil {
			return err
		}
	}

	return wb.Flush()
}

// CleanupStaleTransactions aborts transactions that haven't had a heartbeat within the timeout
func (s *BadgerMetaStore) CleanupStaleTransactions(timeout time.Duration) (int, error) {
	cutoff := time.Now().Add(-timeout).UnixNano()
	cleaned := 0

	// Phase 1: Find stale PENDING transactions
	var staleTxnIDs []uint64
	prefix := []byte(prefixTxnPending)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keyStr := string(it.Item().Key())
			var txnID uint64
			fmt.Sscanf(keyStr[len(prefixTxnPending):], "%016x", &txnID)

			rec, err := s.getTransactionInTxn(txn, txnID)
			if err != nil || rec == nil {
				continue
			}

			if rec.LastHeartbeat < cutoff {
				staleTxnIDs = append(staleTxnIDs, txnID)
			}
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	// Delete stale transactions and their intents
	for _, txnID := range staleTxnIDs {
		s.AbortTransaction(txnID)
		s.DeleteIntentsByTxn(txnID)
		s.DeleteIntentEntries(txnID)
		cleaned++
	}

	// Phase 2: Clean orphaned intents
	var orphanedKeys [][]byte
	intentPrefix := []byte(prefixIntent)

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = intentPrefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(intentPrefix); it.ValidForPrefix(intentPrefix); it.Next() {
			item := it.Item()
			var rec WriteIntentRecord
			err := item.Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &rec)
			})
			if err != nil {
				continue
			}

			// Check if intent is old enough
			if rec.CreatedAt >= cutoff {
				continue
			}

			// Check if transaction exists
			txnRec, _ := s.getTransactionInTxn(txn, rec.TxnID)
			if txnRec == nil {
				orphanedKeys = append(orphanedKeys, append([]byte{}, item.Key()...))
			}
		}
		return nil
	})

	if err != nil {
		return cleaned, err
	}

	// Delete orphaned intents
	if len(orphanedKeys) > 0 {
		s.db.Update(func(txn *badger.Txn) error {
			for _, key := range orphanedKeys {
				txn.Delete(key)
				cleaned++
			}
			return nil
		})
	}

	if cleaned > 0 {
		log.Info().
			Int("stale_txns", len(staleTxnIDs)).
			Int("orphaned_intents", len(orphanedKeys)).
			Msg("MetaStore GC: Cleaned up stale transactions and orphaned intents")
	}

	return cleaned, nil
}

// CleanupOldTransactionRecords removes old COMMITTED/ABORTED transaction records
func (s *BadgerMetaStore) CleanupOldTransactionRecords(minRetention, maxRetention time.Duration, minAppliedTxnID, minAppliedSeqNum uint64) (int, error) {
	now := time.Now()
	minRetentionCutoff := now.Add(-minRetention).UnixNano()
	maxRetentionCutoff := now.Add(-maxRetention).UnixNano()
	deleted := 0
	committedDeleted := 0 // Track committed transactions for counter update

	prefix := []byte(prefixTxn)
	var keysToDelete [][]byte
	var seqKeysToDelete [][]byte

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var rec TransactionRecord
			err := it.Item().Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &rec)
			})
			if err != nil {
				continue
			}

			// Only clean COMMITTED or ABORTED
			if rec.Status != MetaTxnStatusCommitted && rec.Status != MetaTxnStatusAborted {
				continue
			}

			// Check deletion criteria
			shouldDelete := false
			if rec.CreatedAt < maxRetentionCutoff {
				// Beyond max retention - always delete
				shouldDelete = true
			} else if rec.CreatedAt < minRetentionCutoff {
				// Within min-max window - check constraints
				if minAppliedTxnID > 0 && minAppliedSeqNum > 0 {
					// Both constraints
					if rec.TxnID < minAppliedTxnID && (rec.SeqNum == 0 || rec.SeqNum < minAppliedSeqNum) {
						shouldDelete = true
					}
				} else if minAppliedTxnID > 0 {
					if rec.TxnID < minAppliedTxnID {
						shouldDelete = true
					}
				} else if minAppliedSeqNum > 0 {
					if rec.SeqNum == 0 || rec.SeqNum < minAppliedSeqNum {
						shouldDelete = true
					}
				}
			}

			if shouldDelete {
				keysToDelete = append(keysToDelete, append([]byte{}, it.Item().Key()...))
				if rec.SeqNum > 0 {
					seqKeysToDelete = append(seqKeysToDelete, txnSeqKey(rec.SeqNum, rec.TxnID))
				}
				if rec.Status == MetaTxnStatusCommitted {
					committedDeleted++
				}
			}
		}
		return nil
	})

	if err != nil || len(keysToDelete) == 0 {
		return 0, err
	}

	// Use WriteBatch for bulk deletes - 7x faster than db.Update()
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, key := range keysToDelete {
		if err := wb.Delete(key); err == nil {
			deleted++
		}
	}
	for _, key := range seqKeysToDelete {
		wb.Delete(key)
	}

	if err := wb.Flush(); err != nil {
		return 0, err
	}

	// Decrement committed transaction counter via PersistentCounter
	if committedDeleted > 0 {
		if _, err := s.counters.Dec("committed_txn_count", int64(committedDeleted)); err != nil {
			log.Warn().Err(err).Int("count", committedDeleted).Msg("MetaStore GC: Failed to decrement counter")
		}
	}

	if deleted > 0 {
		log.Info().Int("deleted_records", deleted).Int("committed_deleted", committedDeleted).Msg("MetaStore GC: Cleaned up old transaction records")
	}

	return deleted, nil
}

// CleanupOldMVCCVersions removes old MVCC versions, keeping the latest N versions per row.
// Uses stream-based processing to avoid loading all versions into memory.
// Keys are sorted as /mvcc/{tableName}/{rowKey}/{timestamp}, so we process one row at a time.
func (s *BadgerMetaStore) CleanupOldMVCCVersions(keepVersions int) (int, error) {
	prefix := []byte(prefixMVCC)
	prefixLen := len(prefixMVCC)

	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	deleted := 0
	var currentRowPrefix string
	var currentRowKeys [][]byte // Keys for current row, sorted by timestamp ascending (older first)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Only need keys, not values
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keyStr := string(key)

			// Parse row prefix: /mvcc/{tableName}/{rowKey}/
			rowPrefix := extractMVCCRowPrefix(keyStr, prefixLen)
			if rowPrefix == "" {
				continue
			}

			if rowPrefix != currentRowPrefix {
				// New row - process previous row's versions
				if len(currentRowKeys) > keepVersions {
					// Keys sorted ascending by timestamp (older first)
					// Keep last N (newest), delete first (len - keepVersions) entries
					deleteCount := len(currentRowKeys) - keepVersions
					for i := 0; i < deleteCount; i++ {
						if err := wb.Delete(currentRowKeys[i]); err == nil {
							deleted++
						}
					}
				}
				// Start new row
				currentRowPrefix = rowPrefix
				currentRowKeys = currentRowKeys[:0] // Reuse slice
			}

			currentRowKeys = append(currentRowKeys, key)
		}

		// Process final row
		if len(currentRowKeys) > keepVersions {
			deleteCount := len(currentRowKeys) - keepVersions
			for i := 0; i < deleteCount; i++ {
				if err := wb.Delete(currentRowKeys[i]); err == nil {
					deleted++
				}
			}
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	if deleted == 0 {
		return 0, nil
	}

	if err := wb.Flush(); err != nil {
		return 0, err
	}

	log.Info().Int("deleted_versions", deleted).Msg("MetaStore GC: Cleaned up old MVCC versions")
	return deleted, nil
}

// extractMVCCRowPrefix extracts the row prefix from an MVCC key.
// Key format: /mvcc/{tableName}/{rowKey}/{timestamp}
// Returns: /mvcc/{tableName}/{rowKey}/ or empty string on error
func extractMVCCRowPrefix(key string, prefixLen int) string {
	// Find position after tableName (first / after prefix)
	idx1 := strings.Index(key[prefixLen:], "/")
	if idx1 < 0 {
		return ""
	}
	idx1 += prefixLen

	// Find position after rowKey (second / after prefix)
	idx2 := strings.Index(key[idx1+1:], "/")
	if idx2 < 0 {
		return ""
	}
	idx2 += idx1 + 1

	return key[:idx2+1]
}

// seqBandwidth is the number of sequence numbers to pre-allocate at once
// Higher = less contention but more potential gaps on restart
const seqBandwidth = 1000

// GetNextSeqNum returns the next sequence number for a node using contention-free Sequence API
func (s *BadgerMetaStore) GetNextSeqNum(nodeID uint64) (uint64, error) {
	seq, err := s.getOrCreateSequence(nodeID)
	if err != nil {
		return 0, err
	}
	num, err := seq.Next()
	if err != nil {
		return 0, err
	}
	// Badger sequences start at 0, we want 1-based
	return num + 1, nil
}

// getOrCreateSequence returns or creates a Badger Sequence for the given nodeID
func (s *BadgerMetaStore) getOrCreateSequence(nodeID uint64) (*badger.Sequence, error) {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()

	if seq, ok := s.sequences[nodeID]; ok {
		return seq, nil
	}

	// Create new sequence with the seqKey prefix
	seq, err := s.db.GetSequence(seqKey(nodeID), seqBandwidth)
	if err != nil {
		return nil, fmt.Errorf("failed to create sequence for node %d: %w", nodeID, err)
	}

	s.sequences[nodeID] = seq
	return seq, nil
}

// GetMaxSeqNum returns the maximum sequence number across all committed transactions
func (s *BadgerMetaStore) GetMaxSeqNum() (uint64, error) {
	var maxSeq uint64
	prefix := []byte(prefixTxnSeq)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.Reverse = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to end of prefix range
		endKey := append(prefix, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
		it.Seek(endKey)

		if it.ValidForPrefix(prefix) {
			keyStr := string(it.Item().Key())
			parts := strings.Split(keyStr[len(prefixTxnSeq):], "/")
			if len(parts) >= 1 {
				fmt.Sscanf(parts[0], "%016x", &maxSeq)
			}
		}
		return nil
	})

	return maxSeq, err
}

// GetMinAppliedSeqNum returns the minimum applied sequence number across all peers for a database
func (s *BadgerMetaStore) GetMinAppliedSeqNum(dbName string) (uint64, error) {
	// For now, proxy through GetMinAppliedTxnID since we track by txn_id
	return s.GetMinAppliedTxnID(dbName)
}

// GetMaxCommittedTxnID returns the maximum committed transaction ID.
// Uses O(1) counter lookup instead of full table scan.
func (s *BadgerMetaStore) GetMaxCommittedTxnID() (uint64, error) {
	return s.counters.LoadUint64("max_committed_txn_id")
}

// GetCommittedTxnCount returns the count of committed transactions.
// Uses O(1) counter lookup instead of full table scan.
func (s *BadgerMetaStore) GetCommittedTxnCount() (int64, error) {
	return s.counters.Load("committed_txn_count")
}

// StreamCommittedTransactions streams committed transactions after fromTxnID
func (s *BadgerMetaStore) StreamCommittedTransactions(fromTxnID uint64, callback func(*TransactionRecord) error) error {
	prefix := []byte(prefixTxnSeq)

	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keyStr := string(it.Item().Key())
			parts := strings.Split(keyStr[len(prefixTxnSeq):], "/")
			if len(parts) != 2 {
				continue
			}

			var txnID uint64
			fmt.Sscanf(parts[1], "%016x", &txnID)

			if txnID <= fromTxnID {
				continue
			}

			rec, err := s.getTransactionInTxn(txn, txnID)
			if err != nil || rec == nil {
				continue
			}

			if rec.Status != MetaTxnStatusCommitted {
				continue
			}

			if err := callback(rec); err != nil {
				return err
			}
		}
		return nil
	})
}
