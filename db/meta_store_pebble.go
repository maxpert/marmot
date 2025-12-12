package db

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

// Key prefixes for Pebble (same as BadgerDB, sorted for efficient iteration)
const (
	pebblePrefixTxn         = "/txn/"           // /txn/{txnID:016x}
	pebblePrefixTxnPending  = "/txn_idx/pend/"  // /txn_idx/pend/{txnID:016x}
	pebblePrefixTxnSeq      = "/txn_idx/seq/"   // /txn_idx/seq/{seqNum:016x}/{txnID:016x}
	pebblePrefixIntent      = "/intent/"        // /intent/{tableName}/{rowKey}
	pebblePrefixCDC         = "/cdc/"           // /cdc/{txnID:016x}/{seq:08x}
	pebblePrefixRepl        = "/repl/"          // /repl/{peerNodeID:016x}/{dbName}
	pebblePrefixSchema      = "/schema/"        // /schema/{dbName}
	pebblePrefixDDLLock     = "/ddl/"           // /ddl/{dbName}
	pebblePrefixSeq         = "/seq/"           // /seq/{name}
	pebblePrefixIntentByTxn = "/intent_txn/"    // /intent_txn/{txnID:016x}/{tableName}/{rowKey}
	pebblePrefixCounter     = "/meta/"          // /meta/{counterName}
	pebblePrefixCDCActive   = "/cdc/active/"    // /cdc/active/{tableName}/{rowKey|__ddl__}
	pebblePrefixCDCTxnLocks = "/cdc/txn_locks/" // /cdc/txn_locks/{txnID:016x}/{tableName}/{rowKey}

	pebbleCDCDDLKeyMarker = "__ddl__" // Sentinel key for DDL locks
)

// Group commit configuration (same as BadgerDB)
const (
	pebbleBatchMaxSize     = 100                  // Max operations per batch
	pebbleBatchMaxWait     = 2 * time.Millisecond // Max time to wait for batch
	pebbleBatchChannelSize = 1000                 // Channel buffer size
)

// Sharded lock for WriteIntent serialization (prevents TOCTOU race)
const intentLockShards = 256

// pebbleBatchOp represents a batched write operation
type pebbleBatchOp struct {
	fn     func(batch *pebble.Batch) error
	result chan error
}

// PebbleMetaStore implements MetaStore using Pebble
type PebbleMetaStore struct {
	db   *pebble.DB
	path string

	// Batch writer
	batchCh     chan *pebbleBatchOp
	stopBatch   chan struct{}
	batchWg     sync.WaitGroup
	batchClosed atomic.Bool

	// Idempotent close
	closed atomic.Bool

	// Sequence generators (Pebble doesn't have native Sequence API)
	sequences map[uint64]*AtomicSequence
	seqMu     sync.Mutex

	// Persistent counters for O(1) lookups
	counters *PebbleCounter

	// Sharded locks for WriteIntent serialization (prevents TOCTOU race)
	intentLocks [intentLockShards]sync.Mutex

	// Cuckoo filter for fast-path intent conflict detection
	intentFilter *IntentFilter
}

// Ensure PebbleMetaStore implements MetaStore
var _ MetaStore = (*PebbleMetaStore)(nil)

// intentLockFor returns the sharded mutex for a given table+rowKey
func (s *PebbleMetaStore) intentLockFor(tableName, rowKey string) *sync.Mutex {
	key := tableName + ":" + rowKey
	return &s.intentLocks[xxhash.Sum64String(key)%intentLockShards]
}

// GetIntentFilter returns the Cuckoo filter for fast-path conflict detection.
func (s *PebbleMetaStore) GetIntentFilter() *IntentFilter {
	return s.intentFilter
}

// rebuildIntentFilter scans all existing intents and populates the filter.
// Called on startup to restore filter state after crash/restart.
func (s *PebbleMetaStore) rebuildIntentFilter() error {
	prefix := []byte(pebblePrefixIntent)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	count := 0
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		var rec WriteIntentRecord
		if err := encoding.Unmarshal(val, &rec); err != nil {
			continue
		}

		// Skip intents marked for cleanup
		if rec.MarkedForCleanup {
			continue
		}

		tbHash := ComputeIntentHash(rec.TableName, rec.RowKey)
		s.intentFilter.Add(rec.TxnID, tbHash)
		count++
	}

	if count > 0 {
		log.Info().Int("intents", count).Msg("Rebuilt intent filter from existing intents")
	}

	return nil
}

// PebbleMetaStoreOptions configures Pebble
type PebbleMetaStoreOptions struct {
	// Memory settings (explicit, no mmap surprise)
	CacheSizeMB    int64 // Block cache size (default: 64MB)
	MemTableSizeMB int64 // Write buffer size (default: 32MB)
	MemTableCount  int   // Number of memtables (default: 2)

	// Write optimization
	WALDir             string        // Separate WAL directory (optional)
	DisableWAL         bool          // Only for testing!
	WALBytesPerSync    int           // Sync WAL every N bytes (default: 512KB)
	WALMinSyncInterval time.Duration // Min delay between syncs (default: 0)

	// Compaction (CockroachDB-tested defaults from cfg.Config)
	L0CompactionThreshold int // L0 files before compaction
	L0StopWrites          int // L0 files to pause writes
	MaxConcurrentCompact  int // Parallel compactors (default: 3)
}

// DefaultPebbleOptions returns Pebble options from cfg.Config.MetaStore.
// All defaults are defined in cfg/config.go (single source of truth).
func DefaultPebbleOptions() PebbleMetaStoreOptions {
	ms := cfg.Config.MetaStore
	return PebbleMetaStoreOptions{
		CacheSizeMB:           ms.CacheSizeMB,
		MemTableSizeMB:        ms.MemTableSizeMB,
		MemTableCount:         ms.MemTableCount,
		WALBytesPerSync:       ms.WALBytesPerSyncKB * 1024,
		WALMinSyncInterval:    time.Duration(ms.WALSyncIntervalMS) * time.Millisecond,
		L0CompactionThreshold: ms.L0CompactionThreshold,
		L0StopWrites:          ms.L0StopWrites,
		MaxConcurrentCompact:  3,
	}
}

// pebbleLogger wraps zerolog for Pebble
type pebbleLogger struct{}

func (l *pebbleLogger) Infof(format string, args ...interface{}) {
	log.Debug().Msgf("[pebble] "+format, args...)
}

func (l *pebbleLogger) Errorf(format string, args ...interface{}) {
	log.Error().Msgf("[pebble] "+format, args...)
}

func (l *pebbleLogger) Fatalf(format string, args ...interface{}) {
	log.Fatal().Msgf("[pebble] "+format, args...)
}

// NewPebbleMetaStore creates a new Pebble-backed MetaStore
func NewPebbleMetaStore(path string, opts PebbleMetaStoreOptions) (*PebbleMetaStore, error) {
	cache := pebble.NewCache(opts.CacheSizeMB << 20)
	defer cache.Unref() // DB will hold reference

	pebbleOpts := &pebble.Options{
		Cache:                       cache,
		MemTableSize:                uint64(opts.MemTableSizeMB << 20),
		MemTableStopWritesThreshold: opts.MemTableCount,
		WALDir:                      opts.WALDir,
		WALBytesPerSync:             opts.WALBytesPerSync,
		DisableWAL:                  opts.DisableWAL,
		L0CompactionThreshold:       opts.L0CompactionThreshold,
		L0StopWritesThreshold:       opts.L0StopWrites,
		MaxConcurrentCompactions:    func() int { return opts.MaxConcurrentCompact },
		Logger:                      &pebbleLogger{},
	}

	// WALMinSyncInterval enables group commit batching (like CockroachDB)
	if opts.WALMinSyncInterval > 0 {
		interval := opts.WALMinSyncInterval
		pebbleOpts.WALMinSyncInterval = func() time.Duration { return interval }
	}

	db, err := pebble.Open(path, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	store := &PebbleMetaStore{
		db:           db,
		path:         path,
		batchCh:      make(chan *pebbleBatchOp, pebbleBatchChannelSize),
		stopBatch:    make(chan struct{}),
		sequences:    make(map[uint64]*AtomicSequence),
		intentFilter: NewIntentFilter(),
	}

	// Initialize persistent counters
	store.counters = NewPebbleCounter(db, pebblePrefixCounter, 10)

	// Rebuild intent filter from existing intents (for crash recovery)
	if err := store.rebuildIntentFilter(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to rebuild intent filter: %w", err)
	}

	// Clear any stale CDC locks from previous crash (CDC locks are ephemeral)
	if err := store.clearAllCDCLocks(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to clear stale CDC locks: %w", err)
	}

	// Start batch writer goroutine
	store.batchWg.Add(1)
	go store.batchWriter()

	return store, nil
}

// batchWriter runs in a goroutine and batches write operations
func (s *PebbleMetaStore) batchWriter() {
	defer s.batchWg.Done()

	ops := make([]*pebbleBatchOp, 0, pebbleBatchMaxSize)
	timer := time.NewTimer(pebbleBatchMaxWait)
	timer.Stop()
	timerRunning := false

	flush := func() {
		if len(ops) == 0 {
			return
		}

		batch := s.db.NewBatch()
		defer batch.Close()

		// Execute all ops in the batch
		for _, op := range ops {
			if opErr := op.fn(batch); opErr != nil {
				op.result <- opErr
			}
		}

		// Commit with sync
		commitErr := batch.Commit(pebble.NoSync)

		// Notify all ops of completion
		for _, op := range ops {
			select {
			case <-op.result:
				// Already sent error above
			default:
				op.result <- commitErr
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

			if len(ops) >= pebbleBatchMaxSize {
				flush()
			} else if !timerRunning {
				timer.Reset(pebbleBatchMaxWait)
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

// Close closes the Pebble DB (idempotent - safe to call multiple times)
func (s *PebbleMetaStore) Close() error {
	// Ensure we only close once
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	// Stop batch writer
	if !s.batchClosed.Swap(true) {
		close(s.stopBatch)
	}

	// Wait for batch writer to finish
	s.batchWg.Wait()

	// Release all sequence generators (persist final values)
	s.seqMu.Lock()
	for _, seq := range s.sequences {
		seq.Close()
	}
	s.sequences = nil
	s.seqMu.Unlock()

	return s.db.Close()
}

// Checkpoint is a no-op for Pebble (no WAL checkpoint like SQLite)
func (s *PebbleMetaStore) Checkpoint() error {
	return nil
}

// IsBusy returns true if the batch channel is over 50% full.
// Used by GC to skip non-critical Phase 2 work under load.
func (s *PebbleMetaStore) IsBusy() bool {
	if s.batchClosed.Load() {
		return false
	}
	return len(s.batchCh) > pebbleBatchChannelSize/2
}

// AtomicSequence provides contention-free sequence number generation.
// Pre-allocates batches of IDs to minimize disk writes.
type AtomicSequence struct {
	db        *pebble.DB
	key       []byte
	bandwidth uint64 // IDs to pre-allocate (e.g., 1000)

	mu       sync.Mutex
	nextVal  uint64 // Next value to return
	leaseEnd uint64 // End of current lease
}

// NewAtomicSequence creates a new sequence generator.
// On startup, reads the persisted lease end and continues from there.
func NewAtomicSequence(db *pebble.DB, key []byte, bandwidth uint64) (*AtomicSequence, error) {
	var leaseEnd uint64

	val, closer, err := db.Get(key)
	if err == nil {
		if len(val) >= 8 {
			leaseEnd = binary.BigEndian.Uint64(val)
		}
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return nil, fmt.Errorf("failed to read sequence: %w", err)
	}

	return &AtomicSequence{
		db:        db,
		key:       key,
		bandwidth: bandwidth,
		nextVal:   leaseEnd, // Resume from where we left off
		leaseEnd:  leaseEnd,
	}, nil
}

// Next returns the next sequence number.
// Pre-allocates a batch of IDs when the current lease is exhausted.
func (s *AtomicSequence) Next() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.nextVal >= s.leaseEnd {
		// Allocate new batch
		newLease := s.leaseEnd + s.bandwidth

		// Persist new lease end to disk
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, newLease)
		if err := s.db.Set(s.key, buf, pebble.NoSync); err != nil {
			return 0, fmt.Errorf("failed to persist sequence: %w", err)
		}

		s.leaseEnd = newLease
	}

	val := s.nextVal
	s.nextVal++
	return val, nil
}

// Close persists any unused portion of the current lease.
// This minimizes gaps on restart.
func (s *AtomicSequence) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Persist current nextVal so we don't have gaps on restart
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, s.nextVal)
	return s.db.Set(s.key, buf, pebble.NoSync)
}

// seqBandwidth is the number of sequence numbers to pre-allocate at once
const pebbleSeqBandwidth = 1000

// Key helper functions

func pebbleTxnKey(txnID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x", pebblePrefixTxn, txnID))
}

func pebbleTxnPendingKey(txnID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x", pebblePrefixTxnPending, txnID))
}

func pebbleTxnSeqKey(seqNum, txnID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x/%016x", pebblePrefixTxnSeq, seqNum, txnID))
}

func pebbleIntentKey(tableName, rowKey string) []byte {
	return []byte(fmt.Sprintf("%s%s/%s", pebblePrefixIntent, tableName, rowKey))
}

func pebbleIntentByTxnKey(txnID uint64, tableName, rowKey string) []byte {
	return []byte(fmt.Sprintf("%s%016x/%s/%s", pebblePrefixIntentByTxn, txnID, tableName, rowKey))
}

func pebbleCdcKey(txnID, seq uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x/%08x", pebblePrefixCDC, txnID, seq))
}

func pebbleCdcPrefix(txnID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x/", pebblePrefixCDC, txnID))
}

func pebbleReplKey(peerNodeID uint64, dbName string) []byte {
	return []byte(fmt.Sprintf("%s%016x/%s", pebblePrefixRepl, peerNodeID, dbName))
}

func pebbleSchemaKey(dbName string) []byte {
	return []byte(fmt.Sprintf("%s%s", pebblePrefixSchema, dbName))
}

func pebbleDdlLockKey(dbName string) []byte {
	return []byte(fmt.Sprintf("%s%s", pebblePrefixDDLLock, dbName))
}

func pebbleSeqKey(nodeID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x", pebblePrefixSeq, nodeID))
}

func pebbleCDCActiveRowKey(tableName, rowKey string) []byte {
	return []byte(fmt.Sprintf("%s%s/%s", pebblePrefixCDCActive, tableName, rowKey))
}

func pebbleCDCActiveDDLKey(tableName string) []byte {
	return []byte(fmt.Sprintf("%s%s/%s", pebblePrefixCDCActive, tableName, pebbleCDCDDLKeyMarker))
}

func pebbleCDCActiveTablePrefix(tableName string) []byte {
	return []byte(fmt.Sprintf("%s%s/", pebblePrefixCDCActive, tableName))
}

// pebbleCDCTxnLockKey returns the reverse index key for a CDC row lock
// Format: /cdc/txn_locks/{txnID:016x}/{tableName}/{rowKey}
func pebbleCDCTxnLockKey(txnID uint64, tableName, rowKey string) []byte {
	return []byte(fmt.Sprintf("%s%016x/%s/%s", pebblePrefixCDCTxnLocks, txnID, tableName, rowKey))
}

// pebbleCDCTxnLockPrefix returns the prefix for all locks held by a transaction
// Format: /cdc/txn_locks/{txnID:016x}/
func pebbleCDCTxnLockPrefix(txnID uint64) []byte {
	return []byte(fmt.Sprintf("%s%016x/", pebblePrefixCDCTxnLocks, txnID))
}

// prefixUpperBound returns prefix + 0xFF... for range iteration
func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix)+8)
	copy(upper, prefix)
	for i := len(prefix); i < len(upper); i++ {
		upper[i] = 0xFF
	}
	return upper
}

// getValueCopy reads a key and returns a copy of the value
func (s *PebbleMetaStore) getValueCopy(key []byte) ([]byte, error) {
	val, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

// BeginTransaction creates a new transaction record with PENDING status
func (s *PebbleMetaStore) BeginTransaction(txnID, nodeID uint64, startTS hlc.Timestamp) error {
	log.Debug().
		Uint64("txn_id", txnID).
		Uint64("node_id", nodeID).
		Int64("start_ts", startTS.WallTime).
		Msg("CDC: BeginTransaction")

	rec := &TransactionRecord{
		TxnID:          txnID,
		NodeID:         nodeID,
		Status:         TxnStatusPending,
		StartTSWall:    startTS.WallTime,
		StartTSLogical: startTS.Logical,
		CreatedAt:      time.Now().UnixNano(),
		LastHeartbeat:  time.Now().UnixNano(),
	}

	data, err := encoding.Marshal(rec)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction record: %w", err)
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(pebbleTxnKey(txnID), data, nil); err != nil {
		return err
	}
	if err := batch.Set(pebbleTxnPendingKey(txnID), nil, nil); err != nil {
		return err
	}

	// NoSync: BeginTransaction is not a durability checkpoint.
	// If crash occurs before PREPARE (WriteIntent), transaction never existed.
	return batch.Commit(pebble.NoSync)
}

// CommitTransaction marks a transaction as COMMITTED
func (s *PebbleMetaStore) CommitTransaction(txnID uint64, commitTS hlc.Timestamp, statements []byte, dbName, tablesInvolved string) error {
	log.Debug().
		Uint64("txn_id", txnID).
		Int64("commit_ts", commitTS.WallTime).
		Str("database", dbName).
		Int("statements_len", len(statements)).
		Msg("CDC: CommitTransaction")

	// Step 1: Read nodeID from transaction record
	recData, err := s.getValueCopy(pebbleTxnKey(txnID))
	if err == pebble.ErrNotFound {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if err != nil {
		return err
	}

	var rec TransactionRecord
	if err := encoding.Unmarshal(recData, &rec); err != nil {
		return err
	}

	// Step 2: Get sequence number using contention-free Sequence API
	seqNum, err := s.GetNextSeqNum(rec.NodeID)
	if err != nil {
		return fmt.Errorf("failed to get next seq_num: %w", err)
	}

	// Step 3: Update transaction record
	rec.Status = TxnStatusCommitted
	rec.CommitTSWall = commitTS.WallTime
	rec.CommitTSLogical = commitTS.Logical
	rec.CommittedAt = time.Now().UnixNano()
	rec.SerializedStatements = statements
	rec.DatabaseName = dbName
	rec.TablesInvolved = tablesInvolved
	rec.SeqNum = seqNum

	data, err := encoding.Marshal(&rec)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	// Update transaction record
	if err := batch.Set(pebbleTxnKey(txnID), data, nil); err != nil {
		return err
	}

	// Remove from pending index
	if err := batch.Delete(pebbleTxnPendingKey(txnID), nil); err != nil {
		return err
	}

	// Add to sequence index
	if err := batch.Set(pebbleTxnSeqKey(seqNum, txnID), nil, nil); err != nil {
		return err
	}

	// Update commit counters (O(1) lookups via PersistentCounter)
	if err := s.counters.UpdateMaxInBatch(batch, "max_committed_txn_id", int64(txnID)); err != nil {
		return err
	}
	if err := s.counters.IncInBatch(batch, "committed_txn_count", 1); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync) // Rely on replication for durability (like CDB/TiKV/FDB)
}

// StoreReplayedTransaction inserts a fully-committed transaction record directly.
func (s *PebbleMetaStore) StoreReplayedTransaction(txnID, nodeID uint64, commitTS hlc.Timestamp, statements []byte, dbName string) error {
	log.Debug().
		Uint64("txn_id", txnID).
		Uint64("node_id", nodeID).
		Int64("commit_ts", commitTS.WallTime).
		Str("database", dbName).
		Int("statements_len", len(statements)).
		Msg("StoreReplayedTransaction: storing replayed transaction")

	// Get sequence number
	seqNum, err := s.GetNextSeqNum(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get next seq_num: %w", err)
	}

	now := time.Now().UnixNano()
	rec := TransactionRecord{
		TxnID:                txnID,
		NodeID:               nodeID,
		SeqNum:               seqNum,
		Status:               TxnStatusCommitted,
		StartTSWall:          commitTS.WallTime,
		StartTSLogical:       commitTS.Logical,
		CommitTSWall:         commitTS.WallTime,
		CommitTSLogical:      commitTS.Logical,
		CreatedAt:            now,
		CommittedAt:          now,
		LastHeartbeat:        now,
		SerializedStatements: statements,
		DatabaseName:         dbName,
	}

	data, err := encoding.Marshal(rec)
	if err != nil {
		return fmt.Errorf("failed to serialize transaction record: %w", err)
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(pebbleTxnKey(txnID), data, nil); err != nil {
		return err
	}
	if err := batch.Set(pebbleTxnSeqKey(seqNum, txnID), nil, nil); err != nil {
		return err
	}
	if err := s.counters.UpdateMaxInBatch(batch, "max_committed_txn_id", int64(txnID)); err != nil {
		return err
	}
	if err := s.counters.IncInBatch(batch, "committed_txn_count", 1); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// AbortTransaction deletes a transaction record
func (s *PebbleMetaStore) AbortTransaction(txnID uint64) error {
	// Get existing record to find seq_num if committed
	recData, err := s.getValueCopy(pebbleTxnKey(txnID))
	if err == pebble.ErrNotFound {
		return nil // Already deleted
	}
	if err != nil {
		return err
	}

	var rec TransactionRecord
	if err := encoding.Unmarshal(recData, &rec); err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	// Delete transaction record
	if err := batch.Delete(pebbleTxnKey(txnID), nil); err != nil {
		return err
	}

	// Remove from pending index (best-effort cleanup)
	_ = batch.Delete(pebbleTxnPendingKey(txnID), nil)

	// Remove from sequence index if it had one (best-effort cleanup)
	if rec.SeqNum > 0 {
		_ = batch.Delete(pebbleTxnSeqKey(rec.SeqNum, txnID), nil)
	}

	// NoSync: AbortTransaction is cleanup. Idempotent - can be redone.
	return batch.Commit(pebble.NoSync)
}

// GetTransaction retrieves a transaction record by ID
func (s *PebbleMetaStore) GetTransaction(txnID uint64) (*TransactionRecord, error) {
	val, closer, err := s.db.Get(pebbleTxnKey(txnID))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	rec := &TransactionRecord{}
	if err := encoding.Unmarshal(val, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// GetPendingTransactions retrieves all PENDING transactions
func (s *PebbleMetaStore) GetPendingTransactions() ([]*TransactionRecord, error) {
	var records []*TransactionRecord
	prefix := []byte(pebblePrefixTxnPending)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		keyStr := string(iter.Key())
		var txnID uint64
		_, _ = fmt.Sscanf(keyStr[len(pebblePrefixTxnPending):], "%016x", &txnID)

		rec, err := s.GetTransaction(txnID)
		if err == nil && rec != nil && rec.Status == TxnStatusPending {
			records = append(records, rec)
		}
	}

	return records, iter.Error()
}

// Heartbeat updates the last_heartbeat timestamp
func (s *PebbleMetaStore) Heartbeat(txnID uint64) error {
	recData, err := s.getValueCopy(pebbleTxnKey(txnID))
	if err != nil {
		return err
	}

	var rec TransactionRecord
	if err := encoding.Unmarshal(recData, &rec); err != nil {
		return err
	}

	rec.LastHeartbeat = time.Now().UnixNano()
	data, err := encoding.Marshal(&rec)
	if err != nil {
		return err
	}

	// NoSync: SaveTransaction happens before PREPARE.
	// If crash before PREPARE (WriteIntent), transaction never existed.
	return s.db.Set(pebbleTxnKey(txnID), data, pebble.NoSync)
}

// WriteIntent creates a write intent (distributed lock)
func (s *PebbleMetaStore) WriteIntent(txnID uint64, intentType IntentType, tableName, rowKey string, op OpType, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error {
	// Acquire sharded lock to serialize concurrent writes to same row (prevents TOCTOU race)
	mu := s.intentLockFor(tableName, rowKey)
	mu.Lock()
	defer mu.Unlock()

	tbHash := ComputeIntentHash(tableName, rowKey)

	// Fast path: Cuckoo filter miss = definitely no conflict
	if s.intentFilter != nil && !s.intentFilter.Check(tbHash) {
		telemetry.IntentFilterChecks.With("fast_path").Inc()
		return s.writeIntentFastPath(txnID, intentType, tableName, rowKey, op, sqlStmt, data, ts, nodeID, tbHash)
	}

	// Slow path: Filter hit (or no filter) - check Pebble
	return s.writeIntentSlowPath(txnID, intentType, tableName, rowKey, op, sqlStmt, data, ts, nodeID, tbHash)
}

// writeIntentFastPath writes intent without Pebble conflict check (filter miss).
func (s *PebbleMetaStore) writeIntentFastPath(txnID uint64, intentType IntentType, tableName, rowKey string, op OpType, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64, tbHash uint64) error {
	key := pebbleIntentKey(tableName, rowKey)

	rec := &WriteIntentRecord{
		IntentType:   intentType,
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

	recData, err := encoding.Marshal(rec)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(key, recData, nil); err != nil {
		return err
	}
	if err := batch.Set(pebbleIntentByTxnKey(txnID, tableName, rowKey), nil, nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	// Add to filter after successful write
	s.intentFilter.Add(txnID, tbHash)
	return nil
}

// writeIntentSlowPath writes intent with Pebble conflict check (filter hit).
func (s *PebbleMetaStore) writeIntentSlowPath(txnID uint64, intentType IntentType, tableName, rowKey string, op OpType, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64, tbHash uint64) error {
	key := pebbleIntentKey(tableName, rowKey)

	batch := s.db.NewBatch()
	defer batch.Close()

	// Check if intent already exists
	existingData, closer, err := s.db.Get(key)
	if err == nil {
		defer closer.Close()

		var existing WriteIntentRecord
		if err := encoding.Unmarshal(existingData, &existing); err != nil {
			return err
		}

		// Same transaction - update intent in place
		if existing.TxnID == txnID {
			telemetry.IntentFilterChecks.With("slow_path_same_txn").Inc()
			existing.Operation = op
			existing.SQLStatement = sqlStmt
			existing.DataSnapshot = data
			existing.CreatedAt = time.Now().UnixNano()
			newData, err := encoding.Marshal(&existing)
			if err != nil {
				return err
			}
			return s.db.Set(key, newData, pebble.NoSync)
		}

		// Different transaction - check if we can overwrite
		telemetry.IntentFilterChecks.With("slow_path_conflict").Inc()
		if err := s.resolveIntentConflictPebble(batch, &existing, txnID, tableName, rowKey); err != nil {
			telemetry.WriteConflictsTotal.With("intent", "slow").Inc()
			return err
		}
	} else if err != pebble.ErrNotFound {
		return err
	} else {
		// Filter hit but no intent in Pebble = false positive
		telemetry.IntentFilterChecks.With("slow_path_miss").Inc()
		telemetry.IntentFilterFalsePositives.Inc()
	}

	// Create new intent
	rec := &WriteIntentRecord{
		IntentType:   intentType,
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

	recData, err := encoding.Marshal(rec)
	if err != nil {
		return err
	}

	if err := batch.Set(key, recData, nil); err != nil {
		return err
	}
	if err := batch.Set(pebbleIntentByTxnKey(txnID, tableName, rowKey), nil, nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	// Add to filter after successful write
	if s.intentFilter != nil {
		s.intentFilter.Add(txnID, tbHash)
	}

	return nil
}

// resolveIntentConflictPebble handles conflict with existing intent from different transaction
func (s *PebbleMetaStore) resolveIntentConflictPebble(batch *pebble.Batch, existing *WriteIntentRecord, txnID uint64, tableName, rowKey string) error {
	// Marked for cleanup - safe to overwrite
	if existing.MarkedForCleanup {
		_ = batch.Delete(pebbleIntentByTxnKey(existing.TxnID, tableName, rowKey), nil)
		// Clean up filter for the old transaction's intent
		if s.intentFilter != nil {
			tbHash := ComputeIntentHash(tableName, rowKey)
			s.intentFilter.RemoveHash(existing.TxnID, tbHash)
		}
		return nil
	}

	// Check conflicting transaction status
	conflictTxnRec, _ := s.GetTransaction(existing.TxnID)

	canOverwrite := false
	switch {
	case conflictTxnRec == nil:
		log.Debug().
			Uint64("orphan_txn_id", existing.TxnID).
			Str("table", tableName).
			Str("row_key", rowKey).
			Msg("Cleaning up orphaned intent (no transaction record)")
		canOverwrite = true

	case conflictTxnRec.Status == TxnStatusCommitted:
		canOverwrite = true

	case conflictTxnRec.Status == TxnStatusAborted:
		log.Debug().
			Uint64("aborted_txn_id", existing.TxnID).
			Str("table", tableName).
			Str("row_key", rowKey).
			Msg("Cleaning up intent from aborted transaction")
		canOverwrite = true

	default:
		// Check heartbeat timeout
		heartbeatTimeout := int64(10 * time.Second)
		if cfg.Config != nil && cfg.Config.Transaction.HeartbeatTimeoutSeconds > 0 {
			heartbeatTimeout = int64(time.Duration(cfg.Config.Transaction.HeartbeatTimeoutSeconds) * time.Second)
		}

		timeSinceHeartbeat := time.Now().UnixNano() - conflictTxnRec.LastHeartbeat
		if timeSinceHeartbeat > heartbeatTimeout {
			log.Debug().
				Uint64("stale_txn_id", existing.TxnID).
				Str("table", tableName).
				Str("row_key", rowKey).
				Int64("heartbeat_age_ms", timeSinceHeartbeat/1e6).
				Msg("Cleaning up stale intent (heartbeat timeout)")
			canOverwrite = true
		}
	}

	if !canOverwrite {
		return fmt.Errorf("write-write conflict: row %s:%s locked by transaction %d (current txn: %d)",
			tableName, rowKey, existing.TxnID, txnID)
	}

	_ = batch.Delete(pebbleIntentByTxnKey(existing.TxnID, tableName, rowKey), nil)

	// Clean up filter for the overwritten transaction's intent
	if s.intentFilter != nil {
		tbHash := ComputeIntentHash(tableName, rowKey)
		s.intentFilter.RemoveHash(existing.TxnID, tbHash)
	}

	return nil
}

// ValidateIntent checks if the intent is still held by the expected transaction
func (s *PebbleMetaStore) ValidateIntent(tableName, rowKey string, expectedTxnID uint64) (bool, error) {
	val, closer, err := s.db.Get(pebbleIntentKey(tableName, rowKey))
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()

	var rec WriteIntentRecord
	if err := encoding.Unmarshal(val, &rec); err != nil {
		return false, err
	}

	return rec.TxnID == expectedTxnID, nil
}

// DeleteIntent removes a specific write intent
func (s *PebbleMetaStore) DeleteIntent(tableName, rowKey string, txnID uint64) error {
	key := pebbleIntentKey(tableName, rowKey)

	// Verify the intent belongs to this transaction
	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	var rec WriteIntentRecord
	if err := encoding.Unmarshal(val, &rec); err != nil {
		closer.Close()
		return err
	}
	closer.Close()

	if rec.TxnID != txnID {
		return nil // Intent belongs to different transaction
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Delete(key, nil); err != nil {
		return err
	}
	if err := batch.Delete(pebbleIntentByTxnKey(txnID, tableName, rowKey), nil); err != nil {
		return err
	}

	// NoSync: ResolveIntent is cleanup after commit. Idempotent.
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	// Sync intent filter after successful Pebble delete
	if s.intentFilter != nil {
		tbHash := ComputeIntentHash(tableName, rowKey)
		s.intentFilter.RemoveHash(txnID, tbHash)
	}

	return nil
}

// DeleteIntentsByTxn removes all write intents for a transaction
func (s *PebbleMetaStore) DeleteIntentsByTxn(txnID uint64) error {
	prefix := []byte(fmt.Sprintf("%s%016x/", pebblePrefixIntentByTxn, txnID))

	// Collect keys to delete
	var primaryKeys [][]byte
	var indexKeys [][]byte

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		indexKey := make([]byte, len(iter.Key()))
		copy(indexKey, iter.Key())
		indexKeys = append(indexKeys, indexKey)

		// Parse table/rowKey from index key
		keyStr := string(indexKey)
		suffix := keyStr[len(fmt.Sprintf("%s%016x/", pebblePrefixIntentByTxn, txnID)):]
		parts := strings.SplitN(suffix, "/", 2)
		if len(parts) == 2 {
			primaryKeys = append(primaryKeys, pebbleIntentKey(parts[0], parts[1]))
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	if len(primaryKeys) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for _, key := range primaryKeys {
		_ = batch.Delete(key, nil)
	}
	for _, key := range indexKeys {
		_ = batch.Delete(key, nil)
	}

	// NoSync: Intent cleanup is idempotent. If crash occurs, intents remain
	// (transaction is already committed) and will be cleaned up on next GC.
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	// Sync intent filter after successful Pebble delete
	if s.intentFilter != nil {
		s.intentFilter.Remove(txnID)
	}

	return nil
}

// MarkIntentsForCleanup marks all intents for a transaction as ready for overwrite
func (s *PebbleMetaStore) MarkIntentsForCleanup(txnID uint64) error {
	prefix := []byte(fmt.Sprintf("%s%016x/", pebblePrefixIntentByTxn, txnID))

	// Collect primary intent keys
	var primaryKeys [][]byte

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		keyStr := string(iter.Key())
		suffix := keyStr[len(fmt.Sprintf("%s%016x/", pebblePrefixIntentByTxn, txnID)):]
		parts := strings.SplitN(suffix, "/", 2)
		if len(parts) == 2 {
			primaryKeys = append(primaryKeys, pebbleIntentKey(parts[0], parts[1]))
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	if len(primaryKeys) == 0 {
		return nil
	}

	// Mark each intent for cleanup
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, key := range primaryKeys {
		val, closer, err := s.db.Get(key)
		if err == pebble.ErrNotFound {
			continue
		}
		if err != nil {
			return err
		}

		var rec WriteIntentRecord
		if err := encoding.Unmarshal(val, &rec); err != nil {
			closer.Close()
			return err
		}
		closer.Close()

		rec.MarkedForCleanup = true
		data, err := encoding.Marshal(&rec)
		if err != nil {
			return err
		}
		if err := batch.Set(key, data, nil); err != nil {
			return err
		}
	}

	// NoSync: Cleanup marking is idempotent. If crash occurs, intents remain
	// and will be cleaned up on next GC cycle.
	return batch.Commit(pebble.NoSync)
}

// GetIntentsByTxn retrieves all write intents for a transaction
func (s *PebbleMetaStore) GetIntentsByTxn(txnID uint64) ([]*WriteIntentRecord, error) {
	var intents []*WriteIntentRecord
	prefix := []byte(fmt.Sprintf("%s%016x/", pebblePrefixIntentByTxn, txnID))

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		keyStr := string(iter.Key())
		suffix := keyStr[len(fmt.Sprintf("%s%016x/", pebblePrefixIntentByTxn, txnID)):]
		parts := strings.SplitN(suffix, "/", 2)
		if len(parts) != 2 {
			continue
		}

		// Fetch primary intent
		primaryKey := pebbleIntentKey(parts[0], parts[1])
		val, closer, err := s.db.Get(primaryKey)
		if err != nil {
			continue
		}

		intent := &WriteIntentRecord{}
		if err := encoding.Unmarshal(val, intent); err == nil && intent.TxnID == txnID {
			intents = append(intents, intent)
		}
		closer.Close()
	}

	return intents, iter.Error()
}

// GetIntent retrieves a specific write intent
func (s *PebbleMetaStore) GetIntent(tableName, rowKey string) (*WriteIntentRecord, error) {
	val, closer, err := s.db.Get(pebbleIntentKey(tableName, rowKey))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	intent := &WriteIntentRecord{}
	if err := encoding.Unmarshal(val, intent); err != nil {
		return nil, err
	}
	return intent, nil
}

// GetReplicationState retrieves replication state for a peer
func (s *PebbleMetaStore) GetReplicationState(peerNodeID uint64, dbName string) (*ReplicationStateRecord, error) {
	val, closer, err := s.db.Get(pebbleReplKey(peerNodeID, dbName))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	state := &ReplicationStateRecord{}
	if err := encoding.Unmarshal(val, state); err != nil {
		return nil, err
	}
	return state, nil
}

// UpdateReplicationState updates replication state for a peer
func (s *PebbleMetaStore) UpdateReplicationState(peerNodeID uint64, dbName string, lastTxnID uint64, lastTS hlc.Timestamp) error {
	state := &ReplicationStateRecord{
		PeerNodeID:           peerNodeID,
		DatabaseName:         dbName,
		LastAppliedTxnID:     lastTxnID,
		LastAppliedTSWall:    lastTS.WallTime,
		LastAppliedTSLogical: lastTS.Logical,
		LastSyncTime:         time.Now().UnixNano(),
		SyncStatus:           SyncStatusSynced,
	}

	data, err := encoding.Marshal(state)
	if err != nil {
		return err
	}

	return s.db.Set(pebbleReplKey(peerNodeID, dbName), data, pebble.NoSync)
}

// GetMinAppliedTxnID returns the minimum applied txn_id across all peers for a database
func (s *PebbleMetaStore) GetMinAppliedTxnID(dbName string) (uint64, error) {
	var minTxnID uint64 = ^uint64(0) // Max uint64
	found := false
	prefix := []byte(pebblePrefixRepl)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		var state ReplicationStateRecord
		if err := encoding.Unmarshal(val, &state); err != nil {
			continue
		}

		if state.DatabaseName == dbName {
			found = true
			if state.LastAppliedTxnID < minTxnID {
				minTxnID = state.LastAppliedTxnID
			}
		}
	}

	if err := iter.Error(); err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	return minTxnID, nil
}

// GetAllReplicationStates returns all replication state records
func (s *PebbleMetaStore) GetAllReplicationStates() ([]*ReplicationStateRecord, error) {
	var states []*ReplicationStateRecord
	prefix := []byte(pebblePrefixRepl)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		state := &ReplicationStateRecord{}
		if err := encoding.Unmarshal(val, state); err != nil {
			continue
		}
		states = append(states, state)
	}

	return states, iter.Error()
}

// pebbleSchemaVersionRecord is internal storage for schema
type pebbleSchemaVersionRecord struct {
	Version   int64
	LastDDL   string
	TxnID     uint64
	UpdatedAt int64
}

// GetSchemaVersion retrieves the schema version for a database
func (s *PebbleMetaStore) GetSchemaVersion(dbName string) (int64, error) {
	val, closer, err := s.db.Get(pebbleSchemaKey(dbName))
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()

	var rec pebbleSchemaVersionRecord
	if err := encoding.Unmarshal(val, &rec); err != nil {
		return 0, err
	}
	return rec.Version, nil
}

// UpdateSchemaVersion updates the schema version for a database
func (s *PebbleMetaStore) UpdateSchemaVersion(dbName string, version int64, ddlSQL string, txnID uint64) error {
	rec := &pebbleSchemaVersionRecord{
		Version:   version,
		LastDDL:   ddlSQL,
		TxnID:     txnID,
		UpdatedAt: time.Now().UnixNano(),
	}

	data, err := encoding.Marshal(rec)
	if err != nil {
		return err
	}

	return s.db.Set(pebbleSchemaKey(dbName), data, pebble.NoSync)
}

// GetAllSchemaVersions returns all schema versions indexed by database name
func (s *PebbleMetaStore) GetAllSchemaVersions() (map[string]int64, error) {
	versions := make(map[string]int64)
	prefix := []byte(pebblePrefixSchema)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		dbName := string(key[len(prefix):])

		val, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		var rec pebbleSchemaVersionRecord
		if err := encoding.Unmarshal(val, &rec); err != nil {
			continue
		}
		versions[dbName] = rec.Version
	}

	return versions, iter.Error()
}

// pebbleDdlLockRecord is internal storage for DDL locks
type pebbleDdlLockRecord struct {
	NodeID    uint64
	LockedAt  int64
	ExpiresAt int64
}

// TryAcquireDDLLock attempts to acquire a DDL lock for a database
func (s *PebbleMetaStore) TryAcquireDDLLock(dbName string, nodeID uint64, leaseDuration time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	expiresAt := now + leaseDuration.Nanoseconds()
	key := pebbleDdlLockKey(dbName)

	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		// No lock exists - acquire it
		rec := &pebbleDdlLockRecord{
			NodeID:    nodeID,
			LockedAt:  now,
			ExpiresAt: expiresAt,
		}
		data, err := encoding.Marshal(rec)
		if err != nil {
			return false, err
		}
		return true, s.db.Set(key, data, pebble.NoSync)
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()

	// Lock exists - check if expired
	var rec pebbleDdlLockRecord
	if err := encoding.Unmarshal(val, &rec); err != nil {
		return false, err
	}

	if rec.ExpiresAt < now {
		// Lock expired - acquire it
		newRec := &pebbleDdlLockRecord{
			NodeID:    nodeID,
			LockedAt:  now,
			ExpiresAt: expiresAt,
		}
		data, err := encoding.Marshal(newRec)
		if err != nil {
			return false, err
		}
		return true, s.db.Set(key, data, pebble.NoSync)
	}

	// Lock still held by another node
	return false, nil
}

// ReleaseDDLLock releases a DDL lock
func (s *PebbleMetaStore) ReleaseDDLLock(dbName string, nodeID uint64) error {
	key := pebbleDdlLockKey(dbName)

	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()

	var rec pebbleDdlLockRecord
	if err := encoding.Unmarshal(val, &rec); err != nil {
		return err
	}

	if rec.NodeID != nodeID {
		return nil // Lock held by different node
	}

	return s.db.Delete(key, pebble.NoSync)
}

// WriteIntentEntry writes a CDC intent entry
func (s *PebbleMetaStore) WriteIntentEntry(txnID, seq uint64, op uint8, table, rowKey string, oldVals, newVals []byte) error {
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
		if err := encoding.Unmarshal(oldVals, &entry.OldValues); err != nil {
			log.Error().Err(err).Uint64("txn_id", txnID).Str("table", table).Msg("Failed to unmarshal OldValues")
			return fmt.Errorf("failed to unmarshal old values: %w", err)
		}
	}
	if newVals != nil {
		if err := encoding.Unmarshal(newVals, &entry.NewValues); err != nil {
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

	data, err := encoding.Marshal(entry)
	if err != nil {
		return err
	}

	// NoSync: CDC entries are protected by WriteIntent (PREPARE).
	// If crash occurs, intent exists and CDC can be recovered or transaction aborted.
	return s.db.Set(pebbleCdcKey(txnID, seq), data, pebble.NoSync)
}

// GetIntentEntries retrieves CDC intent entries for a transaction
func (s *PebbleMetaStore) GetIntentEntries(txnID uint64) ([]*IntentEntry, error) {
	var entries []*IntentEntry
	prefix := pebbleCdcPrefix(txnID)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		entry := &IntentEntry{}
		if err := encoding.Unmarshal(val, entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

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

	return entries, nil
}

// DeleteIntentEntries deletes CDC intent entries for a transaction
func (s *PebbleMetaStore) DeleteIntentEntries(txnID uint64) error {
	prefix := pebbleCdcPrefix(txnID)
	var keys [][]byte

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		keys = append(keys, key)
	}
	if err := iter.Close(); err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		_ = batch.Delete(key, nil)
	}

	// NoSync: CDC entry cleanup is idempotent. Transaction is already committed.
	return batch.Commit(pebble.NoSync)
}

// CleanupStaleTransactions aborts transactions that haven't had a heartbeat within the timeout
func (s *PebbleMetaStore) CleanupStaleTransactions(timeout time.Duration) (int, error) {
	cutoff := time.Now().Add(-timeout).UnixNano()
	cleaned := 0

	// Phase 1: Find stale PENDING transactions
	var staleTxnIDs []uint64
	prefix := []byte(pebblePrefixTxnPending)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return 0, err
	}

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		keyStr := string(iter.Key())
		var txnID uint64
		_, _ = fmt.Sscanf(keyStr[len(pebblePrefixTxnPending):], "%016x", &txnID)

		rec, err := s.GetTransaction(txnID)
		if err != nil || rec == nil {
			continue
		}

		if rec.LastHeartbeat < cutoff {
			staleTxnIDs = append(staleTxnIDs, txnID)
			ageMs := (time.Now().UnixNano() - rec.LastHeartbeat) / 1e6
			log.Warn().
				Uint64("txn_id", txnID).
				Str("status", rec.Status.String()).
				Int64("age_ms", ageMs).
				Int64("timeout_ms", timeout.Milliseconds()).
				Msg("GC: Found stale PENDING transaction - WILL BE CLEANED UP")
		}
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}

	// Delete stale transactions and their intents (best-effort cleanup)
	for _, txnID := range staleTxnIDs {
		log.Warn().
			Uint64("txn_id", txnID).
			Msg("GC: Aborting and deleting stale transaction")
		_ = s.AbortTransaction(txnID)
		_ = s.DeleteIntentsByTxn(txnID)
		_ = s.DeleteIntentEntries(txnID)
		_ = s.ReleaseCDCRowLocksByTxn(txnID)
		cleaned++
	}

	// Phase 2: Clean orphaned intents
	type orphanedIntent struct {
		key    []byte
		txnID  uint64
		table  string
		rowKey string
	}
	var orphanedIntents []orphanedIntent
	intentPrefix := []byte(pebblePrefixIntent)

	iter, err = s.db.NewIter(&pebble.IterOptions{
		LowerBound: intentPrefix,
		UpperBound: prefixUpperBound(intentPrefix),
	})
	if err != nil {
		return cleaned, err
	}

	for iter.SeekGE(intentPrefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		var rec WriteIntentRecord
		if err := encoding.Unmarshal(val, &rec); err != nil {
			continue
		}

		// Check if intent is old enough
		if rec.CreatedAt >= cutoff {
			continue
		}

		// Check if transaction exists
		txnRec, _ := s.GetTransaction(rec.TxnID)
		if txnRec == nil {
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			orphanedIntents = append(orphanedIntents, orphanedIntent{
				key:    key,
				txnID:  rec.TxnID,
				table:  rec.TableName,
				rowKey: rec.RowKey,
			})
		}
	}
	if err := iter.Close(); err != nil {
		return cleaned, err
	}

	// Delete orphaned intents and clean up IntentFilter
	if len(orphanedIntents) > 0 {
		batch := s.db.NewBatch()
		for _, orphan := range orphanedIntents {
			_ = batch.Delete(orphan.key, nil)
			// Remove from IntentFilter to prevent false positives
			if s.intentFilter != nil {
				tbHash := ComputeIntentHash(orphan.table, orphan.rowKey)
				s.intentFilter.RemoveHash(orphan.txnID, tbHash)
			}
			cleaned++
		}
		_ = batch.Commit(pebble.NoSync)
		batch.Close()
	}

	if cleaned > 0 {
		log.Info().
			Int("stale_txns", len(staleTxnIDs)).
			Int("orphaned_intents", len(orphanedIntents)).
			Msg("MetaStore GC: Cleaned up stale transactions and orphaned intents")
	}

	return cleaned, nil
}

// CleanupOldTransactionRecords removes old COMMITTED/ABORTED transaction records
func (s *PebbleMetaStore) CleanupOldTransactionRecords(minRetention, maxRetention time.Duration, minAppliedTxnID, minAppliedSeqNum uint64) (int, error) {
	now := time.Now()
	minRetentionCutoff := now.Add(-minRetention).UnixNano()
	maxRetentionCutoff := now.Add(-maxRetention).UnixNano()
	deleted := 0
	committedDeleted := 0

	prefix := []byte(pebblePrefixTxn)
	var keysToDelete [][]byte
	var seqKeysToDelete [][]byte

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return 0, err
	}

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		var rec TransactionRecord
		if err := encoding.Unmarshal(val, &rec); err != nil {
			continue
		}

		// Only clean COMMITTED or ABORTED
		if rec.Status != TxnStatusCommitted && rec.Status != TxnStatusAborted {
			continue
		}

		// Check deletion criteria
		shouldDelete := false
		if rec.CreatedAt < maxRetentionCutoff {
			shouldDelete = true
		} else if rec.CreatedAt < minRetentionCutoff {
			if minAppliedTxnID > 0 && minAppliedSeqNum > 0 {
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
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			keysToDelete = append(keysToDelete, key)
			if rec.SeqNum > 0 {
				seqKeysToDelete = append(seqKeysToDelete, pebbleTxnSeqKey(rec.SeqNum, rec.TxnID))
			}
			if rec.Status == TxnStatusCommitted {
				committedDeleted++
			}
		}
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}

	if len(keysToDelete) == 0 {
		return 0, nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for _, key := range keysToDelete {
		if err := batch.Delete(key, nil); err == nil {
			deleted++
		}
	}
	for _, key := range seqKeysToDelete {
		_ = batch.Delete(key, nil)
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return 0, err
	}

	// Decrement committed transaction counter
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

// GetNextSeqNum returns the next sequence number for a node
func (s *PebbleMetaStore) GetNextSeqNum(nodeID uint64) (uint64, error) {
	seq, err := s.getOrCreateSequence(nodeID)
	if err != nil {
		return 0, err
	}
	num, err := seq.Next()
	if err != nil {
		return 0, err
	}
	// We want 1-based sequence numbers
	return num + 1, nil
}

// getOrCreateSequence returns or creates an AtomicSequence for the given nodeID
func (s *PebbleMetaStore) getOrCreateSequence(nodeID uint64) (*AtomicSequence, error) {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()

	if seq, ok := s.sequences[nodeID]; ok {
		return seq, nil
	}

	seq, err := NewAtomicSequence(s.db, pebbleSeqKey(nodeID), pebbleSeqBandwidth)
	if err != nil {
		return nil, fmt.Errorf("failed to create sequence for node %d: %w", nodeID, err)
	}

	s.sequences[nodeID] = seq
	return seq, nil
}

// GetMaxSeqNum returns the maximum sequence number across all committed transactions
func (s *PebbleMetaStore) GetMaxSeqNum() (uint64, error) {
	var maxSeq uint64
	prefix := []byte(pebblePrefixTxnSeq)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	// Go to last key in range
	if iter.Last() {
		keyStr := string(iter.Key())
		parts := strings.Split(keyStr[len(pebblePrefixTxnSeq):], "/")
		if len(parts) >= 1 {
			_, _ = fmt.Sscanf(parts[0], "%016x", &maxSeq)
		}
	}

	return maxSeq, iter.Error()
}

// GetMinAppliedSeqNum returns the minimum applied sequence number across all peers for a database
func (s *PebbleMetaStore) GetMinAppliedSeqNum(dbName string) (uint64, error) {
	// Proxy through GetMinAppliedTxnID since we track by txn_id
	return s.GetMinAppliedTxnID(dbName)
}

// GetMaxCommittedTxnID returns the maximum committed transaction ID
func (s *PebbleMetaStore) GetMaxCommittedTxnID() (uint64, error) {
	return s.counters.LoadUint64("max_committed_txn_id")
}

// GetCommittedTxnCount returns the count of committed transactions
func (s *PebbleMetaStore) GetCommittedTxnCount() (int64, error) {
	return s.counters.Load("committed_txn_count")
}

// StreamCommittedTransactions streams committed transactions after fromTxnID (ascending order)
func (s *PebbleMetaStore) StreamCommittedTransactions(fromTxnID uint64, callback func(*TransactionRecord) error) error {
	return s.ScanTransactions(fromTxnID, false, func(rec *TransactionRecord) error {
		if rec.Status != TxnStatusCommitted {
			return nil // skip non-committed
		}
		return callback(rec)
	})
}

// ScanTransactions iterates transactions from fromTxnID.
// If descending is true, scans from newest to oldest.
// Callback returns nil to continue, ErrStopIteration to stop, or other error to abort.
func (s *PebbleMetaStore) ScanTransactions(fromTxnID uint64, descending bool, callback func(*TransactionRecord) error) error {
	prefix := []byte(pebblePrefixTxnSeq)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	// Choose iteration direction
	var valid func() bool
	var advance func() bool
	if descending {
		// Start from end, go backwards
		valid = func() bool { return iter.Valid() }
		advance = func() bool { return iter.Prev() }
		iter.SeekLT(prefixUpperBound(prefix))
	} else {
		// Start from beginning, go forwards
		valid = func() bool { return iter.Valid() }
		advance = func() bool { return iter.Next() }
		iter.SeekGE(prefix)
	}

	for valid() {
		keyStr := string(iter.Key())
		parts := strings.Split(keyStr[len(pebblePrefixTxnSeq):], "/")
		if len(parts) != 2 {
			advance()
			continue
		}

		var txnID uint64
		_, _ = fmt.Sscanf(parts[1], "%016x", &txnID)

		// Filter by fromTxnID based on direction
		if descending {
			if fromTxnID > 0 && txnID >= fromTxnID {
				advance()
				continue
			}
		} else {
			if txnID <= fromTxnID {
				advance()
				continue
			}
		}

		rec, err := s.GetTransaction(txnID)
		if err != nil || rec == nil {
			advance()
			continue
		}

		if err := callback(rec); err != nil {
			if err == ErrStopIteration {
				return nil
			}
			return err
		}
		advance()
	}

	return iter.Error()
}

// AcquireCDCRowLock attempts to acquire a row-level lock for CDC conflict detection.
// Returns ErrCDCRowLocked if the row is already locked by a different transaction.
// Same transaction can re-acquire (idempotent).
func (s *PebbleMetaStore) AcquireCDCRowLock(txnID uint64, tableName, rowKey string) error {
	key := pebbleCDCActiveRowKey(tableName, rowKey)

	// Check if lock exists
	val, closer, err := s.db.Get(key)
	if err == nil {
		defer closer.Close()
		if len(val) >= 8 {
			existingTxnID := binary.BigEndian.Uint64(val)
			if existingTxnID != txnID {
				return ErrCDCRowLocked{
					Table:     tableName,
					RowKey:    rowKey,
					HeldByTxn: existingTxnID,
				}
			}
			// Same txn - idempotent re-acquire
			return nil
		}
	} else if err != pebble.ErrNotFound {
		return err
	}

	// No lock exists or same txn - acquire/update it
	// Write both forward and reverse index in batch
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, txnID)

	batch := s.db.NewBatch()
	defer batch.Close()

	// Forward index: /cdc/active/{table}/{rowKey} → txnID
	if err := batch.Set(key, buf, nil); err != nil {
		return err
	}

	// Reverse index: /cdc/txn_locks/{txnID}/{table}/{rowKey} → empty
	reverseKey := pebbleCDCTxnLockKey(txnID, tableName, rowKey)
	if err := batch.Set(reverseKey, nil, nil); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// ReleaseCDCRowLock releases a row-level lock if held by the specified txnID.
// Idempotent - no error if already released.
func (s *PebbleMetaStore) ReleaseCDCRowLock(tableName, rowKey string, txnID uint64) error {
	key := pebbleCDCActiveRowKey(tableName, rowKey)

	// Check if lock exists and belongs to this txn
	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil // Already released
	}
	if err != nil {
		return err
	}

	var existingTxnID uint64
	if len(val) >= 8 {
		existingTxnID = binary.BigEndian.Uint64(val)
		if existingTxnID != txnID {
			closer.Close()
			return nil // Lock held by different txn - don't release
		}
	}
	closer.Close()

	// Delete both forward and reverse index
	batch := s.db.NewBatch()
	defer batch.Close()

	// Forward index
	if err := batch.Delete(key, nil); err != nil {
		return err
	}

	// Reverse index
	reverseKey := pebbleCDCTxnLockKey(txnID, tableName, rowKey)
	if err := batch.Delete(reverseKey, nil); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// ReleaseCDCRowLocksByTxn releases all row locks held by the specified transaction.
// Uses reverse index for O(m) complexity where m = locks held by txn.
func (s *PebbleMetaStore) ReleaseCDCRowLocksByTxn(txnID uint64) error {
	prefix := pebbleCDCTxnLockPrefix(txnID)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	// Compute prefix length once (prefix is /cdc/txn_locks/{txnID:016x}/)
	prefixLen := len(prefix)

	// Iterate over reverse index entries for this txn
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		// Parse table/rowKey from reverse key: /cdc/txn_locks/{txnID}/{table}/{rowKey}
		keyStr := string(iter.Key())
		suffix := keyStr[prefixLen:]

		// Find first slash to separate table from rowKey
		slashIdx := strings.Index(suffix, "/")
		if slashIdx == -1 {
			continue // Malformed key, skip
		}
		tableName := suffix[:slashIdx]
		rowKey := suffix[slashIdx+1:]

		// Delete forward index key
		forwardKey := pebbleCDCActiveRowKey(tableName, rowKey)
		if err := batch.Delete(forwardKey, nil); err != nil {
			iter.Close()
			return err
		}

		// Delete reverse index key
		if err := batch.Delete(iter.Key(), nil); err != nil {
			iter.Close()
			return err
		}
	}

	if err := iter.Close(); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// GetCDCRowLock returns the transaction ID holding the lock, or 0 if no lock exists.
func (s *PebbleMetaStore) GetCDCRowLock(tableName, rowKey string) (uint64, error) {
	key := pebbleCDCActiveRowKey(tableName, rowKey)

	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()

	if len(val) >= 8 {
		return binary.BigEndian.Uint64(val), nil
	}
	return 0, nil
}

// AcquireCDCTableDDLLock attempts to acquire a table-level DDL lock.
// Returns ErrCDCDMLInProgress if any DML row locks exist for the table.
// Returns ErrCDCRowLocked if DDL lock already held by different transaction.
//
// NOTE: There is a TOCTOU race between checking HasCDCRowLocksForTable and
// acquiring the DDL lock. This means DDL and DML operations could theoretically
// overlap in a small window. In practice, this is mitigated by:
// 1. The coordinator acquiring write intents before CDC capture
// 2. The short duration of the window (microseconds)
// 3. The retry mechanism on conflict (MySQL 1213 deadlock error)
// A fully atomic implementation would require Pebble transactions.
func (s *PebbleMetaStore) AcquireCDCTableDDLLock(txnID uint64, tableName string) error {
	// First check if any DML is in progress
	hasDML, err := s.HasCDCRowLocksForTable(tableName)
	if err != nil {
		return err
	}
	if hasDML {
		return ErrCDCDMLInProgress{Table: tableName}
	}

	// Check if DDL lock exists
	key := pebbleCDCActiveDDLKey(tableName)
	val, closer, err := s.db.Get(key)
	if err == nil {
		defer closer.Close()
		if len(val) >= 8 {
			existingTxnID := binary.BigEndian.Uint64(val)
			if existingTxnID != txnID {
				// DDL lock is stored as a special row with "__ddl__" key
				// This allows us to reuse ErrCDCRowLocked for consistency
				return ErrCDCRowLocked{
					Table:     tableName,
					RowKey:    pebbleCDCDDLKeyMarker, // Sentinel value indicating DDL lock
					HeldByTxn: existingTxnID,
				}
			}
			// Same txn - idempotent re-acquire
			return nil
		}
	} else if err != pebble.ErrNotFound {
		return err
	}

	// Acquire DDL lock - write both forward and reverse index
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, txnID)

	batch := s.db.NewBatch()
	defer batch.Close()

	// Forward index
	if err := batch.Set(key, buf, nil); err != nil {
		return err
	}

	// Reverse index (DDL uses __ddl__ as rowKey)
	reverseKey := pebbleCDCTxnLockKey(txnID, tableName, pebbleCDCDDLKeyMarker)
	if err := batch.Set(reverseKey, nil, nil); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// ReleaseCDCTableDDLLock releases a table-level DDL lock if held by the specified txnID.
// Idempotent - no error if already released.
func (s *PebbleMetaStore) ReleaseCDCTableDDLLock(tableName string, txnID uint64) error {
	key := pebbleCDCActiveDDLKey(tableName)

	// Check if lock exists and belongs to this txn
	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil // Already released
	}
	if err != nil {
		return err
	}

	var existingTxnID uint64
	if len(val) >= 8 {
		existingTxnID = binary.BigEndian.Uint64(val)
		if existingTxnID != txnID {
			closer.Close()
			return nil // Lock held by different txn - don't release
		}
	}
	closer.Close()

	// Delete both forward and reverse index
	batch := s.db.NewBatch()
	defer batch.Close()

	// Forward index
	if err := batch.Delete(key, nil); err != nil {
		return err
	}

	// Reverse index
	reverseKey := pebbleCDCTxnLockKey(txnID, tableName, pebbleCDCDDLKeyMarker)
	if err := batch.Delete(reverseKey, nil); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// HasCDCRowLocksForTable checks if any row-level locks exist for a table (excluding DDL lock).
// Returns true if any DML is in progress on this table.
func (s *PebbleMetaStore) HasCDCRowLocksForTable(tableName string) (bool, error) {
	prefix := pebbleCDCActiveTablePrefix(tableName)
	ddlKey := pebbleCDCActiveDDLKey(tableName)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Skip the DDL lock key
		if string(key) == string(ddlKey) {
			continue
		}
		// Found a non-DDL lock
		return true, nil
	}

	return false, iter.Error()
}

// GetCDCTableDDLLock returns the transaction ID holding the DDL lock, or 0 if no lock exists.
func (s *PebbleMetaStore) GetCDCTableDDLLock(tableName string) (uint64, error) {
	key := pebbleCDCActiveDDLKey(tableName)

	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()

	if len(val) >= 8 {
		return binary.BigEndian.Uint64(val), nil
	}
	return 0, nil
}

// clearAllCDCLocks deletes all CDC locks (both forward and reverse index).
// Called on startup since CDC locks should never survive process restart.
// Any surviving lock is from a crashed transaction and is invalid.
func (s *PebbleMetaStore) clearAllCDCLocks() error {
	batch := s.db.NewBatch()
	defer batch.Close()

	// Delete all forward locks: /cdc/active/*
	activePrefix := []byte(pebblePrefixCDCActive)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: activePrefix,
		UpperBound: prefixUpperBound(activePrefix),
	})
	if err != nil {
		return err
	}

	for iter.SeekGE(activePrefix); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		if err := batch.Delete(key, nil); err != nil {
			iter.Close()
			return err
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	// Delete all reverse locks: /cdc/txn_locks/*
	txnLocksPrefix := []byte(pebblePrefixCDCTxnLocks)
	iter, err = s.db.NewIter(&pebble.IterOptions{
		LowerBound: txnLocksPrefix,
		UpperBound: prefixUpperBound(txnLocksPrefix),
	})
	if err != nil {
		return err
	}

	for iter.SeekGE(txnLocksPrefix); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		if err := batch.Delete(key, nil); err != nil {
			iter.Close()
			return err
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync)
}
