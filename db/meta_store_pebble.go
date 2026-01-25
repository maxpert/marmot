package db

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

// Key prefixes for Pebble - uses binary encoding for numeric IDs (8 bytes per uint64)
// and 2-byte length prefix for tableName to avoid delimiter issues with binary data
const (
	pebblePrefixTxn         = "/txn/"           // /txn/{8 bytes txnID}
	pebblePrefixTxnPending  = "/txn_idx/pend/"  // /txn_idx/pend/{8 bytes txnID}
	pebblePrefixTxnSeq      = "/txn_idx/seq/"   // /txn_idx/seq/{8 bytes seqNum}{8 bytes txnID}
	pebblePrefixTxnByID     = "/txn_idx/txnid/" // /txn_idx/txnid/{8 bytes txnID} - primary index for streaming
	pebblePrefixCDCRaw      = "/cdc/raw/"       // /cdc/raw/{8 bytes txnID}{8 bytes seq}
	pebblePrefixRepl        = "/repl/"          // /repl/{8 bytes peerNodeID}/{dbName}
	pebblePrefixSchema      = "/schema/"        // /schema/{dbName}
	pebblePrefixDDLLock     = "/ddl/"           // /ddl/{dbName}
	pebblePrefixSeq         = "/seq/"           // /seq/{8 bytes nodeID}
	pebblePrefixIntentByTxn = "/intent_txn/"    // /intent_txn/{8 bytes txnID}{2 bytes tableNameLen}{tableName}{intentKey}
	pebblePrefixCounter     = "/meta/"          // /meta/{counterName}
	pebblePrefixTxnCommit   = "/txn_commit/"    // /txn_commit/{8 bytes txnID}
	pebblePrefixTxnStatus   = "/txn_status/"    // /txn_status/{8 bytes txnID}
)

// Sharded lock for WriteIntent serialization (prevents TOCTOU race)
const intentLockShards = 256

// PebbleMetaStore implements MetaStore using Pebble
// TransactionGetter is a function type for looking up transaction records.
// Used to allow MemoryMetaStore to inject its own transaction lookup during conflict resolution.
type TransactionGetter func(txnID uint64) (*TransactionRecord, error)

type PebbleMetaStore struct {
	db   *pebble.DB
	path string

	// Idempotent close
	closed atomic.Bool

	// Sequence generators (Pebble doesn't have native Sequence API)
	sequences map[uint64]*AtomicSequence
	seqMu     sync.Mutex

	// Persistent counters for O(1) lookups
	counters *PebbleCounter

	// Sharded locks for WriteIntent serialization (prevents TOCTOU race)
	intentLocks [intentLockShards]sync.Mutex

	// In-memory row locking for fast-path intent conflict detection
	rowLocks *RowLockStore

	// In-memory CDC locks (row + DDL) for conflict detection
	cdcLocks *XsyncCDCLockStore

	// Optional transaction getter for conflict resolution (set by MemoryMetaStore wrapper)
	txnGetter TransactionGetter
}

// Ensure PebbleMetaStore implements MetaStore
var _ MetaStore = (*PebbleMetaStore)(nil)

// intentLockFor returns the sharded mutex for a given table+intentKey
func (s *PebbleMetaStore) intentLockFor(tableName, intentKey string) *sync.Mutex {
	key := tableName + ":" + intentKey
	return &s.intentLocks[xxhash.Sum64String(key)%intentLockShards]
}

// SetTransactionGetter sets a custom transaction getter for conflict resolution.
// This allows MemoryMetaStore to inject its GetTransaction which reads from memory.
func (s *PebbleMetaStore) SetTransactionGetter(getter TransactionGetter) {
	s.txnGetter = getter
}

// PebbleMetaStoreOptions configures Pebble
type PebbleMetaStoreOptions struct {
	// Memory settings (explicit, no mmap surprise)
	CacheSizeMB    int64 // Block cache size (default: 128MB)
	MemTableSizeMB int64 // Write buffer size (default: 64MB)
	MemTableCount  int   // Number of memtables (default: 2)

	// Write optimization
	WALDir             string        // Separate WAL directory (optional)
	DisableWAL         bool          // Only for testing!
	WALBytesPerSync    int           // Sync WAL every N bytes (default: 512KB)
	WALMinSyncInterval time.Duration // Min delay between syncs for group commit (default: 2ms)

	// Compaction (CockroachDB-tested defaults from cfg.Config)
	L0CompactionThreshold int   // L0 files before compaction
	L0StopWrites          int   // L0 files to pause writes
	MaxConcurrentCompact  int   // Parallel compactors (default: 3)
	LBaseMaxBytes         int64 // Base level compaction target (default: 64MB)
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
		L0CompactionThreshold: ms.L0CompactionThreshold,
		L0StopWrites:          ms.L0StopWrites,
		MaxConcurrentCompact:  3,
		LBaseMaxBytes:         64 << 20, // 64MB base level target
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
		// Bloom filters for faster point lookups (10 bits per key)
		Levels: []pebble.LevelOptions{
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
		},
	}

	// LBaseMaxBytes controls base level (L1) compaction target
	if opts.LBaseMaxBytes > 0 {
		pebbleOpts.LBaseMaxBytes = opts.LBaseMaxBytes
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
		db:        db,
		path:      path,
		sequences: make(map[uint64]*AtomicSequence),
		rowLocks:  NewRowLockStore(),
		cdcLocks:  NewXsyncCDCLockStore(),
	}

	// Initialize persistent counters
	store.counters = NewPebbleCounter(db, pebblePrefixCounter, 10)

	return store, nil
}

// Close closes the Pebble DB (idempotent - safe to call multiple times)
func (s *PebbleMetaStore) Close() error {
	// Ensure we only close once
	if s.closed.Swap(true) {
		return nil // Already closed
	}

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

// Key helper functions - use binary encoding for uint64 (8 bytes vs 16 hex chars)
// Big-endian preserves lexicographic sort order for range scans

// buildKeyUint64 builds a key with prefix + single uint64 suffix
func buildKeyUint64(prefix string, id uint64) []byte {
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], id)
	return key
}

// buildKeyUint64x2 builds a key with prefix + two uint64 suffixes
func buildKeyUint64x2(prefix string, id1, id2 uint64) []byte {
	key := make([]byte, len(prefix)+16)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], id1)
	binary.BigEndian.PutUint64(key[len(prefix)+8:], id2)
	return key
}

// buildKeyString builds a key with prefix + string suffix
func buildKeyString(prefix, suffix string) []byte {
	key := make([]byte, len(prefix)+len(suffix))
	copy(key, prefix)
	copy(key[len(prefix):], suffix)
	return key
}

func pebbleTxnKey(txnID uint64) []byte {
	return buildKeyUint64(pebblePrefixTxn, txnID)
}

func pebbleTxnPendingKey(txnID uint64) []byte {
	return buildKeyUint64(pebblePrefixTxnPending, txnID)
}

func pebbleTxnSeqKey(seqNum, txnID uint64) []byte {
	return buildKeyUint64x2(pebblePrefixTxnSeq, seqNum, txnID)
}

// pebbleTxnByIDKey creates the TxnID-ordered index key for streaming.
// This is the primary index for ScanTransactions to ensure TxnID ordering.
func pebbleTxnByIDKey(txnID uint64) []byte {
	return buildKeyUint64(pebblePrefixTxnByID, txnID)
}

func pebbleTxnCommitKey(txnID uint64) []byte {
	return buildKeyUint64(pebblePrefixTxnCommit, txnID)
}

func pebbleTxnStatusKey(txnID uint64) []byte {
	return buildKeyUint64(pebblePrefixTxnStatus, txnID)
}

// pebbleIntentByTxnKey uses 2-byte length prefix for tableName
func pebbleIntentByTxnKey(txnID uint64, tableName, intentKey string) []byte {
	key := make([]byte, len(pebblePrefixIntentByTxn)+8+2+len(tableName)+len(intentKey))
	n := copy(key, pebblePrefixIntentByTxn)
	binary.BigEndian.PutUint64(key[n:], txnID)
	n += 8
	binary.BigEndian.PutUint16(key[n:], uint16(len(tableName)))
	n += 2
	n += copy(key[n:], tableName)
	copy(key[n:], intentKey)
	return key
}

func pebbleIntentByTxnPrefix(txnID uint64) []byte {
	return buildKeyUint64(pebblePrefixIntentByTxn, txnID)
}

func pebbleCdcRawKey(txnID, seq uint64) []byte {
	return buildKeyUint64x2(pebblePrefixCDCRaw, txnID, seq)
}

func pebbleCdcRawPrefix(txnID uint64) []byte {
	return buildKeyUint64(pebblePrefixCDCRaw, txnID)
}

func pebbleReplKey(peerNodeID uint64, dbName string) []byte {
	key := make([]byte, len(pebblePrefixRepl)+8+1+len(dbName))
	n := copy(key, pebblePrefixRepl)
	binary.BigEndian.PutUint64(key[n:], peerNodeID)
	n += 8
	key[n] = '/'
	copy(key[n+1:], dbName)
	return key
}

func pebbleSchemaKey(dbName string) []byte {
	return buildKeyString(pebblePrefixSchema, dbName)
}

func pebbleDdlLockKey(dbName string) []byte {
	return buildKeyString(pebblePrefixDDLLock, dbName)
}

func pebbleSeqKey(nodeID uint64) []byte {
	return buildKeyUint64(pebblePrefixSeq, nodeID)
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

// readTxnStatus reads 1-byte status from status key
func (s *PebbleMetaStore) readTxnStatus(txnID uint64) (TxnStatus, error) {
	val, closer, err := s.db.Get(pebbleTxnStatusKey(txnID))
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(val) != 1 {
		return 0, fmt.Errorf("invalid status length: %d", len(val))
	}
	return TxnStatus(val[0]), nil
}

// writeImmutableTxnRecord writes the immutable transaction record to /txn/{txnID}.
// Used by MemoryMetaStore to write durable data while keeping status/heartbeat in memory.
func (s *PebbleMetaStore) writeImmutableTxnRecord(txnID, nodeID uint64, startTS hlc.Timestamp) error {
	now := time.Now().UnixNano()

	immutable := &TxnImmutableRecord{
		TxnID:          txnID,
		NodeID:         nodeID,
		StartTSWall:    startTS.WallTime,
		StartTSLogical: startTS.Logical,
		CreatedAt:      now,
	}

	native, err := encoding.MarshalNative(immutable)
	if err != nil {
		return fmt.Errorf("failed to marshal immutable record: %w", err)
	}
	defer native.Dispose()

	return s.db.Set(pebbleTxnKey(txnID), native.Bytes(), pebble.NoSync)
}

// readImmutableTxnRecord reads the immutable transaction record from /txn/{txnID}.
func (s *PebbleMetaStore) readImmutableTxnRecord(txnID uint64) (*TxnImmutableRecord, error) {
	val, closer, err := s.db.Get(pebbleTxnKey(txnID))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var immutable TxnImmutableRecord
	if err := encoding.Unmarshal(val, &immutable); err != nil {
		return nil, err
	}

	return &immutable, nil
}

// readCommitRecord reads the commit record from /txn_commit/{txnID}.
func (s *PebbleMetaStore) readCommitRecord(txnID uint64) (*TxnCommitRecord, error) {
	val, closer, err := s.db.Get(pebbleTxnCommitKey(txnID))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var commit TxnCommitRecord
	if err := encoding.Unmarshal(val, &commit); err != nil {
		return nil, err
	}

	return &commit, nil
}

// deleteTransactionKeys deletes transaction records from Pebble.
// Used by MemoryMetaStore which manages status/heartbeat in memory.
func (s *PebbleMetaStore) deleteTransactionKeys(txnID uint64, isCommitted bool) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	// Delete immutable record
	if err := batch.Delete(pebbleTxnKey(txnID), nil); err != nil {
		return err
	}

	// Delete commit record if exists
	_ = batch.Delete(pebbleTxnCommitKey(txnID), nil)

	// If committed, try to remove from sequence index
	if isCommitted {
		// Try to read commit record to get seqNum before deleting
		commit, err := s.readCommitRecord(txnID)
		if err == nil && commit != nil {
			_ = batch.Delete(pebbleTxnSeqKey(commit.SeqNum, txnID), nil)
		}
	}

	return batch.Commit(pebble.NoSync)
}

// BeginTransaction creates a new transaction record with PENDING status
func (s *PebbleMetaStore) BeginTransaction(txnID, nodeID uint64, startTS hlc.Timestamp) error {
	log.Debug().
		Uint64("txn_id", txnID).
		Uint64("node_id", nodeID).
		Int64("start_ts", startTS.WallTime).
		Msg("CDC: BeginTransaction")

	now := time.Now().UnixNano()

	// Create immutable record (written once, never modified)
	immutable := &TxnImmutableRecord{
		TxnID:          txnID,
		NodeID:         nodeID,
		StartTSWall:    startTS.WallTime,
		StartTSLogical: startTS.Logical,
		CreatedAt:      now,
	}

	native, err := encoding.MarshalNative(immutable)
	if err != nil {
		return fmt.Errorf("failed to marshal immutable record: %w", err)
	}
	defer native.Dispose()

	batch := s.db.NewBatch()
	defer batch.Close()

	// Write immutable record to /txn/{txnID}
	if err := batch.Set(pebbleTxnKey(txnID), native.Bytes(), nil); err != nil {
		return err
	}

	// Write status byte to /txn_status/{txnID}
	statusBuf := []byte{byte(TxnStatusPending)}
	if err := batch.Set(pebbleTxnStatusKey(txnID), statusBuf, nil); err != nil {
		return err
	}

	// Heartbeat is now in-memory only (via MemoryMetaStore.txnStore)

	// Write pending index
	if err := batch.Set(pebbleTxnPendingKey(txnID), nil, nil); err != nil {
		return err
	}

	// NoSync: BeginTransaction is not a durability checkpoint.
	// If crash occurs before PREPARE (WriteIntent), transaction never existed.
	return batch.Commit(pebble.NoSync)
}

// CommitTransaction marks a transaction as COMMITTED
func (s *PebbleMetaStore) CommitTransaction(txnID uint64, commitTS hlc.Timestamp, statements []byte, dbName, tablesInvolved string, requiredSchemaVersion uint64, rowCount uint32) error {
	log.Debug().
		Uint64("txn_id", txnID).
		Int64("commit_ts", commitTS.WallTime).
		Str("database", dbName).
		Uint32("row_count", rowCount).
		Uint64("required_schema_version", requiredSchemaVersion).
		Msg("CDC: CommitTransaction")

	// Step 1: Read ONLY immutable record to get NodeID
	immutableData, err := s.getValueCopy(pebbleTxnKey(txnID))
	if err == pebble.ErrNotFound {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if err != nil {
		return err
	}

	var immutable TxnImmutableRecord
	if err := encoding.Unmarshal(immutableData, &immutable); err != nil {
		return err
	}

	// Step 2: Get sequence number using contention-free Sequence API
	seqNum, err := s.GetNextSeqNum(immutable.NodeID)
	if err != nil {
		return fmt.Errorf("failed to get next seq_num: %w", err)
	}

	now := time.Now().UnixNano()

	// Step 3: Create commit record (NEW key, not update)
	commit := &TxnCommitRecord{
		SeqNum:                seqNum,
		CommitTSWall:          commitTS.WallTime,
		CommitTSLogical:       commitTS.Logical,
		CommittedAt:           now,
		TablesInvolved:        tablesInvolved,
		DatabaseName:          dbName,
		RequiredSchemaVersion: requiredSchemaVersion,
		RowCount:              rowCount,
	}

	native, err := encoding.MarshalNative(commit)
	if err != nil {
		return err
	}
	defer native.Dispose()

	batch := s.db.NewBatch()
	defer batch.Close()

	// Write TxnCommitRecord to /txn_commit/{txnID}
	if err := batch.Set(pebbleTxnCommitKey(txnID), native.Bytes(), nil); err != nil {
		return err
	}

	// Write status byte (TxnStatusCommitted)
	statusBuf := []byte{byte(TxnStatusCommitted)}
	if err := batch.Set(pebbleTxnStatusKey(txnID), statusBuf, nil); err != nil {
		return err
	}

	// Heartbeat is in-memory only - no need to write at commit

	// Remove from pending index
	if err := batch.Delete(pebbleTxnPendingKey(txnID), nil); err != nil {
		return err
	}

	// Add to sequence index (kept for backward compatibility and GC)
	if err := batch.Set(pebbleTxnSeqKey(seqNum, txnID), nil, nil); err != nil {
		return err
	}

	// Add to TxnID index (primary index for streaming - ensures TxnID ordering)
	if err := batch.Set(pebbleTxnByIDKey(txnID), nil, nil); err != nil {
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
func (s *PebbleMetaStore) StoreReplayedTransaction(txnID, nodeID uint64, commitTS hlc.Timestamp, dbName string, rowCount uint32) error {
	log.Debug().
		Uint64("txn_id", txnID).
		Uint64("node_id", nodeID).
		Int64("commit_ts", commitTS.WallTime).
		Str("database", dbName).
		Uint32("row_count", rowCount).
		Msg("StoreReplayedTransaction: storing replayed transaction")

	// Get sequence number
	seqNum, err := s.GetNextSeqNum(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get next seq_num: %w", err)
	}

	now := time.Now().UnixNano()

	// Create immutable record
	immutable := &TxnImmutableRecord{
		TxnID:          txnID,
		NodeID:         nodeID,
		StartTSWall:    commitTS.WallTime,
		StartTSLogical: commitTS.Logical,
		CreatedAt:      now,
	}

	nativeImmutable, err := encoding.MarshalNative(immutable)
	if err != nil {
		return fmt.Errorf("failed to serialize immutable record: %w", err)
	}
	defer nativeImmutable.Dispose()

	// Create commit record
	commit := &TxnCommitRecord{
		SeqNum:                seqNum,
		CommitTSWall:          commitTS.WallTime,
		CommitTSLogical:       commitTS.Logical,
		CommittedAt:           now,
		TablesInvolved:        "",
		DatabaseName:          dbName,
		RequiredSchemaVersion: 0,
		RowCount:              rowCount,
	}

	nativeCommit, err := encoding.MarshalNative(commit)
	if err != nil {
		return fmt.Errorf("failed to serialize commit record: %w", err)
	}
	defer nativeCommit.Dispose()

	batch := s.db.NewBatch()
	defer batch.Close()

	// Write TxnImmutableRecord to /txn/{txnID}
	if err := batch.Set(pebbleTxnKey(txnID), nativeImmutable.Bytes(), nil); err != nil {
		return err
	}

	// Write TxnCommitRecord to /txn_commit/{txnID}
	if err := batch.Set(pebbleTxnCommitKey(txnID), nativeCommit.Bytes(), nil); err != nil {
		return err
	}

	// Write status byte (TxnStatusCommitted)
	statusBuf := []byte{byte(TxnStatusCommitted)}
	if err := batch.Set(pebbleTxnStatusKey(txnID), statusBuf, nil); err != nil {
		return err
	}

	// Heartbeat not needed for replayed transactions (already committed)

	// Add to sequence index (kept for backward compatibility and GC)
	if err := batch.Set(pebbleTxnSeqKey(seqNum, txnID), nil, nil); err != nil {
		return err
	}

	// Add to TxnID index (primary index for streaming - ensures TxnID ordering)
	if err := batch.Set(pebbleTxnByIDKey(txnID), nil, nil); err != nil {
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
	// Read status first (1-byte read, no unmarshal)
	status, err := s.readTxnStatus(txnID)
	if err == pebble.ErrNotFound {
		return nil // Already deleted
	}
	if err != nil {
		return err
	}

	var seqNum uint64
	// If committed, read commit record to get SeqNum for cleanup
	if status == TxnStatusCommitted {
		commitData, err := s.getValueCopy(pebbleTxnCommitKey(txnID))
		if err != nil && err != pebble.ErrNotFound {
			return err
		}
		if err == nil {
			var commit TxnCommitRecord
			if err := encoding.Unmarshal(commitData, &commit); err != nil {
				return err
			}
			seqNum = commit.SeqNum
		}
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	// Delete all keys: /txn/, /txn_commit/, /txn_status/
	if err := batch.Delete(pebbleTxnKey(txnID), nil); err != nil {
		return err
	}
	_ = batch.Delete(pebbleTxnCommitKey(txnID), nil)
	_ = batch.Delete(pebbleTxnStatusKey(txnID), nil)

	// Remove from pending index (best-effort cleanup)
	_ = batch.Delete(pebbleTxnPendingKey(txnID), nil)

	// Remove from sequence index if it had one (best-effort cleanup)
	if seqNum > 0 {
		_ = batch.Delete(pebbleTxnSeqKey(seqNum, txnID), nil)
	}

	// NoSync: AbortTransaction is cleanup. Idempotent - can be redone.
	return batch.Commit(pebble.NoSync)
}

// GetTransaction retrieves a transaction record by ID
// Reconstructs TransactionRecord from split keys for backward compatibility
func (s *PebbleMetaStore) GetTransaction(txnID uint64) (*TransactionRecord, error) {
	// Read TxnImmutableRecord from /txn/
	immutableVal, closer, err := s.db.Get(pebbleTxnKey(txnID))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var immutable TxnImmutableRecord
	if err := encoding.Unmarshal(immutableVal, &immutable); err != nil {
		return nil, err
	}

	// Read status from /txn_status/
	status, err := s.readTxnStatus(txnID)
	if err != nil {
		return nil, err
	}

	// Reconstruct TransactionRecord
	// Heartbeat is in-memory only - use CreatedAt as fallback for direct PebbleMetaStore usage
	rec := &TransactionRecord{
		TxnID:          immutable.TxnID,
		NodeID:         immutable.NodeID,
		Status:         status,
		StartTSWall:    immutable.StartTSWall,
		StartTSLogical: immutable.StartTSLogical,
		CreatedAt:      immutable.CreatedAt,
		LastHeartbeat:  immutable.CreatedAt,
	}

	// If status == Committed, read TxnCommitRecord from /txn_commit/
	if status == TxnStatusCommitted {
		commitData, err := s.getValueCopy(pebbleTxnCommitKey(txnID))
		if err != nil && err != pebble.ErrNotFound {
			return nil, err
		}
		if err == nil {
			var commit TxnCommitRecord
			if err := encoding.Unmarshal(commitData, &commit); err != nil {
				return nil, err
			}
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
		key := iter.Key()
		if len(key) < len(pebblePrefixTxnPending)+8 {
			continue
		}
		txnID := binary.BigEndian.Uint64(key[len(pebblePrefixTxnPending):])

		rec, err := s.GetTransaction(txnID)
		if err == nil && rec != nil && rec.Status == TxnStatusPending {
			records = append(records, rec)
		}
	}

	return records, iter.Error()
}

// Heartbeat is a no-op in PebbleMetaStore - heartbeats are managed in-memory by MemoryMetaStore
func (s *PebbleMetaStore) Heartbeat(_ uint64) error {
	return nil
}

// WriteIntent creates a write intent (distributed lock)
func (s *PebbleMetaStore) WriteIntent(txnID uint64, intentType IntentType, tableName, intentKey string, op OpType, sqlStmt string, data []byte, ts hlc.Timestamp, nodeID uint64) error {
	// Acquire sharded lock to serialize concurrent writes to same row (prevents TOCTOU race)
	mu := s.intentLockFor(tableName, intentKey)
	mu.Lock()
	defer mu.Unlock()

	// Try to acquire row lock
	existingTxnID, acquired := s.rowLocks.AcquireLock("", tableName, intentKey, txnID)
	if !acquired {
		// Lock held by different transaction - check GC marker first
		if s.rowLocks.CheckGCMarker("", tableName, intentKey) {
			// GC marker exists - intent is marked for cleanup, delete marker and acquire
			s.rowLocks.DeleteGCMarker("", tableName, intentKey)
			s.rowLocks.ReleaseLock("", tableName, intentKey)
			existingTxnID, acquired = s.rowLocks.AcquireLock("", tableName, intentKey, txnID)
			if !acquired {
				// Race condition - another transaction acquired the lock
				telemetry.WriteConflictsTotal.With("intent", "gc_race").Inc()
				return fmt.Errorf("write-write conflict: row %s:%s locked by transaction %d (current txn: %d)",
					tableName, intentKey, existingTxnID, txnID)
			}
		} else {
			// No GC marker - resolve conflict
			if err := s.resolveIntentConflictPebble(nil, existingTxnID, txnID, tableName, intentKey); err != nil {
				telemetry.WriteConflictsTotal.With("intent", "conflict").Inc()
				return err
			}
			// Conflict resolved - release old lock and acquire new one
			s.rowLocks.ReleaseLock("", tableName, intentKey)
			existingTxnID, acquired = s.rowLocks.AcquireLock("", tableName, intentKey, txnID)
			if !acquired {
				// Race condition
				telemetry.WriteConflictsTotal.With("intent", "resolve_race").Inc()
				return fmt.Errorf("write-write conflict: row %s:%s locked by transaction %d (current txn: %d)",
					tableName, intentKey, existingTxnID, txnID)
			}
		}
	}

	// Lock acquired - write full record to /intent_txn/
	rec := &WriteIntentRecord{
		IntentType:   intentType,
		TableName:    tableName,
		IntentKey:    []byte(intentKey),
		TxnID:        txnID,
		TSWall:       ts.WallTime,
		TSLogical:    ts.Logical,
		NodeID:       nodeID,
		Operation:    op,
		SQLStatement: sqlStmt,
		DataSnapshot: data,
		CreatedAt:    time.Now().UnixNano(),
	}
	recBytes, err := encoding.MarshalNative(rec)
	if err != nil {
		return err
	}
	defer recBytes.Dispose()

	if err := s.db.Set(pebbleIntentByTxnKey(txnID, tableName, intentKey), recBytes.Bytes(), pebble.NoSync); err != nil {
		return err
	}

	// Heartbeat refresh is handled by MemoryMetaStore in-memory

	return nil
}

// resolveIntentConflictPebble handles conflict with existing intent from different transaction.
// Called after GC marker check - if GC marker exists, caller handles overwrite directly.
func (s *PebbleMetaStore) resolveIntentConflictPebble(batch *pebble.Batch, existingTxnID, txnID uint64, tableName, intentKey string) error {
	// Check conflicting transaction status
	// Use custom txnGetter if set (allows MemoryMetaStore to inject its GetTransaction)
	getTxn := s.GetTransaction
	if s.txnGetter != nil {
		getTxn = s.txnGetter
	}
	conflictTxnRec, _ := getTxn(existingTxnID)

	canOverwrite := false
	switch {
	case conflictTxnRec == nil:
		log.Debug().
			Uint64("orphan_txn_id", existingTxnID).
			Str("table", tableName).
			Str("intent_key", intentKey).
			Msg("Cleaning up orphaned intent (no transaction record)")
		canOverwrite = true

	case conflictTxnRec.Status == TxnStatusCommitted:
		canOverwrite = true

	case conflictTxnRec.Status == TxnStatusAborted:
		log.Debug().
			Uint64("aborted_txn_id", existingTxnID).
			Str("table", tableName).
			Str("intent_key", intentKey).
			Msg("Cleaning up intent from aborted transaction")
		canOverwrite = true

	default:
		// Check heartbeat timeout - use heartbeat from txnGetter (in-memory via MemoryMetaStore)
		heartbeatTimeout := int64(10 * time.Second)
		if cfg.Config != nil && cfg.Config.Transaction.HeartbeatTimeoutSeconds > 0 {
			heartbeatTimeout = int64(time.Duration(cfg.Config.Transaction.HeartbeatTimeoutSeconds) * time.Second)
		}

		heartbeat := conflictTxnRec.LastHeartbeat
		timeSinceHeartbeat := time.Now().UnixNano() - heartbeat
		if timeSinceHeartbeat > heartbeatTimeout {
			log.Debug().
				Uint64("stale_txn_id", existingTxnID).
				Str("table", tableName).
				Str("intent_key", intentKey).
				Int64("heartbeat_age_ms", timeSinceHeartbeat/1e6).
				Msg("Cleaning up stale intent (heartbeat timeout)")
			canOverwrite = true
		}
	}

	if !canOverwrite {
		return fmt.Errorf("write-write conflict: row %s:%s locked by transaction %d (current txn: %d)",
			tableName, intentKey, existingTxnID, txnID)
	}

	// Delete /intent_txn/ index for the overwritten transaction
	if err := s.db.Delete(pebbleIntentByTxnKey(existingTxnID, tableName, intentKey), pebble.NoSync); err != nil {
		return err
	}

	return nil
}

// ValidateIntent checks if the intent is still held by the expected transaction
func (s *PebbleMetaStore) ValidateIntent(tableName, intentKey string, expectedTxnID uint64) (bool, error) {
	holder, exists := s.rowLocks.CheckLock("", tableName, intentKey)
	return exists && holder == expectedTxnID, nil
}

// DeleteIntent removes a specific write intent
func (s *PebbleMetaStore) DeleteIntent(tableName, intentKey string, txnID uint64) error {
	// Verify the intent belongs to this transaction
	holder, exists := s.rowLocks.CheckLock("", tableName, intentKey)
	if !exists || holder != txnID {
		return nil // Intent doesn't exist or belongs to different transaction
	}

	// Release row lock
	s.rowLocks.ReleaseLock("", tableName, intentKey)

	// Delete /intent_txn/ index
	if err := s.db.Delete(pebbleIntentByTxnKey(txnID, tableName, intentKey), pebble.NoSync); err != nil {
		return err
	}

	return nil
}

// DeleteIntentsByTxn removes all write intents for a transaction
func (s *PebbleMetaStore) DeleteIntentsByTxn(txnID uint64) error {
	prefix := pebbleIntentByTxnPrefix(txnID)

	// Release all row locks for this transaction
	s.rowLocks.ReleaseByTxn(txnID)

	// Collect /intent_txn/ index keys to delete
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
	}
	if err := iter.Close(); err != nil {
		return err
	}

	if len(indexKeys) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for _, key := range indexKeys {
		_ = batch.Delete(key, nil)
	}

	// NoSync: Intent cleanup is idempotent. If crash occurs, intents remain
	// (transaction is already committed) and will be cleaned up on next GC.
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	return nil
}

// MarkIntentsForCleanup marks all intents for a transaction as ready for overwrite.
// Uses in-memory RowLockStore.byTxn index instead of Pebble iteration.
func (s *PebbleMetaStore) MarkIntentsForCleanup(txnID uint64) error {
	s.rowLocks.MarkGCByTxn(txnID)
	return nil
}

// CleanupAfterCommit performs all cleanup operations for a committed transaction.
// Phase 1: Marks intents with GC markers (other txns can overwrite)
// Phase 2: Deletes all intents and CDC entries in a single batch
// This consolidates MarkIntentsForCleanup + DeleteIntentsByTxn + DeleteIntentEntries
func (s *PebbleMetaStore) CleanupAfterCommit(txnID uint64) error {
	// Single pass: mark GC, release locks, get keys (no Pebble iteration)
	lockKeys := s.rowLocks.MarkGCAndRelease(txnID)

	// Single batch for all deletions
	batch := s.db.NewBatch()
	defer batch.Close()

	// Delete intent index keys using in-memory data (no Pebble iteration)
	for _, fullKey := range lockKeys {
		_, table, rowKey := parseFullKey(fullKey)
		if table != "" {
			_ = batch.Delete(pebbleIntentByTxnKey(txnID, table, rowKey), nil)
		}
	}

	// Collect CDC entry keys (/cdc/raw/{txnID}/*) - still need iteration for now
	cdcPrefix := pebbleCdcRawPrefix(txnID)

	cdcIter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: cdcPrefix,
		UpperBound: prefixUpperBound(cdcPrefix),
	})
	if err != nil {
		return err
	}

	for cdcIter.SeekGE(cdcPrefix); cdcIter.Valid(); cdcIter.Next() {
		key := make([]byte, len(cdcIter.Key()))
		copy(key, cdcIter.Key())
		_ = batch.Delete(key, nil)
	}

	if err := cdcIter.Close(); err != nil {
		return err
	}

	// Early return if nothing to delete
	if batch.Empty() {
		return nil
	}

	// NoSync: Intent cleanup is idempotent. If crash occurs, intents remain
	// (transaction is already committed) and will be cleaned up on next GC.
	return batch.Commit(pebble.NoSync)
}

// GetIntentsByTxn retrieves all write intents for a transaction
func (s *PebbleMetaStore) GetIntentsByTxn(txnID uint64) ([]*WriteIntentRecord, error) {
	var intents []*WriteIntentRecord
	prefix := pebbleIntentByTxnPrefix(txnID)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		// Full record is stored inline in /intent_by_txn/ - no random seeks needed
		val, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		intent := &WriteIntentRecord{}
		if err := encoding.Unmarshal(val, intent); err == nil {
			intents = append(intents, intent)
		}
	}

	return intents, iter.Error()
}

// GetIntent retrieves a specific write intent (two-step lookup for admin API)
func (s *PebbleMetaStore) GetIntent(tableName, intentKey string) (*WriteIntentRecord, error) {
	// Step 1: Check RowLockStore to find TxnID
	txnID, exists := s.rowLocks.CheckLock("", tableName, intentKey)
	if !exists {
		return nil, nil
	}

	// Step 2: Get full record from /intent_by_txn/
	recVal, recCloser, err := s.db.Get(pebbleIntentByTxnKey(txnID, tableName, intentKey))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer recCloser.Close()

	intent := &WriteIntentRecord{}
	if err := encoding.Unmarshal(recVal, intent); err != nil {
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

// WriteIntentEntry writes a CDC intent entry using the unified EncodedCapturedRow format
func (s *PebbleMetaStore) WriteIntentEntry(txnID, seq uint64, op uint8, table, intentKey string, oldVals, newVals map[string][]byte) error {
	// Create EncodedCapturedRow for unified storage format (same as WriteCapturedRow)
	row := &EncodedCapturedRow{
		Table:     table,
		Op:        op,
		IntentKey: []byte(intentKey),
		OldValues: oldVals,
		NewValues: newVals,
	}

	log.Debug().
		Uint64("txn_id", txnID).
		Uint64("seq", seq).
		Uint8("op", op).
		Str("table", table).
		Str("intent_key", intentKey).
		Int("old_values_count", len(oldVals)).
		Int("new_values_count", len(newVals)).
		Msg("CDC: WriteIntentEntry")

	data, err := EncodeRow(row)
	if err != nil {
		return err
	}

	// Write to /cdc/raw/ for unified access with hookCallback path
	// NoSync: CDC entries are protected by WriteIntent (PREPARE).
	// If crash occurs, intent exists and CDC can be recovered or transaction aborted.
	return s.db.Set(pebbleCdcRawKey(txnID, seq), data, pebble.NoSync)
}

// GetIntentEntries retrieves CDC intent entries for a transaction.
// Reads from /cdc/raw/ and converts EncodedCapturedRow to IntentEntry format.
func (s *PebbleMetaStore) GetIntentEntries(txnID uint64) ([]*IntentEntry, error) {
	var entries []*IntentEntry
	prefix := pebbleCdcRawPrefix(txnID)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var seq uint64
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		val, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		// Extract sequence from key (/cdc/raw/{8 bytes txnID}{8 bytes seq})
		expectedLen := len(pebblePrefixCDCRaw) + 16
		if len(key) >= expectedLen {
			seq = binary.BigEndian.Uint64(key[len(pebblePrefixCDCRaw)+8:])
		}

		// Decode EncodedCapturedRow
		row, err := DecodeRow(val)
		if err != nil {
			continue
		}

		// Convert to IntentEntry format
		entries = append(entries, &IntentEntry{
			TxnID:     txnID,
			Seq:       seq,
			Operation: row.Op,
			Table:     row.Table,
			IntentKey: row.IntentKey,
			OldValues: row.OldValues,
			NewValues: row.NewValues,
		})
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
	prefix := pebbleCdcRawPrefix(txnID)
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

// WriteCapturedRow stores a raw captured row during hook callback.
// This is the fast path - just store bytes with minimal processing.
// Data is pre-serialized by the caller (CapturedRow msgpack).
func (s *PebbleMetaStore) WriteCapturedRow(txnID, seq uint64, data []byte) error {
	key := pebbleCdcRawKey(txnID, seq)
	return s.db.Set(key, data, pebble.NoSync)
}

// pebbleCapturedRowCursor implements CapturedRowCursor for Pebble
type pebbleCapturedRowCursor struct {
	iter    *pebble.Iterator
	txnID   uint64
	started bool
	seq     uint64
	data    []byte
	err     error
}

// Next advances to the next row
func (c *pebbleCapturedRowCursor) Next() bool {
	if c.err != nil || c.iter == nil {
		return false
	}

	if !c.started {
		c.started = true
		if !c.iter.First() {
			c.err = c.iter.Error()
			return false
		}
	} else {
		if !c.iter.Next() {
			c.err = c.iter.Error()
			return false
		}
	}

	if !c.iter.Valid() {
		return false
	}

	// Extract seq from key (binary format): prefix + 8 bytes txnID + 8 bytes seq
	key := c.iter.Key()
	expectedLen := len(pebblePrefixCDCRaw) + 16
	if len(key) < expectedLen {
		// Skip malformed keys
		return c.Next()
	}
	c.seq = binary.BigEndian.Uint64(key[len(pebblePrefixCDCRaw)+8:])

	val, err := c.iter.ValueAndErr()
	if err != nil {
		c.err = err
		return false
	}

	// Copy data since iter.Value() is only valid until Next()
	c.data = make([]byte, len(val))
	copy(c.data, val)

	return true
}

// Row returns current row's seq and data
func (c *pebbleCapturedRowCursor) Row() (uint64, []byte) {
	return c.seq, c.data
}

// Err returns any iteration error
func (c *pebbleCapturedRowCursor) Err() error {
	return c.err
}

// Close releases the iterator
func (c *pebbleCapturedRowCursor) Close() error {
	if c.iter != nil {
		return c.iter.Close()
	}
	return nil
}

// IterateCapturedRows returns a cursor over raw captured rows for a transaction.
func (s *PebbleMetaStore) IterateCapturedRows(txnID uint64) (CapturedRowCursor, error) {
	prefix := pebbleCdcRawPrefix(txnID)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}

	return &pebbleCapturedRowCursor{
		iter:  iter,
		txnID: txnID,
	}, nil
}

// DeleteCapturedRow deletes a single captured row after processing.
func (s *PebbleMetaStore) DeleteCapturedRow(txnID, seq uint64) error {
	key := pebbleCdcRawKey(txnID, seq)
	return s.db.Delete(key, pebble.NoSync)
}

// DeleteCapturedRows deletes all raw captured rows for a transaction.
// Called after ProcessCapturedRows completes.
func (s *PebbleMetaStore) DeleteCapturedRows(txnID uint64) error {
	prefix := pebbleCdcRawPrefix(txnID)
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

	return batch.Commit(pebble.NoSync)
}

// findOrphanedCDCRawTxnIDs finds transaction IDs that have /cdc/raw/ data but no /txn_commit/ record.
// These are orphaned transactions from crashes that never completed commit.
func (s *PebbleMetaStore) findOrphanedCDCRawTxnIDs() ([]uint64, error) {
	prefix := []byte(pebblePrefixCDCRaw)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	seenTxnIDs := make(map[uint64]bool)

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < len(pebblePrefixCDCRaw)+8 {
			continue
		}
		txnID := binary.BigEndian.Uint64(key[len(pebblePrefixCDCRaw):])
		seenTxnIDs[txnID] = true
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	var orphaned []uint64
	for txnID := range seenTxnIDs {
		_, closer, err := s.db.Get(pebbleTxnCommitKey(txnID))
		if err == pebble.ErrNotFound {
			orphaned = append(orphaned, txnID)
		} else if err == nil {
			closer.Close()
		}
	}

	return orphaned, nil
}

// CleanupStaleTransactions aborts transactions that haven't had a heartbeat within the timeout
func (s *PebbleMetaStore) CleanupStaleTransactions(timeout time.Duration) (int, error) {
	// Check if store is closed - return early to avoid pebble: closed panic
	if s.closed.Load() {
		return 0, nil
	}

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
		key := iter.Key()
		if len(key) < len(pebblePrefixTxnPending)+8 {
			continue
		}
		txnID := binary.BigEndian.Uint64(key[len(pebblePrefixTxnPending):])

		// Heartbeats are in-memory only - for PebbleMetaStore direct use,
		// use CreatedAt to determine staleness (MemoryMetaStore uses in-memory heartbeats)
		rec, err := s.GetTransaction(txnID)
		if err != nil || rec == nil {
			continue
		}

		nowNs := time.Now().UnixNano()
		ageNs := nowNs - rec.CreatedAt
		if ageNs > timeout.Nanoseconds() {
			staleTxnIDs = append(staleTxnIDs, txnID)
			log.Warn().
				Uint64("txn_id", txnID).
				Int64("age_ms", ageNs/1e6).
				Int64("timeout_ms", timeout.Milliseconds()).
				Msg("GC: Found stale PENDING transaction (no active heartbeat)")
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
		cleaned++
	}

	// Phase 2: No orphaned intent cleanup needed - RowLockStore is ephemeral
	// ReleaseByTxn in Phase 1 already cleaned up in-memory locks

	if cleaned > 0 {
		log.Info().
			Int("stale_txns", len(staleTxnIDs)).
			Msg("MetaStore GC: Cleaned up stale transactions")
	}

	return cleaned, nil
}

// CleanupOldTransactionRecords removes old COMMITTED/ABORTED transaction records
func (s *PebbleMetaStore) CleanupOldTransactionRecords(minRetention, maxRetention time.Duration, minAppliedTxnID, minAppliedSeqNum uint64) (int, error) {
	// Check if store is closed - return early to avoid pebble: closed panic
	if s.closed.Load() {
		return 0, nil
	}

	now := time.Now()
	minRetentionCutoff := now.Add(-minRetention).UnixNano()
	maxRetentionCutoff := now.Add(-maxRetention).UnixNano()
	deleted := 0
	committedDeleted := 0

	prefix := []byte(pebblePrefixTxn)
	var keysToDelete [][]byte
	var seqKeysToDelete [][]byte
	var txnIDsToClean []uint64

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
				txnIDsToClean = append(txnIDsToClean, rec.TxnID)
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

	// Clean up CDC raw captured rows for deleted committed transactions
	for _, txnID := range txnIDsToClean {
		if err := s.DeleteCapturedRows(txnID); err != nil {
			log.Debug().Err(err).Uint64("txn_id", txnID).Msg("MetaStore GC: Failed to delete CDC raw rows")
		}
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

	// Go to last key in range (key format: prefix + 8 bytes seqNum + 8 bytes txnID)
	if iter.Last() {
		key := iter.Key()
		if len(key) >= len(pebblePrefixTxnSeq)+8 {
			maxSeq = binary.BigEndian.Uint64(key[len(pebblePrefixTxnSeq):])
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

// ScanTransactions iterates transactions from fromTxnID in TxnID order.
// If descending is true, scans from newest to oldest.
// Callback returns nil to continue, ErrStopIteration to stop, or other error to abort.
func (s *PebbleMetaStore) ScanTransactions(fromTxnID uint64, descending bool, callback func(*TransactionRecord) error) error {
	prefix := []byte(pebblePrefixTxnByID)

	var iterOpts pebble.IterOptions
	if descending {
		// For descending, we want txnID < fromTxnID (or all if fromTxnID == 0)
		iterOpts = pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixUpperBound(prefix),
		}
	} else {
		// For ascending from fromTxnID, start at fromTxnID+1
		startKey := pebbleTxnByIDKey(fromTxnID + 1)
		iterOpts = pebble.IterOptions{
			LowerBound: startKey,
			UpperBound: prefixUpperBound(prefix),
		}
	}

	iter, err := s.db.NewIter(&iterOpts)
	if err != nil {
		return err
	}
	defer iter.Close()

	// Choose iteration direction
	var advance func() bool
	if descending {
		advance = func() bool { return iter.Prev() }
		if fromTxnID > 0 {
			// SeekLT to position just before fromTxnID
			iter.SeekLT(pebbleTxnByIDKey(fromTxnID))
		} else {
			// Start from end
			iter.SeekLT(prefixUpperBound(prefix))
		}
	} else {
		advance = func() bool { return iter.Next() }
		iter.First()
	}

	prefixLen := len(pebblePrefixTxnByID)
	for iter.Valid() {
		key := iter.Key()
		// Key format: prefix + 8 bytes txnID
		if len(key) < prefixLen+8 {
			advance()
			continue
		}

		txnID := binary.BigEndian.Uint64(key[prefixLen:])

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

// CDC Lock Methods (in-memory via cdcLocks)

// AcquireCDCRowLock acquires a row-level CDC lock.
func (s *PebbleMetaStore) AcquireCDCRowLock(txnID uint64, tableName, intentKey string) error {
	return s.cdcLocks.Acquire(txnID, tableName, intentKey)
}

// ReleaseCDCRowLock releases a row-level CDC lock.
func (s *PebbleMetaStore) ReleaseCDCRowLock(tableName, intentKey string, txnID uint64) error {
	s.cdcLocks.Release(tableName, intentKey, txnID)
	return nil
}

// ReleaseCDCRowLocksByTxn releases all CDC row locks held by a transaction.
func (s *PebbleMetaStore) ReleaseCDCRowLocksByTxn(txnID uint64) error {
	s.cdcLocks.ReleaseByTxn(txnID)
	return nil
}

// GetCDCRowLock returns the transaction holding a row lock, or 0 if none.
func (s *PebbleMetaStore) GetCDCRowLock(tableName, intentKey string) (uint64, error) {
	txnID, _ := s.cdcLocks.GetHolder(tableName, intentKey)
	return txnID, nil
}

// AcquireCDCTableDDLLock acquires a table-level DDL lock.
func (s *PebbleMetaStore) AcquireCDCTableDDLLock(txnID uint64, tableName string) error {
	return s.cdcLocks.AcquireDDL(txnID, tableName)
}

// ReleaseCDCTableDDLLock releases a table-level DDL lock.
func (s *PebbleMetaStore) ReleaseCDCTableDDLLock(tableName string, txnID uint64) error {
	s.cdcLocks.ReleaseDDL(tableName, txnID)
	return nil
}

// HasCDCRowLocksForTable checks if any row locks exist for a table.
func (s *PebbleMetaStore) HasCDCRowLocksForTable(tableName string) (bool, error) {
	return s.cdcLocks.HasRowLocks(tableName), nil
}

// GetCDCTableDDLLock returns the transaction holding a DDL lock, or 0 if none.
func (s *PebbleMetaStore) GetCDCTableDDLLock(tableName string) (uint64, error) {
	txnID, _ := s.cdcLocks.GetDDLHolder(tableName)
	return txnID, nil
}

// GetRowLockStats returns statistics from the in-memory row lock store
func (s *PebbleMetaStore) GetRowLockStats() (activeLocks, activeTransactions, gcMarkers, tablesWithLocks int) {
	return s.rowLocks.Stats()
}

// IntentStats returns statistics about pending intents in the store
func (s *PebbleMetaStore) IntentStats() (pendingIntents int, err error) {
	prefix := []byte(pebblePrefixIntentByTxn)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		pendingIntents++
	}

	return pendingIntents, iter.Error()
}
