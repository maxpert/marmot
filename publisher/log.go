package publisher

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/encoding"
	"github.com/rs/zerolog/log"
)

// Key prefixes for Pebble storage
const (
	prefixPubLog    = "/publog/"    // /publog/{16-digit-zero-padded-seq}
	prefixPubCursor = "/pubcursor/" // /pubcursor/{sinkName}
	prefixPubSeq    = "/pubseq"     // /pubseq -> uint64 (next sequence)
)

// Pebble configuration constants
const (
	memTableSize                = 64 << 20 // 64MB
	memTableStopWritesThreshold = 4
	l0CompactionThreshold       = 2
	l0StopWritesThreshold       = 12
	lBaseMaxBytes               = 256 << 20 // 256MB
	maxConcurrentCompactions    = 3
)

// Read and cleanup constants
const (
	defaultReadLimit    = 100  // Default limit for ReadFrom
	cleanupIntervalMask = 0x7F // Cleanup every 128 sequences (newSeq & cleanupIntervalMask == 0)
)

// PublishLog provides a Pebble-backed append-only log for CDC events
type PublishLog struct {
	db   *pebble.DB
	path string

	// In-memory cursor map for fast lookups
	cursors   map[string]uint64
	cursorsMu sync.RWMutex

	// Next sequence number (atomic)
	nextSeq atomic.Uint64

	// Cleanup tracking
	cleanupMu      sync.Mutex
	cleanupRunning atomic.Bool
	cleanupWg      sync.WaitGroup

	// Closed state
	closed atomic.Bool
}

// NewPublishLog creates or opens a Pebble-backed publish log
func NewPublishLog(dataDir string) (*PublishLog, error) {
	logPath := filepath.Join(dataDir, "publish_log")

	opts := &pebble.Options{
		// Optimize for sequential writes
		MemTableSize:                memTableSize,
		MemTableStopWritesThreshold: memTableStopWritesThreshold,
		L0CompactionThreshold:       l0CompactionThreshold,
		L0StopWritesThreshold:       l0StopWritesThreshold,
		LBaseMaxBytes:               lBaseMaxBytes,
		MaxConcurrentCompactions:    func() int { return maxConcurrentCompactions },
		DisableWAL:                  false,
	}

	db, err := pebble.Open(logPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open publish log at %s: %w", logPath, err)
	}

	pl := &PublishLog{
		db:      db,
		path:    logPath,
		cursors: make(map[string]uint64),
	}

	// Load next sequence number from Pebble
	if err := pl.loadNextSeq(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load sequence number: %w", err)
	}

	// Load all cursors from Pebble into memory
	if err := pl.loadCursors(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load cursors: %w", err)
	}

	return pl, nil
}

// loadNextSeq loads the next sequence number from Pebble
func (pl *PublishLog) loadNextSeq() error {
	val, closer, err := pl.db.Get([]byte(prefixPubSeq))
	if err == pebble.ErrNotFound {
		// First run - start at 0 (first Add(1) will give us sequence 1)
		pl.nextSeq.Store(0)
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()

	if len(val) != 8 {
		return fmt.Errorf("invalid sequence value length: %d", len(val))
	}

	seq := binary.LittleEndian.Uint64(val)
	pl.nextSeq.Store(seq)
	return nil
}

// loadCursors loads all cursors from Pebble into the in-memory map
func (pl *PublishLog) loadCursors() error {
	prefix := []byte(prefixPubCursor)
	iter, err := pl.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	count := 0
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := string(iter.Key()[len(prefixPubCursor):])
		val, err := iter.ValueAndErr()
		if err != nil {
			return err
		}
		if len(val) != 8 {
			return fmt.Errorf("corrupted cursor data for sink %s: invalid length %d", key, len(val))
		}

		cursor := binary.LittleEndian.Uint64(val)
		pl.cursors[key] = cursor
		count++
	}

	if err := iter.Error(); err != nil {
		return err
	}

	if count > 0 {
		log.Info().Int("cursors", count).Msg("Loaded publish log cursors")
	}

	return nil
}

// Append adds CDC events to the log and assigns sequence numbers.
// Note: This function modifies the input events slice by setting SeqNum on each event.
func (pl *PublishLog) Append(events []CDCEvent) error {
	if len(events) == 0 {
		return nil
	}

	if pl.closed.Load() {
		return fmt.Errorf("publish log is closed")
	}

	// Reserve sequence numbers locally first (before commit)
	startSeq := pl.nextSeq.Load()
	localSeq := startSeq

	batch := pl.db.NewBatch()
	defer batch.Close()

	for i := range events {
		// Assign monotonic sequence number locally
		localSeq++
		events[i].SeqNum = localSeq

		// Marshal event to msgpack
		val, err := encoding.Marshal(&events[i])
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		// Key: /publog/{16-digit-zero-padded-seq}
		key := formatPubLogKey(localSeq)
		if err := batch.Set([]byte(key), val, pebble.Sync); err != nil {
			return fmt.Errorf("failed to write event: %w", err)
		}
	}

	// Update next sequence number in Pebble
	seqBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqBuf, localSeq)
	if err := batch.Set([]byte(prefixPubSeq), seqBuf, pebble.Sync); err != nil {
		return fmt.Errorf("failed to update sequence: %w", err)
	}

	// Commit the batch
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	// Only update in-memory nextSeq AFTER successful commit
	pl.nextSeq.Store(localSeq)

	return nil
}

// ReadFrom reads CDC events starting from the given cursor, up to limit events
func (pl *PublishLog) ReadFrom(cursor uint64, limit int) ([]CDCEvent, error) {
	if pl.closed.Load() {
		return nil, fmt.Errorf("publish log is closed")
	}

	if limit <= 0 {
		limit = defaultReadLimit
	}

	// Start from cursor + 1 (cursor is the last processed event)
	startKey := formatPubLogKey(cursor + 1)
	prefix := []byte(prefixPubLog)

	iter, err := pl.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(startKey),
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	events := make([]CDCEvent, 0, limit)
	for iter.SeekGE([]byte(startKey)); iter.Valid() && len(events) < limit; iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}

		var event CDCEvent
		if err := encoding.Unmarshal(val, &event); err != nil {
			// Log and skip corrupted events
			log.Warn().Err(err).Str("key", string(iter.Key())).Msg("Failed to unmarshal CDC event")
			continue
		}

		events = append(events, event)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return events, nil
}

// GetCursor returns the current cursor for a sink
func (pl *PublishLog) GetCursor(sinkName string) (uint64, error) {
	if pl.closed.Load() {
		return 0, fmt.Errorf("publish log is closed")
	}

	pl.cursorsMu.RLock()
	cursor, exists := pl.cursors[sinkName]
	pl.cursorsMu.RUnlock()

	if exists {
		return cursor, nil
	}

	// Not in memory - check Pebble
	key := prefixPubCursor + sinkName
	val, closer, err := pl.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return 0, nil // New sink - start from beginning
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()

	if len(val) != 8 {
		return 0, fmt.Errorf("invalid cursor value length: %d", len(val))
	}

	cursor = binary.LittleEndian.Uint64(val)

	// Cache in memory with proper double-check locking
	pl.cursorsMu.Lock()
	// Recheck after acquiring write lock - another goroutine might have populated it
	if existingCursor, exists := pl.cursors[sinkName]; exists {
		pl.cursorsMu.Unlock()
		return existingCursor, nil
	}
	pl.cursors[sinkName] = cursor
	pl.cursorsMu.Unlock()

	return cursor, nil
}

// AdvanceCursor updates the cursor for a sink and triggers cleanup periodically
func (pl *PublishLog) AdvanceCursor(sinkName string, newSeq uint64) error {
	if pl.closed.Load() {
		return fmt.Errorf("publish log is closed")
	}

	// Update in-memory map
	pl.cursorsMu.Lock()
	pl.cursors[sinkName] = newSeq
	pl.cursorsMu.Unlock()

	// Persist to Pebble
	key := prefixPubCursor + sinkName
	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, newSeq)

	if err := pl.db.Set([]byte(key), val, pebble.Sync); err != nil {
		return fmt.Errorf("failed to update cursor: %w", err)
	}

	// Trigger cleanup every 128 sequence numbers using TryLock to prevent goroutine accumulation
	if newSeq&cleanupIntervalMask == 0 {
		// Only spawn cleanup if one isn't already running
		if pl.cleanupRunning.CompareAndSwap(false, true) {
			pl.cleanupWg.Add(1)
			go pl.cleanupAsync()
		}
	}

	return nil
}

// cleanup deletes old log entries below the minimum cursor.
// This method is safe to call directly (e.g., from tests) and does not use WaitGroup tracking.
func (pl *PublishLog) cleanup() {
	pl.cleanupMu.Lock()
	defer pl.cleanupMu.Unlock()

	// Check if closed before accessing db
	if pl.closed.Load() {
		return
	}

	// Find minimum cursor across all sinks
	pl.cursorsMu.RLock()
	if len(pl.cursors) == 0 {
		pl.cursorsMu.RUnlock()
		return
	}

	minCursor := uint64(^uint64(0)) // Max uint64
	for _, cursor := range pl.cursors {
		if cursor < minCursor {
			minCursor = cursor
		}
	}
	pl.cursorsMu.RUnlock()

	if minCursor == 0 {
		return // Nothing to cleanup
	}

	// Delete all entries with seq < minCursor
	startKey := []byte(prefixPubLog)
	endKey := []byte(formatPubLogKey(minCursor))

	if err := pl.db.DeleteRange(startKey, endKey, pebble.Sync); err != nil {
		log.Warn().Err(err).Uint64("min_cursor", minCursor).Msg("Failed to cleanup publish log")
		return
	}

	log.Debug().Uint64("min_cursor", minCursor).Msg("Cleaned up publish log entries")
}

// cleanupAsync wraps cleanup with WaitGroup tracking for async execution
func (pl *PublishLog) cleanupAsync() {
	defer pl.cleanupWg.Done()
	defer pl.cleanupRunning.Store(false)
	pl.cleanup()
}

// Close closes the Pebble database and waits for in-flight cleanup goroutines
func (pl *PublishLog) Close() error {
	// Mark as closed to prevent new operations
	if !pl.closed.CompareAndSwap(false, true) {
		return fmt.Errorf("publish log already closed")
	}

	// Wait for any in-flight cleanup goroutines to finish
	pl.cleanupWg.Wait()

	// Close the database
	if pl.db != nil {
		return pl.db.Close()
	}
	return nil
}

// formatPubLogKey formats a sequence number as a 16-digit zero-padded key
func formatPubLogKey(seq uint64) string {
	return fmt.Sprintf("%s%016x", prefixPubLog, seq)
}

// prefixUpperBound returns the upper bound for a prefix scan
func prefixUpperBound(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}
	return nil // Prefix is all 0xff
}
