package db

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// CommitBatcher batches SQLite commits to reduce fsync overhead
// Unlike coordinator-level batching, this operates at the SQLite layer
// where batching actually helps (reduces fsync calls)
type CommitBatcher struct {
	db           *sql.DB
	maxBatchSize int
	maxWaitTime  time.Duration

	mu           sync.Mutex
	pendingBatch []*commitRequest
	timer        *time.Timer
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

type commitRequest struct {
	fn       func(*sql.Tx) error
	resultCh chan error
}

// NewCommitBatcher creates a new commit batcher for SQLite
func NewCommitBatcher(db *sql.DB, maxBatchSize int, maxWaitTime time.Duration) *CommitBatcher {
	return &CommitBatcher{
		db:           db,
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		pendingBatch: make([]*commitRequest, 0, maxBatchSize),
		stopCh:       make(chan struct{}),
	}
}

// Start begins the batch commit loop
func (cb *CommitBatcher) Start() {
	cb.wg.Add(1)
	go cb.batchLoop()
}

// Stop gracefully stops the batcher
func (cb *CommitBatcher) Stop() {
	close(cb.stopCh)
	cb.wg.Wait()
}

// ExecuteInBatch executes a function within a batched transaction
// Multiple concurrent calls will be batched into a single SQLite transaction
func (cb *CommitBatcher) ExecuteInBatch(ctx context.Context, fn func(*sql.Tx) error) error {
	req := &commitRequest{
		fn:       fn,
		resultCh: make(chan error, 1),
	}

	cb.mu.Lock()
	cb.pendingBatch = append(cb.pendingBatch, req)
	batchSize := len(cb.pendingBatch)

	// Start timer if this is the first request in batch
	if batchSize == 1 {
		cb.timer = time.AfterFunc(cb.maxWaitTime, cb.flushBatch)
	}

	// Flush immediately if batch is full
	if batchSize >= cb.maxBatchSize {
		if cb.timer != nil {
			cb.timer.Stop()
		}
		cb.mu.Unlock()
		cb.flushBatch()
	} else {
		cb.mu.Unlock()
	}

	// Wait for result
	select {
	case err := <-req.resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// batchLoop periodically flushes batches
func (cb *CommitBatcher) batchLoop() {
	defer cb.wg.Done()

	ticker := time.NewTicker(cb.maxWaitTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cb.flushBatch()
		case <-cb.stopCh:
			cb.flushBatch() // Final flush
			return
		}
	}
}

// flushBatch executes all pending commits in a single SQLite transaction
func (cb *CommitBatcher) flushBatch() {
	cb.mu.Lock()
	if len(cb.pendingBatch) == 0 {
		cb.mu.Unlock()
		return
	}

	batch := cb.pendingBatch
	cb.pendingBatch = make([]*commitRequest, 0, cb.maxBatchSize)
	cb.mu.Unlock()

	// Execute all commits in a single SQLite transaction
	// This is the RIGHT place for batching - reduces fsync overhead
	tx, err := cb.db.Begin()
	if err != nil {
		// Fail all requests
		for _, req := range batch {
			req.resultCh <- err
		}
		return
	}

	// Execute all functions within the transaction
	var commitErr error
	for _, req := range batch {
		if err := req.fn(tx); err != nil {
			commitErr = err
			break
		}
	}

	// Commit or rollback
	if commitErr != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error().Err(rbErr).Msg("Failed to rollback batch transaction")
		}
		// Fail all requests
		for _, req := range batch {
			req.resultCh <- commitErr
		}
		log.Error().Err(commitErr).Int("batch_size", len(batch)).Msg("Batch commit failed")
		return
	}

	// Single fsync for entire batch!
	if err := tx.Commit(); err != nil {
		// Fail all requests
		for _, req := range batch {
			req.resultCh <- err
		}
		log.Error().Err(err).Int("batch_size", len(batch)).Msg("Batch commit transaction failed")
		return
	}

	// Success - notify all requests
	for _, req := range batch {
		req.resultCh <- nil
	}

	if len(batch) > 1 {
		log.Debug().Int("batch_size", len(batch)).Msg("Batch commit succeeded")
	}
}
