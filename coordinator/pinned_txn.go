package coordinator

import (
	"context"
	"sort"
	"sync"

	"github.com/maxpert/marmot/common"
	"github.com/rs/zerolog/log"
)

// PinnedSession is a real SQLite transaction held open on a dedicated
// connection for the lifetime of an explicit BEGIN...COMMIT/ROLLBACK, used to
// execute DML eagerly: every statement gets the REAL rows-affected/
// last-insert-id SQLite reports (not the fake buffered-statement response),
// and reads on the same database within the transaction observe the
// transaction's own uncommitted writes.
//
// The underlying SQLite transaction is NEVER committed directly. The actual
// write lands in the database only through CDC replay during 2PC, exactly as
// it does today for ExecuteLocalWithHooks (see that method's doc comment):
// PREPARE persists the captured CDC entries as durable intents, and COMMIT
// applies them to the write connection via TransactionManager.
// applyCDCEntries. Release always rolls the pinned SQLite transaction back -
// on an explicit ROLLBACK, on a 2PC rejection, and even after a successful
// COMMIT (whose CDC entries were already read out via CDCEntries before
// Release runs) - so the write is never applied twice.
type PinnedSession interface {
	// ExecuteStatement runs one DML statement on the pinned transaction. It
	// captures CDC data for the statement and acquires the same per-row
	// CDC/intent locks ExecuteLocalWithHooks does, immediately rather than
	// deferred to Release/COMMIT (so a later statement in the same or another
	// transaction that touches the same row blocks/conflicts from this point
	// on, not just from COMMIT).
	ExecuteStatement(ctx context.Context, sql string, params []interface{}) (rowsAffected int64, lastInsertId int64, err error)

	// Query runs a read on the pinned transaction so it observes the
	// transaction's own uncommitted writes. columns/rows use the same shape
	// as ReplicatedDatabase.ExecuteSnapshotRead.
	Query(ctx context.Context, sql string, params []interface{}) (columns []string, rows []map[string]interface{}, err error)

	// CDCEntries returns every CDC entry captured so far (already row-locked),
	// across all ExecuteStatement calls on this session, in the order the
	// statements ran.
	CDCEntries() []common.CDCEntry

	// Release rolls back the pinned SQLite transaction and releases its row
	// locks and connection. Safe to call exactly once; the caller must not
	// use the session afterward. Never commits - see type doc.
	Release() error
}

// pinnedEntry pairs a PinnedSession with the context that owns its SQLite
// transaction's lifetime. Go's database/sql auto-rolls-back a transaction
// when the context passed to BeginTx is cancelled, so this context must stay
// alive until Release() has been called - cancel is invoked right after,
// mirroring the cancelHookCtx pattern already used for the autocommit path
// in handleMutation.
type pinnedEntry struct {
	session PinnedSession
	cancel  context.CancelFunc
}

// connPinnedState holds the pinned sessions for one connection's currently
// open explicit transaction, keyed by database name. A transaction that
// touches more than one database gets one pinned SQLite transaction per
// database, all committed (via 2PC + CDC replay) or rolled back together at
// COMMIT/ROLLBACK.
type connPinnedState struct {
	mu       sync.Mutex
	txnID    uint64
	sessions map[string]pinnedEntry
}

func newConnPinnedState(txnID uint64) *connPinnedState {
	return &connPinnedState{
		txnID:    txnID,
		sessions: make(map[string]pinnedEntry),
	}
}

// get returns the pinned session for database, if one exists.
func (st *connPinnedState) get(database string) (PinnedSession, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	entry, ok := st.sessions[database]
	if !ok {
		return nil, false
	}
	return entry.session, true
}

// getOrPin returns the existing pinned session for database, or creates one
// via begin and registers it. begin is called at most once per database per
// transaction.
func (st *connPinnedState) getOrPin(database string, begin func() (PinnedSession, context.CancelFunc, error)) (PinnedSession, error) {
	st.mu.Lock()
	if entry, ok := st.sessions[database]; ok {
		st.mu.Unlock()
		return entry.session, nil
	}
	st.mu.Unlock()

	// begin runs outside the lock: it does real I/O (opens a connection,
	// starts a SQLite transaction) and must not block other databases in the
	// same multi-database transaction from pinning concurrently.
	session, cancel, err := begin()
	if err != nil {
		return nil, err
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if entry, ok := st.sessions[database]; ok {
		// Lost a race with a concurrent pin of the same database; discard
		// the one we just created and use the winner's.
		cancel()
		_ = session.Release()
		return entry.session, nil
	}
	st.sessions[database] = pinnedEntry{session: session, cancel: cancel}
	return session, nil
}

// databasesSorted returns the pinned databases in a deterministic (sorted)
// order, so the CDC entries fed into 2PC have a reproducible statement order
// instead of depending on Go's randomized map iteration.
func (st *connPinnedState) databasesSorted() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	names := make([]string, 0, len(st.sessions))
	for name := range st.sessions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// isEmpty reports whether any database has been pinned yet.
func (st *connPinnedState) isEmpty() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.sessions) == 0
}

// releaseAll rolls back every pinned session and releases its context. Safe
// to call once, after which the state must be discarded (see
// CoordinatorHandler.takePinnedState).
func (st *connPinnedState) releaseAll() {
	st.mu.Lock()
	defer st.mu.Unlock()
	for database, entry := range st.sessions {
		if err := entry.session.Release(); err != nil {
			log.Error().Err(err).Str("database", database).Uint64("txn_id", st.txnID).
				Msg("Failed to release pinned session")
		}
		entry.cancel()
	}
	st.sessions = nil
}

// getOrCreatePinnedState returns the connPinnedState for connID, creating one
// for txnID if none exists. If a stale state from a different (already
// finished) transaction is somehow still present - COMMIT/ROLLBACK/
// CloseSession always remove it via takePinnedState, so this only guards a
// bug elsewhere - it is discarded rather than reused.
func (h *CoordinatorHandler) getOrCreatePinnedState(connID, txnID uint64) *connPinnedState {
	if v, ok := h.pinnedTxns.Load(connID); ok {
		st := v.(*connPinnedState)
		st.mu.Lock()
		sameTxn := st.txnID == txnID
		st.mu.Unlock()
		if sameTxn {
			return st
		}
		log.Warn().Uint64("conn_id", connID).Uint64("stale_txn_id", st.txnID).Uint64("txn_id", txnID).
			Msg("Discarding stale pinned transaction state")
		st.releaseAll()
	}

	st := newConnPinnedState(txnID)
	h.pinnedTxns.Store(connID, st)
	return st
}

// lookupPinnedState returns the connPinnedState for connID without removing
// it, or nil if none exists. Used by in-transaction read routing, which must
// not disturb COMMIT/ROLLBACK's ownership of the state.
func (h *CoordinatorHandler) lookupPinnedState(connID uint64) *connPinnedState {
	v, ok := h.pinnedTxns.Load(connID)
	if !ok {
		return nil
	}
	return v.(*connPinnedState)
}

// takePinnedState removes and returns the connPinnedState for connID, or nil
// if none exists. The caller takes ownership and must call releaseAll (or
// rely on it already being empty) exactly once.
func (h *CoordinatorHandler) takePinnedState(connID uint64) *connPinnedState {
	v, ok := h.pinnedTxns.LoadAndDelete(connID)
	if !ok {
		return nil
	}
	return v.(*connPinnedState)
}
