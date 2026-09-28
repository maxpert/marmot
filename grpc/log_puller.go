package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Default tuning for LogPuller, applied by NewLogPuller when the
// corresponding LogPullerConfig field is left zero.
const (
	// DefaultLogPullPageSize bounds how many log entries one ListCommittedLog
	// page returns.
	DefaultLogPullPageSize = 256

	// DefaultMaxReplayAttempts is the number of consecutive replay failures
	// (per peer, database and txn) after which a transaction is reported
	// STUCK.
	DefaultMaxReplayAttempts = 5

	// DefaultStuckRetryEveryRounds bounds how often a STUCK transaction is
	// retried, in PullPair calls for its (peer, database) pair, once it has
	// stopped being retried every round.
	DefaultStuckRetryEveryRounds = 20

	// maxPagesPerPull bounds how many ListCommittedLog pages one PullPair
	// call walks, so a pair whose cursor is held back by an entry it cannot
	// apply yet still delivers the entries after it without rescanning an
	// unbounded tail every round.
	maxPagesPerPull = 64
)

// PeerRef identifies a peer node a LogPuller pulls a database's local commit
// log from.
type PeerRef struct {
	NodeID  uint64
	Address string
}

// LogPullerConfig configures a LogPuller. PageSize, MaxReplayAttempts and
// StuckRetryEveryRounds default to the Default* constants above when zero.
type LogPullerConfig struct {
	NodeID                uint64
	Client                *Client
	DBManager             *db.DatabaseManager
	PageSize              int
	MaxReplayAttempts     int
	StuckRetryEveryRounds int
}

// PairResult reports what one LogPuller.PullPair call did for one (peer,
// database) pair.
type PairResult struct {
	// Applied counts transactions this call actually applied (not merely
	// found already covered by an existing marker).
	Applied int

	// CaughtUp is true when this call's final cursor reached the peer's
	// stable point: the last page had no more entries beyond it and the
	// walk covered every entry that page listed. Membership-aware
	// "caught up for the database" is a property of every current member,
	// computed by the caller from a PairResult per peer.
	CaughtUp bool

	// NeedsSnapshot is true when the peer's log no longer covers this
	// pair's cursor (its GC has truncated past it): nothing was applied,
	// and the pair must be restored from a snapshot instead.
	NeedsSnapshot bool

	// DatabaseAbsent is true when the peer does not have this database.
	DatabaseAbsent bool

	// Unimplemented is true when the peer does not implement
	// ListCommittedLog (a rolling-upgrade peer running an older version).
	Unimplemented bool

	// PeerSchemaVersion is the peer's reported schema version for this
	// database, from the last ListCommittedLog response this call read.
	PeerSchemaVersion uint64

	// Stuck counts STUCK transactions this call encountered for this pair
	// (see LogPuller.StuckTxns).
	Stuck int
}

// StuckTxn describes one transaction a LogPuller has stopped retrying every
// round after DefaultMaxReplayAttempts (or LogPullerConfig.MaxReplayAttempts)
// consecutive replay failures.
type StuckTxn struct {
	Database   string
	PeerNodeID uint64
	TxnID      uint64
	Attempts   int
	LastError  string
}

// pairKey identifies one (peer, database) pull pair, for the per-pair call
// counter that drives the STUCK retry cadence.
type pairKey struct {
	peer     uint64
	database string
}

// txnAttemptKey identifies one (peer, database, txn) replay-attempt counter.
type txnAttemptKey struct {
	peer     uint64
	database string
	txnID    uint64
}

// txnAttemptState is the poison-txn bookkeeping for one txnAttemptKey.
type txnAttemptState struct {
	attempts int
	lastErr  string
	stuck    bool
}

// LogPuller pulls one node's per-peer local commit logs, exactly (never
// silently skipping an entry) and exactly-once (via the applied-txn marker,
// db.ApplyReplayedTxn). Every current member's log is pulled independently,
// from a durably persisted cursor, with no shared deadline between pairs, so
// a transaction any member committed reaches this node even when no single
// peer holds every transaction this node lacks.
type LogPuller struct {
	nodeID                uint64
	client                *Client
	dbMgr                 *db.DatabaseManager
	pageSize              int
	maxReplayAttempts     int
	stuckRetryEveryRounds int

	mu        sync.Mutex
	attempts  map[txnAttemptKey]*txnAttemptState
	pairCalls map[pairKey]int
	// restoredGen counts, per database, the snapshot restores it has had;
	// pairRestoredGen is the count each pair last pulled under. A pair whose
	// database was restored since its last pull moves its cursor up to the
	// peer's truncation point instead of reporting NeedsSnapshot.
	restoredGen     map[string]uint64
	pairRestoredGen map[pairKey]uint64

	warnedUnimplemented sync.Map // peer NodeID -> struct{}
}

// NewLogPuller builds a LogPuller from cfg, applying the Default* constants
// for any zero-valued tuning field.
func NewLogPuller(cfg LogPullerConfig) *LogPuller {
	pageSize := cfg.PageSize
	if pageSize <= 0 {
		pageSize = DefaultLogPullPageSize
	}
	maxAttempts := cfg.MaxReplayAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxReplayAttempts
	}
	stuckEvery := cfg.StuckRetryEveryRounds
	if stuckEvery <= 0 {
		stuckEvery = DefaultStuckRetryEveryRounds
	}
	return &LogPuller{
		nodeID:                cfg.NodeID,
		client:                cfg.Client,
		dbMgr:                 cfg.DBManager,
		pageSize:              pageSize,
		maxReplayAttempts:     maxAttempts,
		stuckRetryEveryRounds: stuckEvery,
		attempts:              make(map[txnAttemptKey]*txnAttemptState),
		pairCalls:             make(map[pairKey]int),
		restoredGen:           make(map[string]uint64),
		pairRestoredGen:       make(map[pairKey]uint64),
	}
}

// PullPair pulls peer's local commit log for database, from this pair's
// persisted cursor (db.MetaStore.GetPullCursor/SetPullCursor), applying
// every transaction it is missing through ApplyPulledEvent and advancing
// the cursor over the contiguous prefix of covered entries. PairResult's
// fields document each outcome.
//
// PullPair does not share a context or deadline with any other pair: the
// caller bounds ctx per call. An interrupted call (ctx cancelled, or any
// other error) leaves the cursor exactly where it last persisted it -
// PullPair always persists progress after every page it fully or partially
// covers before returning - so the next call resumes from there.
func (lp *LogPuller) PullPair(ctx context.Context, peer PeerRef, database string) (PairResult, error) {
	result, err := lp.pullPair(ctx, peer, database)
	telemetry.LogPullPairResultsTotal.With(pairOutcome(result, err)).Inc()
	if result.Applied > 0 {
		telemetry.LogPullTxnsAppliedTotal.Add(float64(result.Applied))
	}
	return result, err
}

// pairOutcome classifies a PullPair result into the fixed outcome label set
// LogPullPairResultsTotal is recorded under.
func pairOutcome(result PairResult, err error) string {
	switch {
	case err != nil:
		return "error"
	case result.Unimplemented:
		return "unimplemented"
	case result.NeedsSnapshot:
		return "needs_snapshot"
	case result.CaughtUp:
		return "caught_up"
	default:
		return "behind"
	}
}

// pullState is one PullPair call's working state for its (peer, database)
// pair.
type pullState struct {
	peer      PeerRef
	database  string
	key       pairKey
	round     int
	client    MarmotServiceClient
	mdb       *db.ReplicatedDatabase
	metaStore db.MetaStore

	// cursor is the pair's persisted position: every entry at or before it
	// is covered. listAfter runs ahead of it once an entry cannot be applied
	// yet, so the entries after that one are still applied this call
	// while the cursor, and the consumed
	// position reported to the peer's GC, stay before it.
	cursor     db.LogPosition
	listAfter  db.LogPosition
	contiguous bool

	// localCommitFailed stops further local commits of prepared entries for
	// the rest of this call once one fails: they share its cause (typically
	// the SQLite writer held by a pinned session) and each would wait out
	// the same busy timeout.
	localCommitFailed bool

	result PairResult
}

func (lp *LogPuller) pullPair(ctx context.Context, peer PeerRef, database string) (PairResult, error) {
	st, err := lp.newPullState(peer, database)
	if err != nil {
		return PairResult{}, err
	}
	for page := 0; page < maxPagesPerPull; page++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return st.result, ctxErr
		}
		done, err := lp.pullPage(ctx, st)
		if err != nil || done {
			return st.result, err
		}
	}
	return st.result, nil
}

// newPullState resolves the local database, the peer's client and the
// pair's persisted cursor, and counts this call for the STUCK retry cadence.
func (lp *LogPuller) newPullState(peer PeerRef, database string) (*pullState, error) {
	mdb, err := lp.dbMgr.GetDatabase(database)
	if err != nil {
		return nil, fmt.Errorf("get local database %q: %w", database, err)
	}
	key := pairKey{peer: peer.NodeID, database: database}
	lp.mu.Lock()
	lp.pairCalls[key]++
	round := lp.pairCalls[key]
	lp.mu.Unlock()

	client, err := lp.client.GetClientByAddress(peer.Address)
	if err != nil {
		return nil, fmt.Errorf("connect to peer %d at %s: %w", peer.NodeID, peer.Address, err)
	}
	metaStore := mdb.GetMetaStore()
	cursor, err := metaStore.GetPullCursor(peer.NodeID)
	if err != nil {
		return nil, fmt.Errorf("get pull cursor for peer %d database %q: %w", peer.NodeID, database, err)
	}
	return &pullState{
		peer: peer, database: database, key: key, round: round,
		client: client, mdb: mdb, metaStore: metaStore,
		cursor: cursor, listAfter: cursor, contiguous: true,
	}, nil
}

// pullPage lists and applies one page of the peer's log. done reports that
// the call is over (caught up, or an outcome recorded in st.result).
func (lp *LogPuller) pullPage(ctx context.Context, st *pullState) (done bool, err error) {
	resp, err := st.client.ListCommittedLog(ctx, &LogListRequest{
		Database:         st.database,
		RequestingNodeId: lp.nodeID,
		AfterSeq:         st.listAfter.Seq,
		AfterTxnId:       st.listAfter.TxnID,
		Limit:            uint32(lp.pageSize),
		ConsumedSeq:      st.cursor.Seq,
		ConsumedTxnId:    st.cursor.TxnID,
	})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			lp.warnUnimplementedOnce(st.peer.NodeID)
			st.result.Unimplemented = true
			return true, nil
		}
		return true, fmt.Errorf("list committed log from peer %d database %q: %w", st.peer.NodeID, st.database, err)
	}
	if resp.DatabaseAbsent {
		st.result.DatabaseAbsent = true
		return true, nil
	}
	st.result.PeerSchemaVersion = resp.SchemaVersion

	truncated := db.LogPosition{Seq: resp.TruncatedSeq, TxnID: resp.TruncatedTxnId}
	if lp.takeRestored(st.key) {
		// Everything at or below the peer's truncation point was consumed
		// by every member, the restore's source included, or is in this
		// node's own log for the restore's re-apply. The cursor moves there even when that lowers
		// it: a cursor that passed an entry before this restore (covered by
		// an earlier restore's marker, say) must see it again, because this
		// restore's source may lack it.
		st.cursor, st.listAfter = truncated, truncated
		return false, lp.persistCursor(st)
	}
	if st.cursor.Less(truncated) {
		st.result.NeedsSnapshot = true
		return true, nil
	}
	if len(resp.Entries) == 0 {
		st.result.CaughtUp = st.contiguous && !resp.More
		return true, nil
	}
	return lp.applyListedPage(ctx, st, resp)
}

// applyListedPage applies one non-empty page, advances the cursor over the
// covered prefix, and moves listAfter past the page.
func (lp *LogPuller) applyListedPage(ctx context.Context, st *pullState, resp *LogListResponse) (done bool, err error) {
	coveredPrefixLen, err := lp.applyPage(ctx, st, resp.Entries)
	if st.contiguous && coveredPrefixLen > 0 {
		last := resp.Entries[coveredPrefixLen-1]
		st.cursor = db.LogPosition{Seq: last.Seq, TxnID: last.TxnId}
		if setErr := lp.persistCursor(st); setErr != nil {
			return true, setErr
		}
	}
	if coveredPrefixLen < len(resp.Entries) {
		st.contiguous = false
	}
	if err != nil {
		return true, err
	}
	if !resp.More {
		st.result.CaughtUp = st.contiguous
		return true, nil
	}
	last := resp.Entries[len(resp.Entries)-1]
	st.listAfter = db.LogPosition{Seq: last.Seq, TxnID: last.TxnId}
	return false, nil
}

// persistCursor durably stores st.cursor as the pair's pull cursor.
func (lp *LogPuller) persistCursor(st *pullState) error {
	if err := st.metaStore.SetPullCursor(st.peer.NodeID, st.cursor); err != nil {
		return fmt.Errorf("persist pull cursor for peer %d database %q: %w", st.peer.NodeID, st.database, err)
	}
	return nil
}

// applyPage classifies and applies one ListCommittedLog page's entries, in
// order, and returns the length of the page's contiguous prefix now
// covered. It returns an error only for a failure that is not attributable
// to a specific transaction (checking local state).
//
// Every listed entry is COMMITTED in the peer's log, so a transaction this
// node holds PENDING under the same id was decided COMMITTED, and its
// local record is resolved here, in the same call (resolveLocalPending),
// instead of stopping the walk until the stale-transaction GC ends it.
func (lp *LogPuller) applyPage(ctx context.Context, st *pullState, entries []*LogEntry) (int, error) {
	ids := make([]uint64, len(entries))
	for i, e := range entries {
		ids[i] = e.TxnId
	}
	appliedMarkers, err := st.mdb.AppliedTxns(ids)
	if err != nil {
		return 0, fmt.Errorf("check applied markers: %w", err)
	}

	covered := make([]bool, len(entries))
	toFetch := make([]uint64, 0, len(entries))
	fetchIdx := make(map[uint64]int, len(entries))
	for i, e := range entries {
		if appliedMarkers[e.TxnId] {
			covered[i] = true
			lp.clearAttempt(st.database, st.peer.NodeID, e.TxnId)
			continue
		}
		if lp.isStuckThisRound(st.database, st.peer.NodeID, e.TxnId, st.round) {
			st.result.Stuck++
			continue
		}
		fetch, err := lp.resolveLocalPending(st, e.TxnId, &covered[i])
		if err != nil {
			return 0, err
		}
		if fetch {
			fetchIdx[e.TxnId] = i
			toFetch = append(toFetch, e.TxnId)
		}
	}

	if len(toFetch) > 0 {
		lp.fetchAndApply(ctx, st, toFetch, fetchIdx, covered)
	}

	prefix := 0
	for prefix < len(covered) && covered[prefix] {
		prefix++
	}
	return prefix, nil
}

// resolveLocalPending resolves this node's own record for txnID, which the
// peer's log proves COMMITTED, and reports whether the entry must be fetched
// and replayed from the peer:
//   - no PENDING record: fetch;
//   - durably prepared: commit it through the local commit path
//     (db.DatabaseManager.CommitLocallyPrepared) and mark it covered; on
//     failure leave it for the next call, never replaying over it;
//   - begun but abandoned (its PREPARE died with an earlier process): it
//     promised nothing, so discard it and fetch;
//   - begun and live (a PREPARE executing now): leave it for the next call.
func (lp *LogPuller) resolveLocalPending(st *pullState, txnID uint64, covered *bool) (fetch bool, err error) {
	kind, err := st.metaStore.ClassifyPending(txnID)
	if err != nil {
		return false, fmt.Errorf("check local status for txn %d: %w", txnID, err)
	}
	switch kind {
	case db.PendingNone:
		return true, nil
	case db.PendingPrepared:
		if st.localCommitFailed {
			return false, nil
		}
		if err := lp.dbMgr.CommitLocallyPrepared(st.database, txnID); err != nil {
			st.localCommitFailed = true
			// A busy writer (a pinned session holding it, say) is not this
			// transaction's fault and must not push it toward STUCK, whose
			// throttled retries would delay it long after the writer frees.
			if !db.IsTransientSQLiteError(err) && lp.recordFailedAttempt(st.database, st.peer.NodeID, txnID, err) {
				st.result.Stuck++
			}
			return false, nil
		}
		*covered = true
		st.result.Applied++
		lp.clearAttempt(st.database, st.peer.NodeID, txnID)
		return false, nil
	case db.PendingBegunAbandoned:
		if err := st.mdb.DiscardAbandonedBegin(txnID); err != nil && !errors.Is(err, db.ErrNotAbandonedBegin) {
			return false, fmt.Errorf("discard abandoned local begin of txn %d: %w", txnID, err)
		}
		return true, nil
	default: // db.PendingBegunLive
		return false, nil
	}
}

// fetchAndApply fetches every id in toFetch from the peer and applies each
// event through ApplyPulledEvent as it streams in, marking
// covered[fetchIdx[txnID]] true for every one that ends up covered (applied
// or already applied).
//
// The peer serves ids in the order requested and fails the whole call at
// the first id it cannot serve (FetchTransactions), so one bad transaction
// must not cost the healthy ids after it their turn: the id the call
// failed at counts one failed attempt, and the ids after it are fetched
// again in a fresh call. A transport failure (the peer or ctx gone) is not
// any transaction's fault: it ends this call without counting an attempt.
func (lp *LogPuller) fetchAndApply(ctx context.Context, st *pullState, toFetch []uint64, fetchIdx map[uint64]int, covered []bool) {
	remaining := toFetch
	for len(remaining) > 0 {
		received, err := lp.fetchBatch(ctx, st, remaining, fetchIdx, covered)
		if err == nil || ctx.Err() != nil || !isPerTxnFetchError(err) {
			return
		}
		rest := remaining[:0:0]
		blamed := false
		for _, id := range remaining {
			if received[id] {
				continue
			}
			if !blamed {
				blamed = true
				if lp.recordFailedAttempt(st.database, st.peer.NodeID, id, err) {
					st.result.Stuck++
				}
				continue
			}
			rest = append(rest, id)
		}
		remaining = rest
	}
}

// isPerTxnFetchError reports whether a FetchTransactions failure names the
// transaction it stopped at (a missing or unservable entry), rather than
// the call itself failing.
func isPerTxnFetchError(err error) bool {
	switch status.Code(err) {
	case codes.NotFound, codes.FailedPrecondition, codes.Internal:
		return true
	default:
		return false
	}
}

// fetchBatch runs one FetchTransactions call for ids and applies every event
// it streams. It returns the set of ids an event arrived for, and the call's
// error (nil at a clean end of stream).
func (lp *LogPuller) fetchBatch(ctx context.Context, st *pullState, ids []uint64, fetchIdx map[uint64]int, covered []bool) (map[uint64]bool, error) {
	received := make(map[uint64]bool, len(ids))
	stream, err := st.client.FetchTransactions(ctx, &FetchTransactionsRequest{
		Database:         st.database,
		RequestingNodeId: lp.nodeID,
		TxnIds:           ids,
	})
	if err != nil {
		return received, err
	}
	for {
		ev, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				return received, nil
			}
			return received, recvErr
		}
		received[ev.TxnId] = true
		idx, ok := fetchIdx[ev.TxnId]
		if !ok {
			log.Warn().Uint64("txn_id", ev.TxnId).Str("database", st.database).Uint64("peer_node_id", st.peer.NodeID).
				Msg("LogPuller: peer streamed a transaction that was not requested; ignoring")
			continue
		}
		lp.applyFetched(ctx, st, ev, &covered[idx])
	}
}

// errFetchedWrongDatabase is a fetched event naming a database other than
// the one its pair pulls: applying it would cover the pair's entry with
// another database's transaction.
var errFetchedWrongDatabase = errors.New("peer streamed a transaction of another database")

// applyFetched applies one fetched event, marking it covered on success. An
// intent conflict, or this node's own record of the txn turning PENDING (a
// late PREPARE's begin landing before the replay), is neither covered nor a
// failed attempt: it is retried on a later call while the walk continues past
// it. An event for another database is a failed attempt, never applied.
func (lp *LogPuller) applyFetched(ctx context.Context, st *pullState, ev *ChangeEvent, covered *bool) {
	if ev.Database != st.database {
		err := fmt.Errorf("%w: txn %d names %q, pulling %q", errFetchedWrongDatabase, ev.TxnId, ev.Database, st.database)
		if lp.recordFailedAttempt(st.database, st.peer.NodeID, ev.TxnId, err) {
			st.result.Stuck++
		}
		return
	}
	applyOK, applyErr := ApplyPulledEvent(ctx, lp.dbMgr, ev)
	switch {
	case applyErr == nil:
		*covered = true
		lp.clearAttempt(st.database, st.peer.NodeID, ev.TxnId)
		if applyOK {
			st.result.Applied++
		}
	case errors.Is(applyErr, db.ErrReplayIntentConflict), errors.Is(applyErr, db.ErrReplayPending):
	default:
		if lp.recordFailedAttempt(st.database, st.peer.NodeID, ev.TxnId, applyErr) {
			st.result.Stuck++
		}
	}
}

// isStuckThisRound reports whether (peer, database, txnID) is currently
// STUCK and this round is not one of its throttled retry rounds, so
// applyPage should skip fetching it this call.
func (lp *LogPuller) isStuckThisRound(database string, peerNodeID, txnID uint64, round int) bool {
	lp.mu.Lock()
	st := lp.attempts[txnAttemptKey{peer: peerNodeID, database: database, txnID: txnID}]
	lp.mu.Unlock()
	if st == nil || !st.stuck {
		return false
	}
	return round%lp.stuckRetryEveryRounds != 0
}

// recordFailedAttempt records one failed replay attempt for (peer,
// database, txnID). It returns true if the transaction is STUCK after this
// attempt (whether it just became so, or already was). The first
// transition to STUCK logs one ERROR and raises LogPullStuckTxns.
func (lp *LogPuller) recordFailedAttempt(database string, peerNodeID, txnID uint64, cause error) bool {
	key := txnAttemptKey{peer: peerNodeID, database: database, txnID: txnID}

	lp.mu.Lock()
	st, ok := lp.attempts[key]
	if !ok {
		st = &txnAttemptState{}
		lp.attempts[key] = st
	}
	st.attempts++
	st.lastErr = cause.Error()
	newlyStuck := !st.stuck && st.attempts >= lp.maxReplayAttempts
	if newlyStuck {
		st.stuck = true
	}
	stuck := st.stuck
	attempts := st.attempts
	lp.mu.Unlock()

	if newlyStuck {
		telemetry.LogPullStuckTxns.With(database).Inc()
		log.Error().Err(cause).Uint64("txn_id", txnID).Uint64("peer_node_id", peerNodeID).
			Str("database", database).Int("attempts", attempts).
			Msg("LogPuller: transaction is stuck after repeated replay failures")
	}
	return stuck
}

// clearAttempt drops (peer, database, txnID)'s failed-attempt counter once
// it applies successfully, clearing its STUCK status and metric if it had
// one.
func (lp *LogPuller) clearAttempt(database string, peerNodeID, txnID uint64) {
	key := txnAttemptKey{peer: peerNodeID, database: database, txnID: txnID}

	lp.mu.Lock()
	st, ok := lp.attempts[key]
	if ok {
		delete(lp.attempts, key)
	}
	lp.mu.Unlock()

	if ok && st.stuck {
		telemetry.LogPullStuckTxns.With(database).Dec()
	}
}

// StuckTxns returns every transaction this LogPuller currently reports
// STUCK, across every pair it has pulled.
func (lp *LogPuller) StuckTxns() []StuckTxn {
	lp.mu.Lock()
	defer lp.mu.Unlock()

	out := make([]StuckTxn, 0)
	for key, st := range lp.attempts {
		if !st.stuck {
			continue
		}
		out = append(out, StuckTxn{
			Database:   key.database,
			PeerNodeID: key.peer,
			TxnID:      key.txnID,
			Attempts:   st.attempts,
			LastError:  st.lastErr,
		})
	}
	return out
}

// warnUnimplementedOnce logs, once per peer for this LogPuller's lifetime,
// that peerNodeID does not implement ListCommittedLog.
func (lp *LogPuller) warnUnimplementedOnce(peerNodeID uint64) {
	if _, loaded := lp.warnedUnimplemented.LoadOrStore(peerNodeID, struct{}{}); !loaded {
		log.Warn().Uint64("peer_node_id", peerNodeID).
			Msg("LogPuller: peer does not implement ListCommittedLog (rolling upgrade); skipping")
	}
}

// MarkRestored records that database was just restored from a snapshot.
// The next pull from each peer - every
// peer, including one this node has never pulled from - sets that pair's
// cursor to the peer's truncation point, lowering it if it was past it,
// instead of reporting NeedsSnapshot again: everything at or below that
// point was consumed by every member, the restore's source included, or is
// in this node's own log for the restore's re-apply, while an entry above it
// may be missing from the restored file even if the old cursor had passed
// it, and the marker diff skips whatever the restored file already holds.
// The mark is in memory: a restart before a pair's next pull only costs one
// more restore.
func (lp *LogPuller) MarkRestored(database string) {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	lp.restoredGen[database]++
}

// takeRestored reports whether the pair's database was restored since the
// pair last took the mark, and takes it.
func (lp *LogPuller) takeRestored(key pairKey) bool {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	gen := lp.restoredGen[key.database]
	if lp.pairRestoredGen[key] >= gen {
		return false
	}
	lp.pairRestoredGen[key] = gen
	return true
}
