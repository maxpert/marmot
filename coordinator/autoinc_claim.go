package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/maxpert/marmot/protocol"
)

// maxClaimAttempts bounds how many PREPARE rounds ClaimRange spends on one
// call. A rejection that carries a usable base means real progress - another
// claimant is ahead, not that this claim is wrong - so retrying is the right
// response; this bound exists only so a caller always gets a definitive
// answer within one call instead of spinning against sustained contention.
const maxClaimAttempts = 8

// claimBackoffBase and claimBackoffCap shape the wait before a claim retries
// a round that a participant declined without a verdict - in practice a lost
// race for the claim key, held by another claim until its COMMIT has synced
// the system database on a majority. With c = min(claimBackoffCap,
// claimBackoffBase<<k), the k-th such wait is drawn uniformly from [c/2, c):
// the jitter spreads contending claimants apart, and the lower half keeps
// every wait long enough for the holder's COMMIT to progress. Over the at
// most maxClaimAttempts-1 retries one call makes, the waits total less than
// 4+8+16+32+64+64+64 = 252ms.
const (
	claimBackoffBase = 4 * time.Millisecond
	claimBackoffCap  = 64 * time.Millisecond
)

// ClaimRange is the entry point the range allocator will call to obtain a narrow AUTO_INCREMENT range: it is not dead code
// despite having no production caller yet, since that allocator is a
// separate, later step. Its only callers today are the tests in this file.
//
// It pushes one protocol.Statement through the existing 2PC machinery per
// attempt: an AUTO_INCREMENT range claim, carrying AutoIDClaim=true and a
// msgpack AutoIDClaimPayload, with a non-empty IntentKey and deliberately no
// EncodedRow (protocol/transaction.go). newBase == prevBase on every attempt,
// matching the shape prepareAutoIncClaim (db/replication_engine.go) requires:
// storedBase <= prevBase && newBase == prevBase && size >= 1 &&
// newBase+size <= widthMax.
//
// WriteConsistency is pinned to ConsistencyQuorum unconditionally - it never
// reads a caller-supplied level or the cluster's configured write default.
// QuorumSize(ConsistencyOne, n) and QuorumSize(ConsistencyLocalOne, n) are
// both 1 (coordinator/quorum.go), and a quorum of 1 does not intersect
// another quorum of 1: two claimants could each get a "yes" from a disjoint
// single node and mint the same range. The claim protocol's soundness
// (db/replication_engine.go's prepareAutoIncClaim doc) depends on any two
// quorums sharing a participant. A client can otherwise steer consistency
// with a SQL hint (coordinator/handler.go's routeQuery) or via the cluster's
// configured default (cfg.Config.Replication.DefaultWriteConsist) - neither
// must be allowed to downgrade a claim to a single node.
//
// On a PREPARE rejection, ClaimRange retries above the highest base any
// participant reported that round (executePreparePhase tracks this as
// maxAutoIDStoredBase and attaches it to whichever of *LocalPrepareError or
// *RemotePrepareRejectedError is on record - see coordinator/write_coordinator.go).
// That retry is immediate: the rejection means another claim committed.
//
// A round that failed because a participant declined without a verdict -
// quorum not achieved while at least one participant answered, which for a
// claim means it lost the race for the claim key - is retried with the same
// proposal after a jittered backoff (claimBackoffBase, claimBackoffCap). Any
// other failure that carries no usable base - quorum not achieved with no
// participant answering, a transport failure - is not something retrying can
// fix by itself, so it ends the attempt loop immediately rather than spending
// the full budget.
//
// On exhausting maxClaimAttempts, or on hitting a failure with no usable
// base, ClaimRange returns protocol.ErrLockWaitTimeout() (MySQL 1205): the
// standard signal this codebase already uses for "retry me" (see
// runPreparePhase's handling of write-write conflicts). It is wrapped with
// %w around the last attempt's cause so a caller using errors.As can still
// recover the retryable code, and errors.Unwrap the underlying reason for
// logs.
//
// It never touches hookDB and never calls ExecuteLocalWithHooks: it only
// builds a statement and drives it through WriteTransaction, exactly like
// any other 2PC write.
func (wc *WriteCoordinator) ClaimRange(ctx context.Context, database, table string, prevBase, size uint64) (newBase uint64, granted uint64, err error) {
	newBase = prevBase

	var lastErr error
	declinedRounds := 0
	for attempt := 0; attempt < maxClaimAttempts; attempt++ {
		payload, encErr := protocol.EncodeAutoIncClaim(protocol.AutoIncClaim{
			Table:    table,
			PrevBase: prevBase,
			NewBase:  newBase,
			Size:     size,
		})
		if encErr != nil {
			return 0, 0, fmt.Errorf("encode auto-increment claim for %s.%s: %w", database, table, encErr)
		}

		startTS := wc.clock.Now()
		txn := &Transaction{
			ID:       startTS.ToTxnID(),
			NodeID:   wc.nodeID,
			StartTS:  startTS,
			Database: database,
			// Pinned regardless of any caller or cluster default - see the
			// method doc for why a quorum of 1 cannot be trusted here.
			WriteConsistency: protocol.ConsistencyQuorum,
			Statements: []protocol.Statement{{
				Type:               protocol.StatementInsert,
				TableName:          table,
				Database:           database,
				IntentKey:          []byte(protocol.AutoIncClaimKey(database, table)),
				AutoIDClaim:        true,
				AutoIDClaimPayload: payload,
			}},
		}

		attemptErr := wc.WriteTransaction(ctx, txn)
		if attemptErr == nil {
			return newBase, size, nil
		}
		lastErr = attemptErr

		if base, ok := autoIDStoredBaseFromRejection(attemptErr); ok && base > prevBase {
			prevBase = base
			newBase = base
			continue
		}
		if !declinedByParticipant(attemptErr) || attempt == maxClaimAttempts-1 {
			break
		}
		if err := sleepClaimBackoff(ctx, declinedRounds); err != nil {
			lastErr = err
			break
		}
		declinedRounds++
	}

	return 0, 0, fmt.Errorf("auto-increment claim for %s.%s could not be granted: %w (last cause: %v)",
		database, table, protocol.ErrLockWaitTimeout(), lastErr)
}

// autoIDStoredBaseFromRejection extracts the highest base a rejecting
// participant reported from either shape runPreparePhase can return: a
// *RemotePrepareRejectedError (quorum failed to form and a remote rejection
// is on record) or a *LocalPrepareError (this node's own PREPARE rejected,
// possibly wrapped in *CoordinatorNotParticipatedError). Neither wraps the
// other, so both are checked. A zero base reports "no usable base" (ok=false):
// per PrepareResult.AutoIDStoredBase's own contract, zero means this
// response was not an AUTO_INCREMENT claim rejection at all.
func autoIDStoredBaseFromRejection(err error) (base uint64, ok bool) {
	var remoteRejection *RemotePrepareRejectedError
	if errors.As(err, &remoteRejection) && remoteRejection.AutoIDStoredBase > 0 {
		return remoteRejection.AutoIDStoredBase, true
	}
	var localErr *LocalPrepareError
	if errors.As(err, &localErr) && localErr.AutoIDStoredBase > 0 {
		return localErr.AutoIDStoredBase, true
	}
	return 0, false
}

// declinedByParticipant reports whether a claim round failed to reach quorum
// while at least one participant answered without a verdict.
func declinedByParticipant(err error) bool {
	var quorumErr *QuorumNotAchievedError
	return errors.As(err, &quorumErr) && quorumErr.Phase == "prepare" && quorumErr.Declined > 0
}

// sleepClaimBackoff waits the k-th jittered claim backoff, or until ctx ends.
func sleepClaimBackoff(ctx context.Context, k int) error {
	ceiling := claimBackoffBase << k
	if ceiling <= 0 || ceiling > claimBackoffCap {
		ceiling = claimBackoffCap
	}
	half := ceiling / 2
	timer := time.NewTimer(half + time.Duration(rand.Int64N(int64(ceiling-half))))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
