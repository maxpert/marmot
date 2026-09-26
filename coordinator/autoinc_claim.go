package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/maxpert/marmot/protocol/query/transform"
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

// ClaimRange obtains a narrow AUTO_INCREMENT range for the range allocator
// (id.RangeAllocator): on success (newBase, newBase+granted] belongs to this
// node alone.
//
// It pushes one protocol.Statement through the existing 2PC machinery per
// attempt: an AUTO_INCREMENT range claim, carrying AutoIDClaim=true and a
// msgpack AutoIDClaimPayload, with a non-empty IntentKey and deliberately no
// EncodedRow (protocol/transaction.go). newBase == prevBase on every attempt,
// matching the shape prepareAutoIncClaim (db/replication_engine.go) requires:
// storedBase <= prevBase && newBase == prevBase && size >= 1 &&
// newBase+size <= widthMax.
//
// Every attempt carries this node's current view of the cluster's total
// membership, the denominator its quorum is computed over, and a participant
// whose own view differs rejects it (prepareAutoIncClaim): majorities of two
// different memberships need not intersect.
//
// size is evaluated afresh for every attempt against the base that attempt
// proposes, because a rejection moves the base and the range's size depends
// on where it starts (the endgame taper and the width ceiling). When size
// reports id.ErrRangeExhausted no claim is sent and ClaimRange returns that
// error.
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
// The two ways a claim can end are kept apart structurally, because a client
// must retry one and must never retry the other:
//   - exhaustion - size found no room above the base, or a participant
//     rejected the range as passing its own column ceiling (the rejection
//     carries ER_DUP_ENTRY, see prepareAutoIncClaim) - returns an error
//     wrapping id.ErrRangeExhausted;
//   - anything else, on exhausting maxClaimAttempts or on a failure with no
//     usable base, returns protocol.ErrLockWaitTimeout() (MySQL 1205), the
//     standard "retry me" signal (see runPreparePhase's handling of
//     write-write conflicts), wrapped with %w around the last attempt's cause.
//
// It never touches hookDB and never calls ExecuteLocalWithHooks: it only
// builds a statement and drives it through WriteTransaction, exactly like
// any other 2PC write.
func (wc *WriteCoordinator) ClaimRange(ctx context.Context, database, table string, prevBase uint64, size id.RangeSizer) (newBase uint64, granted uint64, err error) {
	var lastErr error
	declinedRounds := 0
	for attempt := 0; attempt < maxClaimAttempts; attempt++ {
		claimSize, sizeErr := size(prevBase)
		if sizeErr != nil {
			return 0, 0, fmt.Errorf("auto-increment claim for %s.%s: %w", database, table, sizeErr)
		}
		payload, encErr := protocol.EncodeAutoIncClaim(protocol.AutoIncClaim{
			Table:      table,
			PrevBase:   prevBase,
			NewBase:    prevBase,
			Size:       claimSize,
			Membership: uint32(wc.nodeProvider.GetTotalMembershipSize()),
		})
		if encErr != nil {
			return 0, 0, fmt.Errorf("encode auto-increment claim for %s.%s: %w", database, table, encErr)
		}

		// Read each attempt: a DDL applied between attempts raises it.
		schemaVersion, versionErr := wc.claimSchemaVersion(database)
		if versionErr != nil {
			return 0, 0, fmt.Errorf("auto-increment claim for %s.%s: read schema version: %w", database, table, versionErr)
		}

		startTS := wc.clock.Now()
		txn := &Transaction{
			ID:                    startTS.ToTxnID(),
			NodeID:                wc.nodeID,
			StartTS:               startTS,
			Database:              database,
			RequiredSchemaVersion: schemaVersion,
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
			return prevBase, claimSize, nil
		}
		lastErr = attemptErr

		if exhaustedByParticipant(attemptErr) {
			return 0, 0, fmt.Errorf("auto-increment claim for %s.%s: %w (%v)", database, table, id.ErrRangeExhausted, attemptErr)
		}
		if base, ok := autoIDStoredBaseFromRejection(attemptErr); ok && base > prevBase {
			prevBase = base
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

// exhaustedByParticipant reports whether a claim round was rejected because
// the range passed a participant's own column ceiling: prepareAutoIncClaim
// gives exactly that rejection ER_DUP_ENTRY, the code MySQL reports when an
// AUTO_INCREMENT column is full, and no other claim rejection carries it.
func exhaustedByParticipant(err error) bool {
	var coded *transform.CodedError
	return errors.As(err, &coded) && coded.Code == mysqlcode.ErrCodeDupEntry
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

// claimSchemaVersion is the schema version a claim for database carries:
// this node's own, so a participant that has not applied every DDL this node
// has declines the claim, exactly as it declines DML
// (grpc ReplicationHandler's schema version check). A base a lagging
// participant kept for a table name belongs to the incarnation this node
// may already have replaced, a rename's source for one, and voting with it
// could grant ids the new incarnation already holds.
func (wc *WriteCoordinator) claimSchemaVersion(database string) (uint64, error) {
	if wc.schemaVersion == nil {
		return 0, nil
	}
	return wc.schemaVersion(database)
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
