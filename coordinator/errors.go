package coordinator

import (
	"fmt"

	"github.com/maxpert/marmot/protocol/query/transform"
)

// PrepareConflictError represents a write-write conflict detected during the prepare phase
type PrepareConflictError struct {
	NodeID  uint64
	Details string
}

func (e *PrepareConflictError) Error() string {
	return fmt.Sprintf("conflict on node %d: %s", e.NodeID, e.Details)
}

// QuorumNotAchievedError represents a failure to achieve quorum during prepare or commit phase
type QuorumNotAchievedError struct {
	Phase           string // "prepare" or "commit"
	AcksReceived    int
	QuorumRequired  int
	TotalMembership int
	AliveNodes      int
	IsRemoteQuorum  bool // True if this is remote commit quorum (quorum-1)
	// Declined counts participants that answered PREPARE without preparing
	// and without a verdict - a lost race for a row lock, a storage error -
	// as opposed to participants that never answered.
	Declined int
}

func (e *QuorumNotAchievedError) Error() string {
	return fmt.Sprintf("%s quorum not achieved: got %d acks, need %d (majority of %d total members, %d alive)",
		e.Phase, e.AcksReceived, e.QuorumRequired, e.TotalMembership, e.AliveNodes)
}

// LocalPrepareError indicates this node explicitly rejected the transaction
// during PREPARE, as opposed to failing to answer. The rejection is final for
// this attempt, so the coordinator surfaces the participant's own reason instead
// of the retry signal used for write-write conflicts.
type LocalPrepareError struct {
	Reason string
	// AutoIDStoredBase is the highest base any participant reported this
	// PREPARE round, when this rejection came from an AUTO_INCREMENT range
	// claim. Zero for every other rejection.
	AutoIDStoredBase uint64
	// ErrorCode is the MySQL server error code the participant chose for this
	// refusal, or 0 when it named none.
	ErrorCode uint16
}

func (e *LocalPrepareError) Error() string {
	return e.Reason
}

// Unwrap re-types the rejection as the coded error the participant raised, so
// the protocol layer maps it to the participant's own MySQL error code rather
// than flattening every deterministic refusal to ER_UNKNOWN_ERROR. It returns
// nil when the participant named no code, which leaves the existing
// message-based classification in charge.
func (e *LocalPrepareError) Unwrap() error {
	if e.ErrorCode == 0 {
		return nil
	}
	return &transform.CodedError{Code: e.ErrorCode, Message: e.Reason}
}

// RemotePrepareRejectedError indicates prepare quorum was not achieved and at
// least one remote participant explicitly rejected the transaction during
// PREPARE (a deterministic "this can never apply here" verdict, for example
// DDL SQLite cannot apply), rather than merely failing to respond in time.
// Retrying cannot change that participant's verdict, so this replaces the
// retryable QuorumNotAchievedError when a rejection is on record. A remote
// rejection alone - with quorum otherwise achieved - never produces this
// error; it carries no veto over cluster DDL.
type RemotePrepareRejectedError struct {
	NodeID uint64
	Reason string
	// AutoIDStoredBase is the highest base any participant reported this
	// PREPARE round, when this rejection came from an AUTO_INCREMENT range
	// claim. Zero for every other rejection.
	AutoIDStoredBase uint64
	// ErrorCode is the MySQL server error code the rejecting participant chose,
	// or 0 when it named none.
	ErrorCode uint16
}

// Error reports the rejecting participant's own reason so clients receive the
// actual SQL failure rather than 2PC internals.
func (e *RemotePrepareRejectedError) Error() string {
	return e.Reason
}

// Unwrap re-types the rejection as the coded error the remote participant
// raised; see LocalPrepareError.Unwrap.
func (e *RemotePrepareRejectedError) Unwrap() error {
	if e.ErrorCode == 0 {
		return nil
	}
	return &transform.CodedError{Code: e.ErrorCode, Message: e.Reason}
}

// CoordinatorNotParticipatedError indicates the coordinator failed to participate in the prepare phase
type CoordinatorNotParticipatedError struct {
	TxnID uint64
	Err   error // Reason the local prepare failed, nil when it never responded
}

// Error reports the participant's own reason when it rejected the transaction so
// clients receive the actual SQL failure rather than 2PC internals. The generic
// message is only used when the local node never answered.
func (e *CoordinatorNotParticipatedError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "coordinator must participate: local prepare failed"
}

// Unwrap exposes the underlying prepare failure so callers can map it to a
// protocol-level error code.
func (e *CoordinatorNotParticipatedError) Unwrap() error {
	return e.Err
}

// PartialCommitError represents a partial commit where some nodes committed but quorum was not achieved
type PartialCommitError struct {
	IsLocal            bool  // True if local commit failed, false if remote quorum failed
	RemoteAcks         int   // Number of remote ACKs received (only for remote failures)
	RemoteQuorumNeeded int   // Required remote ACKs (only for remote failures)
	LocalError         error // Underlying local error (only for local failures)
}

func (e *PartialCommitError) Error() string {
	if e.IsLocal {
		return fmt.Sprintf("partial commit: local commit failed after remote quorum: %v", e.LocalError)
	}
	return fmt.Sprintf("partial commit: got %d remote commit acks, needed %d (some nodes may have committed)",
		e.RemoteAcks, e.RemoteQuorumNeeded)
}
