package coordinator

import "fmt"

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
}

func (e *QuorumNotAchievedError) Error() string {
	return fmt.Sprintf("%s quorum not achieved: got %d acks, need %d (majority of %d total members, %d alive)",
		e.Phase, e.AcksReceived, e.QuorumRequired, e.TotalMembership, e.AliveNodes)
}

// CoordinatorNotParticipatedError indicates the coordinator failed to participate in the prepare phase
type CoordinatorNotParticipatedError struct {
	TxnID uint64
}

func (e *CoordinatorNotParticipatedError) Error() string {
	return "coordinator must participate: local prepare failed"
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
		return fmt.Sprintf("partial commit: local commit failed after remote quorum (bug in PREPARE): %v", e.LocalError)
	}
	return fmt.Sprintf("partial commit: got %d remote commit acks, needed %d (some nodes may have committed)",
		e.RemoteAcks, e.RemoteQuorumNeeded)
}
