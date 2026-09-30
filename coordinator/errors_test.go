package coordinator

import (
	"errors"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

func TestPrepareConflictError(t *testing.T) {
	tests := []struct {
		name     string
		nodeID   uint64
		details  string
		expected string
	}{
		{
			name:     "basic conflict",
			nodeID:   1,
			details:  "write-write conflict",
			expected: "conflict on node 1: write-write conflict",
		},
		{
			name:     "conflict with detailed message",
			nodeID:   42,
			details:  "key 'user:123' modified by txn 456",
			expected: "conflict on node 42: key 'user:123' modified by txn 456",
		},
		{
			name:     "zero node ID",
			nodeID:   0,
			details:  "conflict detected",
			expected: "conflict on node 0: conflict detected",
		},
		{
			name:     "large node ID",
			nodeID:   18446744073709551615, // max uint64
			details:  "conflict",
			expected: "conflict on node 18446744073709551615: conflict",
		},
		{
			name:     "empty details",
			nodeID:   5,
			details:  "",
			expected: "conflict on node 5: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &PrepareConflictError{
				NodeID:  tt.nodeID,
				Details: tt.details,
			}
			if got := err.Error(); got != tt.expected {
				t.Errorf("PrepareConflictError.Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestQuorumNotAchievedError(t *testing.T) {
	tests := []struct {
		name           string
		phase          string
		acksReceived   int
		quorumRequired int
		totalMembers   int
		aliveNodes     int
		isRemote       bool
		expected       string
	}{
		{
			name:           "prepare quorum not achieved",
			phase:          "prepare",
			acksReceived:   1,
			quorumRequired: 2,
			totalMembers:   3,
			aliveNodes:     3,
			isRemote:       false,
			expected:       "prepare quorum not achieved: got 1 acks, need 2 (majority of 3 total members, 3 alive)",
		},
		{
			name:           "commit quorum not achieved",
			phase:          "commit",
			acksReceived:   2,
			quorumRequired: 3,
			totalMembers:   5,
			aliveNodes:     4,
			isRemote:       false,
			expected:       "commit quorum not achieved: got 2 acks, need 3 (majority of 5 total members, 4 alive)",
		},
		{
			name:           "remote quorum not achieved",
			phase:          "commit",
			acksReceived:   1,
			quorumRequired: 2,
			totalMembers:   5,
			aliveNodes:     5,
			isRemote:       true,
			expected:       "commit quorum not achieved: got 1 acks, need 2 (majority of 5 total members, 5 alive)",
		},
		{
			name:           "zero acks received",
			phase:          "prepare",
			acksReceived:   0,
			quorumRequired: 2,
			totalMembers:   3,
			aliveNodes:     3,
			isRemote:       false,
			expected:       "prepare quorum not achieved: got 0 acks, need 2 (majority of 3 total members, 3 alive)",
		},
		{
			name:           "some nodes dead",
			phase:          "prepare",
			acksReceived:   2,
			quorumRequired: 3,
			totalMembers:   5,
			aliveNodes:     4,
			isRemote:       false,
			expected:       "prepare quorum not achieved: got 2 acks, need 3 (majority of 5 total members, 4 alive)",
		},
		{
			name:           "single node cluster",
			phase:          "prepare",
			acksReceived:   0,
			quorumRequired: 1,
			totalMembers:   1,
			aliveNodes:     1,
			isRemote:       false,
			expected:       "prepare quorum not achieved: got 0 acks, need 1 (majority of 1 total members, 1 alive)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &QuorumNotAchievedError{
				Phase:           tt.phase,
				AcksReceived:    tt.acksReceived,
				QuorumRequired:  tt.quorumRequired,
				TotalMembership: tt.totalMembers,
				AliveNodes:      tt.aliveNodes,
				IsRemoteQuorum:  tt.isRemote,
			}
			if got := err.Error(); got != tt.expected {
				t.Errorf("QuorumNotAchievedError.Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestCoordinatorNotParticipatedError(t *testing.T) {
	tests := []struct {
		name     string
		txnID    uint64
		expected string
	}{
		{
			name:     "basic coordinator not participated",
			txnID:    123,
			expected: "coordinator must participate: local prepare failed",
		},
		{
			name:     "zero txn ID",
			txnID:    0,
			expected: "coordinator must participate: local prepare failed",
		},
		{
			name:     "large txn ID",
			txnID:    18446744073709551615, // max uint64
			expected: "coordinator must participate: local prepare failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &CoordinatorNotParticipatedError{
				TxnID: tt.txnID,
			}
			if got := err.Error(); got != tt.expected {
				t.Errorf("CoordinatorNotParticipatedError.Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestPartialCommitError(t *testing.T) {
	tests := []struct {
		name               string
		isLocal            bool
		remoteAcks         int
		remoteQuorumNeeded int
		localError         error
		expected           string
	}{
		{
			name:               "remote quorum failed",
			isLocal:            false,
			remoteAcks:         1,
			remoteQuorumNeeded: 2,
			localError:         nil,
			expected:           "partial commit: got 1 remote commit acks, needed 2 (some nodes may have committed)",
		},
		{
			name:               "remote quorum failed with zero acks",
			isLocal:            false,
			remoteAcks:         0,
			remoteQuorumNeeded: 2,
			localError:         nil,
			expected:           "partial commit: got 0 remote commit acks, needed 2 (some nodes may have committed)",
		},
		{
			name:               "local commit failed after remote quorum",
			isLocal:            true,
			remoteAcks:         0,
			remoteQuorumNeeded: 0,
			localError:         errors.New("database locked"),
			expected:           "partial commit: local commit failed after remote quorum: database locked",
		},
		{
			name:               "local commit failed with nil error",
			isLocal:            true,
			remoteAcks:         0,
			remoteQuorumNeeded: 0,
			localError:         nil,
			expected:           "partial commit: local commit failed after remote quorum: <nil>",
		},
		{
			name:               "local commit failed with wrapped error",
			isLocal:            true,
			remoteAcks:         0,
			remoteQuorumNeeded: 0,
			localError:         errors.New("failed to acquire lock: timeout"),
			expected:           "partial commit: local commit failed after remote quorum: failed to acquire lock: timeout",
		},
		{
			name:               "remote quorum failed with high numbers",
			isLocal:            false,
			remoteAcks:         4,
			remoteQuorumNeeded: 5,
			localError:         nil,
			expected:           "partial commit: got 4 remote commit acks, needed 5 (some nodes may have committed)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &PartialCommitError{
				IsLocal:            tt.isLocal,
				RemoteAcks:         tt.remoteAcks,
				RemoteQuorumNeeded: tt.remoteQuorumNeeded,
				LocalError:         tt.localError,
			}
			if got := err.Error(); got != tt.expected {
				t.Errorf("PartialCommitError.Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestPartialCommitRefusedLocallyIsNotRetryable: a local commit the write
// gate refused after the remote quorum committed is a partial commit, not a
// rolled-back transaction. Telling the client to restart it (1213) would apply
// its writes twice, so the refusal must not show through PartialCommitError.
//
// Mutation: give PartialCommitError an Unwrap returning LocalError. "a partial
// commit was reported as retryable" fires.
func TestPartialCommitRefusedLocallyIsNotRetryable(t *testing.T) {
	refused := sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintCommitHook}
	require.Equal(t, protocol.ErrCodeDeadlock, protocol.ConvertToMySQLError(refused).Code)
	got := protocol.ConvertToMySQLError(&PartialCommitError{IsLocal: true, LocalError: refused})
	require.NotEqual(t, protocol.ErrCodeDeadlock, got.Code, "a partial commit was reported as retryable")
	require.NotEqual(t, protocol.SQLStateDeadlock, got.SQLState, "a partial commit was reported as retryable")
}

// TestPrepareRejectionErrorsCarryTheParticipantsCode pins the last hop of the
// same plumbing: the coordinator turns a participant's refusal into an error
// whose only typed content is its own struct, so unless that struct re-types
// itself as the coded error the participant raised, every deterministic
// refusal reaches the client as ER_UNKNOWN_ERROR (1105, HY000).
//
// Mutation: delete either Unwrap, or make it return the error unconditionally
// instead of nil at code 0. The matching assertion below fires.
func TestPrepareRejectionErrorsCarryTheParticipantsCode(t *testing.T) {
	t.Parallel()

	local := &LocalPrepareError{Reason: "existing max value 200 exceeds width ceiling 127", ErrorCode: 1264}
	if got := protocol.ConvertToMySQLError(local); got.Code != 1264 {
		t.Errorf("local rejection reached the client as %d, want 1264 (the participant's own code)", got.Code)
	}

	remote := &RemotePrepareRejectedError{NodeID: 2, Reason: "same refusal on a remote participant", ErrorCode: 1264}
	if got := protocol.ConvertToMySQLError(remote); got.Code != 1264 {
		t.Errorf("remote rejection reached the client as %d, want 1264", got.Code)
	}

	// A refusal that named no code must NOT be dressed up as one: the
	// message-based classification stays in charge.
	plain := &LocalPrepareError{Reason: "local prepare rejected the transaction"}
	if got := protocol.ConvertToMySQLError(plain); got.Code == 1264 {
		t.Error("a rejection with no code was given one")
	}
}
