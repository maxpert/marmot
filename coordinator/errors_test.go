package coordinator

import (
	"errors"
	"testing"
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
			expected:           "partial commit: local commit failed after remote quorum (bug in PREPARE): database locked",
		},
		{
			name:               "local commit failed with nil error",
			isLocal:            true,
			remoteAcks:         0,
			remoteQuorumNeeded: 0,
			localError:         nil,
			expected:           "partial commit: local commit failed after remote quorum (bug in PREPARE): <nil>",
		},
		{
			name:               "local commit failed with wrapped error",
			isLocal:            true,
			remoteAcks:         0,
			remoteQuorumNeeded: 0,
			localError:         errors.New("failed to acquire lock: timeout"),
			expected:           "partial commit: local commit failed after remote quorum (bug in PREPARE): failed to acquire lock: timeout",
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
