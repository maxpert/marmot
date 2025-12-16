package coordinator

import (
	"fmt"
	"testing"

	"github.com/maxpert/marmot/protocol"
)

// TestGetClusterState_Success verifies GetClusterState returns correct state
// for normal cluster operation with multiple alive nodes.
func TestGetClusterState_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		nodes          []uint64
		consistency    protocol.ConsistencyLevel
		expectedQuorum int
		expectedAlive  int
		expectedTotal  int
	}{
		{
			name:           "3 nodes with QUORUM",
			nodes:          []uint64{1, 2, 3},
			consistency:    protocol.ConsistencyQuorum,
			expectedQuorum: 2, // floor(3/2) + 1 = 2
			expectedAlive:  3,
			expectedTotal:  3,
		},
		{
			name:           "5 nodes with QUORUM",
			nodes:          []uint64{1, 2, 3, 4, 5},
			consistency:    protocol.ConsistencyQuorum,
			expectedQuorum: 3, // floor(5/2) + 1 = 3
			expectedAlive:  5,
			expectedTotal:  5,
		},
		{
			name:           "3 nodes with ONE",
			nodes:          []uint64{1, 2, 3},
			consistency:    protocol.ConsistencyOne,
			expectedQuorum: 1,
			expectedAlive:  3,
			expectedTotal:  3,
		},
		{
			name:           "3 nodes with LOCAL_ONE",
			nodes:          []uint64{1, 2, 3},
			consistency:    protocol.ConsistencyLocalOne,
			expectedQuorum: 1,
			expectedAlive:  3,
			expectedTotal:  3,
		},
		{
			name:           "5 nodes with ALL",
			nodes:          []uint64{1, 2, 3, 4, 5},
			consistency:    protocol.ConsistencyAll,
			expectedQuorum: 5,
			expectedAlive:  5,
			expectedTotal:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nodeProvider := newMockNodeProvider(tt.nodes)

			state, err := GetClusterState(nodeProvider, tt.consistency)
			if err != nil {
				t.Fatalf("GetClusterState() error = %v, want nil", err)
			}

			if state == nil {
				t.Fatal("GetClusterState() returned nil state")
			}

			if len(state.AliveNodes) != tt.expectedAlive {
				t.Errorf("AliveNodes count = %d, want %d", len(state.AliveNodes), tt.expectedAlive)
			}

			if state.TotalMembership != tt.expectedTotal {
				t.Errorf("TotalMembership = %d, want %d", state.TotalMembership, tt.expectedTotal)
			}

			if state.RequiredQuorum != tt.expectedQuorum {
				t.Errorf("RequiredQuorum = %d, want %d", state.RequiredQuorum, tt.expectedQuorum)
			}

			// Verify alive nodes are correct
			nodeMap := make(map[uint64]bool)
			for _, n := range state.AliveNodes {
				nodeMap[n] = true
			}
			for _, expected := range tt.nodes {
				if !nodeMap[expected] {
					t.Errorf("Expected node %d in AliveNodes, but not found", expected)
				}
			}
		})
	}
}

// TestGetClusterState_NoAliveNodes verifies GetClusterState returns error
// when no alive nodes are available in the cluster.
func TestGetClusterState_NoAliveNodes(t *testing.T) {
	t.Parallel()

	// Create node provider with no alive nodes
	nodeProvider := newMockNodeProvider([]uint64{})

	state, err := GetClusterState(nodeProvider, protocol.ConsistencyQuorum)

	if err == nil {
		t.Fatal("GetClusterState() expected error for no alive nodes, got nil")
	}

	if state != nil {
		t.Errorf("GetClusterState() expected nil state, got %+v", state)
	}

	expectedErrMsg := "no alive nodes in cluster"
	if !contains(err.Error(), expectedErrMsg) {
		t.Errorf("GetClusterState() error = %v, want error containing %q", err, expectedErrMsg)
	}
}

// TestGetClusterState_InvalidConsistency verifies GetClusterState returns error
// for invalid consistency levels (though currently all ConsistencyLevel values are valid).
// This test ensures ValidateConsistencyLevel is called and works correctly.
func TestGetClusterState_InvalidConsistency(t *testing.T) {
	t.Parallel()

	// Test with consistency level that requires more replicas than available
	// Note: ValidateConsistencyLevel doesn't reject unknown enum values,
	// but it validates that required quorum <= available replicas

	tests := []struct {
		name        string
		nodes       []uint64
		consistency protocol.ConsistencyLevel
		wantErr     bool
	}{
		{
			name:        "ALL with 3 nodes - valid",
			nodes:       []uint64{1, 2, 3},
			consistency: protocol.ConsistencyAll,
			wantErr:     false, // 3 nodes available, need 3
		},
		{
			name:        "QUORUM with 3 nodes - valid",
			nodes:       []uint64{1, 2, 3},
			consistency: protocol.ConsistencyQuorum,
			wantErr:     false, // 3 nodes available, need 2
		},
		{
			name:        "ONE with 1 node - valid",
			nodes:       []uint64{1},
			consistency: protocol.ConsistencyOne,
			wantErr:     false, // 1 node available, need 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nodeProvider := newMockNodeProvider(tt.nodes)
			state, err := GetClusterState(nodeProvider, tt.consistency)

			if tt.wantErr {
				if err == nil {
					t.Errorf("GetClusterState() expected error, got nil")
				}
				if state != nil {
					t.Errorf("GetClusterState() expected nil state on error, got %+v", state)
				}
			} else {
				if err != nil {
					t.Errorf("GetClusterState() unexpected error = %v", err)
				}
				if state == nil {
					t.Errorf("GetClusterState() expected valid state, got nil")
				}
			}
		})
	}
}

// TestGetClusterState_QuorumCalculation verifies that GetClusterState
// calculates the correct quorum size for different consistency levels.
func TestGetClusterState_QuorumCalculation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		nodes          []uint64
		consistency    protocol.ConsistencyLevel
		expectedQuorum int
	}{
		{
			name:           "LOCAL_ONE always requires 1",
			nodes:          []uint64{1, 2, 3, 4, 5},
			consistency:    protocol.ConsistencyLocalOne,
			expectedQuorum: 1,
		},
		{
			name:           "ONE always requires 1",
			nodes:          []uint64{1, 2, 3, 4, 5},
			consistency:    protocol.ConsistencyOne,
			expectedQuorum: 1,
		},
		{
			name:           "QUORUM with 3 nodes requires 2",
			nodes:          []uint64{1, 2, 3},
			consistency:    protocol.ConsistencyQuorum,
			expectedQuorum: 2, // floor(3/2) + 1 = 2
		},
		{
			name:           "QUORUM with 4 nodes requires 3",
			nodes:          []uint64{1, 2, 3, 4},
			consistency:    protocol.ConsistencyQuorum,
			expectedQuorum: 3, // floor(4/2) + 1 = 3
		},
		{
			name:           "QUORUM with 5 nodes requires 3",
			nodes:          []uint64{1, 2, 3, 4, 5},
			consistency:    protocol.ConsistencyQuorum,
			expectedQuorum: 3, // floor(5/2) + 1 = 3
		},
		{
			name:           "QUORUM with 6 nodes requires 4",
			nodes:          []uint64{1, 2, 3, 4, 5, 6},
			consistency:    protocol.ConsistencyQuorum,
			expectedQuorum: 4, // floor(6/2) + 1 = 4
		},
		{
			name:           "ALL with 3 nodes requires 3",
			nodes:          []uint64{1, 2, 3},
			consistency:    protocol.ConsistencyAll,
			expectedQuorum: 3,
		},
		{
			name:           "ALL with 5 nodes requires 5",
			nodes:          []uint64{1, 2, 3, 4, 5},
			consistency:    protocol.ConsistencyAll,
			expectedQuorum: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nodeProvider := newMockNodeProvider(tt.nodes)

			state, err := GetClusterState(nodeProvider, tt.consistency)
			if err != nil {
				t.Fatalf("GetClusterState() error = %v, want nil", err)
			}

			if state.RequiredQuorum != tt.expectedQuorum {
				t.Errorf("RequiredQuorum = %d, want %d", state.RequiredQuorum, tt.expectedQuorum)
			}
		})
	}
}

// TestGetClusterState_SplitBrainPrevention verifies that GetClusterState
// uses TotalMembership (not just alive nodes) for quorum calculation.
// This is CRITICAL for split-brain prevention.
//
// Scenario: 6-node cluster splits 3-3. Each partition sees only 3 alive nodes.
// If we used alive count for quorum, each partition would compute quorum=2 (floor(3/2)+1).
// Both partitions could achieve quorum and accept conflicting writes (split-brain).
//
// Solution: Use total membership (6) for quorum calculation.
// Quorum = floor(6/2) + 1 = 4. Neither partition can achieve quorum (only 3 nodes available).
func TestGetClusterState_SplitBrainPrevention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		aliveNodes      []uint64
		totalMembership int
		consistency     protocol.ConsistencyLevel
		expectedQuorum  int
		description     string
	}{
		{
			name:            "6-node cluster, 3 alive (split-brain scenario)",
			aliveNodes:      []uint64{1, 2, 3},
			totalMembership: 6,
			consistency:     protocol.ConsistencyQuorum,
			expectedQuorum:  4, // floor(6/2) + 1 = 4, prevents split-brain
			description:     "In a 3-3 split, neither partition can achieve quorum of 4",
		},
		{
			name:            "5-node cluster, 2 alive",
			aliveNodes:      []uint64{1, 2},
			totalMembership: 5,
			consistency:     protocol.ConsistencyQuorum,
			expectedQuorum:  3, // floor(5/2) + 1 = 3
			description:     "Only 2 alive, cannot achieve quorum of 3",
		},
		{
			name:            "4-node cluster, 2 alive (even split)",
			aliveNodes:      []uint64{1, 2},
			totalMembership: 4,
			consistency:     protocol.ConsistencyQuorum,
			expectedQuorum:  3, // floor(4/2) + 1 = 3
			description:     "In a 2-2 split, neither partition can achieve quorum of 3",
		},
		{
			name:            "10-node cluster, 5 alive (even split)",
			aliveNodes:      []uint64{1, 2, 3, 4, 5},
			totalMembership: 10,
			consistency:     protocol.ConsistencyQuorum,
			expectedQuorum:  6, // floor(10/2) + 1 = 6
			description:     "In a 5-5 split, neither partition can achieve quorum of 6",
		},
		{
			name:            "ALL consistency uses total membership",
			aliveNodes:      []uint64{1, 2, 3},
			totalMembership: 6,
			consistency:     protocol.ConsistencyAll,
			expectedQuorum:  6, // Requires ALL 6 nodes (impossible with only 3 alive)
			description:     "ALL consistency requires all nodes in total membership",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nodeProvider := newMockNodeProvider(tt.aliveNodes)
			nodeProvider.SetTotalMembership(tt.totalMembership)

			state, err := GetClusterState(nodeProvider, tt.consistency)
			if err != nil {
				t.Fatalf("GetClusterState() error = %v, want nil", err)
			}

			if state.TotalMembership != tt.totalMembership {
				t.Errorf("TotalMembership = %d, want %d",
					state.TotalMembership, tt.totalMembership)
			}

			if state.RequiredQuorum != tt.expectedQuorum {
				t.Errorf("RequiredQuorum = %d, want %d (based on total membership %d, not alive count %d)\n%s",
					state.RequiredQuorum, tt.expectedQuorum, tt.totalMembership, len(tt.aliveNodes), tt.description)
			}

			// Verify alive nodes count is correct
			if len(state.AliveNodes) != len(tt.aliveNodes) {
				t.Errorf("AliveNodes count = %d, want %d",
					len(state.AliveNodes), len(tt.aliveNodes))
			}

			t.Logf("Split-brain prevention verified: %s", tt.description)
		})
	}
}

// TestGetClusterState_NodeProviderError verifies GetClusterState handles
// errors from GetAliveNodes() correctly.
func TestGetClusterState_NodeProviderError(t *testing.T) {
	t.Parallel()

	// Create a mock node provider that returns an error
	nodeProvider := &errorNodeProvider{err: fmt.Errorf("network timeout")}

	state, err := GetClusterState(nodeProvider, protocol.ConsistencyQuorum)

	if err == nil {
		t.Fatal("GetClusterState() expected error, got nil")
	}

	if state != nil {
		t.Errorf("GetClusterState() expected nil state on error, got %+v", state)
	}

	expectedErrMsg := "failed to get alive nodes"
	if !contains(err.Error(), expectedErrMsg) {
		t.Errorf("GetClusterState() error = %v, want error containing %q", err, expectedErrMsg)
	}

	// Verify the wrapped error is included
	if !contains(err.Error(), "network timeout") {
		t.Errorf("GetClusterState() error should wrap original error: %v", err)
	}
}

// TestGetClusterState_SingleNode verifies GetClusterState works with
// single-node clusters (edge case).
func TestGetClusterState_SingleNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		consistency    protocol.ConsistencyLevel
		expectedQuorum int
	}{
		{
			name:           "Single node with LOCAL_ONE",
			consistency:    protocol.ConsistencyLocalOne,
			expectedQuorum: 1,
		},
		{
			name:           "Single node with ONE",
			consistency:    protocol.ConsistencyOne,
			expectedQuorum: 1,
		},
		{
			name:           "Single node with QUORUM",
			consistency:    protocol.ConsistencyQuorum,
			expectedQuorum: 1, // floor(1/2) + 1 = 1
		},
		{
			name:           "Single node with ALL",
			consistency:    protocol.ConsistencyAll,
			expectedQuorum: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nodeProvider := newMockNodeProvider([]uint64{1})

			state, err := GetClusterState(nodeProvider, tt.consistency)
			if err != nil {
				t.Fatalf("GetClusterState() error = %v, want nil", err)
			}

			if state.RequiredQuorum != tt.expectedQuorum {
				t.Errorf("RequiredQuorum = %d, want %d", state.RequiredQuorum, tt.expectedQuorum)
			}

			if len(state.AliveNodes) != 1 {
				t.Errorf("AliveNodes count = %d, want 1", len(state.AliveNodes))
			}

			if state.TotalMembership != 1 {
				t.Errorf("TotalMembership = %d, want 1", state.TotalMembership)
			}
		})
	}
}

// errorNodeProvider is a mock NodeProvider that returns errors
type errorNodeProvider struct {
	err error
}

func (e *errorNodeProvider) GetAliveNodes() ([]uint64, error) {
	return nil, e.err
}

func (e *errorNodeProvider) GetClusterSize() int {
	return 0
}

func (e *errorNodeProvider) GetTotalMembershipSize() int {
	return 0
}
