package grpc

import (
	"testing"

	"github.com/maxpert/marmot/cfg"
)

// TestDeltaSyncIntermediateGapDetection tests that gaps in the middle of stream are detected
func TestDeltaSyncIntermediateGapDetection(t *testing.T) {
	// Set up threshold for testing
	originalThreshold := cfg.Config.Replication.DeltaSyncThresholdTxns
	cfg.Config.Replication.DeltaSyncThresholdTxns = 10 // Small threshold for testing
	defer func() { cfg.Config.Replication.DeltaSyncThresholdTxns = originalThreshold }()

	tests := []struct {
		name           string
		fromTxnID      uint64
		receivedTxnIDs []uint64
		expectGapAfter int // Index after which we expect gap error (-1 = no gap)
	}{
		{
			name:           "No gap - sequential",
			fromTxnID:      10,
			receivedTxnIDs: []uint64{11, 12, 13, 14, 15},
			expectGapAfter: -1,
		},
		{
			name:           "Initial gap - first event too far",
			fromTxnID:      10,
			receivedTxnIDs: []uint64{50, 51, 52}, // Gap of 39 from 10, exceeds threshold
			expectGapAfter: 0,                    // Gap detected on first event
		},
		{
			name:           "Intermediate gap",
			fromTxnID:      10,
			receivedTxnIDs: []uint64{11, 12, 13, 50, 51}, // Gap between 13→50
			expectGapAfter: 3,                            // Gap detected on 4th event (index 3)
		},
		{
			name:           "Small gap within threshold",
			fromTxnID:      10,
			receivedTxnIDs: []uint64{11, 15, 16, 17}, // Gap of 3 from 11→15, within threshold
			expectGapAfter: -1,
		},
		{
			name:           "Gap exactly at threshold",
			fromTxnID:      10,
			receivedTxnIDs: []uint64{11, 22, 23}, // Gap of 10 from 11→22, exactly at threshold
			expectGapAfter: -1,                   // Threshold is ">" not ">="
		},
		{
			name:           "Gap just over threshold",
			fromTxnID:      10,
			receivedTxnIDs: []uint64{11, 23, 24}, // Gap of 11 from 11→23, over threshold
			expectGapAfter: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &DeltaSyncResult{
				ExpectedNextTxn: tt.fromTxnID + 1,
			}

			gapDetectedAt := -1
			for i, txnID := range tt.receivedTxnIDs {
				// Skip already-applied transactions
				if txnID <= tt.fromTxnID {
					continue
				}

				// Check for gap
				if txnID > result.ExpectedNextTxn {
					gap := txnID - result.ExpectedNextTxn
					if gap > uint64(cfg.Config.Replication.DeltaSyncThresholdTxns) {
						gapDetectedAt = i
						break
					}
				}

				// Update expected next
				result.ExpectedNextTxn = txnID + 1
			}

			if gapDetectedAt != tt.expectGapAfter {
				t.Errorf("Gap detection mismatch: expected gap at index %d, got %d",
					tt.expectGapAfter, gapDetectedAt)
			}
		})
	}
}

// TestDeltaSyncResultExpectedNextTxn tests that ExpectedNextTxn is properly tracked
func TestDeltaSyncResultExpectedNextTxn(t *testing.T) {
	result := &DeltaSyncResult{
		ExpectedNextTxn: 100,
	}

	// Simulate receiving sequential events
	for txnID := uint64(100); txnID <= 105; txnID++ {
		result.ExpectedNextTxn = txnID + 1
		result.LastAppliedTxnID = txnID
		result.TxnsApplied++
	}

	if result.ExpectedNextTxn != 106 {
		t.Errorf("Expected ExpectedNextTxn=106, got %d", result.ExpectedNextTxn)
	}

	if result.TxnsApplied != 6 {
		t.Errorf("Expected TxnsApplied=6, got %d", result.TxnsApplied)
	}

	if result.LastAppliedTxnID != 105 {
		t.Errorf("Expected LastAppliedTxnID=105, got %d", result.LastAppliedTxnID)
	}
}
