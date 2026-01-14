package cfg

import (
	"testing"
)

func TestValidate_AntiEntropyGCAlignment(t *testing.T) {
	tests := []struct {
		name                      string
		enableAntiEntropy         bool
		antiEntropyIntervalS      int
		gcIntervalS               int
		deltaSyncThresholdSeconds int
		gcMinRetentionHours       int
		gcMaxRetentionHours       int
		expectError               bool
		errorContains             string
	}{
		{
			name:                      "Valid: Default production settings",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      30,   // AE runs more frequently
			gcIntervalS:               60,   // GC runs less frequently (MUST be >= AE)
			deltaSyncThresholdSeconds: 3600, // 1 hour
			gcMinRetentionHours:       2,    // 2x delta threshold
			gcMaxRetentionHours:       24,   // 24x delta threshold
			expectError:               false,
		},
		{
			name:                      "Valid: Exactly at minimum (gcMin = delta threshold)",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      30,
			gcIntervalS:               60,
			deltaSyncThresholdSeconds: 3600, // 1 hour
			gcMinRetentionHours:       1,    // Equal to delta threshold
			gcMaxRetentionHours:       24,
			expectError:               false,
		},
		{
			name:                      "Valid: Exactly at 2x minimum (gcMax = 2x delta)",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      30,
			gcIntervalS:               60,
			deltaSyncThresholdSeconds: 3600, // 1 hour
			gcMinRetentionHours:       1,
			gcMaxRetentionHours:       2, // Exactly 2x delta threshold, > gcMin
			expectError:               false,
		},
		{
			name:                      "Valid: Unlimited GC (gcMax = 0)",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      30,
			gcIntervalS:               60,
			deltaSyncThresholdSeconds: 3600,
			gcMinRetentionHours:       2,
			gcMaxRetentionHours:       0, // Unlimited
			expectError:               false,
		},
		{
			name:                      "Invalid: gcMin < delta threshold",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      30,
			gcIntervalS:               60,
			deltaSyncThresholdSeconds: 7200, // 2 hours
			gcMinRetentionHours:       1,    // Less than 2 hours
			gcMaxRetentionHours:       24,
			expectError:               true,
			errorContains:             "gc_min_retention_hours",
		},
		{
			name:                      "Invalid: gcMax < 2x delta threshold",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      30,
			gcIntervalS:               60,
			deltaSyncThresholdSeconds: 3600, // 1 hour
			gcMinRetentionHours:       2,
			gcMaxRetentionHours:       1, // Less than 2 hours (2x delta)
			expectError:               true,
			errorContains:             "gc_max_retention_hours",
		},
		{
			name:                      "Invalid: gcMin >= gcMax",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      30,
			gcIntervalS:               60,
			deltaSyncThresholdSeconds: 3600,
			gcMinRetentionHours:       24,
			gcMaxRetentionHours:       24, // Equal (should be greater)
			expectError:               true,
			errorContains:             "must be <",
		},
		{
			name:                      "Invalid: gcMin > gcMax",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      30,
			gcIntervalS:               60,
			deltaSyncThresholdSeconds: 3600,
			gcMinRetentionHours:       48,
			gcMaxRetentionHours:       24, // Less than min
			expectError:               true,
			errorContains:             "must be <",
		},
		{
			name:                      "Valid: Anti-entropy disabled (no validation)",
			enableAntiEntropy:         false,
			antiEntropyIntervalS:      30,
			gcIntervalS:               60,
			deltaSyncThresholdSeconds: 3600,
			gcMinRetentionHours:       0, // Would be invalid if AE enabled
			gcMaxRetentionHours:       1,
			expectError:               false, // Validation skipped when AE disabled
		},
		{
			name:                      "Invalid: GC interval < AE interval",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      60, // AE runs every 60s
			gcIntervalS:               30, // GC runs every 30s - INVALID!
			deltaSyncThresholdSeconds: 3600,
			gcMinRetentionHours:       2,
			gcMaxRetentionHours:       24,
			expectError:               true,
			errorContains:             "gc_interval_seconds",
		},
		{
			name:                      "Valid: GC interval equals AE interval",
			enableAntiEntropy:         true,
			antiEntropyIntervalS:      60,
			gcIntervalS:               60, // Equal is valid
			deltaSyncThresholdSeconds: 3600,
			gcMinRetentionHours:       2,
			gcMaxRetentionHours:       24,
			expectError:               false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original config
			original := Config
			defer func() { Config = original }()

			// Set test config
			Config = &Configuration{
				Cluster: ClusterConfiguration{
					GRPCPort: 8081,
				},
				MySQL: MySQLConfiguration{
					Enabled: false,
				},
				Replication: ReplicationConfiguration{
					DefaultWriteConsist:       "QUORUM",
					DefaultReadConsist:        "LOCAL_ONE",
					EnableAntiEntropy:         tt.enableAntiEntropy,
					AntiEntropyIntervalS:      tt.antiEntropyIntervalS,
					GCIntervalS:               tt.gcIntervalS,
					DeltaSyncThresholdSeconds: tt.deltaSyncThresholdSeconds,
					GCMinRetentionHours:       tt.gcMinRetentionHours,
					GCMaxRetentionHours:       tt.gcMaxRetentionHours,
				},
				Transaction: TransactionConfiguration{
					HeartbeatTimeoutSeconds: 10,
					ConflictWindowSeconds:   10,
				},
				ConnectionPool: ConnectionPoolConfiguration{
					PoolSize:           4,
					MaxIdleTimeSeconds: 10,
					MaxLifetimeSeconds: 300,
				},
				GRPCClient: GRPCClientConfiguration{
					KeepaliveTimeSeconds:    10,
					KeepaliveTimeoutSeconds: 3,
					MaxRetries:              3,
					RetryBackoffMS:          100,
				},
				Coordinator: CoordinatorConfiguration{
					PrepareTimeoutMS: 2000,
					CommitTimeoutMS:  2000,
					AbortTimeoutMS:   2000,
				},
			}

			err := Validate()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error containing '%s', but got no error", tt.errorContains)
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing '%s', but got: %v", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, but got: %v", err)
				}
			}
		})
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
