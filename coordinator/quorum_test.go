package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/protocol"
)

func TestQuorumSize(t *testing.T) {
	tests := []struct {
		name         string
		level        protocol.ConsistencyLevel
		replicaCount int
		want         int
	}{
		// LOCAL_ONE tests
		{
			name:         "LOCAL_ONE with 3 replicas",
			level:        protocol.ConsistencyLocalOne,
			replicaCount: 3,
			want:         1,
		},
		{
			name:         "LOCAL_ONE with 1 replica",
			level:        protocol.ConsistencyLocalOne,
			replicaCount: 1,
			want:         1,
		},

		// ONE tests
		{
			name:         "ONE with 3 replicas",
			level:        protocol.ConsistencyOne,
			replicaCount: 3,
			want:         1,
		},

		// QUORUM tests
		{
			name:         "QUORUM with 3 replicas",
			level:        protocol.ConsistencyQuorum,
			replicaCount: 3,
			want:         2, // floor(3/2) + 1 = 2
		},
		{
			name:         "QUORUM with 5 replicas",
			level:        protocol.ConsistencyQuorum,
			replicaCount: 5,
			want:         3, // floor(5/2) + 1 = 3
		},
		{
			name:         "QUORUM with 4 replicas",
			level:        protocol.ConsistencyQuorum,
			replicaCount: 4,
			want:         3, // floor(4/2) + 1 = 3
		},
		{
			name:         "QUORUM with 1 replica",
			level:        protocol.ConsistencyQuorum,
			replicaCount: 1,
			want:         1, // floor(1/2) + 1 = 1
		},

		// ALL tests
		{
			name:         "ALL with 3 replicas",
			level:        protocol.ConsistencyAll,
			replicaCount: 3,
			want:         3,
		},
		{
			name:         "ALL with 5 replicas",
			level:        protocol.ConsistencyAll,
			replicaCount: 5,
			want:         5,
		},

		// Edge cases
		{
			name:         "Zero replicas",
			level:        protocol.ConsistencyQuorum,
			replicaCount: 0,
			want:         0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuorumSize(tt.level, tt.replicaCount)
			if got != tt.want {
				t.Errorf("QuorumSize(%v, %d) = %d, want %d",
					tt.level, tt.replicaCount, got, tt.want)
			}
		})
	}
}

func TestIsQuorumAchieved(t *testing.T) {
	tests := []struct {
		name         string
		level        protocol.ConsistencyLevel
		successCount int
		replicaCount int
		want         bool
	}{
		{
			name:         "QUORUM achieved (2/3)",
			level:        protocol.ConsistencyQuorum,
			successCount: 2,
			replicaCount: 3,
			want:         true,
		},
		{
			name:         "QUORUM not achieved (1/3)",
			level:        protocol.ConsistencyQuorum,
			successCount: 1,
			replicaCount: 3,
			want:         false,
		},
		{
			name:         "QUORUM achieved (3/5)",
			level:        protocol.ConsistencyQuorum,
			successCount: 3,
			replicaCount: 5,
			want:         true,
		},
		{
			name:         "QUORUM not achieved (2/5)",
			level:        protocol.ConsistencyQuorum,
			successCount: 2,
			replicaCount: 5,
			want:         false,
		},
		{
			name:         "ALL achieved",
			level:        protocol.ConsistencyAll,
			successCount: 3,
			replicaCount: 3,
			want:         true,
		},
		{
			name:         "ALL not achieved",
			level:        protocol.ConsistencyAll,
			successCount: 2,
			replicaCount: 3,
			want:         false,
		},
		{
			name:         "ONE achieved",
			level:        protocol.ConsistencyOne,
			successCount: 1,
			replicaCount: 3,
			want:         true,
		},
		{
			name:         "ONE not achieved",
			level:        protocol.ConsistencyOne,
			successCount: 0,
			replicaCount: 3,
			want:         false,
		},
		{
			name:         "LOCAL_ONE achieved",
			level:        protocol.ConsistencyLocalOne,
			successCount: 1,
			replicaCount: 3,
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsQuorumAchieved(tt.level, tt.successCount, tt.replicaCount)
			if got != tt.want {
				t.Errorf("IsQuorumAchieved(%v, %d, %d) = %v, want %v",
					tt.level, tt.successCount, tt.replicaCount, got, tt.want)
			}
		})
	}
}

func TestCalculateReadQuorum(t *testing.T) {
	tests := []struct {
		name         string
		writeLevel   protocol.ConsistencyLevel
		replicaCount int
		want         int
	}{
		{
			name:         "LOCAL_ONE write -> LOCAL_ONE read",
			writeLevel:   protocol.ConsistencyLocalOne,
			replicaCount: 3,
			want:         1,
		},
		{
			name:         "ONE write -> QUORUM read (for consistency)",
			writeLevel:   protocol.ConsistencyOne,
			replicaCount: 3,
			want:         2, // QUORUM of 3
		},
		{
			name:         "QUORUM write -> ONE read (R+W>N satisfied)",
			writeLevel:   protocol.ConsistencyQuorum,
			replicaCount: 3,
			want:         1,
		},
		{
			name:         "ALL write -> ONE read (safest)",
			writeLevel:   protocol.ConsistencyAll,
			replicaCount: 3,
			want:         1,
		},
		{
			name:         "QUORUM write with 5 replicas",
			writeLevel:   protocol.ConsistencyQuorum,
			replicaCount: 5,
			want:         1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateReadQuorum(tt.writeLevel, tt.replicaCount)
			if got != tt.want {
				t.Errorf("CalculateReadQuorum(%v, %d) = %d, want %d",
					tt.writeLevel, tt.replicaCount, got, tt.want)
			}
		})
	}
}

func TestValidateConsistencyLevel(t *testing.T) {
	tests := []struct {
		name         string
		level        protocol.ConsistencyLevel
		replicaCount int
		wantErr      bool
	}{
		{
			name:         "Valid QUORUM with 3 replicas",
			level:        protocol.ConsistencyQuorum,
			replicaCount: 3,
			wantErr:      false,
		},
		{
			name:         "Valid ALL with 3 replicas",
			level:        protocol.ConsistencyAll,
			replicaCount: 3,
			wantErr:      false,
		},
		{
			name:         "Invalid zero replicas",
			level:        protocol.ConsistencyQuorum,
			replicaCount: 0,
			wantErr:      true,
		},
		{
			name:         "Invalid negative replicas",
			level:        protocol.ConsistencyQuorum,
			replicaCount: -1,
			wantErr:      true,
		},
		{
			name:         "Valid ONE with 1 replica",
			level:        protocol.ConsistencyOne,
			replicaCount: 1,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConsistencyLevel(tt.level, tt.replicaCount)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConsistencyLevel(%v, %d) error = %v, wantErr %v",
					tt.level, tt.replicaCount, err, tt.wantErr)
			}
		})
	}
}

func TestIsStrongConsistency(t *testing.T) {
	tests := []struct {
		name         string
		writeLevel   protocol.ConsistencyLevel
		readLevel    protocol.ConsistencyLevel
		replicaCount int
		want         bool
	}{
		{
			name:         "QUORUM write + QUORUM read with 3 replicas (R+W=4>3)",
			writeLevel:   protocol.ConsistencyQuorum,
			readLevel:    protocol.ConsistencyQuorum,
			replicaCount: 3,
			want:         true,
		},
		{
			name:         "QUORUM write + ONE read with 3 replicas (R+W=3, not >3)",
			writeLevel:   protocol.ConsistencyQuorum,
			readLevel:    protocol.ConsistencyOne,
			replicaCount: 3,
			want:         false,
		},
		{
			name:         "ALL write + ONE read with 3 replicas (R+W=4>3)",
			writeLevel:   protocol.ConsistencyAll,
			readLevel:    protocol.ConsistencyOne,
			replicaCount: 3,
			want:         true,
		},
		{
			name:         "ONE write + ALL read with 3 replicas (R+W=4>3)",
			writeLevel:   protocol.ConsistencyOne,
			readLevel:    protocol.ConsistencyAll,
			replicaCount: 3,
			want:         true,
		},
		{
			name:         "ONE write + ONE read with 3 replicas (R+W=2, not >3)",
			writeLevel:   protocol.ConsistencyOne,
			readLevel:    protocol.ConsistencyOne,
			replicaCount: 3,
			want:         false,
		},
		{
			name:         "QUORUM write + QUORUM read with 5 replicas (R+W=6>5)",
			writeLevel:   protocol.ConsistencyQuorum,
			readLevel:    protocol.ConsistencyQuorum,
			replicaCount: 5,
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsStrongConsistency(tt.writeLevel, tt.readLevel, tt.replicaCount)
			if got != tt.want {
				t.Errorf("IsStrongConsistency(%v, %v, %d) = %v, want %v",
					tt.writeLevel, tt.readLevel, tt.replicaCount, got, tt.want)
			}
		})
	}
}

func BenchmarkQuorumSize(b *testing.B) {
	for i := 0; i < b.N; i++ {
		QuorumSize(protocol.ConsistencyQuorum, 5)
	}
}

func BenchmarkIsQuorumAchieved(b *testing.B) {
	for i := 0; i < b.N; i++ {
		IsQuorumAchieved(protocol.ConsistencyQuorum, 3, 5)
	}
}
