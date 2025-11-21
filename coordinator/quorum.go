package coordinator

import (
	"fmt"
	"math"

	"github.com/maxpert/marmot/protocol"
)

// QuorumSize calculates the required number of successful operations
// for a given consistency level and replica count.
func QuorumSize(level protocol.ConsistencyLevel, replicaCount int) int {
	if replicaCount <= 0 {
		return 0
	}

	switch level {
	case protocol.ConsistencyLocalOne:
		return 1 // Only local node

	case protocol.ConsistencyOne:
		return 1 // Any single replica

	case protocol.ConsistencyQuorum:
		// Majority: floor(N/2) + 1
		return int(math.Floor(float64(replicaCount)/2)) + 1

	case protocol.ConsistencyAll:
		return replicaCount // All replicas

	default:
		// Default to quorum for safety
		return int(math.Floor(float64(replicaCount)/2)) + 1
	}
}

// IsQuorumAchieved checks if the number of successful operations
// meets the quorum requirement for the given consistency level.
func IsQuorumAchieved(level protocol.ConsistencyLevel, successCount, replicaCount int) bool {
	required := QuorumSize(level, replicaCount)
	return successCount >= required
}

// CalculateReadQuorum calculates quorum size for read operations.
// For reads, we need enough responses to ensure we see the latest write.
func CalculateReadQuorum(writeLevel protocol.ConsistencyLevel, replicaCount int) int {
	// Read quorum calculation: R + W > N (where R=read quorum, W=write quorum, N=replicas)
	// This ensures read-write overlap for strong consistency

	// For LOCAL_ONE writes, we can do LOCAL_ONE reads
	if writeLevel == protocol.ConsistencyLocalOne {
		return 1
	}

	// For ONE writes, quorum reads ensure consistency
	if writeLevel == protocol.ConsistencyOne {
		return QuorumSize(protocol.ConsistencyQuorum, replicaCount)
	}

	// For QUORUM writes, ONE read is sufficient (R + W > N satisfied)
	if writeLevel == protocol.ConsistencyQuorum {
		return 1
	}

	// For ALL writes, any read level is safe
	if writeLevel == protocol.ConsistencyAll {
		return 1
	}

	// Default to quorum for safety
	return QuorumSize(protocol.ConsistencyQuorum, replicaCount)
}

// ValidateConsistencyLevel checks if a consistency level is valid
// for the given replica count.
func ValidateConsistencyLevel(level protocol.ConsistencyLevel, replicaCount int) error {
	if replicaCount <= 0 {
		return fmt.Errorf("replica count must be positive, got %d", replicaCount)
	}

	required := QuorumSize(level, replicaCount)
	if required > replicaCount {
		return fmt.Errorf("consistency level %v requires %d replicas, but only %d available",
			level, required, replicaCount)
	}

	return nil
}

// IsStrongConsistency returns true if the consistency level provides
// strong consistency guarantees (linearizability).
func IsStrongConsistency(writeLevel, readLevel protocol.ConsistencyLevel, replicaCount int) bool {
	// Strong consistency requires R + W > N
	R := QuorumSize(readLevel, replicaCount)
	W := QuorumSize(writeLevel, replicaCount)
	N := replicaCount

	return R+W > N
}
