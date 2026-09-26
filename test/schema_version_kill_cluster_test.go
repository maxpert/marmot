package test

import (
	"testing"
	"time"
)

// TestSchemaVersionSurvivesKillNine: a node applies DDL, is killed with
// SIGKILL and restarts with its tables intact. It must also keep its schema
// version: a node reporting a lower version than the schema it runs refuses
// every peer transaction that requires the newer one, and nothing but a
// snapshot restore heals it. With node 3 stopped, node 2's write needs node
// 1's vote, so it commits only if node 1 kept its version.
//
// Mutation: write the schema version with pebble.NoSync
// (PebbleMetaStore.UpdateSchemaVersion). "node 2 cannot commit a replicated
// write" fires: node 1 restarts at version 0.
func TestSchemaVersionSurvivesKillNine(t *testing.T) {
	harness := startNarrowCluster(t, "sv", "CREATE TABLE sv (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	defer harness.Cleanup()
	createQuorumProbe(t, harness)

	if err := harness.StopNode(1); err != nil {
		t.Fatalf("StopNode(1): %v", err)
	}
	if err := harness.StartNode(1); err != nil {
		t.Fatalf("StartNode(1): %v", err)
	}
	if err := harness.WaitForAlive(1, 90*time.Second); err != nil {
		t.Fatalf("node 1 did not come back: %v", err)
	}
	if err := harness.StopNode(3); err != nil {
		t.Fatalf("StopNode(3): %v", err)
	}
	waitForQuorumWrites(t, harness, 60*time.Second, 2)
}
