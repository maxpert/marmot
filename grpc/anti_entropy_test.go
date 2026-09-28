package grpc

import (
	"os"
	"testing"
	"time"
)

// TestAntiEntropyRestoreContextOutlivesInterval: a snapshot restore's own
// deadline must be independent of ae.interval, which bounds one anti-entropy round, not a
// one-shot transfer of a database's full current size. With a short
// interval and a much longer configured restore timeout, the context
// restoreContext returns must reflect the restore timeout, not the interval.
func TestAntiEntropyRestoreContextOutlivesInterval(t *testing.T) {
	ae := NewAntiEntropyService(AntiEntropyConfig{
		NodeID:                 1,
		Interval:               time.Second,
		SnapshotRestoreTimeout: time.Hour,
		Enabled:                true,
	})

	ctx, cancel := ae.restoreContext()
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("restoreContext returned a context with no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= ae.interval {
		t.Fatalf("restore context deadline (%s remaining) did not outlive ae.interval (%s); "+
			"it appears bounded by the anti-entropy round interval instead of SnapshotRestoreTimeout",
			remaining, ae.interval)
	}
}

// TestAntiEntropyCurrentMembers tests that currentMembers excludes self and
// REMOVED nodes, but keeps LEAVING/DEAD/SUSPECT as members while only
// ALIVE nodes appear in the alive slice.
func TestAntiEntropyCurrentMembers(t *testing.T) {
	registry := NewNodeRegistry(1, "localhost:8081")
	registry.Add(&NodeState{NodeId: 1, Address: "localhost:8081", Status: NodeStatus_ALIVE})
	registry.Add(&NodeState{NodeId: 2, Address: "localhost:8082", Status: NodeStatus_ALIVE})
	registry.Add(&NodeState{NodeId: 3, Address: "localhost:8083", Status: NodeStatus_SUSPECT})
	registry.Add(&NodeState{NodeId: 4, Address: "localhost:8084", Status: NodeStatus_DEAD})
	registry.Add(&NodeState{NodeId: 5, Address: "localhost:8085", Status: NodeStatus_LEAVING})
	registry.Add(&NodeState{NodeId: 6, Address: "localhost:8086", Status: NodeStatus_REMOVED})

	ae := &AntiEntropyService{nodeID: 1, registry: registry}

	members, alive := ae.currentMembers()

	memberIDs := make(map[uint64]bool)
	for _, m := range members {
		memberIDs[m.NodeId] = true
	}
	// Self and REMOVED are excluded; everything else (including
	// LEAVING/DEAD/SUSPECT) is a member.
	for _, id := range []uint64{2, 3, 4, 5} {
		if !memberIDs[id] {
			t.Errorf("expected node %d to be a member", id)
		}
	}
	if memberIDs[1] || memberIDs[6] {
		t.Errorf("expected self and REMOVED to be excluded, got members %v", memberIDs)
	}

	if len(alive) != 1 || alive[0].NodeId != 2 {
		t.Errorf("expected only node 2 to be alive, got %v", alive)
	}
}

// TestConsiderSnapshotSource tests the restore-source tie-break rule:
// highest schema version wins, ties broken by the lowest node id.
func TestConsiderSnapshotSource(t *testing.T) {
	peer2 := &NodeState{NodeId: 2}
	peer3 := &NodeState{NodeId: 3}
	peer5 := &NodeState{NodeId: 5}

	var best *snapshotSource
	best = considerSnapshotSource(best, snapshotSource{peer: peer5, schemaVersion: 3})
	if best.peer.NodeId != 5 {
		t.Fatalf("expected first candidate to become best, got node %d", best.peer.NodeId)
	}

	// Lower schema version never displaces a higher one.
	best = considerSnapshotSource(best, snapshotSource{peer: peer2, schemaVersion: 1})
	if best.peer.NodeId != 5 {
		t.Fatalf("expected node 5 (higher schema version) to remain best, got node %d", best.peer.NodeId)
	}

	// Higher schema version wins outright.
	best = considerSnapshotSource(best, snapshotSource{peer: peer3, schemaVersion: 9})
	if best.peer.NodeId != 3 {
		t.Fatalf("expected node 3 (higher schema version) to become best, got node %d", best.peer.NodeId)
	}

	// Same schema version: lower node id wins.
	best = considerSnapshotSource(best, snapshotSource{peer: peer2, schemaVersion: 9})
	if best.peer.NodeId != 2 {
		t.Fatalf("expected node 2 (tie broken by lowest id) to become best, got node %d", best.peer.NodeId)
	}
}

// TestAntiEntropyCaughtUpDefaultsFalse tests that a database anti-entropy
// has never run a round for reports not caught up.
func TestAntiEntropyCaughtUpDefaultsFalse(t *testing.T) {
	ae := NewAntiEntropyService(AntiEntropyConfig{NodeID: 1})
	if ae.CaughtUp("marmot") {
		t.Error("expected a database with no completed round to report not caught up")
	}
}

// TestAntiEntropySetCaughtUp tests that setCaughtUp/CaughtUp round-trip per
// database independently.
func TestAntiEntropySetCaughtUp(t *testing.T) {
	ae := NewAntiEntropyService(AntiEntropyConfig{NodeID: 1})

	ae.setCaughtUp("a", true)
	ae.setCaughtUp("b", false)

	if !ae.CaughtUp("a") {
		t.Error("expected database a to be caught up")
	}
	if ae.CaughtUp("b") {
		t.Error("expected database b to not be caught up")
	}
	if ae.CaughtUp("c") {
		t.Error("expected an untouched database to default to not caught up")
	}
}

// TestAntiEntropyGetStats tests the stats shape: overall fields plus a
// per-database caught-up/stuck-txns entry.
func TestAntiEntropyGetStats(t *testing.T) {
	registry := NewNodeRegistry(1, "localhost:8081")
	tmpDir, dbMgr, _ := setupTestEnvironment(t, "test_anti_entropy_get_stats")
	t.Cleanup(func() { dbMgr.Close() })
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	if err := dbMgr.CreateDatabase("marmot"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	ae := NewAntiEntropyService(AntiEntropyConfig{
		NodeID:    1,
		Registry:  registry,
		DBManager: dbMgr,
		LogPuller: NewLogPuller(LogPullerConfig{NodeID: 1, Client: NewClient(1), DBManager: dbMgr}),
		Interval:  60 * time.Second,
		Enabled:   true,
	})
	ae.setCaughtUp("marmot", true)

	stats := ae.GetStats()

	if enabled, ok := stats["enabled"].(bool); !ok || !enabled {
		t.Errorf("expected enabled=true, got %v", stats["enabled"])
	}
	if running, ok := stats["running"].(bool); !ok || running {
		t.Errorf("expected running=false, got %v", stats["running"])
	}
	if interval, ok := stats["interval_seconds"].(float64); !ok || interval != 60.0 {
		t.Errorf("expected interval_seconds=60, got %v", stats["interval_seconds"])
	}

	dbStats, ok := stats["databases"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected databases map in stats, got %T", stats["databases"])
	}
	marmotStats, ok := dbStats["marmot"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected marmot entry in databases stats, got %v", dbStats)
	}
	if caughtUp, ok := marmotStats["caught_up"].(bool); !ok || !caughtUp {
		t.Errorf("expected marmot caught_up=true, got %v", marmotStats["caught_up"])
	}
	if stuck, ok := marmotStats["stuck_txns"].(int); !ok || stuck != 0 {
		t.Errorf("expected marmot stuck_txns=0, got %v", marmotStats["stuck_txns"])
	}
}

// TestAntiEntropyStartStop tests start/stop lifecycle.
func TestAntiEntropyStartStop(t *testing.T) {
	registry := NewNodeRegistry(1, "localhost:8081")
	tmpDir, dbMgr, _ := setupTestEnvironment(t, "test_anti_entropy_start_stop")
	t.Cleanup(func() { dbMgr.Close() })
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	ae := NewAntiEntropyService(AntiEntropyConfig{
		NodeID:    1,
		Registry:  registry,
		DBManager: dbMgr,
		Client:    NewClient(1),
		Interval:  100 * time.Millisecond,
		Enabled:   true,
	})

	ae.Start()
	time.Sleep(50 * time.Millisecond)

	ae.mu.Lock()
	running := ae.running
	ae.mu.Unlock()
	if !running {
		t.Error("expected service to be running after Start()")
	}

	ae.Stop()
	time.Sleep(150 * time.Millisecond)

	ae.mu.Lock()
	running = ae.running
	ae.mu.Unlock()
	if running {
		t.Error("expected service to be stopped after Stop()")
	}
}

// TestAntiEntropyDisabled tests that a disabled service doesn't start.
func TestAntiEntropyDisabled(t *testing.T) {
	ae := NewAntiEntropyService(AntiEntropyConfig{NodeID: 1, Enabled: false})

	ae.Start()
	time.Sleep(50 * time.Millisecond)

	ae.mu.Lock()
	running := ae.running
	ae.mu.Unlock()
	if running {
		t.Error("disabled service should not be running")
	}
}
