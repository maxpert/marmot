package grpc

import (
	"os"
	"path/filepath"
	"testing"
)

// registryAt builds a registry rooted at dir, as production does.
func registryAt(t *testing.T, nodeID uint64, address, dir string) *NodeRegistry {
	t.Helper()
	return NewNodeRegistryWithDataDir(nodeID, address, dir)
}

// TestMembershipSurvivesRestart is the defect this step exists to close: a node
// restarted while its peers are unreachable must not compute a quorum from a
// membership of one.
//
// Mutation: delete the restoreMembershipLocked call in the constructor. Count()
// after restart drops to 1 and the first assertion fires.
func TestMembershipSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	first := registryAt(t, 1, "10.0.0.1:7100", dir)
	first.Add(&NodeState{NodeId: 2, Address: "10.0.0.2:7100", Status: NodeStatus_ALIVE, Incarnation: 4})
	first.Add(&NodeState{NodeId: 3, Address: "10.0.0.3:7100", Status: NodeStatus_ALIVE, Incarnation: 7})
	if got := first.Count(); got != 3 {
		t.Fatalf("fixture: membership before restart = %d, want 3", got)
	}

	// A restart: a brand new registry over the same data directory, with no
	// gossip and no reachable peer.
	restarted := registryAt(t, 1, "10.0.0.1:7100", dir)

	if got := restarted.Count(); got != 3 {
		t.Fatalf("membership after restart = %d, want 3; a lone node would be its own quorum", got)
	}

	// Liveness is not restored: nothing has been heard from these peers.
	// Mutation: restore peers as ALIVE instead of SUSPECT. This fires, and so
	// would the cluster's notion of who can receive a write.
	if got := restarted.CountAlive(); got != 1 {
		t.Errorf("alive after restart = %d, want 1 (self only)", got)
	}
	for _, id := range []uint64{2, 3} {
		node, ok := restarted.Get(id)
		if !ok {
			t.Fatalf("peer %d missing after restart", id)
		}
		if node.Status != NodeStatus_SUSPECT {
			t.Errorf("peer %d restored as %s, want SUSPECT", id, node.Status)
		}
	}

	// Incarnation must survive, or SWIM refutation compares against zero and a
	// stale rumour can overwrite a peer that has moved on.
	// Mutation: drop Incarnation from the snapshot record.
	if node, _ := restarted.Get(2); node.Incarnation != 4 {
		t.Errorf("peer 2 incarnation = %d, want 4", node.Incarnation)
	}
	if node, _ := restarted.Get(3); node.Incarnation != 7 {
		t.Errorf("peer 3 incarnation = %d, want 7", node.Incarnation)
	}
	// The address is what a restored node needs in order to gossip back.
	// Mutation: drop Address from the snapshot record.
	if node, _ := restarted.Get(2); node.Address != "10.0.0.2:7100" {
		t.Errorf("peer 2 address = %q, want 10.0.0.2:7100", node.Address)
	}
}

// TestRemovedNodeStaysRemovedAcrossRestart pins that a decommissioned peer does
// not come back as a member and inflate the quorum denominator forever.
//
// Mutation: restore every peer as SUSPECT unconditionally; Count() becomes 3.
func TestRemovedNodeStaysRemovedAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	first := registryAt(t, 1, "10.0.0.1:7100", dir)
	first.Add(&NodeState{NodeId: 2, Address: "10.0.0.2:7100", Status: NodeStatus_ALIVE})
	first.Add(&NodeState{NodeId: 3, Address: "10.0.0.3:7100", Status: NodeStatus_ALIVE})
	if err := first.MarkRemoved(3); err != nil {
		t.Fatalf("MarkRemoved: %v", err)
	}
	if got := first.Count(); got != 2 {
		t.Fatalf("fixture: membership after removal = %d, want 2", got)
	}

	restarted := registryAt(t, 1, "10.0.0.1:7100", dir)

	if got := restarted.Count(); got != 2 {
		t.Errorf("membership after restart = %d, want 2; a REMOVED peer must not count", got)
	}
	node, ok := restarted.Get(3)
	if !ok {
		t.Fatal("removed peer absent from the registry entirely; it must stay REMOVED, not vanish")
	}
	if node.Status != NodeStatus_REMOVED {
		t.Errorf("removed peer restored as %s, want REMOVED", node.Status)
	}
}

// TestMembershipPersistedOnEveryMembershipChange walks the mutators that change
// membership and asserts the snapshot on disk follows each one. It is the
// enumeration, made executable: the persist hook sits on
// updateClusterMetricsLocked, and this is what proves each operation reaches it.
//
// Mutation: move the persist call to TouchLastSeen (persist on heartbeat only).
// Every row fires.
func TestMembershipPersistedOnEveryMembershipChange(t *testing.T) {
	dir := t.TempDir()
	nr := registryAt(t, 1, "10.0.0.1:7100", dir)
	store := newMembershipStore(dir)

	countOnDisk := func(t *testing.T) int {
		t.Helper()
		snapshot, ok := store.load()
		if !ok {
			t.Fatal("no membership snapshot on disk")
		}
		counted := 0
		for _, n := range snapshot.Nodes {
			if NodeStatus(n.Status) != NodeStatus_REMOVED && NodeStatus(n.Status) != NodeStatus_LEAVING {
				counted++
			}
		}
		return counted
	}

	steps := []struct {
		name string
		op   func()
		want int
	}{
		{"Add peer 2", func() { nr.Add(&NodeState{NodeId: 2, Address: "a2", Status: NodeStatus_ALIVE}) }, 2},
		{"Add peer 3", func() { nr.Add(&NodeState{NodeId: 3, Address: "a3", Status: NodeStatus_ALIVE}) }, 3},
		{"MarkSuspect 2 (still a member)", func() { nr.MarkSuspect(2) }, 3},
		{"MarkDead 2 (still a member)", func() { nr.MarkDead(2) }, 3},
		{"MarkLeaving 3", func() { _ = nr.MarkLeaving(3) }, 2},
		{"MarkRemoved 2", func() { _ = nr.MarkRemoved(2) }, 1},
		{"Remove 3 entirely", func() { nr.Remove(3) }, 1},
	}

	observed := 0
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			step.op()
			if got := countOnDisk(t); got != step.want {
				t.Errorf("membership on disk = %d, want %d", got, step.want)
			}
		})
		observed++
	}
	// Guards the walk against silently skipping operations.
	if observed != len(steps) {
		t.Fatalf("walked %d operations, want %d", observed, len(steps))
	}
}

// TestHeartbeatDoesNotRewriteTheSnapshot pins that the per-heartbeat path stays
// off the persistence path: an fsync per heartbeat would put a disk write on
// gossip's hot path.
//
// Mutation: call persistMembershipLocked from TouchLastSeen, or drop the
// fingerprint guard so any mutator rewrites the file.
func TestHeartbeatDoesNotRewriteTheSnapshot(t *testing.T) {
	dir := t.TempDir()
	nr := registryAt(t, 1, "10.0.0.1:7100", dir)
	nr.Add(&NodeState{NodeId: 2, Address: "a2", Status: NodeStatus_ALIVE})

	path := filepath.Join(dir, membershipSnapshotFile)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("snapshot missing after Add: %v", err)
	}

	const heartbeats = 50
	for i := 0; i < heartbeats; i++ {
		nr.TouchLastSeen(2)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after heartbeats: %v", err)
	}
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Errorf("%d heartbeats rewrote the snapshot (mtime %v -> %v); persistence must not be on the heartbeat path",
			heartbeats, before.ModTime(), after.ModTime())
	}
}

// TestCorruptOrAbsentSnapshotStartsSelfOnly pins the failure path: a snapshot
// that cannot be read must leave a working self-only registry, never a panic
// and never a refusal to boot. The seed-node belt in the coordinator is what
// still protects a node in this state.
//
// Mutation: return an error from load() and let the constructor propagate it.
func TestCorruptOrAbsentSnapshotStartsSelfOnly(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		nr := registryAt(t, 1, "10.0.0.1:7100", t.TempDir())
		if got := nr.Count(); got != 1 {
			t.Errorf("membership = %d, want 1", got)
		}
	})

	t.Run("corrupt", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, membershipSnapshotFile), []byte("not msgpack at all"), 0o600); err != nil {
			t.Fatalf("writing corrupt snapshot: %v", err)
		}
		nr := registryAt(t, 1, "10.0.0.1:7100", dir)
		if got := nr.Count(); got != 1 {
			t.Errorf("membership = %d, want 1", got)
		}
		// And the node must still be able to learn membership afterwards.
		nr.Add(&NodeState{NodeId: 2, Address: "a2", Status: NodeStatus_ALIVE})
		if got := nr.Count(); got != 2 {
			t.Errorf("membership after Add = %d, want 2", got)
		}
	})

	t.Run("no data directory disables persistence", func(t *testing.T) {
		nr := NewNodeRegistry(1, "10.0.0.1:7100")
		nr.Add(&NodeState{NodeId: 2, Address: "a2", Status: NodeStatus_ALIVE})
		if got := nr.Count(); got != 2 {
			t.Errorf("membership = %d, want 2", got)
		}
	})
}

// TestAddressAndIncarnationChangeIsPersisted covers the gossip update that
// changes a peer's address or incarnation without changing its status: a peer
// that restarted on a new address, or refuted a suspicion, while staying ALIVE.
//
// The higher-incarnation branch of Update replaces the whole record, but for a
// while it only reported a change when the STATUS changed, so these updates were
// applied in memory and never written. A restart then restored the stale address
// and the stale incarnation - and a stale incarnation is worse than a stale
// address, because SWIM compares incarnations to decide who wins, so an older
// rumour could overwrite the record.
//
// Mutation: gate the persist hook on the status change alone again (drop
// recordReplaced from the condition in Update). Every assertion below fires.
func TestAddressAndIncarnationChangeIsPersisted(t *testing.T) {
	dir := t.TempDir()
	nr := registryAt(t, 1, "10.0.0.1:7100", dir)
	nr.Add(&NodeState{NodeId: 2, Address: "10.0.0.2:7100", Status: NodeStatus_ALIVE, Incarnation: 1})

	// Same status, new address, higher incarnation.
	nr.Update(&NodeState{NodeId: 2, Address: "10.0.0.99:7100", Status: NodeStatus_ALIVE, Incarnation: 5})

	// Precondition: the update was accepted in memory. If it was not, the
	// on-disk assertions below would pass for the wrong reason.
	inMemory, ok := nr.Get(2)
	if !ok {
		t.Fatal("peer 2 missing from the registry")
	}
	if inMemory.Address != "10.0.0.99:7100" || inMemory.Incarnation != 5 {
		t.Fatalf("fixture: in-memory record is %s/%d, want 10.0.0.99:7100/5",
			inMemory.Address, inMemory.Incarnation)
	}

	// On disk.
	snapshot, loaded := newMembershipStore(dir).load()
	if !loaded {
		t.Fatal("no membership snapshot on disk")
	}
	var found bool
	for _, record := range snapshot.Nodes {
		if record.NodeID != 2 {
			continue
		}
		found = true
		if record.Address != "10.0.0.99:7100" {
			t.Errorf("snapshot address = %q, want 10.0.0.99:7100", record.Address)
		}
		if record.Incarnation != 5 {
			t.Errorf("snapshot incarnation = %d, want 5", record.Incarnation)
		}
	}
	if !found {
		t.Fatal("peer 2 absent from the snapshot")
	}

	// And through a restart, which is what the snapshot exists for.
	restarted := registryAt(t, 1, "10.0.0.1:7100", dir)
	restoredPeer, ok := restarted.Get(2)
	if !ok {
		t.Fatal("peer 2 missing after restart")
	}
	if restoredPeer.Address != "10.0.0.99:7100" {
		t.Errorf("restored address = %q, want 10.0.0.99:7100", restoredPeer.Address)
	}
	if restoredPeer.Incarnation != 5 {
		t.Errorf("restored incarnation = %d, want 5; a stale incarnation lets an older rumour win", restoredPeer.Incarnation)
	}
}
