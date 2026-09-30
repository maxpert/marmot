package grpc

import (
	"sync/atomic"
	"testing"
)

// leavingWatch counts how often the registry asks this node to shut down.
func leavingWatch(nr *NodeRegistry) *atomic.Int32 {
	var fired atomic.Int32
	nr.SetOnNodeLeaving(func() { fired.Add(1) })
	return &fired
}

// TestGracefulStopThenRestartStaysUp: a node that stopped gracefully announced
// itself LEAVING, and its peers keep that record. When it restarts, that record
// is its own previous departure, not an admin decommission, and must not shut
// the new process down.
func TestGracefulStopThenRestartStaysUp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	first := registryAt(t, 3, "10.0.0.3:7100", dir)
	first.Add(&NodeState{NodeId: 1, Address: "10.0.0.1:7100", Status: NodeStatus_ALIVE, Incarnation: 2})
	first.MarkAlive(3)
	if err := first.MarkSelfLeaving(); err != nil {
		t.Fatalf("MarkSelfLeaving: %v", err)
	}
	departed, _ := first.Get(3)

	restarted := registryAt(t, 3, "10.0.0.3:7100", dir)
	fired := leavingWatch(restarted)
	// A node the startup pull finds current keeps its status, so this restart
	// is ALIVE from the first gossip it hears. The peers' record of it is the
	// departure the previous process gossiped.
	restarted.Update(&NodeState{NodeId: 3, Status: NodeStatus_LEAVING, Incarnation: departed.Incarnation})

	self, _ := restarted.Get(3)
	if self.Status != NodeStatus_ALIVE || fired.Load() != 0 {
		t.Fatalf("restarted node took its own departure as a decommission: status=%v shutdowns=%d",
			self.Status, fired.Load())
	}
	if self.Incarnation <= departed.Incarnation {
		t.Fatalf("restarted incarnation %d does not supersede the departure at %d",
			self.Incarnation, departed.Incarnation)
	}
}

// TestDecommissionWhileLocallyJoiningTakesEffect: the startup catch-up marks a
// node JOINING without an incarnation bump, so peers still see it ALIVE and an
// admin may decommission it. The node must accept that decommission.
func TestDecommissionWhileLocallyJoiningTakesEffect(t *testing.T) {
	t.Parallel()
	admin := NewNodeRegistry(1, "10.0.0.1:7100")
	node := NewNodeRegistry(3, "10.0.0.3:7100")
	fired := leavingWatch(node)

	gossiped, _ := node.Get(3)
	admin.Update(copyNodeState(gossiped))
	node.MarkJoining(3)
	if v, _ := admin.Get(3); v.Status != NodeStatus_ALIVE {
		t.Fatalf("fixture: admin view %v, want ALIVE", v.Status)
	}
	if err := admin.MarkLeaving(3); err != nil {
		t.Fatal(err)
	}
	leaving, _ := admin.Get(3)
	node.Update(copyNodeState(leaving))

	self, _ := node.Get(3)
	if self.Status != NodeStatus_LEAVING || fired.Load() != 1 {
		t.Fatalf("decommission refuted: status=%v@%d shutdowns=%d", self.Status, self.Incarnation, fired.Load())
	}
}

// TestOwnIncarnationBumpsSurviveRestart: a restart starts above every
// incarnation the previous run announced, including a SWIM refutation and a
// MarkAlive that left the status unchanged.
func TestOwnIncarnationBumpsSurviveRestart(t *testing.T) {
	t.Parallel()
	cases := map[string]func(nr *NodeRegistry){
		"refutation": func(nr *NodeRegistry) {
			nr.Update(&NodeState{NodeId: 3, Status: NodeStatus_DEAD, Incarnation: 5})
		},
		"mark-alive-while-alive": func(nr *NodeRegistry) { nr.MarkAlive(3) },
	}
	for name, bump := range cases {
		dir := t.TempDir()
		first := registryAt(t, 3, "10.0.0.3:7100", dir)
		bump(first)
		announced, _ := first.Get(3)

		restarted := registryAt(t, 3, "10.0.0.3:7100", dir)
		self, _ := restarted.Get(3)
		if self.Incarnation <= announced.Incarnation {
			t.Errorf("%s: restart at incarnation %d, not above the announced %d",
				name, self.Incarnation, announced.Incarnation)
		}
	}
}

// TestRemovedViewOfSelfIsNotRefuted: REMOVED is terminal. Refuting it would
// only climb this node's incarnation against a record every peer keeps sticky.
func TestRemovedViewOfSelfIsNotRefuted(t *testing.T) {
	t.Parallel()
	for _, joining := range []bool{false, true} {
		nr := NewNodeRegistry(3, "10.0.0.3:7100")
		if joining {
			nr.MarkJoining(3)
		}
		before, _ := nr.Get(3)

		nr.Update(&NodeState{NodeId: 3, Status: NodeStatus_REMOVED, Incarnation: before.Incarnation + 1})

		self, _ := nr.Get(3)
		if self.Incarnation != before.Incarnation {
			t.Fatalf("joining=%v: REMOVED view refuted, incarnation %d -> %d",
				joining, before.Incarnation, self.Incarnation)
		}
	}
}

// TestLiveAdminDecommissionStillTakesEffect: a peer's admin decommission of this
// node while it is ALIVE marks it LEAVING at the node's incarnation + 1; the node
// accepts it and starts shutting down, also after a restart.
func TestLiveAdminDecommissionStillTakesEffect(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	first := registryAt(t, 3, "10.0.0.3:7100", dir)
	first.MarkAlive(3)
	if err := first.MarkSelfLeaving(); err != nil {
		t.Fatalf("MarkSelfLeaving: %v", err)
	}

	nr := registryAt(t, 3, "10.0.0.3:7100", dir)
	fired := leavingWatch(nr)
	nr.MarkJoining(3)
	nr.MarkAlive(3)
	live, _ := nr.Get(3)

	nr.Update(&NodeState{NodeId: 3, Status: NodeStatus_LEAVING, Incarnation: live.Incarnation + 1})

	self, _ := nr.Get(3)
	if self.Status != NodeStatus_LEAVING || fired.Load() != 1 {
		t.Fatalf("live decommission not accepted: status=%v shutdowns=%d", self.Status, fired.Load())
	}
}
