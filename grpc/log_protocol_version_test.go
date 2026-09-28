package grpc

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNodeRegistry_SelfEntryReportsLogPullProtocolVersion verifies a freshly
// constructed registry's own node state advertises LogPullProtocolVersion, and
// that GetAll (what gossip actually sends) carries it through copyNodeState.
// Without setting the field on the self entry, or without copying it in
// copyNodeState, every peer would observe this node as version 0 - the same
// as an older release that does not serve the commit-log pull protocol - and
// the rolling-upgrade gate would refuse DDL forever even on a fully upgraded
// cluster.
func TestNodeRegistry_SelfEntryReportsLogPullProtocolVersion(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	self, ok := nr.Get(1)
	if !ok {
		t.Fatal("expected self entry in registry")
	}
	if self.LogProtocolVersion != LogPullProtocolVersion {
		t.Errorf("Get(self).LogProtocolVersion = %d, want %d", self.LogProtocolVersion, LogPullProtocolVersion)
	}

	all := nr.GetAll()
	if len(all) != 1 {
		t.Fatalf("expected 1 node, got %d", len(all))
	}
	if all[0].LogProtocolVersion != LogPullProtocolVersion {
		t.Errorf("GetAll()[0].LogProtocolVersion = %d, want %d (gossip would send a stale version)", all[0].LogProtocolVersion, LogPullProtocolVersion)
	}
}

// TestNodeRegistry_UpdateNeverDowngradesLogProtocolVersion_SameIncarnation
// pins that a relayed same-incarnation gossip copy of a node's
// own state can lack the version an earlier copy carried (for example a
// seed's Join response, which builds a bare NodeState for the joining node).
// Update() must take the max for the same incarnation, never overwrite a
// known version with a lower or missing one.
func TestNodeRegistry_UpdateNeverDowngradesLogProtocolVersion_SameIncarnation(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	nr.Add(&NodeState{NodeId: 2, Address: "node2:8080", Status: NodeStatus_ALIVE, Incarnation: 5, LogProtocolVersion: LogPullProtocolVersion})

	// A same-incarnation relay that lacks the field (zero value) must not
	// downgrade the known version.
	nr.Update(&NodeState{NodeId: 2, Address: "node2:8080", Status: NodeStatus_ALIVE, Incarnation: 5, LogProtocolVersion: 0})

	got, ok := nr.Get(2)
	if !ok {
		t.Fatal("expected node 2 in registry")
	}
	if got.LogProtocolVersion != LogPullProtocolVersion {
		t.Errorf("LogProtocolVersion downgraded: got %d, want %d", got.LogProtocolVersion, LogPullProtocolVersion)
	}
}

// TestNodeRegistry_UpdateTakesMaxLogProtocolVersion_SameIncarnation covers the
// other half: a same-incarnation update carrying a HIGHER version than what
// is on record must raise it (the max, not "first write wins").
func TestNodeRegistry_UpdateTakesMaxLogProtocolVersion_SameIncarnation(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")

	nr.Add(&NodeState{NodeId: 2, Address: "node2:8080", Status: NodeStatus_ALIVE, Incarnation: 5, LogProtocolVersion: 0})
	nr.Update(&NodeState{NodeId: 2, Address: "node2:8080", Status: NodeStatus_ALIVE, Incarnation: 5, LogProtocolVersion: LogPullProtocolVersion})

	got, ok := nr.Get(2)
	if !ok {
		t.Fatal("expected node 2 in registry")
	}
	if got.LogProtocolVersion != LogPullProtocolVersion {
		t.Errorf("LogProtocolVersion not raised: got %d, want %d", got.LogProtocolVersion, LogPullProtocolVersion)
	}
}

// TestNodeRegistry_LegacyLogProtocolMembers verifies the membership scan the
// coordinator and PREPARE handler gate on: every member except REMOVED,
// self included, whose LogProtocolVersion is below LogPullProtocolVersion,
// sorted ascending.
func TestNodeRegistry_LegacyLogProtocolMembers(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081") // self: LogPullProtocolVersion

	nr.Add(&NodeState{NodeId: 4, Address: "n4", Status: NodeStatus_ALIVE, LogProtocolVersion: LogPullProtocolVersion})
	nr.Add(&NodeState{NodeId: 3, Address: "n3", Status: NodeStatus_SUSPECT, LogProtocolVersion: 0}) // older release
	nr.Add(&NodeState{NodeId: 2, Address: "n2", Status: NodeStatus_ALIVE, LogProtocolVersion: 0})   // older release
	nr.Add(&NodeState{NodeId: 5, Address: "n5", Status: NodeStatus_REMOVED, LogProtocolVersion: 0}) // excluded: REMOVED

	got := nr.LegacyLogProtocolMembers()
	want := []uint64{2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LegacyLogProtocolMembers() = %v, want %v", got, want)
	}
}

// TestNodeRegistry_LegacyLogProtocolMembers_EmptyWhenEveryMemberIsCurrent verifies the
// all-upgraded cluster reports no legacy members, so DDL is not refused.
func TestNodeRegistry_LegacyLogProtocolMembers_EmptyWhenEveryMemberIsCurrent(t *testing.T) {
	nr := NewNodeRegistry(1, "localhost:8081")
	nr.Add(&NodeState{NodeId: 2, Address: "n2", Status: NodeStatus_ALIVE, LogProtocolVersion: LogPullProtocolVersion})

	got := nr.LegacyLogProtocolMembers()
	if len(got) != 0 {
		t.Errorf("LegacyLogProtocolMembers() = %v, want empty", got)
	}
}

// TestNodeRegistry_RefutesStaleLogProtocolVersionOfSelf pins the rolling
// upgrade's last step: a node restarted onto the new release keeps its old
// incarnation, while its peers may still hold their view of it from before
// the upgrade at a higher incarnation. Such a view must be refuted with a
// higher incarnation still - SWIM never lets a lower incarnation replace it
// - or every peer keeps treating the node as not yet upgraded and refusing
// DDL forever.
func TestNodeRegistry_RefutesStaleLogProtocolVersionOfSelf(t *testing.T) {
	upgraded := NewNodeRegistry(3, "localhost:8083")
	upgraded.Update(&NodeState{NodeId: 3, Address: "localhost:8083", Status: NodeStatus_ALIVE, Incarnation: 4, LogProtocolVersion: 0})

	var self *NodeState
	for _, n := range upgraded.GetAll() {
		if n.NodeId == 3 {
			self = n
		}
	}
	require.NotNil(t, self)
	require.Greater(t, self.Incarnation, uint64(4), "a stale pre-upgrade view of self must be refuted with a higher incarnation")

	peer := NewNodeRegistry(1, "localhost:8081")
	peer.Update(&NodeState{NodeId: 3, Address: "localhost:8083", Status: NodeStatus_ALIVE, Incarnation: 4, LogProtocolVersion: 0})
	require.Equal(t, []uint64{3}, peer.LegacyLogProtocolMembers())
	peer.Update(self)
	require.Empty(t, peer.LegacyLogProtocolMembers(), "the refutation must replace the peer's pre-upgrade view")
}

// TestNodeRegistry_StrippedRelaysDoNotClimbIncarnation pins the bound on the
// version refutation: a peer on an older release relays this node's state
// with its log protocol version stripped, at this node's current
// incarnation, on every gossip round. Refuting each of those would climb the incarnation for as
// long as that peer runs; it must not move at all.
func TestNodeRegistry_StrippedRelaysDoNotClimbIncarnation(t *testing.T) {
	nr := NewNodeRegistry(3, "localhost:8083")
	self := func() *NodeState {
		for _, n := range nr.GetAll() {
			if n.NodeId == 3 {
				return n
			}
		}
		t.Fatal("self missing")
		return nil
	}
	start := self().Incarnation
	for i := 0; i < 100; i++ {
		cur := self()
		nr.Update(&NodeState{NodeId: 3, Address: cur.Address, Status: NodeStatus_ALIVE, Incarnation: cur.Incarnation})
	}
	require.Equal(t, start, self().Incarnation, "stripped relays at the current incarnation must not bump it")

	// One stale view from a higher incarnation is refuted exactly once, and
	// the stripped relays of the refuted incarnation that follow are not.
	nr.Update(&NodeState{NodeId: 3, Address: "localhost:8083", Status: NodeStatus_ALIVE, Incarnation: start + 5})
	refuted := self().Incarnation
	require.Equal(t, start+6, refuted)
	for i := 0; i < 100; i++ {
		nr.Update(&NodeState{NodeId: 3, Address: "localhost:8083", Status: NodeStatus_ALIVE, Incarnation: refuted})
	}
	require.Equal(t, refuted, self().Incarnation)
}
