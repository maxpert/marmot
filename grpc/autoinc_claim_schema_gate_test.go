package grpc

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// gateNode is one participant of an in-process cluster whose remote
// participants are reached through their real ReplicationHandler, schema
// version check included.
type gateNode struct {
	id       uint64
	clock    *hlc.Clock
	dm       *db.DatabaseManager
	versions *db.SchemaVersionManager
	handler  *ReplicationHandler
	wc       *coordinator.WriteCoordinator
}

// handlerFanout delivers every remote request to the target node's
// ReplicationHandler, converted exactly as GRPCReplicator converts it.
type handlerFanout struct {
	mu          sync.Mutex
	nodes       map[uint64]*gateNode
	unreachable map[uint64]bool
}

func (f *handlerFanout) setUnreachable(id uint64, unreachable bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unreachable[id] = unreachable
}

func (f *handlerFanout) ReplicateTransaction(ctx context.Context, nodeID uint64, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	f.mu.Lock()
	node, unreachable := f.nodes[nodeID], f.unreachable[nodeID]
	f.mu.Unlock()
	if unreachable {
		return nil, fmt.Errorf("node %d unreachable", nodeID)
	}
	grpcReq, err := transactionRequestToProto(req)
	if err != nil {
		return nil, err
	}
	resp, err := node.handler.HandleReplicateTransaction(ctx, grpcReq)
	if err != nil {
		return nil, err
	}
	return convertTransactionResponse(resp), nil
}

// gateNodeProvider is a fixed membership of every node.
type gateNodeProvider struct{ nodes []uint64 }

func (p gateNodeProvider) GetAliveNodes() ([]uint64, error) { return p.nodes, nil }
func (p gateNodeProvider) GetClusterSize() int              { return len(p.nodes) }
func (p gateNodeProvider) GetTotalMembershipSize() int      { return len(p.nodes) }
func (p gateNodeProvider) HasSeedNodes() bool               { return false }

func newGateCluster(t *testing.T, ids []uint64) (*handlerFanout, map[uint64]*gateNode) {
	t.Helper()
	fanout := &handlerFanout{nodes: map[uint64]*gateNode{}, unreachable: map[uint64]bool{}}
	provider := gateNodeProvider{nodes: ids}
	for _, id := range ids {
		clock := hlc.NewClock(id)
		dm, err := db.NewDatabaseManager(t.TempDir(), id, clock)
		require.NoError(t, err)
		t.Cleanup(func() { dm.Close() })
		// A new cluster: every node is held, and all of them release.
		require.NoError(t, dm.MergeAutoIncBasesAndReleaseVotes(nil))
		dm.SetClusterMembership(provider.GetTotalMembershipSize)
		require.NoError(t, dm.CreateDatabase("gate"))
		systemDB, err := dm.GetDatabase(db.SystemDatabaseName)
		require.NoError(t, err)
		versions := db.NewSchemaVersionManager(systemDB.GetMetaStore())
		wc := coordinator.NewWriteCoordinator(id, provider, fanout, db.NewLocalReplicator(id, dm, clock), 30*time.Second, clock)
		wc.SetSchemaVersionSource(versions.GetSchemaVersion)
		fanout.nodes[id] = &gateNode{id: id, clock: clock, dm: dm, versions: versions,
			handler: NewReplicationHandler(id, dm, clock, versions), wc: wc}
	}
	return fanout, fanout.nodes
}

// gateDDL replicates ddl from node through 2PC and bumps that node's own
// schema version, as CoordinatorHandler does after a DDL commit; remote
// participants bump theirs when they apply the COMMIT.
func gateDDL(t *testing.T, node *gateNode, table, ddl string) {
	t.Helper()
	startTS := node.clock.Now()
	txn := &coordinator.Transaction{
		ID: startTS.ToTxnID(), NodeID: node.id, StartTS: startTS, Database: "gate",
		WriteConsistency: protocol.ConsistencyQuorum,
		Statements:       []protocol.Statement{{Type: protocol.StatementDDL, SQL: ddl, TableName: table, Database: "gate"}},
	}
	require.NoError(t, node.wc.WriteTransaction(context.Background(), txn), ddl)
	_, err := node.versions.IncrementSchemaVersion("gate", ddl, txn.ID)
	require.NoError(t, err)
}

// waitGate polls until check passes, failing after a deadline: a commit
// returns once a quorum applied it, so the last participant may lag.
func waitGate(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (n *gateNode) version(t *testing.T) uint64 {
	v, err := n.versions.GetSchemaVersion("gate")
	require.NoError(t, err)
	return v
}

func (n *gateNode) base(table string) (uint64, error) {
	return db.NewAutoIncClaimStore(n.dm.GetSystemDatabase()).ReadBase("gate", table)
}

// TestClaimFromALaggingVoterIsDeclined is R3c-12's counterexample, driven
// through the real DDL apply path and every participant's ReplicationHandler.
//
// Nodes g=1, h=2, k=3. A claim on t2, (0,64], commits on {k,g} while h is
// away, and row 50 lands in t2 on k and g. While g is away, k replicates
// DROP TABLE t and RENAME t2 TO t to {k,h}: k's t base inherits 64, h's
// stays 0 (h missed the claim and holds no row). g still has the old t, base
// 0. When h claims t from base 0, {h,g} is a majority that would grant (0,64]
// again, over row 50 that the renamed table holds on k and g. The claim
// carries h's schema version, so g, which has not applied the two DDLs,
// declines; k refuses with its base 64; h's claim lands above it.
//
// Mutation: claims carry schema version 0 (claimSchemaVersion returns 0).
// g votes with the old incarnation's base and "a claim from a lagging voter's
// base overlapped the renamed rows" fires.
func TestClaimFromALaggingVoterIsDeclined(t *testing.T) {
	fanout, nodes := newGateCluster(t, []uint64{1, 2, 3})
	g, h, k := nodes[1], nodes[2], nodes[3]
	ctx := context.Background()
	size := func(uint64) (uint64, error) { return 64, nil }

	gateDDL(t, k, "t", "CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	gateDDL(t, k, "t2", "CREATE TABLE t2 (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	for _, n := range []*gateNode{g, h, k} {
		waitGate(t, fmt.Sprintf("node %d at schema version 2", n.id), func() bool { return n.version(t) == 2 })
	}

	fanout.setUnreachable(h.id, true)
	cBase, cSize, err := k.wc.ClaimRange(ctx, "gate", "t2", 0, size)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cBase)
	for _, n := range []*gateNode{g, k} {
		waitGate(t, fmt.Sprintf("node %d t2 base 64", n.id), func() bool { b, err := n.base("t2"); return err == nil && b == cBase+cSize })
		mdb, err := n.dm.GetDatabase("gate")
		require.NoError(t, err)
		_, err = mdb.GetWriteDB().Exec("INSERT INTO t2 (id, v) VALUES (50, 'moved')")
		require.NoError(t, err)
	}
	fanout.setUnreachable(h.id, false)

	fanout.setUnreachable(g.id, true)
	gateDDL(t, k, "t", "DROP TABLE t")
	gateDDL(t, k, "t2", "ALTER TABLE t2 RENAME TO t")
	waitGate(t, "node 2 at schema version 4", func() bool { return h.version(t) == 4 })
	fanout.setUnreachable(g.id, false)

	hBase, err := h.base("t")
	require.NoError(t, err)
	require.Equal(t, uint64(0), hBase, "h must hold the low base for the sequence to mean anything")
	gBase, err := g.base("t")
	require.NoError(t, err)
	require.Equal(t, uint64(0), gBase, "g must hold the old incarnation's base")
	require.Equal(t, uint64(2), g.version(t), "g must lag the two DDLs")

	dBase, dSize, err := h.wc.ClaimRange(ctx, "gate", "t", 0, size)
	require.NoError(t, err)
	require.True(t, dBase >= 50 || dBase+dSize < 50,
		"a claim from a lagging voter's base overlapped the renamed rows: granted (%d,%d] over id 50", dBase, dBase+dSize)
}
