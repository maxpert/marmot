//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClaimRange_JoinersHoldVotesUntilMerged is the regression for the
// membership-growth overlap (step 3c, D8 F1).
//
// Claim C = [0,64) commits on {n1, n2} while n3 is unreachable, so n3 still
// holds base 0. Two nodes then join by catching up; claims never enter CDC,
// so each holds only the DDL-time seed, base 0. The membership is now five
// and a quorum is three: {n3, n4, n5} is a majority that shares no node with
// C's. Before joiners held their votes, n3 proposing base 0 was granted
// [0,64) a second time. With the hold, n4 and n5 decline until they have
// merged bases from a majority, so that majority cannot form, and n3 is
// taught C's end by n1 and n2 and granted a disjoint range.
//
// Mutation: ignore the vote hold in prepareAutoIncClaim. n3's first round is
// then granted at base 0 and "a claim after the membership grew overlapped
// C" fires.
func TestClaimRange_JoinersHoldVotesUntilMerged(t *testing.T) {
	const database, table = "testdb", "grow"
	c := setupClaimCluster(t, database, table)

	c.fanout.setUnreachable(3, true)
	cBase, cSize, err := c.nodes[1].wc.ClaimRange(context.Background(), database, table, 0, fixedClaimSize(64))
	require.NoError(t, err)
	require.Equal(t, uint64(0), cBase)
	waitForQuorumBase(t, []*inprocNode{c.nodes[1], c.nodes[2]}, database, table, 64, 2)
	c.fanout.setUnreachable(3, false)
	base3, err := c.nodes[3].claimStore().ReadBase(database, table)
	require.NoError(t, err)
	require.Equal(t, uint64(0), base3, "n3 must have missed C for the sequence to mean anything")

	for _, id := range []uint64{4, 5} {
		joiner := c.buildNode(id, freshDir(t))
		c.nodes[id] = joiner
		require.NoError(t, joiner.dm.CreateDatabase(database))
		mdb, err := joiner.dm.GetDatabase(database)
		require.NoError(t, err)
		_, err = mdb.GetWriteDB().Exec(sprintfDDL(table))
		require.NoError(t, err)
		require.NoError(t, mdb.ReloadSchema())
		// What replaying the table's CREATE seeds on a joiner.
		require.NoError(t, joiner.claimStore().Seed(database, table, 0, id))
	}
	c.provider.setNodes([]uint64{1, 2, 3, 4, 5})

	mark4 := c.fanout.prepareCallCount(4)
	dBase, dSize, err := c.nodes[3].wc.ClaimRange(context.Background(), database, table, 0, fixedClaimSize(64))
	require.NoError(t, err)
	require.True(t, rangesDisjoint(cBase, cSize, dBase, dSize),
		"a claim after the membership grew overlapped C: C=[%d,%d) D=[%d,%d)", cBase, cBase+cSize, dBase, dBase+dSize)

	round1Node4 := c.fanout.prepareCallsSince(4, mark4)
	require.NotEmpty(t, round1Node4)
	require.False(t, round1Node4[0].resp.Success, "a joiner voted before merging claim bases")
	require.False(t, round1Node4[0].resp.Rejected, "a held joiner must decline, not cast a verdict")
}

// TestClaimRange_RebuiltNodeHoldsVotesUntilMerged is reviewer B's C2 (a): a
// seed node configured with no seeds loses its data directory and restarts.
// Its hold no longer depends on seeds or on a catch-up decision: any start
// that initialises the system database holds.
//
// C = [0,64) commits on {n1, n2} while n3 is unreachable. n1 is rebuilt from
// an empty directory, and its user table comes back from n3 (which missed C)
// with ids up to 10, so its DDL-time seed is 10. Unheld, n1 would join n3 in
// granting a range starting at 10 - {n1, n3} is a majority of three - and
// overlap C. Held, it declines, and n3 is taught C's end by n2.
//
// Mutation: never place the hold in createAutoIncTables. n1 votes, the claim
// is granted at 10 and "a claim after n1 was rebuilt overlapped C" fires.
func TestClaimRange_RebuiltNodeHoldsVotesUntilMerged(t *testing.T) {
	const database, table = "testdb", "rebuilt"
	c := setupClaimCluster(t, database, table)

	c.fanout.setUnreachable(3, true)
	cBase, cSize, err := c.nodes[1].wc.ClaimRange(context.Background(), database, table, 0, fixedClaimSize(64))
	require.NoError(t, err)
	waitForQuorumBase(t, []*inprocNode{c.nodes[1], c.nodes[2]}, database, table, 64, 2)
	c.fanout.setUnreachable(3, false)

	c.closeNode(1)
	rebuilt := c.buildNode(1, freshDir(t))
	c.nodes[1] = rebuilt
	require.NoError(t, rebuilt.dm.CreateDatabase(database))
	mdb, err := rebuilt.dm.GetDatabase(database)
	require.NoError(t, err)
	_, err = mdb.GetWriteDB().Exec(sprintfDDL(table))
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())
	// What the restored table's MAX(id) seeds on the rebuilt node.
	require.NoError(t, rebuilt.claimStore().Seed(database, table, 10, 1))

	dBase, dSize, err := c.nodes[3].wc.ClaimRange(context.Background(), database, table, 10, fixedClaimSize(64))
	require.NoError(t, err)
	require.True(t, rangesDisjoint(cBase, cSize, dBase, dSize),
		"a claim after n1 was rebuilt overlapped C: C=[%d,%d) D=[%d,%d)", cBase, cBase+cSize, dBase, dBase+dSize)
}
