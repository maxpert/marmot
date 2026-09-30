package test

import "testing"

// TestSchemaVersionSurvivesKillNine: a node applies DDL, is SIGKILLed and
// restarts with its tables intact. It must also keep its schema version: a
// node reporting a lower version than the schema it runs refuses every peer
// transaction that requires the newer one, and nothing but a snapshot
// restore heals it. With node 3 killed, node 2's write needs node 1's vote,
// so it commits only if node 1 kept its version.
//
// Mutation: write the schema version with pebble.NoSync
// (PebbleMetaStore.UpdateSchemaVersion). Node 1 restarts at version 0 and
// node 2's write never commits.
func TestSchemaVersionSurvivesKillNine(t *testing.T) {
	c := startNarrowCluster(t, "sv", "CREATE TABLE sv (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	c.createTable(1, "marmot", "probe", "CREATE TABLE probe (k INT PRIMARY KEY, v TEXT)")
	c.kill(1)
	c.restart(1)
	c.kill(3)
	c.execAcrossReconnect(2, "marmot", "REPLACE INTO probe (k, v) VALUES (2, 'after-kill')")
	c.waitRows("marmot", "SELECT k, v FROM probe", []string{"2|after-kill"}, 1, 2)
}
