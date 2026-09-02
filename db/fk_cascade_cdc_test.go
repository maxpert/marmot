//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFKCascadeCDC_ParentDeleteReplicatesAndConverges is an integration-style
// regression test for the audit's code-read finding that ON DELETE CASCADE
// child deletes are captured as separate, seq-ordered CDC entries and that
// replaying them on a replica converges correctly (the replica's own FK
// cascade, if enabled, would re-delete an already-absent child harmlessly via
// ApplyCDCDelete's now-tolerant zero-rows-matched handling - see
// TestApplyCDCDelete_NoRowsMatched).
//
// SQLite foreign key enforcement is off by default per connection; it is
// turned on here explicitly on the single-connection hook pool (mirroring
// how a deployment that wants cascade behavior would configure it) rather
// than changed globally, since Marmot's default DSN does not enable it.
func TestFKCascadeCDC_ParentDeleteReplicatesAndConverges(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `
		CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, label TEXT,
			FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE);
	`
	_, err := source.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, source.ReloadSchema())

	_, err = replica.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, replica.ReloadSchema())

	// Enable FK enforcement on the hook connection (single-conn pool, so this
	// pragma sticks for every statement the capture session executes).
	_, err = source.hookDB.Exec(`PRAGMA foreign_keys = ON`)
	require.NoError(t, err)

	// Seed one parent with two children on the source, captured and applied
	// to the replica first so both sides start in the same state.
	seed := captureEntries(t, source, 7001,
		`INSERT INTO parent (id, name) VALUES (1, 'acme')`,
		`INSERT INTO child (id, parent_id, label) VALUES (10, 1, 'a')`,
		`INSERT INTO child (id, parent_id, label) VALUES (11, 1, 'b')`,
	)
	require.Len(t, seed, 3)
	applyEntries(t, replica, seed)

	var childCount int
	require.NoError(t, replica.GetWriteDB().QueryRow(`SELECT COUNT(*) FROM child`).Scan(&childCount))
	require.Equal(t, 2, childCount, "seed data must land on the replica before the cascade test")

	// Deleting the parent must cascade-delete both children on the source,
	// and the preupdate hook must capture ALL of it (parent + both children)
	// as separate CDC entries in one transaction.
	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 7002)
	require.NoError(t, err)
	defer session.Rollback()
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx, `DELETE FROM parent WHERE id = 1`)
	require.NoError(t, err)
	cascadeEntries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())

	require.Len(t, cascadeEntries, 3, "the parent delete plus both FK-cascaded child deletes must all be captured")

	deletedTables := map[string]int{}
	for _, e := range cascadeEntries {
		require.Equal(t, uint8(OpTypeDelete), e.Operation, "every captured row in this transaction must be a DELETE")
		deletedTables[e.Table]++
	}
	require.Equal(t, 1, deletedTables["parent"])
	require.Equal(t, 2, deletedTables["child"])

	// Apply the cascade entries to the replica in the same seq order they
	// were captured in - this is what real replication does.
	applyEntries(t, replica, cascadeEntries)

	var parentCount, childCountAfter int
	require.NoError(t, replica.GetWriteDB().QueryRow(`SELECT COUNT(*) FROM parent`).Scan(&parentCount))
	require.NoError(t, replica.GetWriteDB().QueryRow(`SELECT COUNT(*) FROM child`).Scan(&childCountAfter))
	require.Equal(t, 0, parentCount, "replica must converge: parent gone")
	require.Equal(t, 0, childCountAfter, "replica must converge: both children gone")
}

// TestFKCascadeCDC_ReplicaOwnCascadeThenRedundantDeleteNoOps covers the other
// half of the audit's finding: when the REPLICA also enforces FK cascade, its
// own local cascade removes the children as soon as the parent-delete CDC
// entry is applied - so by the time the source's explicit per-child DELETE
// CDC entries are applied afterward, those rows are already gone. Applying
// them must be a harmless no-op (ApplyCDCDelete tolerates zero rows matched),
// not a replication failure.
func TestFKCascadeCDC_ReplicaOwnCascadeThenRedundantDeleteNoOps(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `
		CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, label TEXT,
			FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE);
	`
	_, err := source.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, source.ReloadSchema())
	_, err = replica.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, replica.ReloadSchema())

	_, err = source.hookDB.Exec(`PRAGMA foreign_keys = ON`)
	require.NoError(t, err)
	// The replica enforces FK cascade too, on both connections it applies
	// CDC through and reads back from.
	_, err = replica.GetWriteDB().Exec(`PRAGMA foreign_keys = ON`)
	require.NoError(t, err)

	seed := captureEntries(t, source, 7101,
		`INSERT INTO parent (id, name) VALUES (1, 'acme')`,
		`INSERT INTO child (id, parent_id, label) VALUES (10, 1, 'a')`,
	)
	require.Len(t, seed, 2)
	applyEntries(t, replica, seed)

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 7102)
	require.NoError(t, err)
	defer session.Rollback()
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx, `DELETE FROM parent WHERE id = 1`)
	require.NoError(t, err)
	cascadeEntries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	require.Len(t, cascadeEntries, 2, "parent delete plus the cascaded child delete")

	// Apply the parent delete first: the replica's own FK cascade removes
	// the child as a side effect, before the child's own CDC delete entry
	// (captured on the source) is ever applied.
	var parentEntry, childEntry *IntentEntry
	for _, e := range cascadeEntries {
		if e.Table == "parent" {
			parentEntry = e
		} else {
			childEntry = e
		}
	}
	require.NotNil(t, parentEntry)
	require.NotNil(t, childEntry)

	require.NoError(t, ApplyCDCEntry(replica.GetWriteDB(), &schemaCacheAdapter{cache: replica.schemaCache}, parentEntry))

	var childCountAfterParentDelete int
	require.NoError(t, replica.GetWriteDB().QueryRow(`SELECT COUNT(*) FROM child`).Scan(&childCountAfterParentDelete))
	require.Equal(t, 0, childCountAfterParentDelete, "replica's own FK cascade must have already removed the child")

	// Now apply the source's redundant, explicit child-delete CDC entry:
	// must be a harmless no-op, not an error.
	require.NoError(t, ApplyCDCEntry(replica.GetWriteDB(), &schemaCacheAdapter{cache: replica.schemaCache}, childEntry))

	var finalParentCount, finalChildCount int
	require.NoError(t, replica.GetWriteDB().QueryRow(`SELECT COUNT(*) FROM parent`).Scan(&finalParentCount))
	require.NoError(t, replica.GetWriteDB().QueryRow(`SELECT COUNT(*) FROM child`).Scan(&finalChildCount))
	require.Equal(t, 0, finalParentCount)
	require.Equal(t, 0, finalChildCount)
}
