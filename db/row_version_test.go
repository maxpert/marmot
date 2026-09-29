package db

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/stretchr/testify/require"
)

// docsKey is the intent key the CDC hook captures for docs.id = id.
func docsKey(id int64) []byte {
	return filter.EncodeIntentKey("docs", []filter.TypedPKValue{{Type: filter.PKTypeInt64, Value: filter.EncodeInt64(id)}})
}

func docsInsert(id int64, title string) *EncodedCapturedRow {
	return &EncodedCapturedRow{
		Table:     "docs",
		Op:        uint8(OpTypeInsert),
		IntentKey: docsKey(id),
		NewValues: encodeTestValues(map[string]interface{}{"id": id, "title": title}),
	}
}

func docsUpdate(id int64, oldTitle, newTitle string) *EncodedCapturedRow {
	return &EncodedCapturedRow{
		Table:     "docs",
		Op:        uint8(OpTypeUpdate),
		IntentKey: docsKey(id),
		OldValues: encodeTestValues(map[string]interface{}{"id": id, "title": oldTitle}),
		NewValues: encodeTestValues(map[string]interface{}{"id": id, "title": newTitle}),
	}
}

func docsDelete(id int64, title string) *EncodedCapturedRow {
	return &EncodedCapturedRow{
		Table:     "docs",
		Op:        uint8(OpTypeDelete),
		IntentKey: docsKey(id),
		OldValues: encodeTestValues(map[string]interface{}{"id": id, "title": title}),
	}
}

// replayAt applies rows as one pulled transaction committed at wall.
func replayAt(t *testing.T, mdb *ReplicatedDatabase, txnID uint64, wall int64, rows ...*EncodedCapturedRow) {
	t.Helper()
	applied, err := mdb.ApplyReplayedTxn(context.Background(), &ReplayTxn{
		TxnID:        txnID,
		OriginNodeID: 2,
		CommitTS:     hlc.Timestamp{WallTime: wall, NodeID: 2},
		Rows:         rows,
	}, true)
	require.NoError(t, err)
	require.True(t, applied, "txn %d must count as applied even when its rows lose", txnID)
}

// docsTitle returns docs.title for id, and whether the row exists.
func docsTitle(t *testing.T, mdb *ReplicatedDatabase, id int64) (string, bool) {
	t.Helper()
	var title string
	err := mdb.GetWriteDB().QueryRow("SELECT title FROM docs WHERE id = ?", id).Scan(&title)
	if err != nil {
		return "", false
	}
	return title, true
}

func TestReplayOlderUpdateAfterNewerKeepsNewerImage(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	replayAt(t, mdb, 1, 5, docsInsert(1, "v0"))
	replayAt(t, mdb, 3, 20, docsUpdate(1, "v1", "v2"))
	replayAt(t, mdb, 2, 10, docsUpdate(1, "v0", "v1"))

	title, ok := docsTitle(t, mdb, 1)
	require.True(t, ok)
	require.Equal(t, "v2", title)
}

func TestReplayOlderInsertDoesNotResurrectNewerDelete(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	replayAt(t, mdb, 5, 20, docsDelete(1, "v1"))
	replayAt(t, mdb, 4, 10, docsInsert(1, "v1"))

	_, ok := docsTitle(t, mdb, 1)
	require.False(t, ok, "an older INSERT must not resurrect a row a newer DELETE removed")
}

func TestReplayUpdateBeforeItsInsertEndsWithNewestImage(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)

	replayAt(t, mdb, 7, 20, docsUpdate(1, "v1", "v2"))
	replayAt(t, mdb, 6, 10, docsInsert(1, "v1"))

	title, ok := docsTitle(t, mdb, 1)
	require.True(t, ok)
	require.Equal(t, "v2", title)
}

// storedVersion reads the version table's row for (table, key).
func storedVersion(t *testing.T, mdb *ReplicatedDatabase, table string, key []byte) (hlc.Timestamp, bool, bool) {
	t.Helper()
	var v hlc.Timestamp
	var deleted bool
	err := mdb.GetWriteDB().QueryRow("SELECT wall, logical, node, deleted FROM __marmot_row_version WHERE tbl = ? AND pk = ?", table, key).
		Scan(&v.WallTime, &v.Logical, &v.NodeID, &deleted)
	if err == sql.ErrNoRows {
		return hlc.Timestamp{}, false, false
	}
	require.NoError(t, err)
	return v, deleted, true
}

func TestCommitStampsRowsWithTheCoordinatorsCommitTimestamp(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()
	txn := beginPreparedDMLTxn(t, tm, mdb.GetMetaStore(), docsInsert(1, "a"))

	decided := hlc.Timestamp{WallTime: time.Now().Add(time.Hour).UnixNano(), Logical: 3, NodeID: txn.NodeID}
	require.NoError(t, tm.CommitTransactionAfter(txn, decided, nil))
	require.True(t, hlc.After(tm.clock.Now(), decided), "a node that commits a transaction must order what it prepares next after it")

	got, deleted, ok := storedVersion(t, mdb, "docs", docsKey(1))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, decided, got)
	rec, err := mdb.GetMetaStore().GetTransaction(txn.ID)
	require.NoError(t, err)
	require.Equal(t, decided.WallTime, rec.CommitTSWall, "the log must serve the same timestamp to pullers")
	require.Equal(t, decided.Logical, rec.CommitTSLogical)
}

// Both commit paths - the batch committer and the direct one used when
// batching is off - version rows the same way.
func TestCommitOlderThanStoredVersionLeavesRowAlone(t *testing.T) {
	for _, batched := range []bool{true, false} {
		_, mdb, _ := newReplayTestDatabase(t)
		tm := mdb.GetTransactionManager()
		if !batched {
			tm.batchCommitter = nil
		}
		replayAt(t, mdb, 1, 50, docsInsert(1, "newer"))

		txn := beginPreparedDMLTxn(t, tm, mdb.GetMetaStore(), docsUpdate(1, "newer", "older"))
		require.NoError(t, tm.CommitTransactionAfter(txn, hlc.Timestamp{WallTime: 10, NodeID: txn.NodeID}, nil))

		title, ok := docsTitle(t, mdb, 1)
		require.True(t, ok)
		require.Equal(t, "newer", title, "batched=%v", batched)
	}
}

// The key computed from a row's decoded CDC values must equal the one the
// hook captured from the raw values, for every PK storage class.
func TestPKKeyFromValuesMatchesCapturedKey(t *testing.T) {
	cases := []struct {
		name string
		raw  interface{} // as the preupdate hook hands it over
		cdc  interface{} // as capture encodes it (TEXT becomes string)
	}{
		{"integer", int64(7), int64(7)},
		{"small integer", int64(1), int64(1)},
		{"negative integer", int64(-300), int64(-300)},
		{"text", []byte("abc"), "abc"},
		{"blob", []byte{0, 1, 2}, []byte{0, 1, 2}},
		{"real", 2.5, 2.5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			captured := filter.EncodeIntentKey("tb", []filter.TypedPKValue{valueToTypedPK(c.raw), valueToTypedPK(int64(9))})
			got, err := pkKeyFromValues("tb", []string{"a", "b"}, encodeTestValues(map[string]interface{}{"a": c.cdc}), encodeTestValues(map[string]interface{}{"b": int64(9)}))
			require.NoError(t, err)
			require.Equal(t, captured, got)
		})
	}
}

// An UPDATE that moves a row to a new key tombstones the old key, so an
// older change to the old key cannot bring the old row back.
func TestUpdateMovingPrimaryKeyTombstonesOldKey(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	replayAt(t, mdb, 1, 10, docsInsert(1, "a"))
	move := &EncodedCapturedRow{
		Table: "docs", Op: uint8(OpTypeUpdate), IntentKey: docsKey(2),
		OldValues: encodeTestValues(map[string]interface{}{"id": int64(1), "title": "a"}),
		NewValues: encodeTestValues(map[string]interface{}{"id": int64(2), "title": "a"}),
	}
	replayAt(t, mdb, 3, 30, move)
	replayAt(t, mdb, 2, 20, docsUpdate(1, "a", "stale"))

	_, ok := docsTitle(t, mdb, 1)
	require.False(t, ok, "an older change must not bring the moved-away row back")
	title, ok := docsTitle(t, mdb, 2)
	require.True(t, ok)
	require.Equal(t, "a", title)
	_, deleted, ok := storedVersion(t, mdb, "docs", docsKey(1))
	require.True(t, ok)
	require.True(t, deleted)
}

func versionCount(t *testing.T, mdb *ReplicatedDatabase, table string) int {
	t.Helper()
	var n int
	require.NoError(t, mdb.GetWriteDB().QueryRow("SELECT COUNT(*) FROM __marmot_row_version WHERE tbl = ?", table).Scan(&n))
	return n
}

func TestDropTableRemovesItsVersions(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	replayAt(t, mdb, 1, 10, docsInsert(1, "a"), docsInsert(2, "b"))
	require.Equal(t, 2, versionCount(t, mdb, "docs"))

	replayDDL(t, mdb, "docs", "DROP TABLE docs")
	require.Zero(t, versionCount(t, mdb, "docs"))
}

func TestRenameTableCarriesItsVersionsUnderTheNewKeys(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	replayAt(t, mdb, 1, 10, docsInsert(1, "a"))

	replayDDL(t, mdb, "docs", "ALTER TABLE docs RENAME TO papers")
	require.Zero(t, versionCount(t, mdb, "docs"))
	papersKey := filter.EncodeIntentKey("papers", []filter.TypedPKValue{{Type: filter.PKTypeInt64, Value: filter.EncodeInt64(1)}})
	got, deleted, ok := storedVersion(t, mdb, "papers", papersKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, hlc.Timestamp{WallTime: 10, NodeID: 2}, got)
}

// A statement that keeps a table but changes its primary-key columns leaves
// its stored keys naming nothing, so its versions are cleared.
func TestPrimaryKeyChangeClearsVersions(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	replayAt(t, mdb, 1, 10, docsInsert(1, "a"))
	tx, err := mdb.GetWriteDB().Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	before := map[string]string{"docs": "id\x00"}
	require.NoError(t, reconcileRowVersions(context.Background(), tx, before, map[string]string{"docs": "title\x00"}))
	var n int
	require.NoError(t, tx.QueryRow("SELECT COUNT(*) FROM __marmot_row_version WHERE tbl = 'docs'").Scan(&n))
	require.Zero(t, n)
}

func TestPurgeTombstonesDeletesOnlyThoseOlderThanEveryMembersRetention(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()
	now := time.Now()
	replayAt(t, mdb, 1, now.Add(-3*time.Hour).UnixNano(), docsDelete(1, "old"))
	replayAt(t, mdb, 2, now.Add(-time.Hour).UnixNano(), docsDelete(2, "recent"))
	replayAt(t, mdb, 3, now.Add(-3*time.Hour).UnixNano(), docsInsert(3, "live"))

	tm.gcMaxRetention = 0
	tm.SetMemberCountFunc(func() int { return 2 })
	purged, err := tm.purgeTombstones()
	require.NoError(t, err)
	require.Zero(t, purged, "unlimited retention bounds nothing, so every tombstone stays")

	tm.gcMaxRetention = time.Hour
	purged, err = tm.purgeTombstones()
	require.NoError(t, err)
	require.Equal(t, int64(1), purged)
	_, _, ok := storedVersion(t, mdb, "docs", docsKey(1))
	require.False(t, ok, "a tombstone older than 2 members x 1h retention goes")
	_, _, ok = storedVersion(t, mdb, "docs", docsKey(2))
	require.True(t, ok, "a younger tombstone stays")
	_, _, ok = storedVersion(t, mdb, "docs", docsKey(3))
	require.True(t, ok, "a live row's version is never purged")
}

// benchmarkApplyUpdate applies b.N updates of one row, each in its own
// transaction, through apply.
func benchmarkApplyUpdate(b *testing.B, apply func(tx *sql.Tx, schema CDCSchemaProvider, row *EncodedCapturedRow, version hlc.Timestamp) error) {
	dbMgr, err := NewDatabaseManager(b.TempDir(), 1, hlc.NewClock(1))
	require.NoError(b, err)
	defer dbMgr.Close()
	require.NoError(b, dbMgr.CreateDatabase("app"))
	mdb, err := dbMgr.GetDatabase("app")
	require.NoError(b, err)
	_, err = mdb.GetWriteDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT)`)
	require.NoError(b, err)
	require.NoError(b, mdb.ReloadSchema())
	_, err = mdb.GetWriteDB().Exec(`INSERT INTO docs VALUES (1, 'v')`)
	require.NoError(b, err)
	schema := &schemaCacheAdapter{cache: mdb.schemaCache}
	row := docsUpdate(1, "v", "w")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := mdb.GetWriteDB().Begin()
		require.NoError(b, err)
		require.NoError(b, apply(tx, schema, row, hlc.Timestamp{WallTime: int64(i + 1), NodeID: 1}))
		require.NoError(b, tx.Commit())
	}
}

func BenchmarkApplyUpdateVersioned(b *testing.B) {
	benchmarkApplyUpdate(b, func(tx *sql.Tx, schema CDCSchemaProvider, row *EncodedCapturedRow, version hlc.Timestamp) error {
		applier, err := newVersionedApplier(tx, schema)
		if err != nil {
			return err
		}
		defer applier.Close()
		_, err = applier.apply(OpType(row.Op), row.Table, row.IntentKey, row.OldValues, row.NewValues, version)
		return err
	})
}

func BenchmarkApplyUpdateUnversioned(b *testing.B) {
	benchmarkApplyUpdate(b, func(tx *sql.Tx, schema CDCSchemaProvider, row *EncodedCapturedRow, _ hlc.Timestamp) error {
		return ApplyCDCValues(tx, schema, OpType(row.Op), row.Table, row.OldValues, row.NewValues)
	})
}

// The coordinator's own node commits through LocalReplicator with the same
// decided commit timestamp it sends every remote participant.
func TestLocalReplicatorCommitsWithTheDecidedTimestamp(t *testing.T) {
	replicator, dm, cleanup := setupTestLocalReplicator(t)
	defer cleanup()
	require.NoError(t, dm.CreateDatabase("app"))
	mdb, err := dm.GetDatabase("app")
	require.NoError(t, err)
	_, err = mdb.GetWriteDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT)`)
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())

	ctx := context.Background()
	insert := testProtocolDMLStatement(protocol.StatementInsert, "app", "docs", docsKey(1), nil,
		encodeTestValues(map[string]interface{}{"id": int64(1), "title": "a"}))
	resp, err := replicator.ReplicateTransaction(ctx, 1, &coordinator.ReplicationRequest{
		TxnID: 9, NodeID: 1, Database: "app", Phase: coordinator.PhasePrep,
		StartTS: hlc.Timestamp{WallTime: 1, NodeID: 1}, Statements: []protocol.Statement{insert},
	})
	require.NoError(t, err)
	require.True(t, resp.Success, resp.Error)

	decided := hlc.Timestamp{WallTime: 888, Logical: 4, NodeID: 1}
	resp, err = replicator.ReplicateTransaction(ctx, 1, &coordinator.ReplicationRequest{
		TxnID: 9, NodeID: 1, Database: "app", Phase: coordinator.PhaseCommit,
		StartTS: hlc.Timestamp{WallTime: 1, NodeID: 1}, CommitTS: decided,
	})
	require.NoError(t, err)
	require.True(t, resp.Success, resp.Error)

	got, _, ok := storedVersion(t, mdb, "docs", docsKey(1))
	require.True(t, ok)
	require.Equal(t, decided, got)
}

// A replayed transaction's commit timestamp is merged into the clock, as a
// committed one is, so what this node prepares next is ordered after it.
func TestReplayMergesCommitTimestampIntoClock(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	future := time.Now().Add(time.Hour).UnixNano()
	replayAt(t, mdb, 1, future, docsInsert(1, "a"))
	require.True(t, hlc.After(mdb.GetTransactionManager().clock.Now(), hlc.Timestamp{WallTime: future, NodeID: 2}))
}

// docsMove is an UPDATE that moves docs row oldID to newID.
func docsMove(oldID, newID int64, title string) *EncodedCapturedRow {
	return &EncodedCapturedRow{
		Table: "docs", Op: uint8(OpTypeUpdate), IntentKey: docsKey(newID),
		OldValues: encodeTestValues(map[string]interface{}{"id": oldID, "title": title}),
		NewValues: encodeTestValues(map[string]interface{}{"id": newID, "title": title}),
	}
}

// docsRows reads every docs row as id -> title.
func docsRows(t *testing.T, mdb *ReplicatedDatabase) map[int64]string {
	t.Helper()
	rows, err := mdb.GetWriteDB().Query("SELECT id, title FROM docs")
	require.NoError(t, err)
	defer rows.Close()
	got := map[int64]string{}
	for rows.Next() {
		var id int64
		var title string
		require.NoError(t, rows.Scan(&id, &title))
		got[id] = title
	}
	require.NoError(t, rows.Err())
	return got
}

// A key-moving UPDATE whose new key a newer change already owns must still
// retire the old key: insert 1 (v5), move 1->2 (v10), update 2 (v20), in
// commit order and with the move arriving last, end in the same state.
func TestKeyMoveArrivingAfterNewerChangeToNewKeyStillRetiresOldRow(t *testing.T) {
	_, inOrder, _ := newReplayTestDatabase(t)
	replayAt(t, inOrder, 1, 5, docsInsert(1, "x"))
	replayAt(t, inOrder, 2, 10, docsMove(1, 2, "x"))
	replayAt(t, inOrder, 3, 20, docsUpdate(2, "x", "y"))

	_, late, _ := newReplayTestDatabase(t)
	replayAt(t, late, 1, 5, docsInsert(1, "x"))
	replayAt(t, late, 3, 20, docsUpdate(2, "x", "y"))
	replayAt(t, late, 2, 10, docsMove(1, 2, "x"))

	require.Equal(t, map[int64]string{2: "y"}, docsRows(t, inOrder))
	require.Equal(t, docsRows(t, inOrder), docsRows(t, late))
}

// A move whose new key is still held by an older row, because the delete
// that cleared it has not arrived yet, replaces that row instead of failing
// on the key: insert 2 (v3), delete 2 (v4), insert 1 (v5), move 1->2 (v10),
// with the delete arriving last.
func TestKeyMoveOntoAKeyStillHeldByAnOlderRowReplacesIt(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	replayAt(t, mdb, 1, 3, docsInsert(2, "stale"))
	replayAt(t, mdb, 3, 5, docsInsert(1, "x"))
	replayAt(t, mdb, 4, 10, docsMove(1, 2, "x"))
	replayAt(t, mdb, 2, 4, docsDelete(2, "stale"))

	require.Equal(t, map[int64]string{2: "x"}, docsRows(t, mdb))
}

// applyEntryInTx applies entry at commitTS in its own transaction on sqlDB.
func applyEntryInTx(t testing.TB, sqlDB *sql.DB, schema CDCSchemaProvider, entry *IntentEntry, commitTS hlc.Timestamp) error {
	t.Helper()
	tx, err := sqlDB.Begin()
	require.NoError(t, err)
	defer tx.Rollback()
	applier, err := newVersionedApplier(tx, schema)
	require.NoError(t, err)
	if err := applier.applyEntry(entry, commitTS); err != nil {
		applier.Close()
		return err
	}
	require.NoError(t, applier.Close())
	return tx.Commit()
}

func docsLoad(lines string) *EncodedCapturedRow {
	return &EncodedCapturedRow{
		Table: "docs", Op: uint8(OpTypeLoadData),
		LoadSQL:  "LOAD DATA LOCAL INFILE 'x.csv' INTO TABLE docs FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' (id, title)",
		LoadData: []byte(lines),
	}
}

// A loaded row is versioned like an inserted one: an older DELETE replayed
// after the load does not remove it.
func TestOlderDeleteReplayedAfterLoadDataKeepsLoadedRow(t *testing.T) {
	_, inOrder, _ := newReplayTestDatabase(t)
	replayAt(t, inOrder, 1, 10, docsDelete(5, "old"))
	replayAt(t, inOrder, 2, 20, docsLoad("5,b\n"))

	_, late, _ := newReplayTestDatabase(t)
	replayAt(t, late, 2, 20, docsLoad("5,b\n"))
	replayAt(t, late, 1, 10, docsDelete(5, "old"))

	require.Equal(t, map[int64]string{5: "b"}, docsRows(t, inOrder))
	require.Equal(t, docsRows(t, inOrder), docsRows(t, late))
}

// A replayed LOAD DATA older than a row's stored version leaves that row
// alone, and still loads its other rows.
func TestOlderLoadDataReplayedAfterNewerRowSkipsOnlyThatRow(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	replayAt(t, mdb, 2, 30, docsInsert(5, "newer"))
	replayAt(t, mdb, 1, 20, docsLoad("5,loaded\n6,loaded\n"))

	require.Equal(t, map[int64]string{5: "newer", 6: "loaded"}, docsRows(t, mdb))
	got, _, ok := storedVersion(t, mdb, "docs", docsKey(6))
	require.True(t, ok)
	require.Equal(t, hlc.Timestamp{WallTime: 20, NodeID: 2}, got)
}

// The commit path versions LOAD DATA rows as replay does.
func TestCommittedLoadDataIsVersionedPerRow(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	replayAt(t, mdb, 1, 30, docsInsert(5, "newer"))
	load := docsLoad("5,loaded\n6,loaded\n")
	snapshot, err := SerializeData(LoadDataSnapshot{Type: int(protocol.StatementLoadData), SQL: load.LoadSQL, TableName: "docs", Data: load.LoadData})
	require.NoError(t, err)
	intents := []*WriteIntentRecord{{IntentType: IntentTypeDDL, TableName: "docs", NodeID: 2, SQLStatement: load.LoadSQL, DataSnapshot: snapshot}}

	commitTS := hlc.Timestamp{WallTime: 20, NodeID: 2}
	require.NoError(t, mdb.GetTransactionManager().applyNonDMLIntents(2, commitTS, intents))

	require.Equal(t, map[int64]string{5: "newer", 6: "loaded"}, docsRows(t, mdb))
	got, _, ok := storedVersion(t, mdb, "docs", docsKey(6))
	require.True(t, ok)
	require.Equal(t, commitTS, got)
}

// The purge finds tombstones through their index, never by scanning every
// row's version, and deletes past one chunk in a single tick.
func TestPurgeTombstonesUsesTheIndexAndSpansChunks(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	var plan strings.Builder
	rows, err := mdb.GetWriteDB().Query("EXPLAIN QUERY PLAN SELECT tbl, pk FROM __marmot_row_version WHERE deleted = 1 AND wall < 1 LIMIT 1")
	require.NoError(t, err)
	for rows.Next() {
		var id, parent, notused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
		plan.WriteString(detail)
	}
	require.NoError(t, rows.Close())
	require.Contains(t, plan.String(), "__marmot_row_version_tombstones")

	old := time.Now().Add(-3 * time.Hour).UnixNano()
	const n = tombstonePurgeChunk*2 + 7
	for i := 0; i < n; i++ {
		_, err := mdb.GetWriteDB().Exec("INSERT INTO __marmot_row_version VALUES ('docs', ?, ?, 0, 1, 1)", docsKey(int64(i)), old)
		require.NoError(t, err)
	}
	tm := mdb.GetTransactionManager()
	tm.gcMaxRetention = time.Hour
	tm.SetMemberCountFunc(func() int { return 2 })
	purged, err := tm.purgeTombstones()
	require.NoError(t, err)
	require.Equal(t, int64(n), purged)
}

// A listed log entry carries its commit timestamp from the position itself,
// for a local commit and a replayed one alike.
func TestListedLogEntriesCarryTheirCommitTimestamps(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	tm := mdb.GetTransactionManager()
	txn := beginPreparedDMLTxn(t, tm, mdb.GetMetaStore(), docsInsert(1, "a"))
	local := hlc.Timestamp{WallTime: 7_000, Logical: 1, NodeID: txn.NodeID}
	require.NoError(t, tm.CommitTransactionAfter(txn, local, nil))
	replayAt(t, mdb, 900, 8_000, docsInsert(2, "b"))

	entries, _, _, err := mdb.GetMetaStore().ListCommittedLog(LogPosition{}, 10)
	require.NoError(t, err)
	got := map[uint64]hlc.Timestamp{}
	for _, e := range entries {
		got[e.TxnID] = e.CommitTS
	}
	require.Equal(t, local, got[txn.ID])
	require.Equal(t, hlc.Timestamp{WallTime: 8_000, NodeID: 2}, got[900])
}

// A restart with the wall clock behind the last logged commit still issues
// later timestamps: opening a database seeds the clock from its log.
func TestReopenSeedsClockPastTheLastLoggedCommit(t *testing.T) {
	dir := t.TempDir()
	dbMgr, err := NewDatabaseManager(dir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	require.NoError(t, dbMgr.CreateDatabase("app"))
	mdb, err := dbMgr.GetDatabase("app")
	require.NoError(t, err)
	_, err = mdb.GetWriteDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT)`)
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())
	future := time.Now().Add(time.Hour).UnixNano()
	replayAt(t, mdb, 1, future, docsInsert(1, "a"))
	require.NoError(t, dbMgr.Close())

	clock := hlc.NewClock(1)
	reopened, err := NewDatabaseManager(dir, 1, clock)
	require.NoError(t, err)
	defer reopened.Close()
	_, err = reopened.GetDatabase("app")
	require.NoError(t, err)
	require.Greater(t, clock.Now().WallTime, future)
}

// A replayed DML row without an intent key is refused before anything is
// logged or applied, never applied unversioned.
func TestReplayRefusesKeylessRowBeforeLoggingIt(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	keyless := docsInsert(1, "a")
	keyless.IntentKey = nil
	_, err := mdb.ApplyReplayedTxn(context.Background(), &ReplayTxn{
		TxnID: 5, OriginNodeID: 2, CommitTS: hlc.Timestamp{WallTime: 10, NodeID: 2}, Rows: []*EncodedCapturedRow{keyless},
	}, true)
	require.ErrorIs(t, err, ErrReplayRowWithoutKey)
	rec, err := mdb.GetMetaStore().GetTransaction(5)
	require.NoError(t, err)
	require.Nil(t, rec, "nothing may be logged")
	require.Empty(t, docsRows(t, mdb))
}

// A move whose old key a newer change already owns still writes its row at
// the new key: insert 1 (v5), move 1->2 (v10), insert 1 (v15), delete 1
// (v20), with the move arriving last.
func TestKeyMoveWhoseOldKeyIsNewerElsewhereStillWritesTheNewKey(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	replayAt(t, mdb, 1, 5, docsInsert(1, "x"))
	replayAt(t, mdb, 3, 15, docsInsert(1, "y"))
	replayAt(t, mdb, 4, 20, docsDelete(1, "y"))
	replayAt(t, mdb, 2, 10, docsMove(1, 2, "x"))

	require.Equal(t, map[int64]string{2: "x"}, docsRows(t, mdb))
}

// A loaded row's version key is the key the CDC hook captures for the same
// row, TEXT primary keys included.
func TestLoadedRowVersionKeyMatchesCapturedKeyForTextKey(t *testing.T) {
	_, mdb, _ := newReplayTestDatabase(t)
	_, err := mdb.GetWriteDB().Exec(`CREATE TABLE tags (name TEXT PRIMARY KEY, n INTEGER)`)
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())
	replayAt(t, mdb, 1, 10, &EncodedCapturedRow{
		Table: "tags", Op: uint8(OpTypeLoadData), LoadData: []byte("go,1\n"),
		LoadSQL: "LOAD DATA LOCAL INFILE 'x.csv' INTO TABLE tags FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' (name, n)",
	})

	captured := filter.EncodeIntentKey("tags", []filter.TypedPKValue{valueToTypedPK([]byte("go"))})
	_, _, ok := storedVersion(t, mdb, "tags", captured)
	require.True(t, ok)
}

// BenchmarkApplyVersionedTxn100Rows applies a 100-row transaction per op
// through one versionedApplier, each row an update of its own row.
func BenchmarkApplyVersionedTxn100Rows(b *testing.B) {
	const rowsPerTxn = 100
	dbMgr, err := NewDatabaseManager(b.TempDir(), 1, hlc.NewClock(1))
	require.NoError(b, err)
	defer dbMgr.Close()
	require.NoError(b, dbMgr.CreateDatabase("app"))
	mdb, err := dbMgr.GetDatabase("app")
	require.NoError(b, err)
	_, err = mdb.GetWriteDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT)`)
	require.NoError(b, err)
	require.NoError(b, mdb.ReloadSchema())
	rows := make([]*EncodedCapturedRow, rowsPerTxn)
	for i := range rows {
		_, err = mdb.GetWriteDB().Exec(`INSERT INTO docs VALUES (?, 'v')`, i)
		require.NoError(b, err)
		rows[i] = docsUpdate(int64(i), "v", "w")
	}
	schema := &schemaCacheAdapter{cache: mdb.schemaCache}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := mdb.GetWriteDB().Begin()
		require.NoError(b, err)
		applier, err := newVersionedApplier(tx, schema)
		require.NoError(b, err)
		for _, row := range rows {
			_, err := applier.apply(OpType(row.Op), row.Table, row.IntentKey, row.OldValues, row.NewValues, hlc.Timestamp{WallTime: int64(i + 1), NodeID: 1})
			require.NoError(b, err)
		}
		require.NoError(b, applier.Close())
		require.NoError(b, tx.Commit())
	}
}
