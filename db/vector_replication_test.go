package db

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

type recordingVectorCDCNotifier struct {
	calls    int
	database string
	txnID    uint64
	seqNum   uint64
	entries  []common.CDCEntry
	err      error
}

func (r *recordingVectorCDCNotifier) ApplyCommittedVectorCDC(_ context.Context, database string, txnID, seqNum uint64, entries []common.CDCEntry) error {
	r.calls++
	r.database = database
	r.txnID = txnID
	r.seqNum = seqNum
	r.entries = append([]common.CDCEntry(nil), entries...)
	return r.err
}

func TestVectorIndexChangeSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	startTS := hlc.Timestamp{WallTime: 12345, Logical: 7, NodeID: 2}
	stmt := protocol.Statement{
		Type:             protocol.StatementCreateVectorIndex,
		Database:         "app",
		TableName:        "docs",
		VectorIndexName:  "docs_embed_idx",
		VectorColumnName: "embed",
		VectorMetric:     "cosine",
		VectorDim:        1536,
		VectorNlist:      128,
		VectorNprobe:     24,
		VectorMaxNorm:    1.0,
	}

	data, change, err := vectorIndexChangeSnapshot(stmt, "app", startTS)
	require.NoError(t, err)
	require.Equal(t, common.VectorIndexActionCreate, change.Action)
	require.Equal(t, "app", change.Database)
	require.Equal(t, "docs_embed_idx", change.IndexName)
	require.Equal(t, "docs", change.TableName)
	require.Equal(t, "embed", change.ColumnName)
	require.Equal(t, "cosine", change.Metric)
	require.Equal(t, 1536, change.Dim)
	require.Equal(t, 128, change.Nlist)
	require.Equal(t, 24, change.Nprobe)
	require.False(t, change.AutoTuneNlist)
	require.False(t, change.AutoTuneNprobe)
	require.Equal(t, defaultTargetPartitionSize, change.TargetPartitionSize)
	require.Equal(t, vectorControlTrainerVersion, change.TrainerVersion)
	require.Equal(t, vectorControlCodecVersion, change.CodecVersion)
	require.NotZero(t, change.Seed)

	var decoded common.VectorIndexChange
	require.NoError(t, DeserializeData(data, &decoded))
	require.Equal(t, change, decoded)
	require.Equal(t, stmt.VectorIndexName, vectorIndexStatementFromChange(decoded).VectorIndexName)
}

func TestTransactionManagerVectorCDCNotifierAfterCommitOnce(t *testing.T) {
	testDB := setupTestDBWithMeta(t)
	_, err := testDB.DB.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB, body TEXT)`)
	require.NoError(t, err)

	clock := hlc.NewClock(1)
	tm := NewTransactionManager(testDB.DB, testDB.MetaStore, clock, NewSchemaCache())
	tm.SetDatabaseName("app")
	recorder := &recordingVectorCDCNotifier{}
	tm.SetVectorCDCNotifier(recorder)

	startTS := clock.Now()
	txn, err := tm.BeginTransactionWithID(9001, 1, startTS)
	require.NoError(t, err)
	stmt := protocol.Statement{
		Type:      protocol.StatementInsert,
		Database:  "app",
		TableName: "docs",
	}
	require.NoError(t, tm.AddStatement(txn, stmt))
	require.NoError(t, testDB.MetaStore.WriteIntentEntry(
		txn.ID,
		1,
		uint8(OpTypeInsert),
		"docs",
		"docs:1",
		nil,
		encodeTestValues(map[string]interface{}{
			"id":    int64(1),
			"embed": []byte{1, 2, 3, 4},
			"body":  "hello",
		}),
	))

	require.NoError(t, tm.CommitTransaction(txn))
	require.Equal(t, 1, recorder.calls)
	require.Equal(t, "app", recorder.database)
	require.Equal(t, txn.ID, recorder.txnID)
	require.NotZero(t, recorder.seqNum)
	require.Len(t, recorder.entries, 1)
	require.Equal(t, "docs", recorder.entries[0].Table)
	require.Equal(t, []byte("docs:1"), recorder.entries[0].IntentKey)
	require.Equal(t, txn.ID, recorder.entries[0].CommitTxnID)
	require.Equal(t, recorder.seqNum, recorder.entries[0].CommitSeqNum)
}

func TestGetIndexByColumnHidesDirtyIndex(t *testing.T) {
	t.Parallel()

	mgr, db := setupManagerWithDB(t)
	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "testdb",
		Metric:     "cosine",
		Dim:        4,
		Nlist:      64,
		Nprobe:     8,
		Status:     "dirty",
		CreatedAt:  123,
	}
	seedManagerCache(t, mgr, db, meta)
	mgr.setCachedStatus("testdb", "docs", "embed", "dirty")

	got, ok := mgr.GetIndexByColumn("testdb", "docs", "embed")
	require.False(t, ok)
	require.Nil(t, got)
}

func TestGetIndexByColumnAllowsBuildingIndexForLiveOverlay(t *testing.T) {
	t.Parallel()

	mgr, db := setupManagerWithDB(t)
	meta := common.VectorIndexMeta{
		IndexName:  "embeddings",
		TableName:  "docs",
		ColumnName: "embed",
		Database:   "testdb",
		Metric:     "cosine",
		Dim:        4,
		Nlist:      64,
		Nprobe:     8,
		Status:     "building",
		CreatedAt:  123,
	}
	seedManagerCache(t, mgr, db, meta)

	got, ok := mgr.GetIndexByColumn("testdb", "docs", "embed")
	require.True(t, ok)
	require.Equal(t, "embeddings", got.IndexName)
}

func TestResolveCreateIndexMetaPreservesReplicatedAutoFlags(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)
	dbMgr, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbMgr.Close()) })
	require.NoError(t, dbMgr.CreateDatabase("app"))
	conn, err := dbMgr.GetDatabaseConnection("app")
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	vecMgr := NewVectorIndexManager(dbMgr)
	meta, err := vecMgr.ResolveCreateIndexMeta(context.Background(), common.VectorIndexMeta{
		IndexName:           "docs_embed_idx",
		TableName:           "docs",
		ColumnName:          "embed",
		Database:            "app",
		Metric:              "cosine",
		Dim:                 4,
		Nlist:               8,
		Nprobe:              8,
		AutoTuneNlist:       true,
		AutoTuneNprobe:      true,
		TargetPartitionSize: defaultTargetPartitionSize,
	})
	require.NoError(t, err)
	require.Equal(t, 8, meta.Nlist)
	require.Equal(t, 8, meta.Nprobe)
	require.True(t, meta.AutoTuneNlist)
	require.True(t, meta.AutoTuneNprobe)
}

func TestApplyVectorControlCreateMigratesNewDatabaseCatalog(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)
	dbMgr, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbMgr.Close()) })
	require.NoError(t, dbMgr.CreateDatabase("app"))
	conn, err := dbMgr.GetDatabaseConnection("app")
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	vecMgr := NewVectorIndexManager(dbMgr)
	err = vecMgr.ApplyVectorControl(context.Background(), common.VectorIndexChange{
		Action:              common.VectorIndexActionCreate,
		Database:            "app",
		IndexName:           "docs_embed_idx",
		TableName:           "docs",
		ColumnName:          "embed",
		Metric:              "cosine",
		Dim:                 4,
		Nlist:               8,
		Nprobe:              8,
		TargetPartitionSize: defaultTargetPartitionSize,
		CreatedAt:           123,
	})
	require.NoError(t, err)

	var status string
	require.NoError(t, conn.QueryRow(
		`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`,
		"docs_embed_idx",
	).Scan(&status))
	require.Equal(t, "building", status)
}
