package grpc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog"
)

type focusedVectorHook struct {
	err error
}

func (h focusedVectorHook) OnIndexCreated(context.Context, common.VectorIndexMeta) error {
	return nil
}

func (h focusedVectorHook) OnIndexLocalChanges(context.Context, common.VectorIndexMeta, []common.CDCEntry) error {
	return h.err
}

func newFocusedReplicationHandler(t *testing.T, testName string) (*ReplicationHandler, *db.DatabaseManager, string) {
	t.Helper()

	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, testName)
	t.Cleanup(func() {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
	})

	const testDB = "replication_suite"
	if err := dbMgr.CreateDatabase(testDB); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	mdb, err := dbMgr.GetDatabase(testDB)
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if _, err := mdb.GetDB().Exec(`
		CREATE TABLE docs (
			id INTEGER PRIMARY KEY,
			title TEXT,
			score INTEGER,
			body TEXT
		)`); err != nil {
		t.Fatalf("create docs table: %v", err)
	}
	if err := mdb.ReloadSchema(); err != nil {
		t.Fatalf("ReloadSchema: %v", err)
	}

	clock := hlc.NewClock(1)
	return NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr), dbMgr, testDB
}

func focusedHLC(clock *hlc.Clock) *HLC {
	ts := clock.Now()
	return &HLC{WallTime: ts.WallTime, Logical: ts.Logical, NodeId: ts.NodeID}
}

func focusedRowStatement(dbName string, stmtType pb.StatementType, row *RowChange) *Statement {
	return &Statement{
		Type:      stmtType,
		TableName: "docs",
		Database:  dbName,
		Payload:   &Statement_RowChange{RowChange: row},
	}
}

func TestReplicationHandlerTwoPCDeferredCDCInsertUpdateDelete(t *testing.T) {
	handler, dbMgr, dbName := newFocusedReplicationHandler(t, "focused_2pc_cdc")
	clock := hlc.NewClock(2)
	ctx := context.Background()

	prepare := func(txnID uint64, stmtType pb.StatementType, intentKey []byte) {
		t.Helper()
		resp, err := handler.HandleReplicateTransaction(ctx, &TransactionRequest{
			TxnId:        txnID,
			SourceNodeId: 2,
			Database:     dbName,
			Phase:        TransactionPhase_PREPARE,
			Timestamp:    focusedHLC(clock),
			Statements: []*Statement{
				focusedRowStatement(dbName, stmtType, &RowChange{IntentKey: intentKey}),
			},
		})
		if err != nil {
			t.Fatalf("prepare txn %d: %v", txnID, err)
		}
		if !resp.Success {
			t.Fatalf("prepare txn %d failed: %s", txnID, resp.ErrorMessage)
		}
	}
	commit := func(txnID uint64, stmt *Statement) {
		t.Helper()
		resp, err := handler.HandleReplicateTransaction(ctx, &TransactionRequest{
			TxnId:     txnID,
			Database:  dbName,
			Phase:     TransactionPhase_COMMIT,
			Timestamp: focusedHLC(clock),
			Statements: []*Statement{
				stmt,
			},
		})
		if err != nil {
			t.Fatalf("commit txn %d: %v", txnID, err)
		}
		if !resp.Success {
			t.Fatalf("commit txn %d failed: %s", txnID, resp.ErrorMessage)
		}
	}

	prepare(1001, pb.StatementType_INSERT, []byte("docs:1"))
	mdb, err := dbMgr.GetDatabase(dbName)
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	var count int
	if err := mdb.GetDB().QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatalf("count before commit: %v", err)
	}
	if count != 0 {
		t.Fatalf("PREPARE applied row before COMMIT: count=%d", count)
	}

	commit(1001, focusedRowStatement(dbName, pb.StatementType_INSERT, &RowChange{
		IntentKey: []byte("docs:1"),
		NewValues: map[string][]byte{
			"id":    mustMarshalMsgpack(t, int64(1)),
			"title": mustMarshalMsgpack(t, "alpha"),
			"score": mustMarshalMsgpack(t, int64(10)),
			"body":  mustMarshalMsgpack(t, "first body"),
		},
	}))
	assertDocRow(t, mdb, 1, "alpha", 10)

	prepare(1002, pb.StatementType_UPDATE, []byte("docs:1"))
	commit(1002, focusedRowStatement(dbName, pb.StatementType_UPDATE, &RowChange{
		IntentKey: []byte("docs:1"),
		OldValues: map[string][]byte{
			"id":    mustMarshalMsgpack(t, int64(1)),
			"title": mustMarshalMsgpack(t, "alpha"),
			"score": mustMarshalMsgpack(t, int64(10)),
		},
		NewValues: map[string][]byte{
			"id":    mustMarshalMsgpack(t, int64(1)),
			"title": mustMarshalMsgpack(t, "beta"),
			"score": mustMarshalMsgpack(t, int64(20)),
			"body":  mustMarshalMsgpack(t, nil),
		},
	}))
	assertDocRow(t, mdb, 1, "beta", 20)

	prepare(1003, pb.StatementType_DELETE, []byte("docs:1"))
	commit(1003, focusedRowStatement(dbName, pb.StatementType_DELETE, &RowChange{
		IntentKey: []byte("docs:1"),
		OldValues: map[string][]byte{
			"id": mustMarshalMsgpack(t, int64(1)),
		},
	}))
	if err := mdb.GetDB().QueryRow(`SELECT COUNT(*) FROM docs WHERE id = 1`).Scan(&count); err != nil {
		t.Fatalf("count after delete: %v", err)
	}
	if count != 0 {
		t.Fatalf("DELETE did not remove row: count=%d", count)
	}
}

func assertDocRow(t *testing.T, mdb *db.ReplicatedDatabase, id int64, wantTitle string, wantScore int64) {
	t.Helper()

	var title string
	var score int64
	if err := mdb.GetDB().QueryRow(`SELECT title, score FROM docs WHERE id = ?`, id).Scan(&title, &score); err != nil {
		t.Fatalf("query doc %d: %v", id, err)
	}
	if title != wantTitle || score != wantScore {
		t.Fatalf("doc %d = title=%q score=%d, want title=%q score=%d", id, title, score, wantTitle, wantScore)
	}
}

func TestReplicationHandlerPrepareConflictAbortRecovery(t *testing.T) {
	handler, _, dbName := newFocusedReplicationHandler(t, "focused_2pc_conflict_recovery")
	clock := hlc.NewClock(2)
	ctx := context.Background()

	prepareReq := func(txnID uint64) *TransactionRequest {
		return &TransactionRequest{
			TxnId:        txnID,
			SourceNodeId: 2,
			Database:     dbName,
			Phase:        TransactionPhase_PREPARE,
			Timestamp:    focusedHLC(clock),
			Statements: []*Statement{
				focusedRowStatement(dbName, pb.StatementType_UPDATE, &RowChange{IntentKey: []byte("docs:1")}),
			},
		}
	}

	first, err := handler.HandleReplicateTransaction(ctx, prepareReq(2001))
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	if !first.Success {
		t.Fatalf("first prepare failed: %s", first.ErrorMessage)
	}

	conflicting, err := handler.HandleReplicateTransaction(ctx, prepareReq(2002))
	if err != nil {
		t.Fatalf("conflicting prepare: %v", err)
	}
	if conflicting.Success || !conflicting.ConflictDetected {
		t.Fatalf("expected conflict on second prepare, got success=%v conflict=%v error=%q",
			conflicting.Success, conflicting.ConflictDetected, conflicting.ErrorMessage)
	}

	abort, err := handler.HandleReplicateTransaction(ctx, &TransactionRequest{
		TxnId:     2001,
		Database:  dbName,
		Phase:     TransactionPhase_ABORT,
		Timestamp: focusedHLC(clock),
	})
	if err != nil {
		t.Fatalf("abort: %v", err)
	}
	if !abort.Success {
		t.Fatalf("abort failed: %s", abort.ErrorMessage)
	}

	retry, err := handler.HandleReplicateTransaction(ctx, prepareReq(2002))
	if err != nil {
		t.Fatalf("retry prepare: %v", err)
	}
	if !retry.Success {
		t.Fatalf("prepare after abort should succeed, got: %s", retry.ErrorMessage)
	}
}

func TestReplicationHandlerReplayFailureCanRetryWithoutAdvancingWatermark(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "focused_replay_failure_retry")
	t.Cleanup(func() {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
	})

	const dbName = "replay_retry"
	if err := dbMgr.CreateDatabase(dbName); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	mdb, err := dbMgr.GetDatabase(dbName)
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	handler := NewReplicationHandler(1, dbMgr, hlc.NewClock(1), schemaVersionMgr)
	clock := hlc.NewClock(2)

	req := &TransactionRequest{
		TxnId:        3001,
		SourceNodeId: 2,
		Database:     dbName,
		Phase:        TransactionPhase_REPLAY,
		Timestamp:    focusedHLC(clock),
		Statements: []*Statement{
			focusedRowStatement(dbName, pb.StatementType_INSERT, &RowChange{
				IntentKey: []byte("docs:1"),
				NewValues: map[string][]byte{
					"id":    mustMarshalMsgpack(t, int64(1)),
					"title": mustMarshalMsgpack(t, "late table"),
				},
			}),
		},
	}

	resp, err := handler.HandleReplicateTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("first replay call: %v", err)
	}
	if resp.Success {
		t.Fatal("replay against a missing table should fail")
	}
	if rec, err := mdb.GetMetaStore().GetTransaction(req.TxnId); err == nil && rec != nil && rec.Status == db.TxnStatusCommitted {
		t.Fatalf("failed replay advanced committed transaction: %+v", rec)
	}

	if _, err := mdb.GetDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT)`); err != nil {
		t.Fatalf("create docs table: %v", err)
	}
	if err := mdb.ReloadSchema(); err != nil {
		t.Fatalf("ReloadSchema: %v", err)
	}

	resp, err = handler.HandleReplicateTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("retry replay call: %v", err)
	}
	if !resp.Success {
		t.Fatalf("retry replay failed: %s", resp.ErrorMessage)
	}
	var title string
	if err := mdb.GetDB().QueryRow(`SELECT title FROM docs WHERE id = 1`).Scan(&title); err != nil {
		t.Fatalf("query replayed row: %v", err)
	}
	if title != "late table" {
		t.Fatalf("replayed title=%q, want late table", title)
	}
	rec, err := mdb.GetMetaStore().GetTransaction(req.TxnId)
	if err != nil {
		t.Fatalf("GetTransaction after retry: %v", err)
	}
	if rec == nil || rec.Status != db.TxnStatusCommitted {
		t.Fatalf("replay retry did not record committed transaction: %+v", rec)
	}
}

func TestReplicationHandlerReplayVectorCDCFailureMarksDirtyButSucceeds(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "focused_replay_vector_dirty")
	t.Cleanup(func() {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
	})

	const dbName = "replay_vector"
	if err := dbMgr.CreateDatabase(dbName); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	mdb, err := dbMgr.GetDatabase(dbName)
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if _, err := mdb.GetDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB, title TEXT)`); err != nil {
		t.Fatalf("create docs table: %v", err)
	}
	if err := mdb.ReloadSchema(); err != nil {
		t.Fatalf("ReloadSchema: %v", err)
	}

	vecMgr := db.NewVectorIndexManager(dbMgr)
	vecMgr.SetLifecycleHook(focusedVectorHook{err: errors.New("overlay write failed")})
	dbMgr.SetVectorIndexManager(vecMgr)
	if err := vecMgr.ApplyVectorControl(context.Background(), common.VectorIndexChange{
		Action:              common.VectorIndexActionCreate,
		Database:            dbName,
		IndexName:           "docs_embed_idx",
		TableName:           "docs",
		ColumnName:          "embed",
		Metric:              "cosine",
		Dim:                 4,
		Nlist:               8,
		Nprobe:              8,
		TargetPartitionSize: 512,
		CreatedAt:           time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("ApplyVectorControl: %v", err)
	}

	handler := NewReplicationHandler(1, dbMgr, hlc.NewClock(1), schemaVersionMgr)
	req := &TransactionRequest{
		TxnId:        3101,
		SourceNodeId: 2,
		Database:     dbName,
		Phase:        TransactionPhase_REPLAY,
		Timestamp:    focusedHLC(hlc.NewClock(2)),
		Statements: []*Statement{
			focusedRowStatement(dbName, pb.StatementType_INSERT, &RowChange{
				IntentKey: []byte("docs:1"),
				NewValues: map[string][]byte{
					"id":    mustMarshalMsgpack(t, int64(1)),
					"embed": mustMarshalMsgpack(t, []byte{1, 2, 3, 4}),
					"title": mustMarshalMsgpack(t, "vector row"),
				},
			}),
		},
	}

	resp, err := handler.HandleReplicateTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("replay call: %v", err)
	}
	if !resp.Success {
		t.Fatalf("vector CDC failure should not fail row replay, got: %s", resp.ErrorMessage)
	}

	var title string
	if err := mdb.GetDB().QueryRow(`SELECT title FROM docs WHERE id = 1`).Scan(&title); err != nil {
		t.Fatalf("query replayed vector row: %v", err)
	}
	if title != "vector row" {
		t.Fatalf("title=%q, want vector row", title)
	}
	var status string
	if err := mdb.GetDB().QueryRow(
		`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`,
		"docs_embed_idx",
	).Scan(&status); err != nil {
		t.Fatalf("query vector status: %v", err)
	}
	if status != "dirty" {
		t.Fatalf("vector status=%q, want dirty", status)
	}
	rec, err := mdb.GetMetaStore().GetTransaction(req.TxnId)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if rec == nil || rec.Status != db.TxnStatusCommitted {
		t.Fatalf("row replay transaction not committed: %+v", rec)
	}
}

func BenchmarkReplicationHandlerReplayRowCDC(b *testing.B) {
	prevLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(prevLogLevel)

	tmpDir := filepath.Join("/tmp/marmot", fmt.Sprintf("bench_replay_row_cdc_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		b.Fatalf("MkdirAll: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		b.Fatalf("NewDatabaseManager: %v", err)
	}
	defer dbMgr.Close()

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	if err != nil {
		b.Fatalf("system database: %v", err)
	}
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetMetaStore())

	const dbName = "bench_replay"
	if err := dbMgr.CreateDatabase(dbName); err != nil {
		b.Fatalf("CreateDatabase: %v", err)
	}
	mdb, err := dbMgr.GetDatabase(dbName)
	if err != nil {
		b.Fatalf("GetDatabase: %v", err)
	}
	if _, err := mdb.GetDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, score INTEGER)`); err != nil {
		b.Fatalf("create docs table: %v", err)
	}
	if err := mdb.ReloadSchema(); err != nil {
		b.Fatalf("ReloadSchema: %v", err)
	}

	handler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)
	sourceClock := hlc.NewClock(2)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := int64(i + 1)
		req := &TransactionRequest{
			TxnId:        uint64(i + 1),
			SourceNodeId: 2,
			Database:     dbName,
			Phase:        TransactionPhase_REPLAY,
			Timestamp:    focusedHLC(sourceClock),
			Statements: []*Statement{
				focusedRowStatement(dbName, pb.StatementType_INSERT, &RowChange{
					IntentKey: []byte(fmt.Sprintf("docs:%d", id)),
					NewValues: map[string][]byte{
						"id":    mustMarshalMsgpack(b, id),
						"title": mustMarshalMsgpack(b, fmt.Sprintf("doc-%d", id)),
						"score": mustMarshalMsgpack(b, id%100),
					},
				}),
			},
		}
		resp, err := handler.HandleReplicateTransaction(ctx, req)
		if err != nil {
			b.Fatalf("HandleReplicateTransaction: %v", err)
		}
		if !resp.Success {
			b.Fatalf("replay failed: %s", resp.ErrorMessage)
		}
	}
}
