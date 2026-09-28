package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
)

func TestStoreAppliedChangeEventPersistsRowsAndIsIdempotent(t *testing.T) {
	dbMgr, err := db.NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	if err != nil {
		t.Fatalf("NewDatabaseManager: %v", err)
	}
	defer dbMgr.Close()
	if err := dbMgr.CreateDatabase("rag"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	mdb, err := dbMgr.GetDatabase("rag")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	store := mdb.GetMetaStore()

	statements := []*Statement{{
		Type:      pb.StatementType_INSERT,
		TableName: "docs",
		Database:  "rag",
		Payload: &Statement_RowChange{RowChange: testInsertRowChange("docs", []byte("pk:1"), map[string][]byte{
			"id":   {1},
			"name": []byte("doc"),
		})},
	}}
	ts := &HLC{WallTime: 100, Logical: 2, NodeId: 7}

	seq1, err := StoreAppliedChangeEvent(mdb, 42, ts, statements, 7, 3)
	if err != nil {
		t.Fatalf("StoreAppliedChangeEvent first call: %v", err)
	}
	seq2, err := StoreAppliedChangeEvent(mdb, 42, ts, statements, 7, 3)
	if err != nil {
		t.Fatalf("StoreAppliedChangeEvent second call: %v", err)
	}
	if seq1 == 0 || seq2 != seq1 {
		t.Fatalf("seq idempotence failed: first=%d second=%d", seq1, seq2)
	}

	rec, err := store.GetTransaction(42)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if rec == nil || rec.Status != db.TxnStatusCommitted || rec.DatabaseName != "rag" {
		t.Fatalf("transaction record = %+v, want committed rag", rec)
	}
	if rec.NodeID != 7 {
		t.Fatalf("NodeID = %d, want the origin node id 7", rec.NodeID)
	}
	if rec.RequiredSchemaVersion != 3 {
		t.Fatalf("RequiredSchemaVersion = %d, want 3", rec.RequiredSchemaVersion)
	}

	cursor, err := store.IterateCapturedRows(42)
	if err != nil {
		t.Fatalf("IterateCapturedRows: %v", err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("expected captured row")
	}
	_, raw := cursor.Row()
	row, err := db.DecodeRow(raw)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	if row.Table != "docs" || row.Op != uint8(db.OpTypeInsert) || string(row.IntentKey) != "pk:1" {
		t.Fatalf("captured row = %+v, want docs insert pk:1", row)
	}
	if cursor.Next() {
		t.Fatal("expected a single captured row")
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("cursor error: %v", err)
	}
}

// TestApplyPulledEventRefusesRowCountMismatch is ApplyPulledEvent's own
// RowCount check: a pulled ChangeEvent whose statement count does not match its
// RowCount must be refused, with nothing applied, even though
// FetchTransactions already checks this server-side - a puller must not
// trust the wire.
func TestApplyPulledEventRefusesRowCountMismatch(t *testing.T) {
	dbMgr, err := db.NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	if err != nil {
		t.Fatalf("NewDatabaseManager: %v", err)
	}
	defer dbMgr.Close()
	if err := dbMgr.CreateDatabase("rag"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	mdb, err := dbMgr.GetDatabase("rag")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}

	statements := []*Statement{{
		Type:      pb.StatementType_INSERT,
		TableName: "docs",
		Database:  "rag",
		Payload: &Statement_RowChange{RowChange: testInsertRowChange("docs", []byte("pk:1"), map[string][]byte{
			"id":   {1},
			"name": []byte("doc"),
		})},
	}}
	ev := &ChangeEvent{
		TxnId:        77,
		OriginNodeId: 7,
		Database:     "rag",
		Timestamp:    &HLC{WallTime: 100, Logical: 2, NodeId: 7},
		Statements:   statements,
		RowCount:     2, // one statement short of what it claims
	}

	applied, err := ApplyPulledEvent(context.Background(), dbMgr, ev)
	if applied {
		t.Fatal("a row-count mismatch must never be applied")
	}
	if !errors.Is(err, ErrRowCountMismatch) {
		t.Fatalf("err = %v, want ErrRowCountMismatch", err)
	}

	rec, err := mdb.GetMetaStore().GetTransaction(77)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if rec != nil {
		t.Fatalf("a refused row-count mismatch must not write a log entry, got: %+v", rec)
	}
}

// TestApplyPulledEventRefusesNilDecodedRow: a wire
// statement whose payload capturedRowFromStatement does not recognize (for
// example an unrecognized payload shape from a newer peer) decodes to a nil
// row. Before the fix, ApplyPulledEvent silently dropped it and applied the
// remaining rows anyway; it must instead refuse the whole event.
func TestApplyPulledEventRefusesNilDecodedRow(t *testing.T) {
	dbMgr, err := db.NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	if err != nil {
		t.Fatalf("NewDatabaseManager: %v", err)
	}
	defer dbMgr.Close()
	if err := dbMgr.CreateDatabase("rag"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	mdb, err := dbMgr.GetDatabase("rag")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}

	// No Payload set at all: none of GetRowChange/GetDdlChange/
	// GetLoadDataChange/GetVectorIndexChange match, so
	// capturedRowFromStatement returns a nil row with no error.
	statements := []*Statement{{
		Type:      pb.StatementType_INSERT,
		TableName: "docs",
		Database:  "rag",
	}}
	ev := &ChangeEvent{
		TxnId:        78,
		OriginNodeId: 7,
		Database:     "rag",
		Timestamp:    &HLC{WallTime: 100, Logical: 2, NodeId: 7},
		Statements:   statements,
		RowCount:     1,
	}

	applied, err := ApplyPulledEvent(context.Background(), dbMgr, ev)
	if applied {
		t.Fatal("a nil decoded row must never be applied")
	}
	if !errors.Is(err, ErrNilCapturedRow) {
		t.Fatalf("err = %v, want ErrNilCapturedRow", err)
	}

	rec, err := mdb.GetMetaStore().GetTransaction(78)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if rec != nil {
		t.Fatalf("a refused nil-row event must not write a log entry, got: %+v", rec)
	}
}
