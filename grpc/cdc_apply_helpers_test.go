package grpc

import (
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
)

func TestStoreAppliedChangeEventPersistsRowsAndIsIdempotent(t *testing.T) {
	store, err := db.NewPebbleMetaStore(filepath.Join(t.TempDir(), "meta"), db.DefaultPebbleOptions())
	if err != nil {
		t.Fatalf("NewPebbleMetaStore: %v", err)
	}
	defer store.Close()

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

	seq1, err := StoreAppliedChangeEvent(store, 42, ts, "rag", statements)
	if err != nil {
		t.Fatalf("StoreAppliedChangeEvent first call: %v", err)
	}
	seq2, err := StoreAppliedChangeEvent(store, 42, ts, "rag", statements)
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
