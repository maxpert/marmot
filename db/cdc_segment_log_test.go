package db

import (
	"bytes"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
)

func TestCDCPrepareSyncStrictConfig(t *testing.T) {
	prevConfig := cfg.Config
	prevEnv, hadEnv := os.LookupEnv("MARMOT_CDC_PREPARE_SYNC")
	t.Cleanup(func() {
		cfg.Config = prevConfig
		if hadEnv {
			_ = os.Setenv("MARMOT_CDC_PREPARE_SYNC", prevEnv)
		} else {
			_ = os.Unsetenv("MARMOT_CDC_PREPARE_SYNC")
		}
	})

	_ = os.Unsetenv("MARMOT_CDC_PREPARE_SYNC")
	cfg.Config = &cfg.Configuration{}
	if cdcPrepareSyncStrict() {
		t.Fatal("strict prepare sync should default to false")
	}

	cfg.Config.MetaStore.StrictPrepareSync = true
	if !cdcPrepareSyncStrict() {
		t.Fatal("strict prepare sync config was not honored")
	}

	cfg.Config.MetaStore.StrictPrepareSync = false
	_ = os.Setenv("MARMOT_CDC_PREPARE_SYNC", "strict")
	if !cdcPrepareSyncStrict() {
		t.Fatal("strict prepare sync env override was not honored")
	}
}

func TestCDCSegmentLogRecoverPreparedManifest(t *testing.T) {
	dir := t.TempDir()
	const txnID uint64 = 7
	payload := []byte("prepared row image")

	log, err := openCDCSegmentLog(dir)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if err := log.appendRow(txnID, 1, payload); err != nil {
		t.Fatalf("append row: %v", err)
	}
	manifest, err := log.sealTxn(txnID, nil, true)
	if err != nil {
		t.Fatalf("seal txn: %v", err)
	}
	if manifest.RowCount != 1 {
		t.Fatalf("row count = %d, want 1", manifest.RowCount)
	}
	if err := log.close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	recovered, err := openCDCSegmentLog(dir)
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer recovered.close()

	recoveredManifest := recovered.getPendingManifest(txnID)
	if recoveredManifest == nil {
		t.Fatal("expected prepared manifest to be recovered")
	}
	cursor := newCDCSegmentCursor(recovered, recoveredManifest)
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatalf("expected recovered row, err=%v", cursor.Err())
	}
	seq, data := cursor.Row()
	if seq != 1 {
		t.Fatalf("seq = %d, want 1", seq)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("payload = %q, want %q", data, payload)
	}
	if cursor.Next() {
		t.Fatal("unexpected extra recovered row")
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("cursor err: %v", err)
	}
}

func TestCDCSegmentLogRejectsOversizedRecord(t *testing.T) {
	log, err := openCDCSegmentLog(t.TempDir())
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	defer log.close()

	payload := make([]byte, cdcSegmentFileSize-cdcSegmentHeaderSize+1)
	if err := log.appendRow(1, 1, payload); err == nil {
		t.Fatal("expected oversized CDC segment record to be rejected")
	}
}

func TestPebbleMetaStoreRecoversPreparedCDCFromSegment(t *testing.T) {
	dir := t.TempDir()
	opts := PebbleMetaStoreOptions{CacheSizeMB: 8, MemTableSizeMB: 4, MemTableCount: 2}
	store, err := NewPebbleMetaStore(dir, opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	const txnID uint64 = 42
	startTS := hlc.Timestamp{WallTime: 1234, Logical: 2, NodeID: 9}
	if err := store.BeginTransaction(txnID, 9, startTS); err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	newVals := map[string][]byte{"id": []byte("1")}
	if err := store.WriteIntentEntry(txnID, 1, uint8(OpTypeInsert), "docs", "docs:1", nil, newVals); err != nil {
		t.Fatalf("write CDC row: %v", err)
	}
	if err := store.sealCapturedRows(txnID, true); err != nil {
		t.Fatalf("seal CDC rows: %v", err)
	}

	// Simulate losing the unsynced Pebble prepare/index keys after the segment
	// prepare record was already fsynced.
	if err := store.db.Delete(pebbleCDCManifestKey(txnID), pebble.NoSync); err != nil {
		t.Fatalf("delete manifest: %v", err)
	}
	if err := store.db.Delete(pebbleTxnKey(txnID), pebble.NoSync); err != nil {
		t.Fatalf("delete txn key: %v", err)
	}
	if err := store.db.Delete(pebbleTxnStatusKey(txnID), pebble.NoSync); err != nil {
		t.Fatalf("delete txn status: %v", err)
	}
	if err := store.db.Delete(pebbleTxnPendingKey(txnID), pebble.NoSync); err != nil {
		t.Fatalf("delete pending key: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	recovered, err := NewPebbleMetaStore(dir, opts)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer recovered.Close()

	txn, err := recovered.GetTransaction(txnID)
	if err != nil {
		t.Fatalf("get recovered txn: %v", err)
	}
	if txn == nil || txn.Status != TxnStatusPending || txn.NodeID != 9 {
		t.Fatalf("recovered txn = %+v, want pending node 9", txn)
	}
	entries, err := recovered.GetIntentEntries(txnID)
	if err != nil {
		t.Fatalf("get recovered entries: %v", err)
	}
	if len(entries) != 1 || entries[0].Table != "docs" || !bytes.Equal(entries[0].NewValues["id"], []byte("1")) {
		t.Fatalf("unexpected recovered entries: %+v", entries)
	}
	ok, err := recovered.ValidateIntent("docs", "docs:1", txnID)
	if err != nil {
		t.Fatalf("validate intent: %v", err)
	}
	if !ok {
		t.Fatal("expected recovered row lock")
	}
}
