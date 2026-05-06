package db

import (
	"bytes"
	"os"
	"path/filepath"
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

func TestCDCSegmentLogPrunesOnlyUnreferencedOldSegments(t *testing.T) {
	baseDir := t.TempDir()
	log, err := openCDCSegmentLog(baseDir)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if err := log.appendRow(1, 1, []byte("row")); err != nil {
		t.Fatalf("append row: %v", err)
	}
	if err := log.close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	segDir := filepath.Join(baseDir, cdcSegmentDirName)
	if err := os.WriteFile(cdcSegmentPath(segDir, 2), nil, 0o644); err != nil {
		t.Fatalf("create second segment: %v", err)
	}
	reopened, err := openCDCSegmentLog(baseDir)
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer reopened.close()

	deleted, err := reopened.pruneUnreferencedSegments(map[uint64]struct{}{2: {}})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted segments = %d, want 1", deleted)
	}
	if _, err := os.Stat(cdcSegmentPath(segDir, 1)); !os.IsNotExist(err) {
		t.Fatalf("segment 1 should be deleted, stat err=%v", err)
	}
	if _, err := os.Stat(cdcSegmentPath(segDir, 2)); err != nil {
		t.Fatalf("current segment should remain: %v", err)
	}
}

func TestCDCSegmentLogRetainsPendingPrepareChunks(t *testing.T) {
	log, err := openCDCSegmentLog(t.TempDir())
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	defer log.close()

	log.mu.Lock()
	log.segmentID = 3
	log.pending[10] = &cdcSegmentTxnManifest{
		TxnID: 10,
		Chunks: []cdcSegmentChunk{{
			SegmentID: 1,
			Offset:    0,
			Length:    64,
		}},
		prepareChunks: []cdcSegmentChunk{{
			SegmentID: 2,
			Offset:    0,
			Length:    64,
		}},
	}
	log.mu.Unlock()

	retained := make(map[uint64]struct{})
	log.addRetainedSegments(retained)
	for _, id := range []uint64{1, 2, 3} {
		if _, ok := retained[id]; !ok {
			t.Fatalf("segment %d was not retained", id)
		}
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
	row, err := EncodeRow(&EncodedCapturedRow{
		Table:     "docs",
		Op:        uint8(OpTypeInsert),
		IntentKey: []byte("docs:1"),
		NewValues: newVals,
	})
	if err != nil {
		t.Fatalf("encode CDC row: %v", err)
	}
	if err := store.WriteCapturedRow(txnID, 1, row); err != nil {
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
	if _, closer, err := recovered.db.Get(pebbleIntentByTxnKey(txnID, "docs", "docs:1")); err == nil {
		closer.Close()
		t.Fatal("recovered DML intent should remain transient, not persisted under /intent_txn")
	} else if err != pebble.ErrNotFound {
		t.Fatalf("get persisted DML intent: %v", err)
	}
}

func TestPebbleMetaStoreSealReleasesPendingInlineRows(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	const txnID uint64 = 77
	payload := []byte("captured row")
	if err := store.WriteCapturedRow(txnID, 1, payload); err != nil {
		t.Fatalf("write captured row: %v", err)
	}
	if store.cdcLog.getPendingManifest(txnID) == nil {
		t.Fatal("expected pending manifest before seal")
	}
	if err := store.SealCapturedRows(txnID); err != nil {
		t.Fatalf("seal captured rows: %v", err)
	}
	if store.cdcLog.getPendingManifest(txnID) != nil {
		t.Fatal("pending manifest should be released after manifest publish")
	}

	cursor, err := store.IterateCapturedRows(txnID)
	if err != nil {
		t.Fatalf("iterate captured rows: %v", err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatalf("expected sealed captured row, err=%v", cursor.Err())
	}
	_, got := cursor.Row()
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if cursor.Next() {
		t.Fatal("unexpected extra row")
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("cursor err: %v", err)
	}
}
