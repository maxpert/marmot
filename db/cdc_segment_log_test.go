//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"errors"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/encoding"
)

func TestCDCSegmentLogRejectsPayloadLargerThanRecoveryLimit(t *testing.T) {
	log, err := openCDCSegmentLog(t.TempDir())
	if err != nil {
		t.Fatalf("openCDCSegmentLog: %v", err)
	}
	defer log.close()

	payload := make([]byte, cdcSegmentMaxPayloadSize+1)
	if err := log.appendRow(1, 1, payload); err == nil {
		t.Fatal("expected oversized payload to be rejected")
	}

	log.mu.Lock()
	offset := log.offset
	pending := len(log.pending)
	segmentPath := cdcSegmentPath(log.dir, log.segmentID)
	log.mu.Unlock()

	if offset != 0 {
		t.Fatalf("oversized payload advanced segment offset: got %d", offset)
	}
	if pending != 0 {
		t.Fatalf("oversized payload created pending manifest: got %d", pending)
	}
	info, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("oversized payload wrote bytes: got file size %d", info.Size())
	}
}

func TestCDCSegmentLogGCDeletesOnlyUnretainedSegments(t *testing.T) {
	log, err := openCDCSegmentLog(t.TempDir())
	if err != nil {
		t.Fatalf("openCDCSegmentLog failed: %v", err)
	}
	defer log.close()

	setCurrentCDCSegmentForTest(t, log, 3)
	writeCDCSegmentFileForTest(t, log, 2)

	deleted, err := log.gcSegments(map[uint64]struct{}{1: {}})
	if err != nil {
		t.Fatalf("gcSegments failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted segments = %d, want 1", deleted)
	}
	requireCDCSegmentExists(t, log, 1)
	requireCDCSegmentMissing(t, log, 2)
	requireCDCSegmentExists(t, log, 3)
}

func TestCDCSegmentLogGCRetainsPendingSyncSegments(t *testing.T) {
	log, err := openCDCSegmentLog(t.TempDir())
	if err != nil {
		t.Fatalf("openCDCSegmentLog failed: %v", err)
	}
	defer log.close()

	setCurrentCDCSegmentForTest(t, log, 2)
	manifest := &cdcSegmentTxnManifest{
		TxnID: 1,
		Chunks: []cdcSegmentChunk{
			{SegmentID: 1, Offset: 0, Length: 1},
		},
	}

	log.syncer.retainSegments(manifest)
	deleted, err := log.gcSegments(nil)
	if err != nil {
		t.Fatalf("gcSegments with pending sync failed: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted segments with pending sync = %d, want 0", deleted)
	}
	requireCDCSegmentExists(t, log, 1)

	log.syncer.releaseSegments([]*cdcSegmentSyncRequest{{manifest: manifest}})
	deleted, err = log.gcSegments(nil)
	if err != nil {
		t.Fatalf("gcSegments after sync release failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted segments after sync release = %d, want 1", deleted)
	}
	requireCDCSegmentMissing(t, log, 1)
	requireCDCSegmentExists(t, log, 2)
}

func TestPebbleMetaStoreDeleteCapturedRowsGCSegmentsByManifestReachability(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	const (
		deletedTxn  = uint64(1001)
		retainedTxn = uint64(1002)
	)
	setCurrentCDCSegmentForTest(t, store.cdcLog, 2)
	writeCDCManifestForTest(t, store, deletedTxn, 1)
	writeCDCManifestForTest(t, store, retainedTxn, 1)

	if err := store.DeleteCapturedRows(deletedTxn); err != nil {
		t.Fatalf("DeleteCapturedRows for first txn failed: %v", err)
	}
	requireCDCSegmentExists(t, store.cdcLog, 1)

	if err := store.DeleteCapturedRows(retainedTxn); err != nil {
		t.Fatalf("DeleteCapturedRows for second txn failed: %v", err)
	}
	requireCDCSegmentMissing(t, store.cdcLog, 1)
	requireCDCSegmentExists(t, store.cdcLog, 2)
}

func setCurrentCDCSegmentForTest(t *testing.T, log *cdcSegmentLog, segmentID uint64) {
	t.Helper()

	log.mu.Lock()
	defer log.mu.Unlock()
	if log.file != nil {
		if err := log.file.Close(); err != nil {
			t.Fatalf("close current CDC segment failed: %v", err)
		}
	}
	f, err := os.OpenFile(cdcSegmentPath(log.dir, segmentID), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open replacement CDC segment failed: %v", err)
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		t.Fatalf("stat replacement CDC segment failed: %v", err)
	}
	log.file = f
	log.segmentID = segmentID
	log.offset = uint64(stat.Size())
}

func writeCDCSegmentFileForTest(t *testing.T, log *cdcSegmentLog, segmentID uint64) {
	t.Helper()
	if err := os.WriteFile(cdcSegmentPath(log.dir, segmentID), []byte("segment"), 0o644); err != nil {
		t.Fatalf("write CDC segment file failed: %v", err)
	}
}

func writeCDCManifestForTest(t *testing.T, store *PebbleMetaStore, txnID, segmentID uint64) {
	t.Helper()
	manifest := &cdcSegmentTxnManifest{
		TxnID:    txnID,
		RowCount: 1,
		FirstSeq: 1,
		LastSeq:  1,
		Chunks: []cdcSegmentChunk{
			{SegmentID: segmentID, Offset: 0, Length: 1},
		},
	}
	native, err := encoding.MarshalNative(manifest)
	if err != nil {
		t.Fatalf("marshal CDC manifest failed: %v", err)
	}
	defer native.Dispose()
	if err := store.db.Set(pebbleCDCManifestKey(txnID), native.Bytes(), pebble.NoSync); err != nil {
		t.Fatalf("write CDC manifest failed: %v", err)
	}
}

func requireCDCSegmentExists(t *testing.T, log *cdcSegmentLog, segmentID uint64) {
	t.Helper()
	if _, err := os.Stat(cdcSegmentPath(log.dir, segmentID)); err != nil {
		t.Fatalf("CDC segment %d should exist: %v", segmentID, err)
	}
}

func requireCDCSegmentMissing(t *testing.T, log *cdcSegmentLog, segmentID uint64) {
	t.Helper()
	if _, err := os.Stat(cdcSegmentPath(log.dir, segmentID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("CDC segment %d should be missing, stat err=%v", segmentID, err)
	}
}
