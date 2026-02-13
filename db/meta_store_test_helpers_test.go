//go:build !sqlite_preupdate_hook
// +build !sqlite_preupdate_hook

package db

import (
	"os"
	"path/filepath"
	"testing"
)

func createTestPebbleMetaStore(t *testing.T) (*PebbleMetaStore, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "pebble_metastore_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	metaPath := filepath.Join(tmpDir, "test_meta.pebble")
	store, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatalf("failed to create pebble meta store: %v", err)
	}

	cleanup := func() {
		_ = store.Close()
		_ = os.RemoveAll(tmpDir)
	}

	return store, cleanup
}

