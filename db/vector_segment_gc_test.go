package db

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPruneSegmentStoreOnStartupRemovesOrphansAndTemps(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	for _, subdir := range []string{"segments", "rowmap", "manifest"} {
		if err := os.MkdirAll(filepath.Join(dir, subdir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string]bool{
		"segments/gen-00000000000000000001.dat":     false,
		"segments/gen-00000000000000000002.dat":     true,
		"segments/gen-00000000000000000003.dat.tmp": false,
		"rowmap/gen-00000000000000000001.rmap":      false,
		"rowmap/gen-00000000000000000002.rmap":      true,
		"manifest/gen-00000000000000000001.mf":      false,
		"manifest/gen-00000000000000000002.mf":      true,
		"manifest/current":                          true,
	}
	for name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	pruneSegmentStoreOnStartup(dir, 2)

	for name, wantExists := range files {
		_, err := os.Stat(filepath.Join(dir, name))
		gotExists := err == nil
		if gotExists != wantExists {
			t.Fatalf("%s exists=%v, want %v", name, gotExists, wantExists)
		}
	}
}
