package snapshot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSanitizeFilename_Valid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"system db", "__marmot_system.db", "__marmot_system.db"},
		{"user db", "databases/marmot.db", "databases/marmot.db"},
		{"user db with name", "databases/myapp.db", "databases/myapp.db"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := SanitizeFilename(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, result)
			}
		})
	}
}

func TestSanitizeFilename_RejectsTraversal(t *testing.T) {
	tests := []string{
		"../etc/passwd",
		"databases/../../../etc/passwd",
		"..\\etc\\passwd",
		"databases/..\\..\\etc\\passwd",
		"databases/foo/../../../etc/passwd",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := SanitizeFilename(input)
			if err == nil {
				t.Errorf("expected error for path traversal: %s", input)
			}
		})
	}
}

func TestSanitizeFilename_RejectsAbsolutePath(t *testing.T) {
	tests := []string{
		"/etc/passwd",
		"/tmp/marmot.db",
		"/var/lib/marmot/databases/marmot.db",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := SanitizeFilename(input)
			if err == nil {
				t.Errorf("expected error for absolute path: %s", input)
			}
		})
	}
}

func TestSanitizeFilename_RejectsEmptyFilename(t *testing.T) {
	_, err := SanitizeFilename("")
	if err == nil {
		t.Error("expected error for empty filename")
	}
}

func TestSanitizeFilename_RejectsInvalidPatterns(t *testing.T) {
	tests := []string{
		"marmot.db",                    // Not in databases/ and not system db
		"databases/subdir/marmot.db",   // Nested directory
		"other/marmot.db",              // Wrong directory
		"databases/marmot.txt",         // Wrong extension
		"__marmot_system.txt",          // Wrong extension for system db
		"databases/",                   // Directory only
		"foo.db",                       // Random db file
		"databases/marmot_meta.pebble", // PebbleDB not allowed
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := SanitizeFilename(input)
			if err == nil {
				t.Errorf("expected error for invalid pattern: %s", input)
			}
		})
	}
}

func TestIsValidSnapshotPath_SystemDB(t *testing.T) {
	if !IsValidSnapshotPath("__marmot_system.db") {
		t.Error("__marmot_system.db should be valid")
	}
}

func TestIsValidSnapshotPath_UserDB(t *testing.T) {
	tests := []string{
		"databases/marmot.db",
		"databases/myapp.db",
		"databases/test_123.db",
	}

	for _, path := range tests {
		if !IsValidSnapshotPath(path) {
			t.Errorf("%s should be valid", path)
		}
	}
}

func TestIsValidSnapshotPath_RejectsInvalid(t *testing.T) {
	tests := []string{
		"marmot.db",
		"databases/subdir/marmot.db",
		"other/marmot.db",
		"databases/marmot.txt",
		"databases/",
		"__marmot_system.txt",
	}

	for _, path := range tests {
		if IsValidSnapshotPath(path) {
			t.Errorf("%s should be invalid", path)
		}
	}
}

func TestCalculateFileSHA256(t *testing.T) {
	// Create temp file with known content
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	content := []byte("hello world")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// SHA256 of "hello world" is known
	expectedHash := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

	hash, err := CalculateFileSHA256(testFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hash != expectedHash {
		t.Errorf("expected %s, got %s", expectedHash, hash)
	}
}

func TestCalculateFileSHA256_FileNotFound(t *testing.T) {
	_, err := CalculateFileSHA256("/nonexistent/file.txt")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestVerifyIntegrity_Success(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	testFile := filepath.Join(tmpDir, "test.db")
	content := []byte("test database content")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Calculate actual hash
	hash, _ := CalculateFileSHA256(testFile)

	manifest := &Manifest{
		Snapshots: []Info{
			{
				Name:     "test",
				Filename: "test.db",
				Size:     int64(len(content)),
				SHA256:   hash,
			},
		},
	}

	if err := VerifyIntegrity(tmpDir, manifest); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestVerifyIntegrity_SizeMismatch(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	testFile := filepath.Join(tmpDir, "test.db")
	content := []byte("test database content")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	hash, _ := CalculateFileSHA256(testFile)

	manifest := &Manifest{
		Snapshots: []Info{
			{
				Name:     "test",
				Filename: "test.db",
				Size:     999, // Wrong size
				SHA256:   hash,
			},
		},
	}

	err := VerifyIntegrity(tmpDir, manifest)
	if err == nil {
		t.Error("expected error for size mismatch")
	}
}

func TestVerifyIntegrity_SHA256Mismatch(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	testFile := filepath.Join(tmpDir, "test.db")
	content := []byte("test database content")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	manifest := &Manifest{
		Snapshots: []Info{
			{
				Name:     "test",
				Filename: "test.db",
				Size:     int64(len(content)),
				SHA256:   "0000000000000000000000000000000000000000000000000000000000000000", // Wrong hash
			},
		},
	}

	err := VerifyIntegrity(tmpDir, manifest)
	if err == nil {
		t.Error("expected error for SHA256 mismatch")
	}
}

func TestVerifyIntegrity_MissingFile(t *testing.T) {
	tmpDir := t.TempDir()

	manifest := &Manifest{
		Snapshots: []Info{
			{
				Name:     "test",
				Filename: "nonexistent.db",
				Size:     100,
				SHA256:   "abc123",
			},
		},
	}

	err := VerifyIntegrity(tmpDir, manifest)
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestVerifyIntegrity_SkipsEmptyChecksum(t *testing.T) {
	tmpDir := t.TempDir()

	// File doesn't need to exist if checksum is empty (backwards compatibility)
	manifest := &Manifest{
		Snapshots: []Info{
			{
				Name:     "test",
				Filename: "test.db",
				Size:     100,
				SHA256:   "", // Empty checksum - should skip
			},
		},
	}

	// Should not error even though file doesn't exist
	if err := VerifyIntegrity(tmpDir, manifest); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestVerifyFileList_Success(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	testFile := filepath.Join(tmpDir, "test.db")
	content := []byte("test database content")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	hash, _ := CalculateFileSHA256(testFile)

	files := []DatabaseFileInfo{
		{
			Name:           "test",
			Filename:       "test.db",
			SizeBytes:      int64(len(content)),
			SHA256Checksum: hash,
		},
	}

	if err := VerifyFileList(tmpDir, files); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
