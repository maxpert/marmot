package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
)

// SanitizeFilename validates and sanitizes a filename from snapshot stream.
// Returns error if filename contains path traversal or absolute paths.
// This prevents malicious servers from writing files outside the data directory.
func SanitizeFilename(filename string) (string, error) {
	if filename == "" {
		return "", fmt.Errorf("empty filename")
	}

	// Reject absolute paths
	if filepath.IsAbs(filename) {
		return "", fmt.Errorf("absolute path not allowed: %s", filename)
	}

	// Clean the path and check for traversal
	cleaned := filepath.Clean(filename)

	// Reject if cleaned path starts with ..
	if strings.HasPrefix(cleaned, "..") {
		return "", fmt.Errorf("path traversal not allowed: %s", filename)
	}

	// Reject if path contains .. anywhere after cleaning
	for _, part := range strings.Split(cleaned, string(filepath.Separator)) {
		if part == ".." {
			return "", fmt.Errorf("path traversal not allowed: %s", filename)
		}
	}

	// Only allow specific patterns for snapshot files
	if !IsValidSnapshotPath(cleaned) {
		return "", fmt.Errorf("invalid snapshot filename pattern: %s", filename)
	}

	return cleaned, nil
}

// IsValidSnapshotPath checks if the path matches expected snapshot file patterns.
// NOTE: Only SQLite .db files are included in snapshots.
// MetaStore (PebbleDB) directories are NOT included due to race conditions
// from WAL rotation and compaction during snapshot streaming.
func IsValidSnapshotPath(path string) bool {
	// System database at root
	if path == "__marmot_system.db" {
		return true
	}
	// User databases (.db files) in databases/ subdirectory
	if strings.HasPrefix(path, "databases"+string(filepath.Separator)) && strings.HasSuffix(path, ".db") {
		// Ensure no additional directory traversal within databases/
		relPath := strings.TrimPrefix(path, "databases"+string(filepath.Separator))
		if !strings.Contains(relPath, string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// CalculateFileSHA256 computes SHA256 checksum of a file
func CalculateFileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// VerifyIntegrity verifies downloaded snapshot files against the manifest.
// It checks file existence, size, and SHA256 checksum for each file.
func VerifyIntegrity(dir string, manifest *Manifest) error {
	for _, expected := range manifest.Snapshots {
		if err := verifyFile(dir, expected); err != nil {
			return err
		}
	}
	return nil
}

// VerifyFileList verifies files against a list of DatabaseFileInfo (gRPC format)
func VerifyFileList(dir string, files []DatabaseFileInfo) error {
	for _, expected := range files {
		// Skip verification if no checksum provided (backwards compatibility)
		if expected.SHA256Checksum == "" {
			continue
		}

		filePath := filepath.Join(dir, expected.Filename)

		// Verify file exists
		info, err := os.Stat(filePath)
		if err != nil {
			return fmt.Errorf("missing file %s: %w", expected.Filename, err)
		}

		// Verify size
		if info.Size() != expected.SizeBytes {
			return fmt.Errorf("size mismatch for %s: expected %d, got %d",
				expected.Filename, expected.SizeBytes, info.Size())
		}

		// Verify SHA256
		actualHash, err := CalculateFileSHA256(filePath)
		if err != nil {
			return fmt.Errorf("failed to hash %s: %w", expected.Filename, err)
		}
		if actualHash != expected.SHA256Checksum {
			return fmt.Errorf("SHA256 mismatch for %s: expected %s, got %s",
				expected.Filename, expected.SHA256Checksum, actualHash)
		}

		log.Debug().
			Str("file", expected.Filename).
			Str("sha256", actualHash[:16]+"...").
			Msg("Verified snapshot file integrity")
	}
	return nil
}

// verifyFile verifies a single file against expected Info
func verifyFile(dir string, expected Info) error {
	// Skip verification if no checksum provided
	if expected.SHA256 == "" {
		return nil
	}

	filePath := filepath.Join(dir, expected.Filename)

	// Verify file exists
	info, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("missing file %s: %w", expected.Filename, err)
	}

	// Verify size
	if info.Size() != expected.Size {
		return fmt.Errorf("size mismatch for %s: expected %d, got %d",
			expected.Filename, expected.Size, info.Size())
	}

	// Verify SHA256
	actualHash, err := CalculateFileSHA256(filePath)
	if err != nil {
		return fmt.Errorf("failed to hash %s: %w", expected.Filename, err)
	}
	if actualHash != expected.SHA256 {
		return fmt.Errorf("SHA256 mismatch for %s: expected %s, got %s",
			expected.Filename, expected.SHA256, actualHash)
	}

	log.Debug().
		Str("file", expected.Filename).
		Str("sha256", actualHash[:16]+"...").
		Msg("Verified snapshot file integrity")

	return nil
}
