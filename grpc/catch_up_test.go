package grpc

import (
	"testing"
)

// TestSanitizeSnapshotFilename tests path sanitization for snapshot files.
// NOTE: MetaStore (PebbleDB) paths are no longer valid - only SQLite .db files.
func TestSanitizeSnapshotFilename(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		expected    string
		shouldError bool
	}{
		// Valid cases - only SQLite .db files
		{
			name:        "system database",
			filename:    "__marmot_system.db",
			expected:    "__marmot_system.db",
			shouldError: false,
		},
		{
			name:        "user database in databases dir",
			filename:    "databases/marmot.db",
			expected:    "databases/marmot.db",
			shouldError: false,
		},
		{
			name:        "user database with different name",
			filename:    "databases/myapp.db",
			expected:    "databases/myapp.db",
			shouldError: false,
		},

		// PebbleDB paths are now INVALID (not included in snapshots)
		{
			name:        "system meta pebble sst - now invalid",
			filename:    "__marmot_system_meta.pebble/000001.sst",
			shouldError: true,
		},
		{
			name:        "system meta pebble manifest - now invalid",
			filename:    "__marmot_system_meta.pebble/MANIFEST",
			shouldError: true,
		},
		{
			name:        "user meta pebble sst - now invalid",
			filename:    "databases/marmot_meta.pebble/000001.sst",
			shouldError: true,
		},
		{
			name:        "user meta pebble manifest - now invalid",
			filename:    "databases/marmot_meta.pebble/MANIFEST",
			shouldError: true,
		},

		// Path traversal attacks
		{
			name:        "simple path traversal",
			filename:    "../etc/passwd",
			shouldError: true,
		},
		{
			name:        "multiple path traversal",
			filename:    "../../../etc/passwd",
			shouldError: true,
		},
		{
			name:        "path traversal in middle",
			filename:    "databases/../../../etc/passwd",
			shouldError: true,
		},
		{
			name:        "hidden path traversal with dots",
			filename:    "databases/foo/../../../etc/passwd",
			shouldError: true,
		},
		{
			name:        "path traversal at end",
			filename:    "databases/foo/..",
			shouldError: true,
		},

		// Absolute paths
		{
			name:        "absolute path unix",
			filename:    "/etc/passwd",
			shouldError: true,
		},
		{
			name:        "absolute path to db",
			filename:    "/var/lib/marmot/databases/marmot.db",
			shouldError: true,
		},

		// Invalid patterns
		{
			name:        "empty filename",
			filename:    "",
			shouldError: true,
		},
		{
			name:        "random file at root",
			filename:    "random.txt",
			shouldError: true,
		},
		{
			name:        "nested db in databases",
			filename:    "databases/subdir/marmot.db",
			shouldError: true,
		},
		{
			name:        "db file at root",
			filename:    "marmot.db",
			shouldError: true,
		},
		{
			name:        "system db with path",
			filename:    "foo/__marmot_system.db",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sanitizeSnapshotFilename(tt.filename)
			if tt.shouldError {
				if err == nil {
					t.Errorf("sanitizeSnapshotFilename(%q) should have returned error, got %q", tt.filename, result)
				}
			} else {
				if err != nil {
					t.Errorf("sanitizeSnapshotFilename(%q) returned unexpected error: %v", tt.filename, err)
				}
				if result != tt.expected {
					t.Errorf("sanitizeSnapshotFilename(%q) = %q, want %q", tt.filename, result, tt.expected)
				}
			}
		})
	}
}

// TestIsValidSnapshotPath tests path validation for snapshot files.
// NOTE: MetaStore (PebbleDB) paths are no longer valid - only SQLite .db files.
func TestIsValidSnapshotPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		// Valid paths - only SQLite .db files
		{"system database", "__marmot_system.db", true},
		{"user database", "databases/marmot.db", true},
		{"user database other name", "databases/test.db", true},

		// PebbleDB paths are now INVALID
		{"system meta pebble sst - now invalid", "__marmot_system_meta.pebble/000001.sst", false},
		{"system meta pebble manifest - now invalid", "__marmot_system_meta.pebble/MANIFEST", false},
		{"user meta pebble sst - now invalid", "databases/marmot_meta.pebble/000001.sst", false},
		{"user meta pebble manifest - now invalid", "databases/marmot_meta.pebble/MANIFEST", false},
		{"nested pebble file", "databases/foo_meta.pebble/subdir/MANIFEST", false},

		// Other invalid paths
		{"nested db", "databases/subdir/test.db", false},
		{"root db", "marmot.db", false},
		{"wrong extension", "databases/marmot.txt", false},
		{"no extension", "databases/marmot", false},
		{"wrong dir", "data/marmot.db", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidSnapshotPath(tt.path)
			if result != tt.expected {
				t.Errorf("isValidSnapshotPath(%q) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}
