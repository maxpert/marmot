package grpc

import (
	"testing"
)

func TestSanitizeSnapshotFilename(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		expected    string
		shouldError bool
	}{
		// Valid cases
		{
			name:        "system database",
			filename:    "__marmot_system.db",
			expected:    "__marmot_system.db",
			shouldError: false,
		},
		{
			name:        "system meta badger vlog",
			filename:    "__marmot_system_meta.badger/000001.vlog",
			expected:    "__marmot_system_meta.badger/000001.vlog",
			shouldError: false,
		},
		{
			name:        "system meta badger manifest",
			filename:    "__marmot_system_meta.badger/MANIFEST",
			expected:    "__marmot_system_meta.badger/MANIFEST",
			shouldError: false,
		},
		{
			name:        "user database in databases dir",
			filename:    "databases/marmot.db",
			expected:    "databases/marmot.db",
			shouldError: false,
		},
		{
			name:        "user meta badger vlog",
			filename:    "databases/marmot_meta.badger/000001.vlog",
			expected:    "databases/marmot_meta.badger/000001.vlog",
			shouldError: false,
		},
		{
			name:        "user meta badger manifest",
			filename:    "databases/marmot_meta.badger/MANIFEST",
			expected:    "databases/marmot_meta.badger/MANIFEST",
			shouldError: false,
		},
		{
			name:        "user database with different name",
			filename:    "databases/myapp.db",
			expected:    "databases/myapp.db",
			shouldError: false,
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

func TestIsValidSnapshotPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"system database", "__marmot_system.db", true},
		{"system meta badger vlog", "__marmot_system_meta.badger/000001.vlog", true},
		{"system meta badger manifest", "__marmot_system_meta.badger/MANIFEST", true},
		{"user database", "databases/marmot.db", true},
		{"user meta badger vlog", "databases/marmot_meta.badger/000001.vlog", true},
		{"user meta badger manifest", "databases/marmot_meta.badger/MANIFEST", true},
		{"user database other name", "databases/test.db", true},
		{"nested db", "databases/subdir/test.db", false},
		{"root db", "marmot.db", false},
		{"wrong extension", "databases/marmot.txt", false},
		{"no extension", "databases/marmot", false},
		{"wrong dir", "data/marmot.db", false},
		{"nested badger file", "databases/foo_meta.badger/subdir/MANIFEST", false},
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
