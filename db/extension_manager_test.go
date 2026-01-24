package db

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestExtensionManager_ResolveExtensionPath_Security(t *testing.T) {
	// Create a temp directory for testing
	tmpDir := t.TempDir()

	// Create a fake extension file
	extPath := filepath.Join(tmpDir, "test-ext.so")
	if err := os.WriteFile(extPath, []byte("fake"), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := &ExtensionManager{
		searchDir:        tmpDir,
		loadedExtensions: make(map[string]string),
	}

	tests := []struct {
		name    string
		extName string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid extension name",
			extName: "test-ext",
			wantErr: false,
		},
		{
			name:    "path traversal attempt with slash",
			extName: "../../../etc/passwd",
			wantErr: true,
			errMsg:  "extension name cannot contain path separators",
		},
		{
			name:    "path traversal attempt with backslash",
			extName: "..\\..\\etc\\passwd",
			wantErr: true,
			errMsg:  "extension name cannot contain path separators",
		},
		{
			name:    "extension not found",
			extName: "nonexistent",
			wantErr: true,
			errMsg:  "extension not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mgr.ResolveExtensionPath(tt.extName)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ResolveExtensionPath() expected error containing %q, got nil", tt.errMsg)
					return
				}
				if tt.errMsg != "" && !containsString(err.Error(), tt.errMsg) {
					t.Errorf("ResolveExtensionPath() error = %v, want error containing %q", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ResolveExtensionPath() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestExtensionManager_ResolveExtensionPath_Suffixes(t *testing.T) {
	// Create a temp directory for testing
	tmpDir := t.TempDir()

	// Determine expected suffix based on platform
	var suffix string
	switch runtime.GOOS {
	case "darwin":
		suffix = ".dylib"
	case "windows":
		suffix = ".dll"
	default:
		suffix = ".so"
	}

	// Create extension files with different patterns
	testCases := []struct {
		filename string
		search   string
	}{
		{"myext" + suffix, "myext"},
		{"libother" + suffix, "other"},
	}

	for _, tc := range testCases {
		extPath := filepath.Join(tmpDir, tc.filename)
		if err := os.WriteFile(extPath, []byte("fake"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	mgr := &ExtensionManager{
		searchDir:        tmpDir,
		loadedExtensions: make(map[string]string),
	}

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			path, err := mgr.ResolveExtensionPath(tc.search)
			if err != nil {
				t.Errorf("ResolveExtensionPath(%q) unexpected error = %v", tc.search, err)
				return
			}

			expectedPath := filepath.Join(tmpDir, tc.filename)
			absExpected, _ := filepath.Abs(expectedPath)
			if path != absExpected {
				t.Errorf("ResolveExtensionPath(%q) = %v, want %v", tc.search, path, absExpected)
			}
		})
	}
}

func TestExtensionManager_LoadExtensionGlobally(t *testing.T) {
	// Create a temp directory for testing
	tmpDir := t.TempDir()

	// Create a fake extension file
	extPath := filepath.Join(tmpDir, "test-ext.so")
	if err := os.WriteFile(extPath, []byte("fake"), 0644); err != nil {
		t.Fatal(err)
	}

	mgr := &ExtensionManager{
		searchDir:        tmpDir,
		loadedExtensions: make(map[string]string),
	}

	// Test loading extension globally (just registration, not actual loading)
	err := mgr.LoadExtensionGlobally("test-ext")
	if err != nil {
		t.Errorf("LoadExtensionGlobally() unexpected error = %v", err)
	}

	// Verify it's registered
	extensions := mgr.GetLoadedExtensions()
	if _, ok := extensions["test-ext"]; !ok {
		t.Error("LoadExtensionGlobally() extension not found in loaded extensions")
	}
}

func TestExtensionManager_NoSearchDir(t *testing.T) {
	mgr := &ExtensionManager{
		searchDir:        "",
		loadedExtensions: make(map[string]string),
	}

	_, err := mgr.ResolveExtensionPath("any-ext")
	if err == nil {
		t.Error("ResolveExtensionPath() expected error for empty search dir, got nil")
	}

	expectedErr := "no extension directory configured"
	if !containsString(err.Error(), expectedErr) {
		t.Errorf("ResolveExtensionPath() error = %v, want error containing %q", err, expectedErr)
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
