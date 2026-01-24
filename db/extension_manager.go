package db

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// ExtensionManager handles SQLite extension loading and management.
// It tracks which extensions have been loaded globally so they can be
// loaded into new connections via the ConnectHook.
type ExtensionManager struct {
	searchDir string
	mu        sync.RWMutex
	// loadedExtensions tracks extensions that should be loaded into all connections.
	// Key is the extension name, value is the resolved path.
	loadedExtensions map[string]string
	// alwaysLoaded is the list of extensions to load on every connection
	alwaysLoaded []string
}

var (
	globalExtMgr     *ExtensionManager
	globalExtMgrOnce sync.Once
)

// InitExtensionManager initializes the global extension manager.
// Must be called before any database connections are opened.
func InitExtensionManager(searchDir string, alwaysLoaded []string) error {
	var initErr error
	globalExtMgrOnce.Do(func() {
		mgr := &ExtensionManager{
			searchDir:        searchDir,
			loadedExtensions: make(map[string]string),
			alwaysLoaded:     alwaysLoaded,
		}

		// Validate search directory if specified
		if searchDir != "" {
			info, err := os.Stat(searchDir)
			if err != nil {
				if os.IsNotExist(err) {
					initErr = fmt.Errorf("extension directory does not exist: %s", searchDir)
					return
				}
				initErr = fmt.Errorf("failed to stat extension directory: %w", err)
				return
			}
			if !info.IsDir() {
				initErr = fmt.Errorf("extension path is not a directory: %s", searchDir)
				return
			}
		}

		// Pre-resolve always_loaded extensions
		for _, name := range alwaysLoaded {
			path, err := mgr.ResolveExtensionPath(name)
			if err != nil {
				initErr = fmt.Errorf("failed to resolve always_loaded extension %q: %w", name, err)
				return
			}
			mgr.loadedExtensions[name] = path
			log.Info().Str("extension", name).Str("path", path).Msg("Registered always_loaded extension")
		}

		globalExtMgr = mgr

		// Register with protocol package to avoid import cycles
		protocol.RegisterExtensionLoader(mgr)
	})

	return initErr
}

// GetExtensionManager returns the global extension manager.
// Returns nil if not initialized.
func GetExtensionManager() *ExtensionManager {
	return globalExtMgr
}

// ResolveExtensionPath resolves an extension name to its full path.
// It tries multiple suffixes based on the platform:
// - Linux: .so, lib<name>.so
// - macOS: .dylib, lib<name>.dylib
// - Windows: .dll
func (m *ExtensionManager) ResolveExtensionPath(name string) (string, error) {
	// Security: reject names with path separators
	if strings.ContainsAny(name, "/\\") {
		return "", fmt.Errorf("extension name cannot contain path separators: %s", name)
	}

	if m.searchDir == "" {
		return "", fmt.Errorf("no extension directory configured")
	}

	// Determine platform-specific suffixes
	var suffixes []string
	switch runtime.GOOS {
	case "darwin":
		suffixes = []string{"", ".dylib", ".so"}
	case "windows":
		suffixes = []string{"", ".dll"}
	default: // Linux and others
		suffixes = []string{"", ".so"}
	}

	// Try each pattern
	patterns := []string{name}
	if !strings.HasPrefix(name, "lib") {
		patterns = append(patterns, "lib"+name)
	}

	for _, pattern := range patterns {
		for _, suffix := range suffixes {
			candidate := filepath.Join(m.searchDir, pattern+suffix)
			if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
				// Security: verify resolved path is under searchDir
				absCandidate, err := filepath.Abs(candidate)
				if err != nil {
					continue
				}
				absSearchDir, err := filepath.Abs(m.searchDir)
				if err != nil {
					continue
				}
				if !strings.HasPrefix(absCandidate, absSearchDir+string(os.PathSeparator)) &&
					absCandidate != absSearchDir {
					return "", fmt.Errorf("resolved path escapes search directory: %s", absCandidate)
				}
				return absCandidate, nil
			}
		}
	}

	return "", fmt.Errorf("extension not found: %s (searched in %s)", name, m.searchDir)
}

// LoadExtension loads an extension into a specific connection.
func (m *ExtensionManager) LoadExtension(conn *sqlite3.SQLiteConn, name string) error {
	path, err := m.ResolveExtensionPath(name)
	if err != nil {
		return err
	}

	return m.loadExtensionByPath(conn, path)
}

// loadExtensionByPath loads an extension from a resolved path.
func (m *ExtensionManager) loadExtensionByPath(conn *sqlite3.SQLiteConn, path string) error {
	if err := conn.LoadExtension(path, ""); err != nil {
		return fmt.Errorf("failed to load extension %s: %w", path, err)
	}
	return nil
}

// LoadExtensionGlobally marks an extension to be loaded into all connections.
// It also loads it into any existing connections (via the connect hook on new connections).
func (m *ExtensionManager) LoadExtensionGlobally(name string) error {
	path, err := m.ResolveExtensionPath(name)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.loadedExtensions[name] = path
	m.mu.Unlock()

	log.Info().Str("extension", name).Str("path", path).Msg("Extension registered globally")
	return nil
}

// GetLoadedExtensions returns the list of extensions that should be loaded into connections.
func (m *ExtensionManager) GetLoadedExtensions() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]string, len(m.loadedExtensions))
	for k, v := range m.loadedExtensions {
		result[k] = v
	}
	return result
}

// LoadAllExtensions loads all registered extensions into a connection.
// This is called from the SQLite ConnectHook.
func (m *ExtensionManager) LoadAllExtensions(conn *sqlite3.SQLiteConn) error {
	m.mu.RLock()
	extensions := make(map[string]string, len(m.loadedExtensions))
	for k, v := range m.loadedExtensions {
		extensions[k] = v
	}
	m.mu.RUnlock()

	for name, path := range extensions {
		if err := m.loadExtensionByPath(conn, path); err != nil {
			log.Error().Err(err).Str("extension", name).Msg("Failed to load extension")
			return err
		}
	}

	return nil
}
