package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

const (
	SystemDatabaseName  = "__marmot_system"
	DefaultDatabaseName = "marmot"
)

// DatabaseProvider interface for accessing databases
type DatabaseProvider interface {
	GetDatabase(name string) (*MVCCDatabase, error)
}

// DatabaseManager manages multiple MVCC databases
type DatabaseManager struct {
	mu        sync.RWMutex
	databases map[string]*MVCCDatabase
	systemDB  *MVCCDatabase
	dataDir   string
	nodeID    uint64
	clock     *hlc.Clock
}

// DatabaseMetadata represents database registry information
type DatabaseMetadata struct {
	Name      string
	CreatedAt time.Time
	Path      string
}

// NewDatabaseManager creates a new database manager
func NewDatabaseManager(dataDir string, nodeID uint64, clock *hlc.Clock) (*DatabaseManager, error) {
	dm := &DatabaseManager{
		databases: make(map[string]*MVCCDatabase),
		dataDir:   dataDir,
		nodeID:    nodeID,
		clock:     clock,
	}

	// Create databases directory if it doesn't exist
	dbDir := filepath.Join(dataDir, "databases")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create databases directory: %w", err)
	}

	// Initialize system database
	if err := dm.initSystemDatabase(); err != nil {
		return nil, fmt.Errorf("failed to initialize system database: %w", err)
	}

	// Load existing databases from registry
	if err := dm.loadDatabases(); err != nil {
		return nil, fmt.Errorf("failed to load databases: %w", err)
	}

	// Ensure default database exists
	if err := dm.ensureDefaultDatabase(); err != nil {
		return nil, fmt.Errorf("failed to ensure default database: %w", err)
	}

	log.Info().Int("count", len(dm.databases)).Msg("DatabaseManager initialized")
	return dm, nil
}

// initSystemDatabase initializes the system database for metadata storage
func (dm *DatabaseManager) initSystemDatabase() error {
	systemDBPath := filepath.Join(dm.dataDir, SystemDatabaseName+".db")

	systemDB, err := NewMVCCDatabase(systemDBPath, dm.nodeID, dm.clock)
	if err != nil {
		return fmt.Errorf("failed to create system database: %w", err)
	}

	dm.systemDB = systemDB

	// Create database registry table
	_, err = systemDB.GetDB().Exec(`
		CREATE TABLE IF NOT EXISTS __marmot_databases (
			name TEXT PRIMARY KEY,
			created_at INTEGER NOT NULL,
			path TEXT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create database registry table: %w", err)
	}

	log.Info().Str("path", systemDBPath).Msg("System database initialized")
	return nil
}

// loadDatabases loads all databases from the registry
func (dm *DatabaseManager) loadDatabases() error {
	rows, err := dm.systemDB.GetDB().Query("SELECT name, created_at, path FROM __marmot_databases")
	if err != nil {
		return fmt.Errorf("failed to query database registry: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var meta DatabaseMetadata
		var createdAtNano int64
		if err := rows.Scan(&meta.Name, &createdAtNano, &meta.Path); err != nil {
			log.Error().Err(err).Msg("Failed to scan database metadata")
			continue
		}
		meta.CreatedAt = time.Unix(0, createdAtNano)

		// Open database
		dbPath := filepath.Join(dm.dataDir, meta.Path)
		if err := dm.openDatabase(meta.Name, dbPath); err != nil {
			log.Error().Err(err).Str("name", meta.Name).Msg("Failed to open database")
			continue
		}

		log.Info().Str("name", meta.Name).Str("path", meta.Path).Msg("Loaded database from registry")
	}

	return rows.Err()
}

// openDatabase opens a database and adds it to the registry
func (dm *DatabaseManager) openDatabase(name, path string) error {
	db, err := NewMVCCDatabase(path, dm.nodeID, dm.clock)
	if err != nil {
		return fmt.Errorf("failed to open database %s: %w", name, err)
	}

	dm.databases[name] = db
	return nil
}

// ensureDefaultDatabase ensures the default database exists
func (dm *DatabaseManager) ensureDefaultDatabase() error {
	dm.mu.RLock()
	_, exists := dm.databases[DefaultDatabaseName]
	dm.mu.RUnlock()

	if !exists {
		log.Info().Str("name", DefaultDatabaseName).Msg("Creating default database")
		if err := dm.CreateDatabase(DefaultDatabaseName); err != nil {
			return fmt.Errorf("failed to create default database: %w", err)
		}
	}

	return nil
}

// CreateDatabase creates a new database
func (dm *DatabaseManager) CreateDatabase(name string) error {
	if name == SystemDatabaseName {
		return fmt.Errorf("cannot create system database")
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if database already exists
	if _, exists := dm.databases[name]; exists {
		return fmt.Errorf("database %s already exists", name)
	}

	// Create database file
	dbPath := filepath.Join("databases", name+".db")
	fullPath := filepath.Join(dm.dataDir, dbPath)

	db, err := NewMVCCDatabase(fullPath, dm.nodeID, dm.clock)
	if err != nil {
		return fmt.Errorf("failed to create database file: %w", err)
	}

	// Register in system database
	createdAt := time.Now().UnixNano()
	_, err = dm.systemDB.GetDB().Exec(
		"INSERT INTO __marmot_databases (name, created_at, path) VALUES (?, ?, ?)",
		name, createdAt, dbPath,
	)
	if err != nil {
		db.Close()
		os.Remove(fullPath)
		return fmt.Errorf("failed to register database in system: %w", err)
	}

	dm.databases[name] = db
	log.Info().Str("name", name).Str("path", dbPath).Msg("Database created")
	return nil
}

// DropDatabase drops a database
func (dm *DatabaseManager) DropDatabase(name string) error {
	if name == SystemDatabaseName {
		return fmt.Errorf("cannot drop system database")
	}

	if name == DefaultDatabaseName {
		return fmt.Errorf("cannot drop default database")
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if database exists
	db, exists := dm.databases[name]
	if !exists {
		return fmt.Errorf("database %s does not exist", name)
	}

	// Get path before deletion
	var dbPath string
	err := dm.systemDB.GetDB().QueryRow(
		"SELECT path FROM __marmot_databases WHERE name = ?", name,
	).Scan(&dbPath)
	if err != nil {
		return fmt.Errorf("failed to get database path: %w", err)
	}

	// Remove from registry
	_, err = dm.systemDB.GetDB().Exec("DELETE FROM __marmot_databases WHERE name = ?", name)
	if err != nil {
		return fmt.Errorf("failed to remove database from registry: %w", err)
	}

	// Close database connection
	if err := db.Close(); err != nil {
		log.Error().Err(err).Str("name", name).Msg("Failed to close database")
	}

	// Delete from map
	delete(dm.databases, name)

	// Delete database files
	fullPath := filepath.Join(dm.dataDir, dbPath)
	if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
		log.Error().Err(err).Str("path", fullPath).Msg("Failed to delete database file")
	}

	// Delete WAL and SHM files
	os.Remove(fullPath + "-wal")
	os.Remove(fullPath + "-shm")

	log.Info().Str("name", name).Msg("Database dropped")
	return nil
}

// GetDatabase returns a database by name
func (dm *DatabaseManager) GetDatabase(name string) (*MVCCDatabase, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, exists := dm.databases[name]
	if !exists {
		return nil, fmt.Errorf("database %s does not exist", name)
	}

	return db, nil
}

// DatabaseExists checks if a database exists
func (dm *DatabaseManager) DatabaseExists(name string) bool {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	_, exists := dm.databases[name]
	return exists
}

// ListDatabases returns all database names
func (dm *DatabaseManager) ListDatabases() []string {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	names := make([]string, 0, len(dm.databases))
	for name := range dm.databases {
		names = append(names, name)
	}

	return names
}

// GetSystemDatabase returns the system database
func (dm *DatabaseManager) GetSystemDatabase() *MVCCDatabase {
	return dm.systemDB
}

// Close closes all databases
func (dm *DatabaseManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	var lastErr error

	// Close all user databases
	for name, db := range dm.databases {
		if err := db.Close(); err != nil {
			log.Error().Err(err).Str("name", name).Msg("Failed to close database")
			lastErr = err
		}
	}

	// Close system database
	if err := dm.systemDB.Close(); err != nil {
		log.Error().Err(err).Msg("Failed to close system database")
		lastErr = err
	}

	log.Info().Msg("DatabaseManager closed")
	return lastErr
}

// MigrateFromLegacy migrates from single database to multi-database structure
func MigrateFromLegacy(oldDBPath, newDataDir string, nodeID uint64, clock *hlc.Clock) error {
	// Check if old database exists
	if _, err := os.Stat(oldDBPath); os.IsNotExist(err) {
		// No migration needed
		return nil
	}

	log.Info().Str("from", oldDBPath).Str("to", newDataDir).Msg("Migrating from legacy database")

	// Create new structure
	dbDir := filepath.Join(newDataDir, "databases")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create databases directory: %w", err)
	}

	// Move old database to new location
	newPath := filepath.Join(dbDir, DefaultDatabaseName+".db")
	if err := os.Rename(oldDBPath, newPath); err != nil {
		return fmt.Errorf("failed to move database: %w", err)
	}

	// Move WAL and SHM files if they exist
	os.Rename(oldDBPath+"-wal", newPath+"-wal")
	os.Rename(oldDBPath+"-shm", newPath+"-shm")

	log.Info().Msg("Legacy database migration completed")
	return nil
}
