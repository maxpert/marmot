package db

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// ImportExistingDatabases scans a directory for existing SQLite .db files
// and imports them into the database manager. This is used on first startup
// of a seed node to make existing databases available.
func (dm *DatabaseManager) ImportExistingDatabases(importDir string) (int, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if importDir == "" {
		return 0, nil
	}

	// Check if import directory exists
	info, err := os.Stat(importDir)
	if os.IsNotExist(err) {
		log.Debug().Str("dir", importDir).Msg("Import directory does not exist, skipping")
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to stat import directory: %w", err)
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("import path is not a directory: %s", importDir)
	}

	// Scan for .db files
	entries, err := os.ReadDir(importDir)
	if err != nil {
		return 0, fmt.Errorf("failed to read import directory: %w", err)
	}

	imported := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".db") {
			continue
		}

		// Skip system database files
		if strings.HasPrefix(name, "__marmot") {
			continue
		}

		// Skip WAL and SHM files
		if strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-shm") {
			continue
		}

		// Extract database name (remove .db suffix)
		dbName := strings.TrimSuffix(name, ".db")

		// Skip if already exists
		if _, exists := dm.databases[dbName]; exists {
			log.Debug().Str("name", dbName).Msg("Database already exists, skipping import")
			continue
		}

		// Copy database file to databases directory
		srcPath := filepath.Join(importDir, name)
		dstPath := filepath.Join(dm.dataDir, "databases", name)

		if err := copyFile(srcPath, dstPath); err != nil {
			log.Warn().Err(err).Str("name", dbName).Msg("Failed to copy database file")
			continue
		}

		// Copy WAL and SHM files if they exist
		copyFile(srcPath+"-wal", dstPath+"-wal")
		copyFile(srcPath+"-shm", dstPath+"-shm")

		// Open and register the database
		db, err := NewMVCCDatabase(dstPath, dm.nodeID, dm.clock)
		if err != nil {
			log.Warn().Err(err).Str("name", dbName).Msg("Failed to open imported database")
			os.Remove(dstPath)
			continue
		}

		// Register in system database
		createdAt := time.Now().UnixNano()
		relPath := filepath.Join("databases", name)
		_, err = dm.systemDB.GetDB().Exec(
			"INSERT OR IGNORE INTO __marmot_databases (name, created_at, path) VALUES (?, ?, ?)",
			dbName, createdAt, relPath,
		)
		if err != nil {
			log.Warn().Err(err).Str("name", dbName).Msg("Failed to register imported database")
			db.Close()
			continue
		}

		dm.databases[dbName] = db
		imported++
		log.Info().Str("name", dbName).Str("src", srcPath).Msg("Imported existing database")
	}

	return imported, nil
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// SnapshotInfo contains information about a database file for snapshot transfer
type SnapshotInfo struct {
	Name     string // Database name (e.g., "marmot", "__marmot_system")
	Filename string // Relative path from data directory
	FullPath string // Absolute path
	Size     int64  // File size in bytes
}

// TakeSnapshot checkpoints all databases and returns their file information
// This should be called before streaming snapshot data to ensure consistency
func (dm *DatabaseManager) TakeSnapshot() ([]SnapshotInfo, uint64, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	var snapshots []SnapshotInfo

	// Checkpoint and get info for system database
	systemDBPath := filepath.Join(dm.dataDir, SystemDatabaseName+".db")
	if err := dm.checkpointDatabase(dm.systemDB); err != nil {
		return nil, 0, fmt.Errorf("failed to checkpoint system database: %w", err)
	}

	info, err := os.Stat(systemDBPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to stat system database: %w", err)
	}

	snapshots = append(snapshots, SnapshotInfo{
		Name:     SystemDatabaseName,
		Filename: SystemDatabaseName + ".db",
		FullPath: systemDBPath,
		Size:     info.Size(),
	})

	// Checkpoint and get info for all user databases
	for name, db := range dm.databases {
		if err := dm.checkpointDatabase(db); err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to checkpoint database")
			continue
		}

		dbPath := filepath.Join(dm.dataDir, "databases", name+".db")
		info, err := os.Stat(dbPath)
		if err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to stat database")
			continue
		}

		snapshots = append(snapshots, SnapshotInfo{
			Name:     name,
			Filename: filepath.Join("databases", name+".db"),
			FullPath: dbPath,
			Size:     info.Size(),
		})
	}

	// Get max committed transaction ID
	maxTxnID, err := dm.GetMaxCommittedTxnID()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get max txn id: %w", err)
	}

	log.Info().
		Int("databases", len(snapshots)).
		Uint64("max_txn_id", maxTxnID).
		Msg("Snapshot prepared")

	return snapshots, maxTxnID, nil
}

// checkpointDatabase forces a WAL checkpoint to ensure data is in the main database file
func (dm *DatabaseManager) checkpointDatabase(db *MVCCDatabase) error {
	_, err := db.GetDB().Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

// GetMaxCommittedTxnID returns the highest committed transaction ID across all databases
func (dm *DatabaseManager) GetMaxCommittedTxnID() (uint64, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	var maxTxnID uint64

	// Check all user databases
	for name, db := range dm.databases {
		var dbMax uint64
		err := db.GetDB().QueryRow(`
			SELECT COALESCE(MAX(txn_id), 0)
			FROM __marmot__txn_records
			WHERE status = 'COMMITTED'
		`).Scan(&dbMax)
		if err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to get max txn_id")
			continue
		}
		if dbMax > maxTxnID {
			maxTxnID = dbMax
		}
	}

	return maxTxnID, nil
}

// GetDataDir returns the data directory path
func (dm *DatabaseManager) GetDataDir() string {
	return dm.dataDir
}

// ReplicationState tracks replication progress with a peer node
type ReplicationState struct {
	PeerNodeID        uint64
	LastAppliedTxnID  uint64
	LastAppliedTSWall int64
	LastAppliedTSLog  int32
	LastSyncTime      int64
	SyncStatus        string // SYNCED, CATCHING_UP, FAILED
}

// GetReplicationState gets the replication state for a specific peer
func (dm *DatabaseManager) GetReplicationState(peerNodeID uint64) (*ReplicationState, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	// Check default database for replication state
	defaultDB, ok := dm.databases[DefaultDatabaseName]
	if !ok {
		return nil, fmt.Errorf("default database not found")
	}

	var state ReplicationState
	err := defaultDB.GetDB().QueryRow(`
		SELECT peer_node_id, last_applied_txn_id, last_applied_ts_wall,
		       last_applied_ts_logical, last_sync_time, sync_status
		FROM __marmot__replication_state
		WHERE peer_node_id = ?
	`, peerNodeID).Scan(
		&state.PeerNodeID, &state.LastAppliedTxnID, &state.LastAppliedTSWall,
		&state.LastAppliedTSLog, &state.LastSyncTime, &state.SyncStatus,
	)
	if err != nil {
		return nil, err
	}
	return &state, nil
}

// UpdateReplicationState updates or inserts replication state for a peer
func (dm *DatabaseManager) UpdateReplicationState(state *ReplicationState) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	defaultDB, ok := dm.databases[DefaultDatabaseName]
	if !ok {
		return fmt.Errorf("default database not found")
	}

	_, err := defaultDB.GetDB().Exec(`
		INSERT OR REPLACE INTO __marmot__replication_state
		(peer_node_id, last_applied_txn_id, last_applied_ts_wall, last_applied_ts_logical, last_sync_time, sync_status)
		VALUES (?, ?, ?, ?, ?, ?)
	`, state.PeerNodeID, state.LastAppliedTxnID, state.LastAppliedTSWall,
		state.LastAppliedTSLog, state.LastSyncTime, state.SyncStatus)
	return err
}

// GetAllReplicationStates returns replication state for all known peers
func (dm *DatabaseManager) GetAllReplicationStates() ([]ReplicationState, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	defaultDB, ok := dm.databases[DefaultDatabaseName]
	if !ok {
		return nil, fmt.Errorf("default database not found")
	}

	rows, err := defaultDB.GetDB().Query(`
		SELECT peer_node_id, last_applied_txn_id, last_applied_ts_wall,
		       last_applied_ts_logical, last_sync_time, sync_status
		FROM __marmot__replication_state
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var states []ReplicationState
	for rows.Next() {
		var s ReplicationState
		if err := rows.Scan(&s.PeerNodeID, &s.LastAppliedTxnID, &s.LastAppliedTSWall,
			&s.LastAppliedTSLog, &s.LastSyncTime, &s.SyncStatus); err != nil {
			return nil, err
		}
		states = append(states, s)
	}
	return states, nil
}
