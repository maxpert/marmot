package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/publisher"
	"github.com/rs/zerolog/log"
)

const (
	SystemDatabaseName  = "__marmot_system"
	DefaultDatabaseName = "marmot"
)

// DatabaseProvider interface for accessing databases
type DatabaseProvider interface {
	GetDatabase(name string) (*ReplicatedDatabase, error)
}

// DatabaseManager manages multiple MVCC databases
type DatabaseManager struct {
	mu        sync.RWMutex
	databases map[string]*ReplicatedDatabase
	systemDB  *ReplicatedDatabase
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
		databases: make(map[string]*ReplicatedDatabase),
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
// System database has its own MetaStore for CREATE/DROP DATABASE 2PC tracking
func (dm *DatabaseManager) initSystemDatabase() error {
	systemDBPath := filepath.Join(dm.dataDir, SystemDatabaseName+".db")

	// Create MetaStore for system database (for CREATE/DROP DATABASE transactions)
	metaStore, err := NewMetaStore(systemDBPath)
	if err != nil {
		return fmt.Errorf("failed to create system meta store: %w", err)
	}

	systemDB, err := NewReplicatedDatabase(systemDBPath, dm.nodeID, dm.clock, metaStore)
	if err != nil {
		metaStore.Close()
		return fmt.Errorf("failed to create system database: %w", err)
	}

	// Wire up GC coordination for system database
	dm.wireGCCoordination(systemDB, SystemDatabaseName)

	dm.systemDB = systemDB

	// Add system database to the databases map so it can be retrieved via GetDatabase()
	dm.databases[SystemDatabaseName] = systemDB

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
// Creates a MetaStore for the database (stored in dbname_meta.pebble/)
func (dm *DatabaseManager) openDatabase(name, path string) error {
	// Create MetaStore for this database
	metaStore, err := NewMetaStore(path)
	if err != nil {
		return fmt.Errorf("failed to create meta store for %s: %w", name, err)
	}

	db, err := NewReplicatedDatabase(path, dm.nodeID, dm.clock, metaStore)
	if err != nil {
		metaStore.Close()
		return fmt.Errorf("failed to open database %s: %w", name, err)
	}

	// Wire up GC coordination for transaction log retention
	dm.wireGCCoordination(db, name)

	dm.databases[name] = db
	return nil
}

// wireGCCoordination sets up GC safe point tracking for a database
// This ensures transaction logs are retained until all peers have applied them
func (dm *DatabaseManager) wireGCCoordination(mdb *ReplicatedDatabase, dbName string) {
	txnMgr := mdb.GetTransactionManager()
	txnMgr.SetDatabaseName(dbName)
	txnMgr.SetMinAppliedTxnIDFunc(dm.GetMinAppliedTxnID)
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

// CreateDatabase creates a new database with its own MetaStore
func (dm *DatabaseManager) CreateDatabase(name string) error {
	if name == SystemDatabaseName {
		return fmt.Errorf("cannot create system database")
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if database already exists - return success for idempotency (IF NOT EXISTS semantics)
	if _, exists := dm.databases[name]; exists {
		log.Debug().Str("database", name).Msg("Database already exists, returning success")
		return nil
	}

	// Create database file
	dbPath := filepath.Join("databases", name+".db")
	fullPath := filepath.Join(dm.dataDir, dbPath)

	// Create MetaStore for this database
	metaStore, err := NewMetaStore(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create meta store: %w", err)
	}

	db, err := NewReplicatedDatabase(fullPath, dm.nodeID, dm.clock, metaStore)
	if err != nil {
		metaStore.Close()
		cleanupMetaStoreFiles(fullPath)
		return fmt.Errorf("failed to create database file: %w", err)
	}

	// Wire up GC coordination for newly created database
	dm.wireGCCoordination(db, name)

	// Register in system database
	createdAt := time.Now().UnixNano()
	_, err = dm.systemDB.GetDB().Exec(
		"INSERT INTO __marmot_databases (name, created_at, path) VALUES (?, ?, ?)",
		name, createdAt, dbPath,
	)
	if err != nil {
		db.Close()
		metaStore.Close()
		os.Remove(fullPath)
		cleanupMetaStoreFiles(fullPath)
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

	// Delete meta database directory (PebbleDB)
	metaPath := strings.TrimSuffix(fullPath, ".db") + "_meta.pebble"
	os.RemoveAll(metaPath)

	log.Info().Str("name", name).Msg("Database dropped")
	return nil
}

// GetDatabase returns a database by name
func (dm *DatabaseManager) GetDatabase(name string) (*ReplicatedDatabase, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, exists := dm.databases[name]
	if !exists {
		return nil, fmt.Errorf("database %s does not exist", name)
	}

	return db, nil
}

// GetDatabaseConnection returns the *sql.DB for a database
func (dm *DatabaseManager) GetDatabaseConnection(name string) (*sql.DB, error) {
	replicatedDB, err := dm.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	return replicatedDB.GetDB(), nil
}

// GetReplicatedDatabase returns the ReplicatedDatabase as coordinator.ReplicatedDatabaseProvider
func (dm *DatabaseManager) GetReplicatedDatabase(name string) (coordinator.ReplicatedDatabaseProvider, error) {
	return dm.GetDatabase(name)
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
		// Exclude system database from user-visible list
		if name != SystemDatabaseName {
			names = append(names, name)
		}
	}

	return names
}

// GetSystemDatabase returns the system database
func (dm *DatabaseManager) GetSystemDatabase() *ReplicatedDatabase {
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

// ReopenDatabase atomically swaps in a new database connection after snapshot apply.
// Uses "create-swap-close" pattern to avoid closing connections while queries are in-flight:
// 1. Create new database connection (before closing old one)
// 2. Atomically swap in the new connection (under lock)
// 3. Close old database after a delay (in goroutine, lets in-flight queries complete)
func (dm *DatabaseManager) ReopenDatabase(name string) error {
	dm.mu.Lock()

	// Get the existing database
	oldDB, exists := dm.databases[name]
	if !exists {
		dm.mu.Unlock()
		return fmt.Errorf("database %s does not exist", name)
	}

	// Get the path from system database before closing
	var dbPath string
	err := dm.systemDB.GetDB().QueryRow(
		"SELECT path FROM __marmot_databases WHERE name = ?", name,
	).Scan(&dbPath)
	if err != nil {
		dm.mu.Unlock()
		return fmt.Errorf("failed to get database path: %w", err)
	}

	fullPath := filepath.Join(dm.dataDir, dbPath)

	// Get old MetaStore and close it FIRST to release PebbleDB lock
	// This must happen before creating the new MetaStore
	oldMetaStore := oldDB.GetMetaStore()

	// Close old database and MetaStore synchronously (we hold the mutex, so no new queries can start)
	log.Info().Str("name", name).Msg("Closing old database for snapshot reload")
	if err := oldDB.Close(); err != nil {
		log.Warn().Err(err).Str("name", name).Msg("Error closing old database connection")
	}
	if oldMetaStore != nil {
		if err := oldMetaStore.Close(); err != nil {
			dm.mu.Unlock()
			return fmt.Errorf("failed to close old meta store for %s: %w", name, err)
		}
	}

	// Now create new MetaStore (PebbleDB lock is released)
	metaStore, err := NewMetaStore(fullPath)
	if err != nil {
		dm.mu.Unlock()
		return fmt.Errorf("failed to create meta store for %s: %w", name, err)
	}

	// Create new database connection
	newDB, err := NewReplicatedDatabase(fullPath, dm.nodeID, dm.clock, metaStore)
	if err != nil {
		metaStore.Close()
		dm.mu.Unlock()
		return fmt.Errorf("failed to reopen database %s: %w", name, err)
	}

	// Wire up GC coordination for new connection
	dm.wireGCCoordination(newDB, name)

	// Atomically swap in the new connection
	dm.databases[name] = newDB
	dm.mu.Unlock()

	log.Info().Str("name", name).Msg("Database connection swapped after snapshot")

	return nil
}

// CloseDatabaseConnections closes SQLite connections for a database.
// Used by replicas BEFORE snapshot file replacement. Must call OpenDatabaseConnections after.
func (dm *DatabaseManager) CloseDatabaseConnections(name string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	mdb, exists := dm.databases[name]
	if !exists {
		return fmt.Errorf("database %s does not exist", name)
	}

	mdb.CloseSQLiteConnections()
	log.Info().Str("database", name).Msg("Closed SQLite connections for snapshot apply")
	return nil
}

// OpenDatabaseConnections opens SQLite connections for a database.
// Used by replicas AFTER snapshot file replacement.
func (dm *DatabaseManager) OpenDatabaseConnections(name string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	mdb, exists := dm.databases[name]
	if !exists {
		return fmt.Errorf("database %s does not exist", name)
	}

	// Get database path
	var dbPath string
	err := dm.systemDB.GetDB().QueryRow(
		"SELECT path FROM __marmot_databases WHERE name = ?", name,
	).Scan(&dbPath)
	if err != nil {
		return fmt.Errorf("failed to get database path: %w", err)
	}

	fullPath := filepath.Join(dm.dataDir, dbPath)
	if err := mdb.OpenSQLiteConnections(fullPath); err != nil {
		return fmt.Errorf("failed to open connections for %s: %w", name, err)
	}

	log.Info().Str("database", name).Str("path", fullPath).Msg("Opened SQLite connections after snapshot apply")
	return nil
}

// GetDatabasePath returns the full path to a database file.
func (dm *DatabaseManager) GetDatabasePath(name string) (string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	var dbPath string
	err := dm.systemDB.GetDB().QueryRow(
		"SELECT path FROM __marmot_databases WHERE name = ?", name,
	).Scan(&dbPath)
	if err != nil {
		return "", fmt.Errorf("failed to get database path: %w", err)
	}

	return filepath.Join(dm.dataDir, dbPath), nil
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

		// Check if database already exists
		if existingDB, exists := dm.databases[dbName]; exists {
			// Check if existing database is empty (no user tables)
			// If so, we can replace it with the imported version
			srcPath := filepath.Join(importDir, name)
			if !dm.shouldReplaceWithImport(existingDB, srcPath) {
				log.Debug().Str("name", dbName).Msg("Database already exists with data, skipping import")
				continue
			}
			// Close existing empty database before replacing
			log.Info().Str("name", dbName).Msg("Replacing empty database with imported version")
			existingDB.Close()
			delete(dm.databases, dbName)
		}

		// Copy database file to databases directory
		srcPath := filepath.Join(importDir, name)
		dstPath := filepath.Join(dm.dataDir, "databases", name)

		if err := copyFile(srcPath, dstPath); err != nil {
			log.Warn().Err(err).Str("name", dbName).Msg("Failed to copy database file")
			continue
		}

		// Copy WAL and SHM files if they exist (ignore errors if files don't exist)
		if err := copyFile(srcPath+"-wal", dstPath+"-wal"); err != nil && !os.IsNotExist(err) {
			log.Warn().Err(err).Str("name", dbName).Msg("Failed to copy WAL file")
		}
		if err := copyFile(srcPath+"-shm", dstPath+"-shm"); err != nil && !os.IsNotExist(err) {
			log.Warn().Err(err).Str("name", dbName).Msg("Failed to copy SHM file")
		}

		// Create MetaStore for this imported database
		metaStore, err := NewMetaStore(dstPath)
		if err != nil {
			log.Warn().Err(err).Str("name", dbName).Msg("Failed to create meta store for imported database")
			os.Remove(dstPath)
			continue
		}

		// Open and register the database
		db, err := NewReplicatedDatabase(dstPath, dm.nodeID, dm.clock, metaStore)
		if err != nil {
			log.Warn().Err(err).Str("name", dbName).Msg("Failed to open imported database")
			metaStore.Close()
			os.Remove(dstPath)
			cleanupMetaStoreFiles(dstPath)
			continue
		}

		// Wire up GC coordination for imported database
		dm.wireGCCoordination(db, dbName)

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

// shouldReplaceWithImport checks if an existing database should be replaced with an imported version.
// Returns true if the existing database is empty (no user tables) and the source has tables.
func (dm *DatabaseManager) shouldReplaceWithImport(existingDB *ReplicatedDatabase, srcPath string) bool {
	// Check if existing database has any user tables
	rows, err := existingDB.GetDB().Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		log.Debug().Err(err).Msg("Failed to get existing tables, not replacing")
		return false
	}
	hasExistingTables := rows.Next()
	rows.Close()

	if hasExistingTables {
		// Existing database has data, don't replace
		return false
	}

	// Existing is empty, check if source has tables
	srcDB, err := sql.Open(SQLiteDriverName, srcPath+"?mode=ro")
	if err != nil {
		log.Debug().Err(err).Msg("Failed to open source database")
		return false
	}
	defer srcDB.Close()

	rows, err = srcDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		log.Debug().Err(err).Msg("Failed to query source tables")
		return false
	}
	defer rows.Close()

	hasSourceTables := rows.Next()
	return hasSourceTables
}

// copyFile copies a file from src to dst with fsync for durability
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

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		return err
	}
	return dstFile.Sync()
}

// cleanupMetaStoreFiles removes MetaStore directory for a database.
func cleanupMetaStoreFiles(dbPath string) {
	basePath := strings.TrimSuffix(dbPath, ".db")
	os.RemoveAll(basePath + "_meta.pebble")
}

// SnapshotInfo contains information about a database file for snapshot transfer
type SnapshotInfo struct {
	Name     string // Database name (e.g., "marmot", "__marmot_system")
	Filename string // Relative path from data directory
	FullPath string // Absolute path
	Size     int64  // File size in bytes
	SHA256   string // SHA256 hex digest for integrity verification
}

// calculateFileSHA256 computes SHA256 checksum of a file
func calculateFileSHA256(path string) (string, error) {
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

	systemSHA256, err := calculateFileSHA256(systemDBPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to hash system database: %w", err)
	}

	snapshots = append(snapshots, SnapshotInfo{
		Name:     SystemDatabaseName,
		Filename: SystemDatabaseName + ".db",
		FullPath: systemDBPath,
		Size:     info.Size(),
		SHA256:   systemSHA256,
	})

	// NOTE: MetaStore (PebbleDB) is NOT included in snapshots.
	// MetaStore files change constantly due to WAL rotation and compaction,
	// causing race conditions during snapshot streaming.
	// After snapshot restore, fresh MetaStore is created automatically.

	// Checkpoint and get info for all user databases (SQLite .db files only)
	for name, db := range dm.databases {
		// Skip system database (already handled above)
		if name == SystemDatabaseName {
			continue
		}

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

		dbSHA256, err := calculateFileSHA256(dbPath)
		if err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to hash database")
			continue
		}

		snapshots = append(snapshots, SnapshotInfo{
			Name:     name,
			Filename: filepath.Join("databases", name+".db"),
			FullPath: dbPath,
			Size:     info.Size(),
			SHA256:   dbSHA256,
		})
		// MetaStore (PebbleDB) is NOT included - see note above
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
func (dm *DatabaseManager) checkpointDatabase(db *ReplicatedDatabase) error {
	_, err := db.GetDB().Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

// TakeSnapshotToDir creates an atomic snapshot copy in the specified directory.
// This method:
// 1. Acquires write lock to block all writes
// 2. Checkpoints all databases (TRUNCATE mode)
// 3. Copies all database files to the target directory
// 4. Releases write lock
//
// The caller should stream from the target directory and clean it up when done.
// This ensures snapshot consistency since files are copied atomically under lock.
func (dm *DatabaseManager) TakeSnapshotToDir(targetDir string) ([]SnapshotInfo, uint64, error) {
	// Create target directory structure
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return nil, 0, fmt.Errorf("failed to create snapshot directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(targetDir, "databases"), 0755); err != nil {
		return nil, 0, fmt.Errorf("failed to create databases directory: %w", err)
	}

	// Acquire write lock to block all concurrent writes
	dm.mu.Lock()
	defer dm.mu.Unlock()

	var snapshots []SnapshotInfo

	// Checkpoint and copy system database
	systemDBPath := filepath.Join(dm.dataDir, SystemDatabaseName+".db")
	systemTargetPath := filepath.Join(targetDir, SystemDatabaseName+".db")

	if err := dm.checkpointDatabase(dm.systemDB); err != nil {
		return nil, 0, fmt.Errorf("failed to checkpoint system database: %w", err)
	}

	if err := copyFile(systemDBPath, systemTargetPath); err != nil {
		return nil, 0, fmt.Errorf("failed to copy system database: %w", err)
	}

	info, err := os.Stat(systemTargetPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to stat system database copy: %w", err)
	}

	systemSHA256, err := calculateFileSHA256(systemTargetPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to hash system database: %w", err)
	}

	snapshots = append(snapshots, SnapshotInfo{
		Name:     SystemDatabaseName,
		Filename: SystemDatabaseName + ".db",
		FullPath: systemTargetPath,
		Size:     info.Size(),
		SHA256:   systemSHA256,
	})

	// Checkpoint and copy all user databases
	for name, db := range dm.databases {
		if name == SystemDatabaseName {
			continue
		}

		if err := dm.checkpointDatabase(db); err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to checkpoint database")
			continue
		}

		srcPath := filepath.Join(dm.dataDir, "databases", name+".db")
		dstPath := filepath.Join(targetDir, "databases", name+".db")

		if err := copyFile(srcPath, dstPath); err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to copy database")
			continue
		}

		info, err := os.Stat(dstPath)
		if err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to stat database copy")
			continue
		}

		dbSHA256, err := calculateFileSHA256(dstPath)
		if err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to hash database")
			continue
		}

		snapshots = append(snapshots, SnapshotInfo{
			Name:     name,
			Filename: filepath.Join("databases", name+".db"),
			FullPath: dstPath,
			Size:     info.Size(),
			SHA256:   dbSHA256,
		})
	}

	// Get max committed transaction ID (without lock since we already hold it)
	maxTxnID := dm.getMaxCommittedTxnIDLocked()

	log.Info().
		Int("databases", len(snapshots)).
		Uint64("max_txn_id", maxTxnID).
		Str("target_dir", targetDir).
		Msg("Snapshot copied to directory")

	return snapshots, maxTxnID, nil
}

// GetMaxCommittedTxnID returns the highest committed transaction ID across all databases
func (dm *DatabaseManager) GetMaxCommittedTxnID() (uint64, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.getMaxCommittedTxnIDLocked(), nil
}

// getMaxCommittedTxnIDLocked returns the max committed txn ID without acquiring lock.
// Caller must hold dm.mu (read or write).
func (dm *DatabaseManager) getMaxCommittedTxnIDLocked() uint64 {
	var maxTxnID uint64

	// Check all user databases via their MetaStores
	for name, db := range dm.databases {
		metaStore := db.GetMetaStore()
		if metaStore == nil {
			continue // System DB has no MetaStore
		}
		dbMax, err := metaStore.GetMaxCommittedTxnID()
		if err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to get max txn_id")
			continue
		}
		if dbMax > maxTxnID {
			maxTxnID = dbMax
		}
	}

	return maxTxnID
}

// GetDataDir returns the data directory path
func (dm *DatabaseManager) GetDataDir() string {
	return dm.dataDir
}

// ReplicationState tracks replication progress with a peer node per database
type ReplicationState struct {
	PeerNodeID        uint64
	DatabaseName      string
	LastAppliedTxnID  uint64
	LastAppliedTSWall int64
	LastAppliedTSLog  int32
	LastSyncTime      int64
	SyncStatus        string // SYNCED, CATCHING_UP, FAILED
}

// GetReplicationState gets the replication state for a specific peer and database
func (dm *DatabaseManager) GetReplicationState(peerNodeID uint64, database string) (*ReplicationState, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, ok := dm.databases[database]
	if !ok {
		return nil, fmt.Errorf("database %s not found", database)
	}

	metaStore := db.GetMetaStore()
	if metaStore == nil {
		return nil, fmt.Errorf("database %s has no meta store", database)
	}

	rec, err := metaStore.GetReplicationState(peerNodeID, database)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		// No replication state yet for this peer/database combination
		return nil, nil
	}

	return &ReplicationState{
		PeerNodeID:        rec.PeerNodeID,
		DatabaseName:      rec.DatabaseName,
		LastAppliedTxnID:  rec.LastAppliedTxnID,
		LastAppliedTSWall: rec.LastAppliedTSWall,
		LastAppliedTSLog:  rec.LastAppliedTSLogical,
		LastSyncTime:      rec.LastSyncTime,
		SyncStatus:        rec.SyncStatus.String(),
	}, nil
}

// UpdateReplicationState updates or inserts replication state for a peer and database
func (dm *DatabaseManager) UpdateReplicationState(state *ReplicationState) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, ok := dm.databases[state.DatabaseName]
	if !ok {
		return fmt.Errorf("database %s not found", state.DatabaseName)
	}

	metaStore := db.GetMetaStore()
	if metaStore == nil {
		return fmt.Errorf("database %s has no meta store", state.DatabaseName)
	}

	return metaStore.UpdateReplicationState(
		state.PeerNodeID,
		state.DatabaseName,
		state.LastAppliedTxnID,
		hlc.Timestamp{WallTime: state.LastAppliedTSWall, Logical: state.LastAppliedTSLog},
	)
}

// GetAllReplicationStates returns replication state for all known peers across all databases
func (dm *DatabaseManager) GetAllReplicationStates() ([]ReplicationState, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	var allStates []ReplicationState

	// Query each database's MetaStore for its replication state
	for _, mdb := range dm.databases {
		metaStore := mdb.GetMetaStore()
		if metaStore == nil {
			continue // System DB has no MetaStore
		}

		states, err := metaStore.GetAllReplicationStates()
		if err != nil {
			continue
		}

		for _, rec := range states {
			allStates = append(allStates, ReplicationState{
				PeerNodeID:        rec.PeerNodeID,
				DatabaseName:      rec.DatabaseName,
				LastAppliedTxnID:  rec.LastAppliedTxnID,
				LastAppliedTSWall: rec.LastAppliedTSWall,
				LastAppliedTSLog:  rec.LastAppliedTSLogical,
				LastSyncTime:      rec.LastSyncTime,
				SyncStatus:        rec.SyncStatus.String(),
			})
		}
	}

	return allStates, nil
}

// GetMinAppliedTxnID returns the minimum last_applied_txn_id across all peers for a specific database
// This is used to determine the GC safe point - we can only GC transactions that all peers have applied
func (dm *DatabaseManager) GetMinAppliedTxnID(database string) (uint64, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, ok := dm.databases[database]
	if !ok {
		return 0, fmt.Errorf("database %s not found", database)
	}

	metaStore := db.GetMetaStore()
	if metaStore == nil {
		return 0, nil // System DB has no replication state
	}

	return metaStore.GetMinAppliedTxnID(database)
}

// GetMaxTxnID returns the maximum COMMITTED transaction ID in a database
// This is used to calculate replication lag and peer selection for anti-entropy
// Only committed transactions are considered to ensure consistency with snapshots
func (dm *DatabaseManager) GetMaxTxnID(database string) (uint64, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, ok := dm.databases[database]
	if !ok {
		return 0, fmt.Errorf("database %s not found", database)
	}

	metaStore := db.GetMetaStore()
	if metaStore == nil {
		return 0, nil // System DB has no transaction records
	}

	return metaStore.GetMaxCommittedTxnID()
}

// GetTableSchema returns the schema for a table in a database
// Used by CDC Publisher to transform events to Debezium format
func (dm *DatabaseManager) GetTableSchema(database, table string) (publisher.TableSchema, error) {
	db, err := dm.GetDatabase(database)
	if err != nil {
		return publisher.TableSchema{}, err
	}

	// Query PRAGMA table_info
	rows, err := db.GetDB().Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return publisher.TableSchema{}, fmt.Errorf("failed to query table info: %w", err)
	}
	defer rows.Close()

	var schema publisher.TableSchema
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dfltValue any
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return publisher.TableSchema{}, fmt.Errorf("failed to scan column info: %w", err)
		}
		schema.Columns = append(schema.Columns, publisher.ColumnInfo{
			Name:     name,
			Type:     colType,
			Nullable: notNull == 0,
			IsPK:     pk > 0,
		})
	}

	if err := rows.Err(); err != nil {
		return publisher.TableSchema{}, fmt.Errorf("error iterating column info: %w", err)
	}

	return schema, nil
}

// GetCommittedTxnCount returns the count of committed transactions in a database
// This is used by anti-entropy to compare data completeness between nodes
func (dm *DatabaseManager) GetCommittedTxnCount(database string) (int64, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, ok := dm.databases[database]
	if !ok {
		return 0, fmt.Errorf("database %s not found", database)
	}

	metaStore := db.GetMetaStore()
	if metaStore == nil {
		return 0, nil // System DB has no transaction records
	}

	return metaStore.GetCommittedTxnCount()
}

// GetMaxSeqNum returns the maximum sequence number in a database
// This is used by anti-entropy for gap detection
func (dm *DatabaseManager) GetMaxSeqNum(database string) (uint64, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, ok := dm.databases[database]
	if !ok {
		return 0, fmt.Errorf("database %s not found", database)
	}

	metaStore := db.GetMetaStore()
	if metaStore == nil {
		return 0, nil // System DB has no transaction records
	}

	return metaStore.GetMaxSeqNum()
}
