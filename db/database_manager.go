package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol/query/transform"
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

// DatabaseManager manages multiple MVCC databases.
//
// Locking: mu guards the maps and the wiring fields and is only ever held for
// map access and wiring, never while a database's GC is stopped, because a GC
// pass takes mu (GetMinAppliedTxnID, and anti-entropy's refresh through
// ListDatabases). lifecycleMu serialises the operations that open, close,
// register or delete a database (Create, Drop, Detach, Attach, Import, Close),
// so one of them can close a database outside mu while the others wait; no GC
// pass ever takes lifecycleMu.
type DatabaseManager struct {
	lifecycleMu              sync.Mutex
	mu                       sync.RWMutex
	databases                map[string]*ReplicatedDatabase
	detached                 map[string]*detachedDatabase // user databases out of service for a snapshot restore
	systemDB                 *ReplicatedDatabase
	dataDir                  string
	nodeID                   uint64
	clock                    *hlc.Clock
	refreshReplicationStates RefreshReplicationStatesFunc // Callback to refresh peer states before GC
	cdcHub                   CDCHub                       // CDC notification hub, can be nil
	vecIndexMgr              *VectorIndexManager          // Optional vector index manager
	autoIncClaimStore        *AutoIncClaimStore           // AUTO_INCREMENT claim store, backed by systemDB
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
		detached:  make(map[string]*detachedDatabase),
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

	// The system database holds the AUTO_INCREMENT claim bases, and a claim
	// COMMIT this node ACKs counts toward the majority the claim protocol's
	// safety rests on: the base must survive an OS crash or power loss, not
	// only a process crash. User databases keep synchronous=NORMAL.
	systemDB, err := NewReplicatedDatabase(systemDBPath, dm.nodeID, dm.clock, metaStore, WithDurableCommits())
	if err != nil {
		metaStore.Close()
		return fmt.Errorf("failed to create system database: %w", err)
	}

	// dm.systemDB must be assigned BEFORE wireGCCoordination runs: that call
	// wires the AUTO_INCREMENT claim store (db/autoinc_claim.go), which is
	// always backed by dm.systemDB, and wireGCCoordination is itself called
	// below for this very database.
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

	// Create the AUTO_INCREMENT claim table (db/autoinc_claim.go). It lives
	// here, in the system database, rather than in each user database: see
	// AutoIncClaimTable's doc comment for why.
	if _, err = systemDB.GetDB().Exec(autoIncClaimDDL); err != nil {
		return fmt.Errorf("failed to create auto-increment claim table: %w", err)
	}

	// Wire up GC coordination for system database
	dm.wireGCCoordination(systemDB, SystemDatabaseName)

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
//
// NOTE: This function must NOT acquire dm.mu as it is called from contexts
// that already hold the write lock (CreateDatabase, AttachDatabase, etc.)
// or during single-threaded initialization. Reading refreshReplicationStates
// is safe because the caller either has exclusive access via write lock
// or we're in initialization before any concurrent access is possible.
func (dm *DatabaseManager) wireGCCoordination(mdb *ReplicatedDatabase, dbName string) {
	txnMgr := mdb.GetTransactionManager()
	txnMgr.SetDatabaseName(dbName)
	txnMgr.SetMinAppliedTxnIDFunc(dm.GetMinAppliedTxnID)

	// Wire refresh function if available (set via SetRefreshReplicationStatesFunc)
	// No lock needed: caller either holds write lock or we're in init
	if dm.refreshReplicationStates != nil {
		txnMgr.SetRefreshReplicationStatesFunc(dm.refreshReplicationStates)
	}

	// Wire CDC notifier if available
	if dm.cdcHub != nil {
		txnMgr.SetNotifier(dm.cdcHub)
	}
	if dm.vecIndexMgr != nil {
		txnMgr.SetVectorCDCNotifier(dm.vecIndexMgr)
	}

	// Wire the AUTO_INCREMENT claim store (db/autoinc_claim.go). It always
	// backs onto the system database, never onto mdb itself, so this wires the
	// same store instance into every TransactionManager - user databases and
	// the system database alike. dm.systemDB is nil only during the brief
	// window before initSystemDatabase assigns it, which precedes this
	// function's own call for the system database.
	if dm.systemDB != nil {
		if dm.autoIncClaimStore == nil {
			dm.autoIncClaimStore = NewAutoIncClaimStore(dm.systemDB)
		}
		txnMgr.SetAutoIncClaimStore(dm.autoIncClaimStore)
	}
}

// SetRefreshReplicationStatesFunc sets the callback for refreshing peer replication states
// This is called by GC before Phase 2 to ensure fresh watermarks before deletion decisions
// The callback is wired to all existing and future TransactionManagers
func (dm *DatabaseManager) SetRefreshReplicationStatesFunc(fn RefreshReplicationStatesFunc) {
	dm.mu.Lock()
	dm.refreshReplicationStates = fn
	dm.mu.Unlock()

	// Wire to all existing databases
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	for _, mdb := range dm.databases {
		txnMgr := mdb.GetTransactionManager()
		txnMgr.SetRefreshReplicationStatesFunc(fn)
	}
}

// SetCDCHub sets the CDC notification hub and wires it to all existing databases
func (dm *DatabaseManager) SetCDCHub(hub CDCHub) {
	dm.mu.Lock()
	dm.cdcHub = hub
	dm.mu.Unlock()

	// Wire to all existing databases
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	for _, mdb := range dm.databases {
		txnMgr := mdb.GetTransactionManager()
		txnMgr.SetNotifier(hub)
	}
}

// GetCDCHub returns the CDC notification hub
func (dm *DatabaseManager) GetCDCHub() CDCHub {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.cdcHub
}

// SetVectorIndexManager sets the vector index manager.
func (dm *DatabaseManager) SetVectorIndexManager(mgr *VectorIndexManager) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.vecIndexMgr = mgr
	for _, mdb := range dm.databases {
		if mgr == nil {
			mdb.GetTransactionManager().SetVectorCDCNotifier(nil)
		} else {
			mdb.GetTransactionManager().SetVectorCDCNotifier(mgr)
		}
	}
}

// GetVectorIndexManager returns the vector index manager (may be nil).
// Returns coordinator.VectorIndexManagerProvider to satisfy the coordinator.DatabaseManager interface.
func (dm *DatabaseManager) GetVectorIndexManager() coordinator.VectorIndexManagerProvider {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	if dm.vecIndexMgr == nil {
		return nil
	}
	return dm.vecIndexMgr
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

	dm.lifecycleMu.Lock()
	defer dm.lifecycleMu.Unlock()

	// Check if database already exists - return success for idempotency (IF NOT EXISTS semantics)
	dm.mu.RLock()
	_, exists := dm.databases[name]
	detached := dm.detached[name]
	dm.mu.RUnlock()
	if detached != nil && detached.dropped {
		// Its files go when the restore that holds them ends (AttachDatabase).
		return fmt.Errorf("database %s is being dropped: %w", name, ErrDatabaseDetached)
	}
	if exists || detached != nil {
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

	// Register in system database
	createdAt := time.Now().UnixNano()
	_, err = dm.systemDB.GetDB().Exec(
		"INSERT INTO __marmot_databases (name, created_at, path) VALUES (?, ?, ?)",
		name, createdAt, dbPath,
	)
	if err != nil {
		db.Close()
		os.Remove(fullPath)
		cleanupMetaStoreFiles(fullPath)
		return fmt.Errorf("failed to register database in system: %w", err)
	}

	// Wire up GC coordination only once the database is registered: until
	// then its GC cannot reach mu, so the failure path above can close it.
	dm.mu.Lock()
	dm.wireGCCoordination(db, name)
	dm.databases[name] = db
	dm.mu.Unlock()
	log.Info().Str("name", name).Str("path", dbPath).Msg("Database created")
	return nil
}

// DropDatabase drops a database
// Returns nil if database doesn't exist (idempotent for IF EXISTS semantics)
//
// A database detached for a snapshot restore is dropped at once in the
// registry; its files go when the restore ends (AttachDatabase), and until
// then it no longer exists for lookups. A database whose reattach failed is
// dropped outright.
func (dm *DatabaseManager) DropDatabase(name string) error {
	if name == SystemDatabaseName {
		return fmt.Errorf("cannot drop system database")
	}

	if name == DefaultDatabaseName {
		return fmt.Errorf("cannot drop default database")
	}

	dm.lifecycleMu.Lock()
	defer dm.lifecycleMu.Unlock()

	dm.mu.RLock()
	db, exists := dm.databases[name]
	detached := dm.detached[name]
	dm.mu.RUnlock()

	// Check if database exists - if not, return success (idempotent)
	if (!exists && detached == nil) || (detached != nil && detached.dropped) {
		log.Info().Str("name", name).Msg("Database does not exist, DROP is no-op")
		return nil
	}

	// Get path before deletion
	var dbPath string
	err := dm.systemDB.GetDB().QueryRow(
		"SELECT path FROM __marmot_databases WHERE name = ?", name,
	).Scan(&dbPath)
	if err != nil {
		return fmt.Errorf("failed to get database path: %w", err)
	}
	fullPath := filepath.Join(dm.dataDir, dbPath)

	// Remove from registry
	_, err = dm.systemDB.GetDB().Exec("DELETE FROM __marmot_databases WHERE name = ?", name)
	if err != nil {
		return fmt.Errorf("failed to remove database from registry: %w", err)
	}

	// Remove this database's AUTO_INCREMENT claim rows (db/autoinc_claim.go).
	// DROP TABLE deliberately does NOT reach this path and leaves its row in
	// place; only DROP DATABASE removes claim rows.
	_, err = dm.systemDB.GetDB().Exec("DELETE FROM "+AutoIncClaimTable+" WHERE db = ?", name)
	if err != nil {
		return fmt.Errorf("failed to remove auto-increment claims for database %s: %w", name, err)
	}

	if detached != nil {
		dm.mu.Lock()
		if !detached.restoreFailed {
			detached.dropped = true
			detached.dropPath = fullPath
			dm.mu.Unlock()
			log.Info().Str("name", name).Msg("Database dropped; its files go when its snapshot restore ends")
			return nil
		}
		delete(dm.detached, name)
		dm.mu.Unlock()
		if err := detached.metaStore.Close(); err != nil {
			log.Error().Err(err).Str("name", name).Msg("Failed to close meta store")
		}
		removeDatabaseFiles(fullPath)
		log.Info().Str("name", name).Msg("Database dropped")
		return nil
	}

	// Out of the map under mu, closed outside it (see DatabaseManager).
	dm.mu.Lock()
	delete(dm.databases, name)
	dm.mu.Unlock()
	if err := db.Close(); err != nil {
		log.Error().Err(err).Str("name", name).Msg("Failed to close database")
	}
	removeDatabaseFiles(fullPath)

	log.Info().Str("name", name).Msg("Database dropped")
	return nil
}

// removeDatabaseFiles deletes a closed database's SQLite file, its WAL and SHM
// files, and its meta store directory.
func removeDatabaseFiles(fullPath string) {
	if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
		log.Error().Err(err).Str("path", fullPath).Msg("Failed to delete database file")
	}
	os.Remove(fullPath + "-wal")
	os.Remove(fullPath + "-shm")
	cleanupMetaStoreFiles(fullPath)
}

// GetDatabase returns a database by name
func (dm *DatabaseManager) GetDatabase(name string) (*ReplicatedDatabase, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.getDatabaseLocked(name)
}

// getDatabaseLocked returns the database in service under name, or
// ErrDatabaseDetached while it is out of service for a restore. Caller must
// hold mu.
func (dm *DatabaseManager) getDatabaseLocked(name string) (*ReplicatedDatabase, error) {
	db, exists := dm.databases[name]
	if !exists {
		if detached := dm.detached[name]; detached != nil && !detached.dropped {
			return nil, fmt.Errorf("database %s: %w", name, ErrDatabaseDetached)
		}
		return nil, fmt.Errorf("database %s does not exist", name)
	}
	return db, nil
}

// GetDatabaseConnection returns the write *sql.DB for a database.
// Writes must use this. Reads should use GetDatabaseReadConnection instead —
// the write handle has SetMaxOpenConns(1) and _txlock=immediate, which
// serialises all access through a single connection.
func (dm *DatabaseManager) GetDatabaseConnection(name string) (*sql.DB, error) {
	replicatedDB, err := dm.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	return replicatedDB.GetDB(), nil
}

// GetDatabaseReadConnection returns the read-only *sql.DB pool for a database.
// Pool size comes from config; WAL mode permits concurrent readers.
func (dm *DatabaseManager) GetDatabaseReadConnection(name string) (*sql.DB, error) {
	replicatedDB, err := dm.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	return replicatedDB.GetReadDB(), nil
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
	detached := dm.detached[name]
	return exists || (detached != nil && !detached.dropped)
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
	dm.lifecycleMu.Lock()
	defer dm.lifecycleMu.Unlock()

	// Closed outside mu (see DatabaseManager).
	dm.mu.RLock()
	databases := make(map[string]*ReplicatedDatabase, len(dm.databases))
	for name, db := range dm.databases {
		databases[name] = db
	}
	detached := make(map[string]*detachedDatabase, len(dm.detached))
	for name, d := range dm.detached {
		detached[name] = d
	}
	dm.mu.RUnlock()

	var lastErr error

	// Close all user databases
	for name, db := range databases {
		if name == SystemDatabaseName {
			continue
		}
		if err := db.Close(); err != nil {
			log.Error().Err(err).Str("name", name).Msg("Failed to close database")
			lastErr = err
		}
	}

	for name, d := range detached {
		if err := d.metaStore.Close(); err != nil {
			log.Error().Err(err).Str("name", name).Msg("Failed to close detached database's meta store")
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

// ErrDatabaseDetached is returned for a database taken out of service while a
// snapshot restore replaces its file (DetachDatabase).
var ErrDatabaseDetached = errors.New("database is detached for a snapshot restore")

// ErrDrainIncomplete is returned by DetachDatabase when the database was
// detached but a write in flight on it did not finish in time.
var ErrDrainIncomplete = errors.New("detached, but in-flight writes did not drain")

// detachedDatabase is a user database out of service for a snapshot restore.
type detachedDatabase struct {
	metaStore     MetaStore // kept open across the restore for AttachDatabase
	restoreFailed bool      // AttachDatabase failed; the next restore of it may take it over
	dropped       bool      // DROP DATABASE arrived during the restore; AttachDatabase completes it
	dropPath      string    // the dropped database's file, removed by AttachDatabase
}

// DetachDatabase takes a user database out of service so a snapshot restore
// can replace its SQLite file. It leaves the map at once, so lookups get
// ErrDatabaseDetached, and every commit on its connections is refused from
// then on (writeGate). It then drains: it waits, bounded by ctx, for the write
// transaction in flight on the database to finish, and stops its batch
// committer, GC and pools. When it returns nil, no write on the database can
// be ACKed until AttachDatabase: a caller still holding the old
// *ReplicatedDatabase gets a refused commit or "sql: database is closed",
// never a nil pool. The meta store stays open, so prepared transactions keep
// their intents, row locks and registration, and commit records are
// untouched. The system database and every other database are left alone.
//
// A database whose reattach failed (DatabasesAwaitingRestore) is already out
// of service, and the call hands it to the new restore.
//
// If the drain does not finish within ctx, DetachDatabase returns
// ErrDrainIncomplete with the database still detached: the caller must
// AttachDatabase it without replacing its file, because a commit that passed
// the gate before it closed may still complete into that file. Any other
// error leaves the database as it was.
func (dm *DatabaseManager) DetachDatabase(ctx context.Context, name string) error {
	mdb, err := dm.takeOutOfService(name)
	if err != nil || mdb == nil {
		return err
	}
	// Drained outside both locks (see DatabaseManager): the detached entry
	// fences Create and Drop.
	if err := mdb.drainSQLite(ctx); err != nil {
		return fmt.Errorf("database %s: %w: %w", name, ErrDrainIncomplete, err)
	}
	return nil
}

// takeOutOfService is DetachDatabase's locked step. It returns the database
// to drain, or nil when a database whose reattach failed is handed to the new
// restore with nothing left to drain.
func (dm *DatabaseManager) takeOutOfService(name string) (*ReplicatedDatabase, error) {
	if name == SystemDatabaseName {
		return nil, fmt.Errorf("cannot detach the system database")
	}
	dm.lifecycleMu.Lock()
	defer dm.lifecycleMu.Unlock()
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if detached := dm.detached[name]; detached != nil {
		if detached.restoreFailed && !detached.dropped {
			detached.restoreFailed = false
			return nil, nil
		}
		return nil, fmt.Errorf("database %s: %w", name, ErrDatabaseDetached)
	}
	mdb, exists := dm.databases[name]
	if !exists {
		return nil, fmt.Errorf("database %s does not exist", name)
	}
	// Commits are refused before the database leaves the map, so a caller
	// that sees it detached cannot still commit through a copy it holds.
	mdb.gate.close()
	delete(dm.databases, name)
	dm.detached[name] = &detachedDatabase{metaStore: mdb.GetMetaStore()}
	return mdb, nil
}

// AttachDatabase returns a database detached by DetachDatabase to service,
// opening whatever file is now at its path over the meta store it kept. If
// that fails, the database stays out of service and is listed by
// DatabasesAwaitingRestore until a later restore of it succeeds. If the
// database was dropped during the restore, the drop is completed instead.
func (dm *DatabaseManager) AttachDatabase(name string) error {
	dm.lifecycleMu.Lock()
	defer dm.lifecycleMu.Unlock()

	dm.mu.RLock()
	detached := dm.detached[name]
	dm.mu.RUnlock()
	if detached == nil {
		return fmt.Errorf("database %s is not detached", name)
	}
	if detached.dropped {
		dm.mu.Lock()
		delete(dm.detached, name)
		dm.mu.Unlock()
		if err := detached.metaStore.Close(); err != nil {
			log.Error().Err(err).Str("name", name).Msg("Failed to close dropped database's meta store")
		}
		removeDatabaseFiles(detached.dropPath)
		log.Info().Str("name", name).Msg("Database dropped during its snapshot restore; drop completed")
		return nil
	}

	mdb, err := dm.openDetached(name, detached.metaStore)
	if err != nil {
		dm.mu.Lock()
		detached.restoreFailed = true
		dm.mu.Unlock()
		return fmt.Errorf("failed to reattach database %s: %w", name, err)
	}
	dm.mu.Lock()
	dm.wireGCCoordination(mdb, name)
	delete(dm.detached, name)
	dm.databases[name] = mdb
	dm.mu.Unlock()
	log.Info().Str("name", name).Msg("Database reattached after snapshot restore")
	return nil
}

// openDetached opens the file registered for a detached database over the
// meta store it kept.
func (dm *DatabaseManager) openDetached(name string, metaStore MetaStore) (*ReplicatedDatabase, error) {
	var dbPath string
	if err := dm.systemDB.GetDB().QueryRow(
		"SELECT path FROM __marmot_databases WHERE name = ?", name,
	).Scan(&dbPath); err != nil {
		return nil, fmt.Errorf("failed to get database path: %w", err)
	}
	return NewReplicatedDatabase(filepath.Join(dm.dataDir, dbPath), dm.nodeID, dm.clock, metaStore)
}

// DatabasesAwaitingRestore lists the databases whose reattach after a
// snapshot restore failed. They stay out of service until a restore of them
// succeeds; anti-entropy retries one each round.
func (dm *DatabaseManager) DatabasesAwaitingRestore() []string {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	var names []string
	for name, detached := range dm.detached {
		if detached.restoreFailed && !detached.dropped {
			names = append(names, name)
		}
	}
	return names
}

// errIfDetachedLocked refuses a snapshot of every database while one the
// registry names is detached: the snapshot would ship a registry naming a
// database it has no file for. The caller retries once it is back. Caller
// must hold mu.
func (dm *DatabaseManager) errIfDetachedLocked() error {
	for name, detached := range dm.detached {
		if !detached.dropped {
			return fmt.Errorf("cannot snapshot while database %s is out of service: %w", name, ErrDatabaseDetached)
		}
	}
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
	dm.lifecycleMu.Lock()
	defer dm.lifecycleMu.Unlock()

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
		dm.mu.RLock()
		existingDB, exists := dm.databases[dbName]
		dm.mu.RUnlock()
		if exists {
			// Check if existing database is empty (no user tables)
			// If so, we can replace it with the imported version
			srcPath := filepath.Join(importDir, name)
			if !dm.shouldReplaceWithImport(existingDB, srcPath) {
				log.Debug().Str("name", dbName).Msg("Database already exists with data, skipping import")
				continue
			}
			// Close existing empty database before replacing: out of the map
			// under mu, closed outside it (see DatabaseManager).
			log.Info().Str("name", dbName).Msg("Replacing empty database with imported version")
			dm.mu.Lock()
			delete(dm.databases, dbName)
			dm.mu.Unlock()
			existingDB.Close()
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

		// Wire up GC coordination only once the database is registered: until
		// then its GC cannot reach mu, so the failure path above can close it.
		dm.mu.Lock()
		dm.wireGCCoordination(db, dbName)
		dm.databases[dbName] = db
		dm.mu.Unlock()
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

	if err := dm.errIfDetachedLocked(); err != nil {
		return nil, 0, err
	}

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

		snapshot, err := dm.describeDatabaseFile(name, db)
		if err != nil {
			log.Warn().Err(err).Str("database", name).Msg("Failed to describe database for snapshot")
			continue
		}
		snapshots = append(snapshots, snapshot)
		// MetaStore (PebbleDB) is NOT included - see note above
	}

	// Get max committed transaction ID (without lock since we already hold it)
	maxTxnID := dm.getMaxCommittedTxnIDLocked()

	log.Info().
		Int("databases", len(snapshots)).
		Uint64("max_txn_id", maxTxnID).
		Msg("Snapshot prepared")

	return snapshots, maxTxnID, nil
}

// TakeDatabaseSnapshotInfo checkpoints one user database and describes its
// file, with its max transaction ID, as TakeSnapshot does for every database.
// Unlike TakeSnapshot it is refused only while this database is out of
// service, never because another one is.
func (dm *DatabaseManager) TakeDatabaseSnapshotInfo(name string) (SnapshotInfo, uint64, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, err := dm.getDatabaseLocked(name)
	if err != nil {
		return SnapshotInfo{}, 0, err
	}
	snapshot, err := dm.describeDatabaseFile(name, db)
	if err != nil {
		return SnapshotInfo{}, 0, err
	}
	maxTxnID, err := dm.getMaxTxnIDLocked(name)
	if err != nil {
		return SnapshotInfo{}, 0, fmt.Errorf("failed to get max txn id: %w", err)
	}
	return snapshot, maxTxnID, nil
}

// describeDatabaseFile checkpoints user database name and describes its file
// in place. Caller must hold mu.
func (dm *DatabaseManager) describeDatabaseFile(name string, db *ReplicatedDatabase) (SnapshotInfo, error) {
	if err := dm.checkpointDatabase(db); err != nil {
		return SnapshotInfo{}, fmt.Errorf("failed to checkpoint database %s: %w", name, err)
	}
	filename := filepath.Join("databases", name+".db")
	dbPath := filepath.Join(dm.dataDir, filename)
	info, err := os.Stat(dbPath)
	if err != nil {
		return SnapshotInfo{}, fmt.Errorf("failed to stat database %s: %w", name, err)
	}
	dbSHA256, err := calculateFileSHA256(dbPath)
	if err != nil {
		return SnapshotInfo{}, fmt.Errorf("failed to hash database %s: %w", name, err)
	}
	return SnapshotInfo{
		Name:     name,
		Filename: filename,
		FullPath: dbPath,
		Size:     info.Size(),
		SHA256:   dbSHA256,
	}, nil
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
// 4. Reads schema versions for the copied databases
// 5. Releases write lock
//
// The caller should stream from the target directory and clean it up when done.
// This ensures snapshot consistency since files are copied atomically under lock.
//
// Schema versions are read from the system MetaStore in the same locked section
// that produces the copied files, so the returned versions describe exactly the
// bytes being handed back - not a value read by some earlier, separate call that
// a concurrent DDL commit could have moved past. A caller that instead reads
// schema versions via a different, earlier RPC (e.g. GetSnapshotInfo) can
// observe a version that no longer matches the file bytes actually streamed
// later; see SnapshotVersionsForRestore in the grpc package for how that is
// resolved on the receiving side.
func (dm *DatabaseManager) TakeSnapshotToDir(targetDir string) ([]SnapshotInfo, uint64, map[string]uint64, error) {
	// Create target directory structure
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return nil, 0, nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(targetDir, "databases"), 0755); err != nil {
		return nil, 0, nil, fmt.Errorf("failed to create databases directory: %w", err)
	}

	// Acquire write lock to block all concurrent writes
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if err := dm.errIfDetachedLocked(); err != nil {
		return nil, 0, nil, err
	}

	var snapshots []SnapshotInfo

	// Checkpoint and copy system database
	systemDBPath := filepath.Join(dm.dataDir, SystemDatabaseName+".db")
	systemTargetPath := filepath.Join(targetDir, SystemDatabaseName+".db")

	if err := dm.checkpointDatabase(dm.systemDB); err != nil {
		return nil, 0, nil, fmt.Errorf("failed to checkpoint system database: %w", err)
	}

	if err := copyFile(systemDBPath, systemTargetPath); err != nil {
		return nil, 0, nil, fmt.Errorf("failed to copy system database: %w", err)
	}

	info, err := os.Stat(systemTargetPath)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to stat system database copy: %w", err)
	}

	systemSHA256, err := calculateFileSHA256(systemTargetPath)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to hash system database: %w", err)
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

	// Read schema versions in this same locked section, immediately after the
	// files above were checkpointed and copied, so the versions describe
	// exactly the bytes being handed back to the caller.
	schemaVersions := dm.schemaVersionsLocked()

	log.Info().
		Int("databases", len(snapshots)).
		Uint64("max_txn_id", maxTxnID).
		Str("target_dir", targetDir).
		Msg("Snapshot copied to directory")

	return snapshots, maxTxnID, schemaVersions, nil
}

// schemaVersionsLocked reads all schema versions from the system MetaStore.
// Caller must hold dm.mu. Returns an empty map when the versions cannot be
// read - the snapshot itself is still usable, the receiver simply has nothing
// to restore and falls back to whatever it already knew.
func (dm *DatabaseManager) schemaVersionsLocked() map[string]uint64 {
	metaStore := dm.systemDB.GetMetaStore()
	if metaStore == nil {
		return nil
	}

	stored, err := metaStore.GetAllSchemaVersions()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to read schema versions for snapshot")
		return nil
	}

	versions := make(map[string]uint64, len(stored))
	for database, version := range stored {
		if version > 0 {
			versions[database] = uint64(version)
		}
	}
	return versions
}

// TakeSnapshotForDatabase creates a snapshot for a single database.
// Unlike TakeSnapshotToDir which snapshots all databases atomically, this method
// snapshots only the specified database using a read lock, allowing concurrent
// per-database snapshots.
//
// Parameters:
//   - targetDir: Directory where the snapshot will be created
//   - dbName: Name of the database to snapshot
//
// Returns:
//   - SnapshotInfo: Metadata about the snapshot
//   - uint64: Max transaction ID for this database at snapshot time
//   - error: Any error encountered during snapshot
func (dm *DatabaseManager) TakeSnapshotForDatabase(
	targetDir string,
	dbName string,
) (SnapshotInfo, uint64, error) {
	// Acquire read lock (allows concurrent per-DB snapshots)
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	// Only this database's own detach refuses its snapshot; other databases
	// out of service are not part of it.
	db, err := dm.getDatabaseLocked(dbName)
	if err != nil {
		return SnapshotInfo{}, 0, err
	}

	// Checkpoint the database (PRAGMA wal_checkpoint(TRUNCATE))
	if err := dm.checkpointDatabase(db); err != nil {
		return SnapshotInfo{}, 0, fmt.Errorf("failed to checkpoint database %s: %w", dbName, err)
	}

	// Copy .db file to target directory
	srcPath := filepath.Join(dm.dataDir, "databases", dbName+".db")
	destPath := filepath.Join(targetDir, "databases", dbName+".db")

	// Create target databases directory if needed
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return SnapshotInfo{}, 0, fmt.Errorf("failed to create target directory: %w", err)
	}

	// Copy file with fsync
	if err := copyFile(srcPath, destPath); err != nil {
		return SnapshotInfo{}, 0, fmt.Errorf("failed to copy database file: %w", err)
	}

	// Get file info
	info, err := os.Stat(destPath)
	if err != nil {
		return SnapshotInfo{}, 0, fmt.Errorf("failed to stat copied file: %w", err)
	}

	// Calculate SHA256 checksum
	sha256, err := calculateFileSHA256(destPath)
	if err != nil {
		return SnapshotInfo{}, 0, fmt.Errorf("failed to calculate checksum: %w", err)
	}

	// Get max txn ID for this specific database (without lock since we already hold it)
	maxTxnID, err := dm.getMaxTxnIDLocked(dbName)
	if err != nil {
		return SnapshotInfo{}, 0, fmt.Errorf("failed to get max txn id: %w", err)
	}

	snapshot := SnapshotInfo{
		Name:     dbName,
		Filename: filepath.Join("databases", dbName+".db"),
		FullPath: destPath,
		Size:     info.Size(),
		SHA256:   sha256,
	}

	log.Info().
		Str("database", dbName).
		Int64("size", info.Size()).
		Uint64("max_txn_id", maxTxnID).
		Msg("Database snapshot created")

	return snapshot, maxTxnID, nil
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
	return dm.getMaxTxnIDLocked(database)
}

// getMaxTxnIDLocked is GetMaxTxnID for a caller that holds dm.mu: taking the
// read lock again would deadlock behind a writer waiting for it.
func (dm *DatabaseManager) getMaxTxnIDLocked(database string) (uint64, error) {
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

// GetTableSchema returns the schema for a table in a database.
// Uses cached schema from SchemaCache - does NOT query SQLite.
// Used by CDC Publisher to transform events to Debezium format.
func (dm *DatabaseManager) GetTableSchema(database, table string) (publisher.TableSchema, error) {
	db, err := dm.GetDatabase(database)
	if err != nil {
		return publisher.TableSchema{}, err
	}

	schema, err := db.GetCachedTableSchema(table)
	if err != nil {
		return publisher.TableSchema{}, fmt.Errorf("schema not cached for table %s: %w", table, err)
	}

	return schema.ToPublisherSchema(), nil
}

// GetAutoIncrementColumn returns the auto-increment column name for a table.
// Uses cached schema - does NOT query SQLite PRAGMA.
// Returns empty string if no auto-increment column exists.
func (dm *DatabaseManager) GetAutoIncrementColumn(database, table string) (string, error) {
	db, err := dm.GetDatabase(database)
	if err != nil {
		return "", err
	}

	schema, err := db.GetCachedTableSchema(table)
	if err != nil {
		return "", fmt.Errorf("schema not cached for table %s: %w", table, err)
	}

	return schema.GetAutoIncrementCol(), nil
}

// columnOrdinal returns name's position in a table's column order, or -1 when
// name is empty or absent. TableSchema.Columns is declaration order with
// GENERATED columns removed, which is exactly the tuple a column-less
// "INSERT INTO t VALUES (...)" supplies - SQLite rejects a value for a
// generated column - so this index is the position of that column's value in
// such a statement. PRAGMA table_xinfo's cid is NOT usable here: it counts
// hidden columns too (see db/schema_cache.go loadSchema).
func columnOrdinal(columns []string, name string) int {
	if name == "" {
		return -1
	}
	for i, col := range columns {
		if strings.EqualFold(col, name) {
			return i
		}
	}
	return -1
}

// GetTranspilerSchema returns schema information used by SQL transpilation rules.
// Uses cached schema - does NOT query SQLite PRAGMA.
func (dm *DatabaseManager) GetTranspilerSchema(database, table string) (*transform.SchemaInfo, error) {
	db, err := dm.GetDatabase(database)
	if err != nil {
		return nil, err
	}

	schema, err := db.GetCachedTableSchema(table)
	if err != nil {
		return nil, fmt.Errorf("schema not cached for table %s: %w", table, err)
	}

	autoIncCol := schema.GetAutoIncrementCol()
	info := &transform.SchemaInfo{
		AutoIncrementColumn:  autoIncCol,
		AutoIncrementOrdinal: columnOrdinal(schema.Columns, autoIncCol),
	}
	// The declared width comes from the marker the transpiler wrote into the
	// CREATE TABLE text; a column without one keeps the 64-bit path.
	for _, col := range schema.FullColumns {
		if autoIncCol != "" && strings.EqualFold(col.Name, autoIncCol) {
			info.AutoIncrementWidth = col.DeclaredWidth
			info.AutoIncrementUnsigned = col.Unsigned
			break
		}
	}

	// PrimaryKeys uses "rowid" sentinel when no explicit PRIMARY KEY is defined.
	// Keep it out of transpiler conflict targeting to preserve fallback behavior.
	if len(schema.PrimaryKeys) > 0 &&
		!(len(schema.PrimaryKeys) == 1 && strings.EqualFold(schema.PrimaryKeys[0], "rowid")) {
		info.PrimaryKey = append([]string(nil), schema.PrimaryKeys...)
	}

	return info, nil
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

// GetDatabaseStatsProvider returns a stats provider for the given database
// Returns nil if database doesn't exist
func (dm *DatabaseManager) GetDatabaseStatsProvider(name string) interface{} {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	db, exists := dm.databases[name]
	if !exists {
		return nil
	}
	return db.GetMetaStore()
}

// ListDatabaseNames returns the names of all databases
func (dm *DatabaseManager) ListDatabaseNames() []string {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	names := make([]string, 0, len(dm.databases))
	for name := range dm.databases {
		names = append(names, name)
	}
	return names
}
