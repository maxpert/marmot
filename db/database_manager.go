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
	"sync/atomic"
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
	// ClusterMembership returns this node's current view of the cluster's
	// total membership, the denominator of every quorum it computes.
	ClusterMembership() (int, error)
}

// DatabaseManager manages multiple MVCC databases.
//
// Locking: mu guards the maps and the wiring fields and is only ever held for
// map access and wiring, never while a database's GC is stopped, because a GC
// pass takes mu (anti-entropy's refresh through ListDatabases). lifecycleMu
// serialises the operations that open, close,
// register or delete a database (Create, Drop, Detach, Attach, Import, Close),
// so one of them can close a database outside mu while the others wait; no GC
// pass ever takes lifecycleMu.
type DatabaseManager struct {
	lifecycleMu       sync.Mutex
	mu                sync.RWMutex
	databases         map[string]*ReplicatedDatabase
	detached          map[string]*detachedDatabase // user databases out of service for a snapshot restore
	systemDB          *ReplicatedDatabase
	dataDir           string
	nodeID            uint64
	clock             *hlc.Clock
	cdcHub            CDCHub                          // CDC notification hub, can be nil
	vecIndexMgr       *VectorIndexManager             // Optional vector index manager
	autoIncClaimStore *AutoIncClaimStore              // AUTO_INCREMENT claim store, backed by systemDB
	membershipView    atomic.Pointer[func() int]      // this node's view of total cluster membership
	gcMembershipFunc  atomic.Pointer[func() []uint64] // current member node ids (self included, not REMOVED); source for each database's GC safe position
}

// SetClusterMembership installs view as the source of this node's view of
// the cluster's total membership (ClusterMembership).
func (dm *DatabaseManager) SetClusterMembership(view func() int) {
	dm.membershipView.Store(&view)
}

// ClusterMembership returns this node's current view of the cluster's total
// membership. An AUTO_INCREMENT claim participant compares it with the
// claimant's, so it fails rather than guess when no view is installed.
func (dm *DatabaseManager) ClusterMembership() (int, error) {
	view := dm.membershipView.Load()
	if view == nil {
		return 0, errors.New("no cluster membership view installed")
	}
	return (*view)(), nil
}

// SetGCMembershipFunc installs view as the source of this node's current
// cluster membership (self included, every node whose registry status is
// not REMOVED) for every database's GC safe deletion position
// (wireGCCoordination). It keeps this package free of a grpc import: the
// caller (marmot.go) closes over grpc's NodeRegistry itself. Until this is
// called, GC treats nothing as safe except entries past max retention.
func (dm *DatabaseManager) SetGCMembershipFunc(view func() []uint64) {
	dm.gcMembershipFunc.Store(&view)
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

	// Create database registry table. generation and dropped implement the
	// registry key (DatabaseRegistryKey): a tombstoned DROP keeps its
	// row (dropped=1) instead of deleting it, so a later CREATE can fence a
	// stale peer with a strictly higher generation. See DatabaseRegistryKey.
	// legacy marks a row that carries no real generation history; see
	// migrateDatabaseRegistrySchema and ApplyDatabaseOp's doc comment.
	_, err = systemDB.GetDB().Exec(`
		CREATE TABLE IF NOT EXISTS __marmot_databases (
			name TEXT PRIMARY KEY,
			created_at INTEGER NOT NULL,
			path TEXT NOT NULL,
			generation INTEGER NOT NULL DEFAULT 1,
			dropped INTEGER NOT NULL DEFAULT 0,
			legacy INTEGER NOT NULL DEFAULT 0
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create database registry table: %w", err)
	}
	if err := migrateDatabaseRegistrySchema(systemDB.GetDB()); err != nil {
		return fmt.Errorf("failed to migrate database registry schema: %w", err)
	}

	// Create the AUTO_INCREMENT claim table (db/autoinc_claim.go) and its vote
	// hold (db/autoinc_vote_hold.go). They live here, in the system database,
	// rather than in each user database: see AutoIncClaimTable's doc comment
	// for why.
	if err = createAutoIncTables(systemDB.GetDB()); err != nil {
		return fmt.Errorf("failed to create auto-increment tables: %w", err)
	}

	// Wire up GC coordination for system database
	dm.wireGCCoordination(systemDB, SystemDatabaseName)

	log.Info().Str("path", systemDBPath).Msg("System database initialized")
	return nil
}

// migrateDatabaseRegistrySchema adds the generation, dropped and legacy
// columns to __marmot_databases when an existing system database predates
// them. It is idempotent: safe to call on
// every startup, including against a brand-new table that already has the
// columns. A row that predates the migration has no generation history of
// its own, so it becomes (generation 1, live) - the same key CREATE DATABASE
// stamps for a name's first-ever generation - and legacy=1: before this
// migration existed, DROP DATABASE deleted its registry row outright, so every row an upgrade finds
// still there was live, and its (1, live) key is a migration default, not a
// stamp from a real CREATE. ApplyDatabaseOp treats a legacy live row
// specially until a real CREATE or DROP stamps the name (createDatabaseAt
// GenerationLocked and dropDatabaseAtGenerationLocked both clear legacy).
func migrateDatabaseRegistrySchema(sqlDB *sql.DB) error {
	rows, err := sqlDB.Query("PRAGMA table_info(__marmot_databases)")
	if err != nil {
		return fmt.Errorf("failed to inspect database registry schema: %w", err)
	}
	have := make(map[string]bool, 5)
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			rows.Close()
			return fmt.Errorf("failed to read database registry schema: %w", err)
		}
		have[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("failed to read database registry schema: %w", err)
	}
	rows.Close()

	if !have["generation"] {
		if _, err := sqlDB.Exec("ALTER TABLE __marmot_databases ADD COLUMN generation INTEGER NOT NULL DEFAULT 1"); err != nil {
			return fmt.Errorf("failed to add generation column to database registry: %w", err)
		}
	}
	if !have["dropped"] {
		if _, err := sqlDB.Exec("ALTER TABLE __marmot_databases ADD COLUMN dropped INTEGER NOT NULL DEFAULT 0"); err != nil {
			return fmt.Errorf("failed to add dropped column to database registry: %w", err)
		}
	}
	if !have["legacy"] {
		// DEFAULT 1: every row already in the table when this ALTER first
		// runs predates generation stamps and has no real generation history
		// (see this function's doc comment). A row inserted after the column exists
		// always states its own legacy value explicitly.
		if _, err := sqlDB.Exec("ALTER TABLE __marmot_databases ADD COLUMN legacy INTEGER NOT NULL DEFAULT 1"); err != nil {
			return fmt.Errorf("failed to add legacy column to database registry: %w", err)
		}
	}
	return nil
}

// loadDatabases loads every live database from the registry. Tombstoned rows
// (dropped=1) are excluded: a dropped database is never opened.
func (dm *DatabaseManager) loadDatabases() error {
	rows, err := dm.systemDB.GetDB().Query("SELECT name, created_at, path FROM __marmot_databases WHERE dropped = 0")
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

// schemaVersionOptions returns the NewReplicatedDatabase options that name a
// user database and wire its __marmot_schema_version migration read
// from the retiring pebble-stored counter in the system database's MetaStore.
// dm.systemDB must already be assigned; every call site below runs after
// initSystemDatabase.
func (dm *DatabaseManager) schemaVersionOptions(name string) []ReplicatedDatabaseOption {
	return []ReplicatedDatabaseOption{
		WithDatabaseName(name),
		WithLegacySchemaVersionSource(dm.systemDB.GetMetaStore().GetSchemaVersion),
	}
}

// openDatabase opens a database and adds it to the registry
// Creates a MetaStore for the database (stored in dbname_meta.pebble/)
func (dm *DatabaseManager) openDatabase(name, path string) error {
	// Create MetaStore for this database
	metaStore, err := NewMetaStore(path)
	if err != nil {
		return fmt.Errorf("failed to create meta store for %s: %w", name, err)
	}

	db, err := NewReplicatedDatabase(path, dm.nodeID, dm.clock, metaStore, dm.schemaVersionOptions(name)...)
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
// or during single-threaded initialization.
//
// GC with partial membership: gcMembershipFunc's
// view of current membership can be only partly populated right after a
// restart, before gossip has re-learned every peer. GC can run against that
// partial view: it then treats a not-yet-gossiped member as absent from
// dm.gcMembershipFunc's list, so its consumed position is deleted
// (SetGCSafePositionFunc's DeleteConsumedPosition loop below) and it no
// longer holds back the safe deletion point. This is never a correctness
// problem - GC's own safe-position computation still only deletes what every
// member it does know about has consumed - but it means that member may find
// its cursor behind this node's truncation point once gossip reports it
// again, forcing a snapshot restore (AntiEntropyService's snapshot fallback)
// where a further log pull round would otherwise
// have sufficed. The cost is one unnecessary snapshot for that member, never
// a lost transaction.
func (dm *DatabaseManager) wireGCCoordination(mdb *ReplicatedDatabase, dbName string) {
	txnMgr := mdb.GetTransactionManager()
	txnMgr.SetDatabaseName(dbName)

	// GC's safe deletion position is the min, across every current member
	// other than self, of this database's log R[self,m,d]
	// (db/meta_store.go's ConsumedPositions). A
	// member with no recorded position counts as the zero position, so GC
	// deletes nothing for that database until every member has reported at
	// least once (bounded by gcMaxRetention regardless). A member no longer
	// current has its consumed position deleted so it stops pinning GC.
	//
	// A member down for longer than gc_max_retention_hours finds its
	// consumed position has been superseded by GC's unconditional
	// max-retention deletion; it restores by snapshot, which anti-entropy
	// does automatically (AntiEntropyService's snapshot fallback).
	metaStore := mdb.GetMetaStore()
	selfID := dm.nodeID
	txnMgr.SetGCSafePositionFunc(func() (LogPosition, bool) {
		viewPtr := dm.gcMembershipFunc.Load()
		if viewPtr == nil {
			return LogPosition{}, false
		}
		members := (*viewPtr)()

		positions, err := metaStore.ConsumedPositions()
		if err != nil {
			log.Warn().Err(err).Str("database", dbName).Msg("GC: failed to read consumed positions")
			return LogPosition{}, false
		}

		memberSet := make(map[uint64]bool, len(members))
		for _, m := range members {
			memberSet[m] = true
		}
		for nodeID := range positions {
			if memberSet[nodeID] {
				continue
			}
			if err := metaStore.DeleteConsumedPosition(nodeID); err != nil {
				log.Warn().Err(err).Str("database", dbName).Uint64("node_id", nodeID).
					Msg("GC: failed to delete consumed position of a departed member")
			}
		}

		var safe LogPosition
		haveOther := false
		for _, m := range members {
			if m == selfID {
				continue
			}
			pos := positions[m] // zero LogPosition when this member has never reported
			if !haveOther || pos.Less(safe) {
				safe = pos
			}
			haveOther = true
		}
		if !haveOther {
			return LogPosition{}, false
		}
		return safe, true
	})

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

// SetAutoIncIncarnationListener registers l to learn of every table
// incarnation a DDL statement ends on this node (AutoIncIncarnationListener).
func (dm *DatabaseManager) SetAutoIncIncarnationListener(l AutoIncIncarnationListener) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.autoIncClaimStore == nil {
		dm.autoIncClaimStore = NewAutoIncClaimStore(dm.systemDB)
	}
	dm.autoIncClaimStore.SetIncarnationListener(l)
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

// CreateDatabase creates a new database with its own MetaStore. Returns nil
// if the database already exists (idempotent, IF NOT EXISTS semantics). A
// name previously dropped (tombstoned in the registry) is re-created one
// generation above its tombstone; see DatabaseRegistryKey.
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

	local, err := dm.registryKeyRow(name)
	if err != nil {
		return err
	}
	return dm.createDatabaseAtGenerationLocked(name, local.Generation+1)
}

// createDatabaseAtGenerationLocked creates name's file and wires it into
// service, stamping the registry row (name, generation, live). Caller must
// hold lifecycleMu.
//
// If a live local incarnation of name already exists, it is retired first -
// closed, its files removed, its AUTO_INCREMENT incarnation ended - exactly
// as DropDatabase would. This only happens through ApplyDatabaseOp
// reconciling a peer's higher live key: DatabaseRegistryKey's total order
// means a live key above the local one can only follow a DROP this node has
// not learned of yet, so the local incarnation is stale and must not survive
// under the new generation.
func (dm *DatabaseManager) createDatabaseAtGenerationLocked(name string, generation uint64) error {
	dm.mu.RLock()
	existingDB, exists := dm.databases[name]
	detached := dm.detached[name]
	dm.mu.RUnlock()

	if detached != nil {
		return fmt.Errorf("database %s is out of service for a snapshot restore: %w", name, ErrDatabaseDetached)
	}
	if exists {
		if err := dm.retireLiveIncarnationLocked(name, existingDB); err != nil {
			return err
		}
	}

	// Create database file
	dbPath := filepath.Join("databases", name+".db")
	fullPath := filepath.Join(dm.dataDir, dbPath)

	// Create MetaStore for this database
	metaStore, err := NewMetaStore(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create meta store: %w", err)
	}

	newDB, err := NewReplicatedDatabase(fullPath, dm.nodeID, dm.clock, metaStore, dm.schemaVersionOptions(name)...)
	if err != nil {
		metaStore.Close()
		cleanupMetaStoreFiles(fullPath)
		return fmt.Errorf("failed to create database file: %w", err)
	}

	// Register in system database. ON CONFLICT covers re-creating a
	// tombstoned name: the row already exists, dropped=1, and this brings it
	// back live at the given generation. legacy=0: this is a real CREATE with
	// a generation stamp, so any legacy divergence for name is resolved
	// (see ApplyDatabaseOp's doc comment).
	createdAt := time.Now().UnixNano()
	_, err = dm.systemDB.GetDB().Exec(
		`INSERT INTO __marmot_databases (name, created_at, path, generation, dropped, legacy)
		 VALUES (?, ?, ?, ?, 0, 0)
		 ON CONFLICT(name) DO UPDATE SET
		   created_at = excluded.created_at,
		   path = excluded.path,
		   generation = excluded.generation,
		   dropped = 0,
		   legacy = 0`,
		name, createdAt, dbPath, generation,
	)
	if err != nil {
		newDB.Close()
		os.Remove(fullPath)
		cleanupMetaStoreFiles(fullPath)
		return fmt.Errorf("failed to register database in system: %w", err)
	}

	// Wire up GC coordination only once the database is registered: until
	// then its GC cannot reach mu, so the failure path above can close it.
	dm.mu.Lock()
	dm.wireGCCoordination(newDB, name)
	dm.databases[name] = newDB
	dm.mu.Unlock()
	log.Info().Str("name", name).Str("path", dbPath).Uint64("generation", generation).Msg("Database created")
	return nil
}

// retireLiveIncarnationLocked closes and removes a live local database
// incarnation that a newer registry generation is about to replace, and ends
// its AUTO_INCREMENT incarnation. Caller must hold lifecycleMu.
func (dm *DatabaseManager) retireLiveIncarnationLocked(name string, liveDB *ReplicatedDatabase) error {
	var dbPath string
	if err := dm.systemDB.GetDB().QueryRow(
		"SELECT path FROM __marmot_databases WHERE name = ?", name,
	).Scan(&dbPath); err != nil {
		return fmt.Errorf("failed to get database path: %w", err)
	}
	fullPath := filepath.Join(dm.dataDir, dbPath)

	dm.mu.Lock()
	delete(dm.databases, name)
	dm.mu.Unlock()
	if err := liveDB.Close(); err != nil {
		log.Error().Err(err).Str("name", name).Msg("Failed to close a stale database incarnation being replaced by a newer registry generation")
	}
	removeDatabaseFiles(fullPath)

	dm.mu.RLock()
	claimStore := dm.autoIncClaimStore
	dm.mu.RUnlock()
	if claimStore != nil {
		claimStore.databaseIncarnationEnded(name)
	}
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
	_, exists := dm.databases[name]
	detached := dm.detached[name]
	dm.mu.RUnlock()

	// Check if database exists - if not, return success (idempotent)
	if (!exists && detached == nil) || (detached != nil && detached.dropped) {
		log.Info().Str("name", name).Msg("Database does not exist, DROP is no-op")
		return nil
	}

	local, err := dm.registryKeyRow(name)
	if err != nil {
		return err
	}
	return dm.dropDatabaseAtGenerationLocked(name, local.Generation)
}

// dropDatabaseAtGenerationLocked tombstones name's registry row at
// (generation, dropped) - keeping the row rather than deleting it, so a later
// CREATE can fence a stale peer with a strictly higher generation - and, if
// this node has a local incarnation, retires it: closes and removes its
// files, keeps its AUTO_INCREMENT claim rows, ends its incarnation.
//
// A name this node never created locally is tombstoned all the same: the
// registry row is what ApplyDatabaseOp reconciles against, not local file
// presence, so a node that missed both CREATE and DROP must still end up with
// the tombstone. Caller must hold lifecycleMu.
func (dm *DatabaseManager) dropDatabaseAtGenerationLocked(name string, generation uint64) error {
	dm.mu.RLock()
	liveDB, exists := dm.databases[name]
	detached := dm.detached[name]
	dm.mu.RUnlock()

	var dbPath string
	err := dm.systemDB.GetDB().QueryRow(
		"SELECT path FROM __marmot_databases WHERE name = ?", name,
	).Scan(&dbPath)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to get database path: %w", err)
	}
	if dbPath == "" {
		dbPath = filepath.Join("databases", name+".db")
	}
	fullPath := filepath.Join(dm.dataDir, dbPath)

	// legacy=0: this is a real DROP with a generation stamp, so any legacy
	// divergence for name is resolved (see ApplyDatabaseOp's doc comment).
	_, err = dm.systemDB.GetDB().Exec(
		`INSERT INTO __marmot_databases (name, created_at, path, generation, dropped, legacy)
		 VALUES (?, ?, ?, ?, 1, 0)
		 ON CONFLICT(name) DO UPDATE SET generation = excluded.generation, dropped = 1, legacy = 0`,
		name, time.Now().UnixNano(), dbPath, generation,
	)
	if err != nil {
		return fmt.Errorf("failed to tombstone database in registry: %w", err)
	}

	// This database's AUTO_INCREMENT claim rows (db/autoinc_claim.go) stay,
	// as DROP TABLE's do: a base is monotone per (database, table) name for
	// the life of the cluster, so a range granted before the drop is never
	// granted again after the database is recreated. The in-memory ranges of
	// its tables end with it.
	dm.mu.RLock()
	claimStore := dm.autoIncClaimStore
	dm.mu.RUnlock()
	if claimStore != nil {
		claimStore.databaseIncarnationEnded(name)
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

	if !exists {
		log.Info().Str("name", name).Uint64("generation", generation).
			Msg("Database tombstoned in registry; no local incarnation to retire")
		return nil
	}

	// Out of the map under mu, closed outside it (see DatabaseManager).
	dm.mu.Lock()
	delete(dm.databases, name)
	dm.mu.Unlock()
	if err := liveDB.Close(); err != nil {
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

// ErrStaleDatabaseOpCoordinator is returned by ReplicationEngine's PREPARE
// gate (prepareDatabaseOperation) when a CREATE/DROP DATABASE statement's
// stamped DatabaseRegistryKey is below this participant's local key for the
// database: the coordinator's view of the database's history is stale. See
// ApplyDatabaseOp's proof for why refusing it here keeps every peer's
// registry from ever regressing.
var ErrStaleDatabaseOpCoordinator = errors.New("stale coordinator: database operation key is below the local registry key")

// DatabaseRegistryKey orders one database name's registry history so that a
// higher key always wins a merge. It is ordered
// lexicographically by (Generation, Dropped), with Dropped=true ranking above
// Dropped=false at the same generation, so a DROP always outranks the CREATE
// it follows. A name this node has never seen has key (0, dropped): lower
// than every generation a real CREATE ever stamps, since CREATE always
// stamps generation >= 1.
type DatabaseRegistryKey struct {
	Generation uint64
	Dropped    bool
}

// Less reports whether k sorts strictly before other in the registry's total
// order.
func (k DatabaseRegistryKey) Less(other DatabaseRegistryKey) bool {
	return k.Compare(other) < 0
}

// Compare returns -1, 0, or 1 as k sorts before, equal to, or after other,
// matching the conventions of cmp.Compare.
func (k DatabaseRegistryKey) Compare(other DatabaseRegistryKey) int {
	if k.Generation != other.Generation {
		if k.Generation < other.Generation {
			return -1
		}
		return 1
	}
	if k.Dropped == other.Dropped {
		return 0
	}
	if other.Dropped {
		return -1
	}
	return 1
}

// DatabaseRegistryEntry is one database name's current registry key, as
// listed by RegistryEntries. Legacy is true when Key carries no real
// generation history - it is a migration default for a row from before
// generation stamps, not a stamp from an actual CREATE or DROP (see
// ApplyDatabaseOp's doc comment).
type DatabaseRegistryEntry struct {
	Name   string
	Key    DatabaseRegistryKey
	Legacy bool
}

// registryKeyRow reads name's raw registry key. It does not take dm.mu:
// dm.systemDB is fixed for the DatabaseManager's lifetime and its *sql.DB is
// safe for concurrent use, so callers that already hold dm.lifecycleMu (the
// Create/Drop/Apply family) can call it directly, while the exported readers
// below take dm.mu themselves for the same lock discipline as this file's
// other exported accessors.
func (dm *DatabaseManager) registryKeyRow(name string) (DatabaseRegistryKey, error) {
	var generation uint64
	var dropped bool
	err := dm.systemDB.GetDB().QueryRow(
		"SELECT generation, dropped FROM __marmot_databases WHERE name = ?", name,
	).Scan(&generation, &dropped)
	if errors.Is(err, sql.ErrNoRows) {
		return DatabaseRegistryKey{Generation: 0, Dropped: true}, nil
	}
	if err != nil {
		return DatabaseRegistryKey{}, fmt.Errorf("failed to read database registry key for %s: %w", name, err)
	}
	return DatabaseRegistryKey{Generation: generation, Dropped: dropped}, nil
}

// RegistryKey returns name's current DatabaseRegistryKey: (0, dropped) when
// the name has never been created on this node.
func (dm *DatabaseManager) RegistryKey(name string) (DatabaseRegistryKey, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.registryKeyRow(name)
}

// RegistryKeyGeneration reports name's current registry generation and
// whether it is presently live. It exists for coordinator.DatabaseManager,
// which cannot import package db's DatabaseRegistryKey without an import
// cycle: it is RegistryKey with the same information carried as two
// primitives instead. The coordinator stamps Statement.DatabaseGeneration
// from it: CREATE stamps generation when
// live (a no-op) or generation+1 otherwise; DROP stamps generation.
func (dm *DatabaseManager) RegistryKeyGeneration(name string) (generation uint64, live bool, err error) {
	key, err := dm.RegistryKey(name)
	if err != nil {
		return 0, false, err
	}
	return key.Generation, !key.Dropped, nil
}

// RegistryEntries lists every database name this node's registry has ever
// known, live or tombstoned, excluding the system database. Anti-entropy
// reconciles a peer's listing with this one by merging each entry through
// ApplyDatabaseOp.
func (dm *DatabaseManager) RegistryEntries() ([]DatabaseRegistryEntry, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	rows, err := dm.systemDB.GetDB().Query(
		"SELECT name, generation, dropped, legacy FROM __marmot_databases WHERE name != ?", SystemDatabaseName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list database registry: %w", err)
	}
	defer rows.Close()

	var entries []DatabaseRegistryEntry
	for rows.Next() {
		var e DatabaseRegistryEntry
		if err := rows.Scan(&e.Name, &e.Key.Generation, &e.Key.Dropped, &e.Legacy); err != nil {
			return nil, fmt.Errorf("failed to read database registry row: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// ApplyDatabaseOp merges a peer's or a coordinator's DatabaseRegistryKey for
// name into this node's registry, adopting it if and only if it exceeds the
// local key: key.Dropped tombstones name at key.Generation through the same
// path DropDatabase uses; otherwise it creates name live at key.Generation
// through the same path CreateDatabase uses. changed reports whether key was
// adopted.
//
// It is the one merge function both the 2PC COMMIT (ReplicationEngine.Commit)
// and anti-entropy's database-set reconciliation use, and it is what the
// PREPARE gate (prepareDatabaseOperation) fences for by refusing a stamp
// below the local key.
//
// Proof that this converges every node's registry for name, and that a
// dropped database stays dropped:
//  1. Local keys only ever increase - this function is a no-op unless
//     key > local - and the merge is a max over a total order
//     (DatabaseRegistryKey.Compare), so every node's registry for name
//     converges to the same key.
//  2. The last op on name that committed was applied by at least Q nodes,
//     2PC's write quorum.
//  3. Any later op's quorum contains at least one of them. That node's
//     PREPARE gate refuses a stamp below its local key, and at most N-Q < Q
//     nodes are stale (behind the last committed op), so a stale coordinator
//     can never gather a quorum to commit an op with a lower key.
//  4. Hence each committed op's key exceeds every earlier committed op's key
//     on name. The maximum over all committed ops is the last one to commit,
//     so an older peer, whose key is lower, can never undo a drop by
//     resurrecting an earlier live key.
//  5. A CREATE that follows a DROP stamps generation+1, live - strictly above
//     the tombstone's (generation, dropped) key - so re-creating a dropped
//     database always wins the merge over the tombstone.
//
// Legacy rows and the pre-upgrade divergence: before generation stamps,
// DROP DATABASE deleted the registry row outright, so
// a node that was down for a pre-upgrade DROP still has name's row after
// upgrading, migrated to (1, live) with Legacy=true
// (migrateDatabaseRegistrySchema). That row is a real disagreement about
// whether name exists, not a case this function's merge proof covers - no
// quorum ever stamped (1, live) for it post-upgrade. Reconciling it as an
// ordinary key would let the stale node's peers adopt it and create name
// empty cluster-wide, spreading a divergence that used to stay on one node.
// The caller (reconcileRegistryWithPeer, grpc/anti_entropy.go) is therefore
// responsible for never calling this function with a legacy live entry when
// the local registry has no row for name at all (DatabaseRegistryKey{0,
// dropped}, i.e. this peer never had name): that is the one case where
// applying the key would CREATE the database. Reconciliation does not
// spread this divergence, and it does not repair it either - the node that missed the
// DROP keeps the database until an operator drops it directly. A legacy row
// stops being legacy the moment any post-upgrade CREATE or DROP - local or
// merged - stamps name with a real generation
// (createDatabaseAtGenerationLocked, dropDatabaseAtGenerationLocked), at
// which point ordinary merge semantics apply to it from then on.
func (dm *DatabaseManager) ApplyDatabaseOp(name string, key DatabaseRegistryKey) (bool, error) {
	if name == SystemDatabaseName {
		return false, fmt.Errorf("cannot apply a database operation to the system database")
	}
	if key.Dropped && name == DefaultDatabaseName {
		return false, fmt.Errorf("cannot drop default database")
	}

	if local, err := dm.RegistryKey(name); err != nil {
		return false, err
	} else if key.Compare(local) <= 0 {
		return false, nil
	}

	dm.lifecycleMu.Lock()
	defer dm.lifecycleMu.Unlock()

	// Re-check under lifecycleMu: a concurrent ApplyDatabaseOp, CreateDatabase
	// or DropDatabase may have advanced the local key past key since the
	// unlocked read above.
	local, err := dm.registryKeyRow(name)
	if err != nil {
		return false, err
	}
	if key.Compare(local) <= 0 {
		return false, nil
	}

	if key.Dropped {
		if err := dm.dropDatabaseAtGenerationLocked(name, key.Generation); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := dm.createDatabaseAtGenerationLocked(name, key.Generation); err != nil {
		return false, err
	}
	return true, nil
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
	// tables are the replaced file's table definitions, which AttachDatabase
	// compares with the restored file's (SchemaChange).
	tables map[string]string
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
	// The file's tables, read before the drain closes its pools, are what the
	// restored file's schema is compared with when it is attached. A DDL
	// commit still in flight here reports its own change as it applies.
	tables, err := tableDefinitions(ctx, mdb.GetReadDB())
	if err != nil {
		log.Warn().Err(err).Str("name", name).Msg("Could not read the detached database's tables; its restore ends every table's incarnation without inheritance")
	}
	dm.mu.Lock()
	if detached := dm.detached[name]; detached != nil {
		detached.tables = tables
	}
	dm.mu.Unlock()
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
	claimStore := dm.autoIncClaimStore
	dm.mu.Unlock()
	if err := restoredSchemaChange(mdb, name, detached.tables, claimStore); err != nil {
		if closeErr := mdb.closeSQLite(); closeErr != nil {
			log.Warn().Err(closeErr).Str("name", name).Msg("Error closing a restored database that failed to reattach")
		}
		dm.mu.Lock()
		detached.restoreFailed = true
		dm.mu.Unlock()
		return fmt.Errorf("failed to reattach database %s: %w", name, err)
	}
	dm.mu.Lock()
	delete(dm.detached, name)
	dm.databases[name] = mdb
	dm.mu.Unlock()
	log.Info().Str("name", name).Msg("Database reattached after snapshot restore")
	return nil
}

// restoredSchemaChange applies to a restored database what any DDL applied
// on this node would (SchemaChange), before the database is back in service:
// every table in it may be a new incarnation, so the whole database's ranges
// are forgotten, as DROP DATABASE's are; every table that took the place of
// one the replaced file had inherits its base; and every table is seeded from
// its own MAX(id).
func restoredSchemaChange(mdb *ReplicatedDatabase, name string, before map[string]string, claimStore *AutoIncClaimStore) error {
	after, err := tableDefinitions(context.Background(), mdb.GetDB())
	if err != nil {
		return fmt.Errorf("read restored tables: %w", err)
	}
	var change SchemaChange
	change.record(before, after, 0)
	for table := range after {
		change.tables = append(change.tables, ddlTableOwner{table: table})
	}
	if claimStore != nil {
		claimStore.databaseIncarnationEnded(name)
	}
	return mdb.txnMgr.seedSchemaChange(mdb.GetDB(), &change)
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
	return NewReplicatedDatabase(filepath.Join(dm.dataDir, dbPath), dm.nodeID, dm.clock, metaStore, dm.schemaVersionOptions(name)...)
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

		// A name this cluster has dropped must never be silently resurrected
		// by an import: the registry, not file presence, is the source of
		// truth for whether a database exists (see DatabaseRegistryKey).
		if key, err := dm.registryKeyRow(dbName); err != nil {
			log.Warn().Err(err).Str("name", dbName).Msg("Failed to read database registry, skipping import")
			continue
		} else if key.Dropped && key.Generation > 0 {
			log.Debug().Str("name", dbName).Msg("Database is tombstoned in the registry, skipping import")
			continue
		}

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
		db, err := NewReplicatedDatabase(dstPath, dm.nodeID, dm.clock, metaStore, dm.schemaVersionOptions(dbName)...)
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
// 4. Releases write lock
//
// The caller should stream from the target directory and clean it up when done.
// This ensures snapshot consistency since files are copied atomically under lock.
//
// Schema versions no longer travel out of band with a snapshot: each
// copied SQLite file carries its own __marmot_schema_version table, so the
// receiver's restored file is authoritative on its own.
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

	if err := dm.errIfDetachedLocked(); err != nil {
		return nil, 0, err
	}

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
		Database:             database,
	}
	// The declared width comes from the marker the transpiler wrote into the
	// CREATE TABLE text; a column without one keeps the 64-bit path.
	for _, col := range schema.FullColumns {
		if autoIncCol != "" && strings.EqualFold(col.Name, autoIncCol) {
			info.AutoIncrementWidth = col.DeclaredWidth
			info.AutoIncrementUnsigned = col.Unsigned
			info.AutoIncrementExplicit = col.ExplicitAutoInc
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
