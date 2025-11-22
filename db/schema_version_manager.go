package db

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// SchemaVersionManager tracks and manages schema versions per database
// Enables QUORUM-based DDL replication with automatic catch-up
type SchemaVersionManager struct {
	db *sql.DB
	mu sync.RWMutex
	// Cache of schema versions per database
	versions map[string]uint64
}

// NewSchemaVersionManager creates a new schema version manager
func NewSchemaVersionManager(db *sql.DB) *SchemaVersionManager {
	return &SchemaVersionManager{
		db:       db,
		versions: make(map[string]uint64),
	}
}

// GetSchemaVersion returns the current schema version for a database
func (svm *SchemaVersionManager) GetSchemaVersion(database string) (uint64, error) {
	svm.mu.RLock()
	if version, ok := svm.versions[database]; ok {
		svm.mu.RUnlock()
		return version, nil
	}
	svm.mu.RUnlock()

	// Not in cache, query database
	svm.mu.Lock()
	defer svm.mu.Unlock()

	// Double-check after acquiring write lock
	if version, ok := svm.versions[database]; ok {
		return version, nil
	}

	var version uint64
	err := svm.db.QueryRow(`
		SELECT schema_version
		FROM __marmot__schema_versions
		WHERE database_name = ?
	`, database).Scan(&version)

	if err == sql.ErrNoRows {
		// Initialize schema version for new database
		version = 0
		_, err = svm.db.Exec(`
			INSERT OR IGNORE INTO __marmot__schema_versions
			(database_name, schema_version, updated_at)
			VALUES (?, ?, ?)
		`, database, version, time.Now().UnixNano())
		if err != nil {
			return 0, fmt.Errorf("failed to initialize schema version: %w", err)
		}
	} else if err != nil {
		return 0, fmt.Errorf("failed to get schema version: %w", err)
	}

	svm.versions[database] = version
	return version, nil
}

// IncrementSchemaVersion increments the schema version after a DDL operation
// Returns the NEW schema version
func (svm *SchemaVersionManager) IncrementSchemaVersion(database string, ddlSQL string, txnID uint64) (uint64, error) {
	svm.mu.Lock()
	defer svm.mu.Unlock()

	// Get current version
	var currentVersion uint64
	err := svm.db.QueryRow(`
		SELECT schema_version
		FROM __marmot__schema_versions
		WHERE database_name = ?
	`, database).Scan(&currentVersion)

	if err == sql.ErrNoRows {
		// Initialize if not exists
		currentVersion = 0
	} else if err != nil {
		return 0, fmt.Errorf("failed to get current schema version: %w", err)
	}

	newVersion := currentVersion + 1

	// Update schema version
	_, err = svm.db.Exec(`
		INSERT OR REPLACE INTO __marmot__schema_versions
		(database_name, schema_version, last_ddl_sql, last_ddl_txn_id, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`, database, newVersion, ddlSQL, txnID, time.Now().UnixNano())

	if err != nil {
		return 0, fmt.Errorf("failed to increment schema version: %w", err)
	}

	// Update cache
	svm.versions[database] = newVersion

	log.Info().
		Str("database", database).
		Uint64("old_version", currentVersion).
		Uint64("new_version", newVersion).
		Uint64("txn_id", txnID).
		Msg("Schema version incremented")

	return newVersion, nil
}

// SetSchemaVersion explicitly sets the schema version (used during catch-up)
func (svm *SchemaVersionManager) SetSchemaVersion(database string, version uint64, ddlSQL string, txnID uint64) error {
	svm.mu.Lock()
	defer svm.mu.Unlock()

	_, err := svm.db.Exec(`
		INSERT OR REPLACE INTO __marmot__schema_versions
		(database_name, schema_version, last_ddl_sql, last_ddl_txn_id, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`, database, version, ddlSQL, txnID, time.Now().UnixNano())

	if err != nil {
		return fmt.Errorf("failed to set schema version: %w", err)
	}

	// Update cache
	svm.versions[database] = version

	log.Info().
		Str("database", database).
		Uint64("version", version).
		Uint64("txn_id", txnID).
		Msg("Schema version set")

	return nil
}

// WaitForSchemaVersion waits until the local schema version reaches the target
// Returns error if timeout is exceeded
// Used by delta sync to ensure DDL is applied before DML
func (svm *SchemaVersionManager) WaitForSchemaVersion(database string, targetVersion uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		currentVersion, err := svm.GetSchemaVersion(database)
		if err != nil {
			return fmt.Errorf("failed to get schema version: %w", err)
		}

		if currentVersion >= targetVersion {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for schema version %d (current: %d)", targetVersion, currentVersion)
		}

		select {
		case <-ticker.C:
			// Continue waiting
		}
	}
}

// GetAllSchemaVersions returns a map of all database schema versions
// Used by gossip protocol to exchange schema version info
func (svm *SchemaVersionManager) GetAllSchemaVersions() (map[string]uint64, error) {
	svm.mu.RLock()
	defer svm.mu.RUnlock()

	rows, err := svm.db.Query(`
		SELECT database_name, schema_version
		FROM __marmot__schema_versions
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema versions: %w", err)
	}
	defer rows.Close()

	versions := make(map[string]uint64)
	for rows.Next() {
		var database string
		var version uint64
		if err := rows.Scan(&database, &version); err != nil {
			return nil, fmt.Errorf("failed to scan schema version: %w", err)
		}
		versions[database] = version
		// Update cache
		svm.versions[database] = version
	}

	return versions, nil
}

// InvalidateCache clears the cached schema versions
// Useful after database deletion or during testing
func (svm *SchemaVersionManager) InvalidateCache(database string) {
	svm.mu.Lock()
	defer svm.mu.Unlock()
	delete(svm.versions, database)
}
