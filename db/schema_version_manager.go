package db

import (
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// SchemaVersionManager tracks and manages schema versions per database
// Enables QUORUM-based DDL replication with automatic catch-up
type SchemaVersionManager struct {
	metaStore MetaStore
	mu        sync.RWMutex
	// Cache of schema versions per database
	versions map[string]uint64
}

// NewSchemaVersionManager creates a new schema version manager
func NewSchemaVersionManager(metaStore MetaStore) *SchemaVersionManager {
	return &SchemaVersionManager{
		metaStore: metaStore,
		versions:  make(map[string]uint64),
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

	// Not in cache, query MetaStore
	svm.mu.Lock()
	defer svm.mu.Unlock()

	// Double-check after acquiring write lock
	if version, ok := svm.versions[database]; ok {
		return version, nil
	}

	version, err := svm.metaStore.GetSchemaVersion(database)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema version: %w", err)
	}

	svm.versions[database] = uint64(version)
	return uint64(version), nil
}

// IncrementSchemaVersion increments the schema version after a DDL operation
// Returns the NEW schema version
func (svm *SchemaVersionManager) IncrementSchemaVersion(database string, ddlSQL string, txnID uint64) (uint64, error) {
	svm.mu.Lock()
	defer svm.mu.Unlock()

	// Get current version from MetaStore
	currentVersion, err := svm.metaStore.GetSchemaVersion(database)
	if err != nil {
		return 0, fmt.Errorf("failed to get current schema version: %w", err)
	}

	newVersion := currentVersion + 1

	// Update schema version in MetaStore
	err = svm.metaStore.UpdateSchemaVersion(database, newVersion, ddlSQL, txnID)
	if err != nil {
		return 0, fmt.Errorf("failed to increment schema version: %w", err)
	}

	// Update cache
	svm.versions[database] = uint64(newVersion)

	log.Info().
		Str("database", database).
		Int64("old_version", currentVersion).
		Int64("new_version", newVersion).
		Uint64("txn_id", txnID).
		Msg("Schema version incremented")

	return uint64(newVersion), nil
}

// SetSchemaVersion explicitly sets the schema version (used during catch-up)
func (svm *SchemaVersionManager) SetSchemaVersion(database string, version uint64, ddlSQL string, txnID uint64) error {
	svm.mu.Lock()
	defer svm.mu.Unlock()

	err := svm.metaStore.UpdateSchemaVersion(database, int64(version), ddlSQL, txnID)
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
	svm.mu.Lock()
	defer svm.mu.Unlock()

	versions, err := svm.metaStore.GetAllSchemaVersions()
	if err != nil {
		return nil, fmt.Errorf("failed to query schema versions: %w", err)
	}

	result := make(map[string]uint64)
	for dbName, version := range versions {
		result[dbName] = uint64(version)
		// Update cache
		svm.versions[dbName] = uint64(version)
	}

	return result, nil
}

// InvalidateCache clears the cached schema versions
// Useful after database deletion or during testing
func (svm *SchemaVersionManager) InvalidateCache(database string) {
	svm.mu.Lock()
	defer svm.mu.Unlock()
	delete(svm.versions, database)
}
