package grpc

import (
	"fmt"
	"path/filepath"

	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
)

// snapshotMetaStoreOptions sizes the short-lived MetaStore handle used to record
// restored schema versions. The store is opened, written and closed immediately,
// so it only needs minimal memory. MemTableCount is 2 because Pebble rejects a
// stop-writes threshold below that when creating a store, and a joining node has
// no system MetaStore yet.
func snapshotMetaStoreOptions() db.PebbleMetaStoreOptions {
	return db.PebbleMetaStoreOptions{
		CacheSizeMB:           16,
		MemTableSizeMB:        16,
		MemTableCount:         2,
		L0CompactionThreshold: 4,
		L0StopWrites:          12,
	}
}

// snapshotSchemaVersions reads this node's schema version for every database so
// they can be advertised alongside a snapshot. Returns an empty map when the
// versions cannot be read - the snapshot itself is still usable, the receiver
// simply has nothing to restore.
func snapshotSchemaVersions(dbManager *db.DatabaseManager) map[string]uint64 {
	systemDB, err := dbManager.GetDatabase(db.SystemDatabaseName)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to reach system database for snapshot schema versions")
		return nil
	}

	stored, err := systemDB.GetMetaStore().GetAllSchemaVersions()
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

// persistSnapshotSchemaVersions records the schema versions carried by a snapshot
// into the system database's MetaStore.
//
// A snapshot transfers SQLite files only, while schema versions live in the
// MetaStore. A node that catches up without this keeps reporting version 0 and
// then refuses every transaction stamped with a higher RequiredSchemaVersion -
// including plain DML - so it silently stops replicating and never recovers,
// because the only other way to advance the version is committing a DDL
// transaction it can no longer take part in.
//
// Used by the join path, where the DatabaseManager does not exist yet and the
// MetaStore has to be opened directly by path.
func persistSnapshotSchemaVersions(dataDir string, metadata []*DatabaseSnapshotMetadata) error {
	versions := SnapshotSchemaVersions(metadata)
	if len(versions) == 0 {
		return nil
	}

	metaPath := filepath.Join(dataDir, db.SystemDatabaseName+"_meta.pebble")
	metaStore, err := db.NewPebbleMetaStore(metaPath, snapshotMetaStoreOptions())
	if err != nil {
		return fmt.Errorf("failed to open system MetaStore to restore schema versions: %w", err)
	}
	defer metaStore.Close()

	return db.RestoreSchemaVersions(metaStore, versions)
}

// SnapshotSchemaVersions extracts the schema versions advertised with a snapshot,
// keyed by database name.
func SnapshotSchemaVersions(metadata []*DatabaseSnapshotMetadata) map[string]uint64 {
	versions := make(map[string]uint64, len(metadata))
	for _, meta := range metadata {
		if meta == nil || meta.DatabaseName == "" || meta.SchemaVersion == 0 {
			continue
		}
		versions[meta.DatabaseName] = meta.SchemaVersion
	}
	return versions
}
