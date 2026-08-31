package db

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

// RestoreSchemaVersions records schema versions carried by a snapshot into the
// system MetaStore, where schema versions are tracked.
//
// Snapshots transfer SQLite files only, so a receiver that does not restore these
// keeps reporting version 0 and then refuses every transaction stamped with a
// higher required schema version - including plain DML. That state never heals on
// its own, because the only way to advance the version is committing a DDL
// transaction the node can no longer take part in.
//
// A database is never moved backwards: the node may have applied DDL after the
// snapshot was taken.
func RestoreSchemaVersions(metaStore MetaStore, versions map[string]uint64) error {
	if metaStore == nil || len(versions) == 0 {
		return nil
	}

	for database, version := range versions {
		if database == "" || version == 0 {
			continue
		}

		current, err := metaStore.GetSchemaVersion(database)
		if err != nil {
			return fmt.Errorf("failed to read schema version for %s: %w", database, err)
		}
		if current >= int64(version) {
			continue
		}

		if err := metaStore.UpdateSchemaVersion(database, int64(version), "", 0); err != nil {
			return fmt.Errorf("failed to restore schema version for %s: %w", database, err)
		}

		log.Info().
			Str("database", database).
			Int64("previous_version", current).
			Uint64("restored_version", version).
			Msg("Restored schema version from snapshot")
	}

	return nil
}
