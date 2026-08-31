package grpc

import (
	"fmt"
	"path/filepath"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/encoding"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/metadata"
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
// into the system database's MetaStore, opening it directly by path.
//
// A snapshot transfers SQLite files only, while schema versions live in the
// MetaStore. A node that catches up without this keeps reporting version 0 and
// then refuses every transaction stamped with a higher RequiredSchemaVersion -
// including plain DML - so it silently stops replicating and never recovers,
// because the only other way to advance the version is committing a DDL
// transaction it can no longer take part in.
//
// Used by the join path, where the DatabaseManager does not exist yet. Opening
// the MetaStore a second time by path while a DatabaseManager already holds it
// open would fail: Pebble takes an exclusive lock per process. Runtime restores
// (anti-entropy) must go through persistSnapshotSchemaVersionsViaManager instead.
func persistSnapshotSchemaVersions(dataDir string, versions map[string]uint64) error {
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

// persistSnapshotSchemaVersionsViaManager records the schema versions carried by
// a snapshot into the system database's MetaStore, writing through an
// already-open DatabaseManager instead of opening a second handle on the same
// Pebble directory (which would deadlock against Pebble's per-process
// exclusive lock). Used by the anti-entropy runtime path, where the
// DatabaseManager already exists.
func persistSnapshotSchemaVersionsViaManager(dbManager *db.DatabaseManager, versions map[string]uint64) error {
	if len(versions) == 0 {
		return nil
	}

	systemDB, err := dbManager.GetDatabase(db.SystemDatabaseName)
	if err != nil {
		return fmt.Errorf("system database unavailable: %w", err)
	}

	return db.RestoreSchemaVersions(systemDB.GetMetaStore(), versions)
}

// SnapshotSchemaVersions extracts the schema versions advertised with a snapshot,
// keyed by database name.
func SnapshotSchemaVersions(snapshotMetadata []*DatabaseSnapshotMetadata) map[string]uint64 {
	versions := make(map[string]uint64, len(snapshotMetadata))
	for _, meta := range snapshotMetadata {
		if meta == nil || meta.DatabaseName == "" || meta.SchemaVersion == 0 {
			continue
		}
		versions[meta.DatabaseName] = meta.SchemaVersion
	}
	return versions
}

// snapshotSchemaVersionsTrailerKey carries schema versions as gRPC trailer
// metadata on the StreamSnapshot RPC. Binary values need the "-bin" key suffix;
// grpc-go base64-encodes/decodes them on the wire automatically.
const snapshotSchemaVersionsTrailerKey = "marmot-snapshot-schema-versions-bin"

// snapshotSchemaVersionsTrailer builds gRPC trailer metadata carrying schema
// versions captured atomically with the snapshot files actually streamed, so
// the receiver does not have to trust the separate (and potentially stale)
// read taken earlier by GetSnapshotInfo. Returns nil when there is nothing to
// carry, so callers see no trailer rather than an empty one.
func snapshotSchemaVersionsTrailer(versions map[string]uint64) metadata.MD {
	if len(versions) == 0 {
		return nil
	}

	payload, err := encoding.Marshal(versions)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to encode schema versions for snapshot trailer")
		return nil
	}

	return metadata.Pairs(snapshotSchemaVersionsTrailerKey, string(payload))
}

// decodeSchemaVersionsTrailer decodes schema versions carried in a snapshot
// stream's trailer. Returns (nil, nil) when no trailer was set - by an older
// server, or a stream with nothing to restore - so callers can fall back to
// the versions advertised earlier by GetSnapshotInfo without treating it as an
// error.
func decodeSchemaVersionsTrailer(md metadata.MD) (map[string]uint64, error) {
	values := md.Get(snapshotSchemaVersionsTrailerKey)
	if len(values) == 0 {
		return nil, nil
	}

	var versions map[string]uint64
	if err := encoding.Unmarshal([]byte(values[0]), &versions); err != nil {
		return nil, fmt.Errorf("failed to decode schema versions trailer: %w", err)
	}
	return versions, nil
}

// SnapshotStreamTrailer is satisfied by both ends of the StreamSnapshot gRPC
// stream. It is used to read the authoritative schema versions the server
// attaches as trailer metadata once the snapshot's files have been produced.
type SnapshotStreamTrailer interface {
	Trailer() metadata.MD
}

// SnapshotVersionsForRestore chooses which schema versions to restore after a
// snapshot. It prefers the versions carried in the stream's trailer - captured
// atomically with the files that were actually streamed - and falls back to
// the versions advertised earlier by GetSnapshotInfo, which can go stale if a
// DDL commits between that call and the StreamSnapshot call that follows it.
func SnapshotVersionsForRestore(snapshotMetadata []*DatabaseSnapshotMetadata, stream SnapshotStreamTrailer) map[string]uint64 {
	trailerVersions, err := decodeSchemaVersionsTrailer(stream.Trailer())
	if err != nil {
		log.Warn().Err(err).Msg("Failed to decode schema versions from snapshot trailer, falling back to GetSnapshotInfo metadata")
	} else if len(trailerVersions) > 0 {
		return trailerVersions
	}
	return SnapshotSchemaVersions(snapshotMetadata)
}
