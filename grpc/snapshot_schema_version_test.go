package grpc

import (
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/stretchr/testify/require"
)

// A node that catches up by snapshot receives only SQLite files; schema versions
// live in the system database's MetaStore and are not part of that transfer.
// Without restoring them the node reports version 0 forever and every later
// transaction carrying a RequiredSchemaVersion is refused, which silently drops
// the node out of replication.
func TestPersistSnapshotSchemaVersions(t *testing.T) {
	dataDir := t.TempDir()

	metadata := []*DatabaseSnapshotMetadata{
		{DatabaseName: "appdb", SchemaVersion: 17},
		{DatabaseName: "otherdb", SchemaVersion: 3},
	}

	require.NoError(t, persistSnapshotSchemaVersions(dataDir, metadata))

	versions := readSchemaVersions(t, dataDir)
	require.Equal(t, int64(17), versions["appdb"])
	require.Equal(t, int64(3), versions["otherdb"])
}

// Restoring an older snapshot must not roll a node's schema version backwards:
// the node may have applied DDL after the snapshot was taken.
func TestPersistSnapshotSchemaVersionsNeverGoesBackwards(t *testing.T) {
	dataDir := t.TempDir()

	require.NoError(t, persistSnapshotSchemaVersions(dataDir,
		[]*DatabaseSnapshotMetadata{{DatabaseName: "appdb", SchemaVersion: 17}}))
	require.NoError(t, persistSnapshotSchemaVersions(dataDir,
		[]*DatabaseSnapshotMetadata{{DatabaseName: "appdb", SchemaVersion: 5}}))

	require.Equal(t, int64(17), readSchemaVersions(t, dataDir)["appdb"])
}

func TestPersistSnapshotSchemaVersionsIgnoresEmptyMetadata(t *testing.T) {
	dataDir := t.TempDir()

	require.NoError(t, persistSnapshotSchemaVersions(dataDir, nil))
	require.NoError(t, persistSnapshotSchemaVersions(dataDir, []*DatabaseSnapshotMetadata{
		{DatabaseName: "", SchemaVersion: 4},      // no database name
		{DatabaseName: "appdb", SchemaVersion: 0}, // nothing to restore
	}))

	require.Empty(t, readSchemaVersions(t, dataDir))
}

func readSchemaVersions(t *testing.T, dataDir string) map[string]int64 {
	t.Helper()

	metaPath := filepath.Join(dataDir, db.SystemDatabaseName+"_meta.pebble")
	metaStore, err := db.NewPebbleMetaStore(metaPath, snapshotMetaStoreOptions())
	require.NoError(t, err)
	defer metaStore.Close()

	versions, err := metaStore.GetAllSchemaVersions()
	require.NoError(t, err)
	return versions
}
