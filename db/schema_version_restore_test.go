package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newRestoreTestMetaStore(t *testing.T) MetaStore {
	t.Helper()

	metaStore, err := NewPebbleMetaStore(t.TempDir(), PebbleMetaStoreOptions{
		CacheSizeMB:           16,
		MemTableSizeMB:        16,
		MemTableCount:         2,
		L0CompactionThreshold: 4,
		L0StopWrites:          12,
	})
	require.NoError(t, err)
	t.Cleanup(func() { metaStore.Close() })
	return metaStore
}

func TestRestoreSchemaVersions(t *testing.T) {
	t.Parallel()
	metaStore := newRestoreTestMetaStore(t)

	require.NoError(t, RestoreSchemaVersions(metaStore, map[string]uint64{"appdb": 17, "otherdb": 3}))

	version, err := metaStore.GetSchemaVersion("appdb")
	require.NoError(t, err)
	require.Equal(t, int64(17), version)

	version, err = metaStore.GetSchemaVersion("otherdb")
	require.NoError(t, err)
	require.Equal(t, int64(3), version)
}

// A node that applied DDL after the snapshot was taken must not be rewound: that
// would make it refuse transactions it is actually able to serve.
func TestRestoreSchemaVersionsNeverGoesBackwards(t *testing.T) {
	t.Parallel()
	metaStore := newRestoreTestMetaStore(t)

	require.NoError(t, metaStore.UpdateSchemaVersion("appdb", 20, "", 0))
	require.NoError(t, RestoreSchemaVersions(metaStore, map[string]uint64{"appdb": 17}))

	version, err := metaStore.GetSchemaVersion("appdb")
	require.NoError(t, err)
	require.Equal(t, int64(20), version)
}

func TestRestoreSchemaVersionsIgnoresEmptyInput(t *testing.T) {
	t.Parallel()
	metaStore := newRestoreTestMetaStore(t)

	require.NoError(t, RestoreSchemaVersions(metaStore, nil))
	require.NoError(t, RestoreSchemaVersions(nil, map[string]uint64{"appdb": 4}))
	require.NoError(t, RestoreSchemaVersions(metaStore, map[string]uint64{"": 4, "appdb": 0}))

	versions, err := metaStore.GetAllSchemaVersions()
	require.NoError(t, err)
	require.Empty(t, versions)
}

// The restored version must be what a subsequent PREPARE compares against, which
// is what SchemaVersionManager reads.
func TestRestoreSchemaVersionsVisibleToSchemaVersionManager(t *testing.T) {
	t.Parallel()
	metaStore := newRestoreTestMetaStore(t)

	require.NoError(t, RestoreSchemaVersions(metaStore, map[string]uint64{"appdb": 17}))

	version, err := NewSchemaVersionManager(metaStore).GetSchemaVersion("appdb")
	require.NoError(t, err)
	require.Equal(t, uint64(17), version, "a restored node must not report itself behind the cluster")
}
