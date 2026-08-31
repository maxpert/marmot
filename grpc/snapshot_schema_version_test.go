package grpc

import (
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
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

	require.NoError(t, persistSnapshotSchemaVersions(dataDir, SnapshotSchemaVersions(metadata)))

	versions := readSchemaVersions(t, dataDir)
	require.Equal(t, int64(17), versions["appdb"])
	require.Equal(t, int64(3), versions["otherdb"])
}

// Restoring an older snapshot must not roll a node's schema version backwards:
// the node may have applied DDL after the snapshot was taken.
func TestPersistSnapshotSchemaVersionsNeverGoesBackwards(t *testing.T) {
	dataDir := t.TempDir()

	require.NoError(t, persistSnapshotSchemaVersions(dataDir,
		SnapshotSchemaVersions([]*DatabaseSnapshotMetadata{{DatabaseName: "appdb", SchemaVersion: 17}})))
	require.NoError(t, persistSnapshotSchemaVersions(dataDir,
		SnapshotSchemaVersions([]*DatabaseSnapshotMetadata{{DatabaseName: "appdb", SchemaVersion: 5}})))

	require.Equal(t, int64(17), readSchemaVersions(t, dataDir)["appdb"])
}

func TestPersistSnapshotSchemaVersionsIgnoresEmptyMetadata(t *testing.T) {
	dataDir := t.TempDir()

	require.NoError(t, persistSnapshotSchemaVersions(dataDir, SnapshotSchemaVersions(nil)))
	require.NoError(t, persistSnapshotSchemaVersions(dataDir, SnapshotSchemaVersions([]*DatabaseSnapshotMetadata{
		{DatabaseName: "", SchemaVersion: 4},      // no database name
		{DatabaseName: "appdb", SchemaVersion: 0}, // nothing to restore
	})))

	require.Empty(t, readSchemaVersions(t, dataDir))
}

// persistSnapshotSchemaVersions opens the system MetaStore by path. That is
// only safe on the startup join path, where no DatabaseManager exists yet. If
// a DatabaseManager is already running against the same dataDir - the runtime
// anti-entropy case - Pebble's per-process exclusive lock makes a second open
// of the same directory fail. This pins that failure mode so a future change
// cannot silently reintroduce the anti-entropy stall this fix removes.
func TestPersistSnapshotSchemaVersions_FailsWhenManagerAlreadyOpen(t *testing.T) {
	dataDir := t.TempDir()

	dbMgr, err := db.NewDatabaseManager(dataDir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dbMgr.Close()

	err = persistSnapshotSchemaVersions(dataDir, SnapshotSchemaVersions(
		[]*DatabaseSnapshotMetadata{{DatabaseName: "appdb", SchemaVersion: 1}}))
	require.Error(t, err)
}

// persistSnapshotSchemaVersionsViaManager is what the runtime path must use
// instead: it writes through the DatabaseManager's already-open system
// MetaStore, so it succeeds even while that same store is held open by the
// DatabaseManager - the exact situation that fails above.
func TestPersistSnapshotSchemaVersionsViaManager(t *testing.T) {
	dataDir := t.TempDir()

	dbMgr, err := db.NewDatabaseManager(dataDir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dbMgr.Close()

	versions := map[string]uint64{"appdb": 9}
	require.NoError(t, persistSnapshotSchemaVersionsViaManager(dbMgr, versions))

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	require.NoError(t, err)
	got, err := systemDB.GetMetaStore().GetSchemaVersion("appdb")
	require.NoError(t, err)
	require.Equal(t, int64(9), got)
}

func TestPersistSnapshotSchemaVersionsViaManager_IgnoresEmptyVersions(t *testing.T) {
	dataDir := t.TempDir()

	dbMgr, err := db.NewDatabaseManager(dataDir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dbMgr.Close()

	require.NoError(t, persistSnapshotSchemaVersionsViaManager(dbMgr, nil))
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

// --- trailer encode/decode ---

func TestSnapshotSchemaVersionsTrailer_RoundTrip(t *testing.T) {
	versions := map[string]uint64{"appdb": 4, "otherdb": 1}

	trailer := snapshotSchemaVersionsTrailer(versions)
	require.NotNil(t, trailer)

	got, err := decodeSchemaVersionsTrailer(trailer)
	require.NoError(t, err)
	require.Equal(t, versions, got)
}

func TestSnapshotSchemaVersionsTrailer_EmptyVersionsProduceNoTrailer(t *testing.T) {
	require.Nil(t, snapshotSchemaVersionsTrailer(nil))
	require.Nil(t, snapshotSchemaVersionsTrailer(map[string]uint64{}))
}

// An absent trailer (older server, or nothing to restore) must decode to a
// nil map with no error, so callers fall back to GetSnapshotInfo metadata
// instead of treating "no trailer" as a decode failure.
func TestDecodeSchemaVersionsTrailer_AbsentIsNotAnError(t *testing.T) {
	versions, err := decodeSchemaVersionsTrailer(metadata.MD{})
	require.NoError(t, err)
	require.Nil(t, versions)
}

func TestDecodeSchemaVersionsTrailer_CorruptPayloadErrors(t *testing.T) {
	md := metadata.Pairs(snapshotSchemaVersionsTrailerKey, "not valid msgpack")
	_, err := decodeSchemaVersionsTrailer(md)
	require.Error(t, err)
}

// --- SnapshotVersionsForRestore ---

type fakeSnapshotStreamTrailer struct {
	trailer metadata.MD
}

func (f *fakeSnapshotStreamTrailer) Trailer() metadata.MD { return f.trailer }

// The trailer - captured atomically with the files actually streamed - must
// win over GetSnapshotInfo's earlier, separately-read metadata whenever both
// are present. This is the fix for the race where a DDL commits between the
// GetSnapshotInfo and StreamSnapshot RPCs.
func TestSnapshotVersionsForRestore_PrefersTrailerOverMetadata(t *testing.T) {
	staleMetadata := []*DatabaseSnapshotMetadata{{DatabaseName: "appdb", SchemaVersion: 3}}
	stream := &fakeSnapshotStreamTrailer{trailer: snapshotSchemaVersionsTrailer(map[string]uint64{"appdb": 4})}

	got := SnapshotVersionsForRestore(staleMetadata, stream)
	require.Equal(t, map[string]uint64{"appdb": 4}, got)
}

// An older server never sets the trailer. Receivers must fall back to
// GetSnapshotInfo's metadata rather than restoring nothing.
func TestSnapshotVersionsForRestore_FallsBackWhenNoTrailer(t *testing.T) {
	fallbackMetadata := []*DatabaseSnapshotMetadata{{DatabaseName: "appdb", SchemaVersion: 3}}
	stream := &fakeSnapshotStreamTrailer{trailer: metadata.MD{}}

	got := SnapshotVersionsForRestore(fallbackMetadata, stream)
	require.Equal(t, map[string]uint64{"appdb": 3}, got)
}

// A corrupt trailer must not be trusted, but must also not make restore fail
// outright - fall back to the metadata instead.
func TestSnapshotVersionsForRestore_FallsBackOnCorruptTrailer(t *testing.T) {
	fallbackMetadata := []*DatabaseSnapshotMetadata{{DatabaseName: "appdb", SchemaVersion: 3}}
	stream := &fakeSnapshotStreamTrailer{
		trailer: metadata.Pairs(snapshotSchemaVersionsTrailerKey, "not valid msgpack"),
	}

	got := SnapshotVersionsForRestore(fallbackMetadata, stream)
	require.Equal(t, map[string]uint64{"appdb": 3}, got)
}
