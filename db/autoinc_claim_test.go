//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"runtime"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// TestClaimIntentReachesPebbleWithItsPayload is the storage-selection property
// the whole claim protocol rests on.
//
// WriteIntent short-circuits IntentTypeDML into storeDMLIntent, whose record
// omits DataSnapshot and which writes only to an in-memory map, while
// GetIntentsByTxn iterates the Pebble prefix. A claim stored on the DML branch
// would therefore never be returned and its payload would never have existed -
// so COMMIT would apply nothing, every participant's base would stay put, and
// the next claimant would compute the same range and mint the same ids.
func TestClaimIntentReachesPebbleWithItsPayload(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	store := source.GetMetaStore()

	payload, err := protocol.EncodeAutoIncClaim(AutoIncClaim{Table: "users", PrevBase: 100, NewBase: 100, Size: 64})
	if err != nil {
		t.Fatalf("EncodeAutoIncClaim: %v", err)
	}

	const txnID = uint64(9001)
	key := protocol.AutoIncClaimKey("testdb", "users")
	if err := store.WriteIntent(txnID, IntentTypeAutoIDClaim, AutoIncClaimTable, key,
		OpTypeInsert, "", payload, hlc.Timestamp{WallTime: 1}, 1); err != nil {
		t.Fatalf("WriteIntent: %v", err)
	}

	// Mutation: use IntentTypeDML for the claim. GetIntentsByTxn returns
	// nothing and this fires - which is exactly the silent-duplicate-ids
	// failure, reduced to a test.
	intents, err := store.GetIntentsByTxn(txnID)
	if err != nil {
		t.Fatalf("GetIntentsByTxn: %v", err)
	}
	if len(intents) != 1 {
		t.Fatalf("GetIntentsByTxn returned %d intents, want 1", len(intents))
	}
	if intents[0].IntentType != IntentTypeAutoIDClaim {
		t.Errorf("intent type = %v, want AUTO_ID_CLAIM", intents[0].IntentType)
	}
	// The payload is the thing COMMIT reads; an intent without it is useless.
	// Mutation: drop DataSnapshot from the non-DML record literal.
	if len(intents[0].DataSnapshot) == 0 {
		t.Fatal("claim intent carries no DataSnapshot; the COMMIT handler would apply nothing")
	}
	got, err := protocol.DecodeAutoIncClaim(intents[0].DataSnapshot)
	if err != nil {
		t.Fatalf("DecodeAutoIncClaim: %v", err)
	}
	if got != (AutoIncClaim{Table: "users", PrevBase: 100, NewBase: 100, Size: 64}) {
		t.Errorf("payload round-tripped as %+v", got)
	}
}

// TestAutoIncBaseAbsentIsAnErrorNotZero pins the fail-closed rule. The natural
// implementation is "absent means 0 means yes", and that is precisely the thing
// that cannot cast the rejection that would repair it.
//
// The claim table now lives in the system database, keyed by (database,
// table): AutoIncClaimStore's receiver stands in for that system database, and
// ReadBase names both the database and the table it is asking about.
//
// Mutation: return (0, nil) for a missing row or a missing table.
func TestAutoIncBaseAbsentIsAnErrorNotZero(t *testing.T) {
	sys := newRowidTestDatabase(t, 1)
	store := NewAutoIncClaimStore(sys)

	// No claim table at all: not an expected state (the system database
	// creates it), but it must still be an error and never a base.
	if base, err := store.ReadBase("testdb", "users"); err == nil {
		t.Errorf("with no claim table, ReadBase returned base %d and no error", base)
	}

	// Seed a DIFFERENT table to bring the claim table into existence without
	// creating a row for "users", so the next read exercises "table exists,
	// row does not" rather than "table absent" again.
	if err := store.Seed("testdb", "other", 1, 1); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// Table exists, row for "users" does not.
	if _, err := store.ReadBase("testdb", "users"); !errors.Is(err, ErrAutoIncBaseAbsent) {
		t.Errorf("with no row, ReadBase returned %v, want ErrAutoIncBaseAbsent", err)
	}
}

// TestSeedAutoIncBaseOnlyRaises pins the DDL-time floor. Tagging a table that
// already holds ids 1..1000 must not leave the base at 0, or the first range is
// [0,R) and the allocator mints over live rows.
func TestSeedAutoIncBaseOnlyRaises(t *testing.T) {
	sys := newRowidTestDatabase(t, 1)
	store := NewAutoIncClaimStore(sys)

	if err := store.Seed("testdb", "users", 1000, 1); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	base, err := store.ReadBase("testdb", "users")
	if err != nil {
		t.Fatalf("ReadBase: %v", err)
	}
	if base != 1000 {
		t.Fatalf("base = %d after seeding 1000, want 1000", base)
	}

	// A later seed with a LOWER floor must not lower the base: re-applying a
	// replicated DDL on a node that has since committed claims would otherwise
	// wind the base backwards and remint over ids already handed out.
	// Mutation: use excluded.base instead of MAX(base, excluded.base).
	if err := store.Seed("testdb", "users", 5, 1); err != nil {
		t.Fatalf("Seed (lower): %v", err)
	}
	base, err = store.ReadBase("testdb", "users")
	if err != nil {
		t.Fatalf("ReadBase: %v", err)
	}
	if base != 1000 {
		t.Errorf("base = %d after seeding a lower floor, want it to stay 1000", base)
	}

	// A higher floor does raise it.
	if err := store.Seed("testdb", "users", 4999, 1); err != nil {
		t.Fatalf("Seed (higher): %v", err)
	}
	base, err = store.ReadBase("testdb", "users")
	if err != nil {
		t.Fatalf("ReadBase: %v", err)
	}
	if base != 4999 {
		t.Errorf("base = %d after seeding 4999, want 4999", base)
	}
}

// TestClaimTableIsCDCSkipped pins the property the __marmot__ prefix buys: a
// write to the claim table must produce no CDC entry, or the claim would enter
// the blind-apply path and be applied twice by two different mechanisms. This
// holds regardless of which database the claim table sits in, so it is
// exercised directly against source's own SQLite rather than through the
// store (source stands in for the system database here).
//
// Mutation: add an exception at the preupdate hook's prefix skip so the table
// is captured. The hook then looks the table up in the schema cache, which
// never holds it, and the capture fails with "schema cache miss" at
// captureEntries' require.NoError; either that or a non-zero entry count
// fails the test.
func TestClaimTableIsCDCSkipped(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	if _, err := source.GetWriteDB().Exec(autoIncClaimDDL); err != nil {
		t.Fatalf("create %s: %v", AutoIncClaimTable, err)
	}
	if err := source.ReloadSchema(); err != nil {
		t.Fatalf("ReloadSchema: %v", err)
	}

	entries := captureEntries(t, source, 7001,
		"INSERT INTO "+AutoIncClaimTable+" (db, tbl, base, owner, granted_at) VALUES ('testdb', 'users', 64, 1, 0)")
	if len(entries) != 0 {
		t.Errorf("writing the claim table produced %d CDC entries, want 0", len(entries))
	}
}

// TestDropDatabaseRemovesItsClaimRowsAndLeavesOthersIntact pins the DROP
// DATABASE side of the relocation: removing a database must take its claim
// rows with it (db/database_manager.go DropDatabase), but must not touch
// another database's rows in the same shared system-database table. This is
// the opposite of DROP TABLE (TestDroppedTableKeepsItsClaimRow,
// db/autoinc_commit_test.go), which deliberately leaves the row behind.
//
// Mutation: scope the DELETE without a WHERE db = ? clause, or drop it
// entirely. Either the row survives its own database's removal, or a
// surviving database's row is deleted too, and the assertions below fire.
func TestDropDatabaseRemovesItsClaimRowsAndLeavesOthersIntact(t *testing.T) {
	_, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	require.NoError(t, dm.CreateDatabase("dropme"))
	require.NoError(t, dm.CreateDatabase("keepme"))

	store := NewAutoIncClaimStore(dm.GetSystemDatabase())
	require.NoError(t, store.Seed("dropme", "users", 1000, 1))
	require.NoError(t, store.Seed("keepme", "users", 2000, 1))

	require.NoError(t, dm.DropDatabase("dropme"))

	if _, err := store.ReadBase("dropme", "users"); !errors.Is(err, ErrAutoIncBaseAbsent) {
		t.Errorf("dropme.users claim row survived DROP DATABASE: ReadBase returned %v, want ErrAutoIncBaseAbsent", err)
	}

	base, err := store.ReadBase("keepme", "users")
	require.NoError(t, err, "DROP DATABASE of one database must not disturb another's claim rows")
	if base != 2000 {
		t.Errorf("keepme.users base = %d after dropping a different database, want 2000", base)
	}
}

// TestSystemDatabaseCommitsAreDurable: a claim COMMIT this node ACKs counts
// toward a majority, so the base it wrote must survive an OS crash or power
// loss. Every connection the system database opens runs synchronous=FULL -
// including one that replaces a connection database/sql discarded - and on
// darwin fullfsync, without which FULL does not flush the drive's cache. User
// databases keep NORMAL.
//
// Mutation: drop _sync from the DSN (the driver then opens every connection
// at NORMAL). "a new system-database connection does not sync commits"
// fires. On darwin, dropping the durable driver's fullfsync pragma fires
// "a new system-database connection does not flush the drive cache".
func TestSystemDatabaseCommitsAreDurable(t *testing.T) {
	dm, err := NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dm.Close()
	require.NoError(t, dm.CreateDatabase("testdb"))
	user, err := dm.GetDatabase("testdb")
	require.NoError(t, err)
	sys := dm.GetSystemDatabase()
	ctx := context.Background()

	const normal, full = 1, 2
	wantFullfsync := 0
	if runtime.GOOS == "darwin" {
		wantFullfsync = 1
	}
	assertDurable := func(conn *sql.Conn, which string) {
		t.Helper()
		var sync, fullfsync int
		require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&sync))
		require.Equal(t, full, sync, "a new system-database connection does not sync commits (%s)", which)
		require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA fullfsync").Scan(&fullfsync))
		require.Equal(t, wantFullfsync, fullfsync, "a new system-database connection does not flush the drive cache (%s)", which)
	}

	// Replace the single write connection the way database/sql does after a
	// driver.ErrBadConn, then check the connection that replaces it.
	old, err := sys.GetWriteDB().Conn(ctx)
	require.NoError(t, err)
	require.ErrorIs(t, old.Raw(func(any) error { return driver.ErrBadConn }), driver.ErrBadConn)
	require.ErrorIs(t, old.Close(), sql.ErrConnDone, "database/sql kept a connection that reported ErrBadConn")
	replacement, err := sys.GetWriteDB().Conn(ctx)
	require.NoError(t, err)
	assertDurable(replacement, "replaced write connection")
	require.NoError(t, replacement.Close())

	// Holding one read connection forces the pool to open another.
	first, err := sys.GetReadDB().Conn(ctx)
	require.NoError(t, err)
	second, err := sys.GetReadDB().Conn(ctx)
	require.NoError(t, err)
	assertDurable(second, "second read connection")
	require.NoError(t, second.Close())
	require.NoError(t, first.Close())

	var sync int
	require.NoError(t, user.GetWriteDB().QueryRow("PRAGMA synchronous").Scan(&sync))
	require.Equal(t, normal, sync, "user databases keep NORMAL")
}
