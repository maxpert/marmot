//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// benchClaimFixture builds one database holding a marked AUTO_INCREMENT table
// and a seeded claim row, and hands back the system-database claim store and
// the user database's meta store - the two halves a claim touches at COMMIT.
func benchClaimFixture(b *testing.B) (*AutoIncClaimStore, MetaStore, func()) {
	b.Helper()

	dm, err := NewDatabaseManager(b.TempDir(), 1, hlc.NewClock(1))
	require.NoError(b, err)
	require.NoError(b, dm.CreateDatabase("testdb"))

	mdb, err := dm.GetDatabase("testdb")
	require.NoError(b, err)
	_, err = mdb.GetWriteDB().Exec("CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	require.NoError(b, err)
	require.NoError(b, mdb.ReloadSchema())

	store := NewAutoIncClaimStore(dm.GetSystemDatabase())
	require.NoError(b, store.Seed("testdb", "users", 0, 1))

	return store, mdb.GetMetaStore(), func() { dm.Close() }
}

// BenchmarkReadBase measures the read a participant does on every PREPARE of a
// claim. It runs through the read pool, so it must not be behind the single
// SQLite writer.
func BenchmarkReadBase(b *testing.B) {
	store, _, cleanup := benchClaimFixture(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.ReadBase("testdb", "users"); err != nil {
			b.Fatalf("ReadBase: %v", err)
		}
	}
}

// BenchmarkApplyAutoIncClaims measures the write a participant does on every
// COMMIT of a claim: read the transaction's claim intents and write the new
// base into the system database. Only the apply is timed; writing the intent
// that stands in for PREPARE is setup and is excluded.
func BenchmarkApplyAutoIncClaims(b *testing.B) {
	store, meta, cleanup := benchClaimFixture(b)
	defer cleanup()

	const size = 64
	key := protocol.AutoIncClaimKey("testdb", "users")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		txnID := uint64(i + 1)
		// Each claim proposes the base the previous one committed, as a
		// claimant with an up-to-date view does.
		base := uint64(i) * size
		payload, err := protocol.EncodeAutoIncClaim(AutoIncClaim{Table: "users", PrevBase: base, NewBase: base, Size: size})
		require.NoError(b, err)
		require.NoError(b, meta.WriteIntent(txnID, IntentTypeAutoIDClaim, AutoIncClaimTable, key,
			OpTypeInsert, "", payload, hlc.Timestamp{WallTime: int64(i + 1)}, 1))
		b.StartTimer()

		if err := store.ApplyClaims("testdb", txnID, meta); err != nil {
			b.Fatalf("ApplyClaims: %v", err)
		}

		b.StopTimer()
		require.NoError(b, meta.DeleteIntentsByTxn(txnID))
		b.StartTimer()
	}
}
