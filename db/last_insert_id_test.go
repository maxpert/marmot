//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/maxpert/marmot/coordinator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file pins the intended LAST_INSERT_ID / OK-packet insert-id behavior:
// the value of the table's AUTO-INCREMENT COLUMN in the FIRST row THIS
// STATEMENT inserted, read by column name from that statement's first insert
// CDC entry. It is 0 when the statement inserted no rows, and 0 when the
// target table has no auto-increment column.
//
// Setup reuses the shared helpers from rowid_sentinel_cdc_test.go:
// newRowidTestDatabase (temp dir + Pebble MetaStore + ReplicatedDatabase,
// with cleanup registered) and execAndReload (DDL exec + schema reload).

// extractSortedInt64Column pulls an INTEGER column out of Query()'s
// []map[string]interface{} rows and returns it sorted ascending. Fails loudly
// (rather than silently returning a wrong/empty slice) if a row is missing
// the column or the value isn't an int64, since that means the test's own
// fixture is broken, not the code under test.
func extractSortedInt64Column(t *testing.T, rows []map[string]interface{}, col string) []int64 {
	t.Helper()
	ids := make([]int64, 0, len(rows))
	for i, row := range rows {
		v, ok := row[col]
		if !ok {
			t.Fatalf("row %d missing column %q: %#v", i, col, row)
		}
		id, ok := v.(int64)
		if !ok {
			t.Fatalf("row %d column %q is %T (%v), want int64", i, col, v, v)
		}
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] < ids[i-1] {
			t.Fatalf("extractSortedInt64Column: caller's query must ORDER BY %s, got unsorted %v", col, ids)
		}
	}
	return ids
}

// TestAutocommitMultiRowInsertReportsFirstID is OBSERVABLE 1: a single
// autocommit statement that inserts 3 rows must report the FIRST generated
// id, not SQLite's sqlite3_last_insert_rowid() (which is the LAST row).
func TestAutocommitMultiRowInsertReportsFirstID(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	require.NoError(t, execAndReload(source, `CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`))

	ctx := context.Background()
	req := coordinator.ExecutionRequest{SQL: "INSERT INTO t1(v) VALUES('a'),('b'),('c')"}
	pending, err := source.ExecuteLocalWithHooks(ctx, 60001, req)
	require.NoError(t, err)

	// Reddens if ExecContext keeps storing whatever
	// result.LastInsertId()/sqlite3_last_insert_rowid() returns (the LAST row
	// of the 3-row insert, id=3) instead of the FIRST id this statement
	// generated.
	assert.Equal(t, int64(1), pending.GetLastInsertId(), "3-row autocommit INSERT must report the FIRST id (1), got %d", pending.GetLastInsertId())

	require.NoError(t, pending.Commit())

	// Sanity check: confirm the "first" assertion above didn't pass by
	// accident (e.g. because only one row actually got inserted). If this
	// fails, the test's own fixture is wrong, not the fix.
	rows, err := source.GetWriteDB().Query("SELECT id FROM t1 ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int64{1, 2, 3}, ids, "sanity: the 3-row insert must have actually produced ids 1,2,3")
}

// TestPinnedSessionMultiRowInsertReportsFirstID is OBSERVABLE 2: the same
// 3-row INSERT run through the explicit-transaction path (BeginPinnedSession
// / ExecuteStatement) must also report the FIRST generated id.
func TestPinnedSessionMultiRowInsertReportsFirstID(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	require.NoError(t, execAndReload(source, `CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`))

	ctx := context.Background()
	session, err := source.BeginPinnedSession(ctx, 61001)
	require.NoError(t, err)
	defer session.Release()

	rowsAffected, lastInsertId, err := session.ExecuteStatement(ctx, "INSERT INTO t1(v) VALUES('a'),('b'),('c')", nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), rowsAffected)

	// Reddens if pinnedHookSession.ExecuteStatement returns the raw shared
	// register (session.GetLastInsertId() sourced from
	// sqlite3_last_insert_rowid(), the LAST row id=3) instead of the FIRST id.
	assert.Equal(t, int64(1), lastInsertId, "3-row pinned-session INSERT must report the FIRST id (1), got %d", lastInsertId)

	// Sanity check via the session's own uncommitted read (Release never
	// commits, so this must be read before Release).
	cols, rows, err := session.Query(ctx, "SELECT id FROM t1 ORDER BY id", nil)
	require.NoError(t, err)
	require.Contains(t, cols, "id")
	ids := extractSortedInt64Column(t, rows, "id")
	require.Equal(t, []int64{1, 2, 3}, ids, "sanity: the 3-row insert must have actually produced ids 1,2,3")
}

// TestPinnedSessionPerStatementFirstIDBoundary is OBSERVABLE 3: within one
// pinned session, each statement must report ITS OWN first id - not another
// statement's table, and not "whichever table was touched last, forever".
func TestPinnedSessionPerStatementFirstIDBoundary(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	require.NoError(t, execAndReload(source, `CREATE TABLE tblA (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`))
	require.NoError(t, execAndReload(source, `CREATE TABLE tblB (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`))

	// Pre-seed tblB so its generated ids start far away from tblA's (which
	// will start at 1), so a wrong answer (e.g. "reports tblA's range")
	// cannot coincide with the right one by chance.
	_, err := source.GetWriteDB().Exec(`INSERT INTO tblB (id, v) VALUES (999, 'seed')`)
	require.NoError(t, err)

	ctx := context.Background()
	session, err := source.BeginPinnedSession(ctx, 62001)
	require.NoError(t, err)
	defer session.Release()

	// Statement 1: 3 rows into tblA.
	rowsA1, firstA1, err := session.ExecuteStatement(ctx, "INSERT INTO tblA(v) VALUES('a1'),('a2'),('a3')", nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), rowsA1)
	_, rowsDataA1, err := session.Query(ctx, "SELECT id FROM tblA ORDER BY id", nil)
	require.NoError(t, err)
	idsA1 := extractSortedInt64Column(t, rowsDataA1, "id")
	require.Equal(t, []int64{1, 2, 3}, idsA1, "sanity: tblA's fresh rows must be 1,2,3, or this test's own fixture is wrong")
	// Reddens if statement 1 reports the LAST row of its own insert (3)
	// instead of the FIRST (1) - same defect as observable 1, checked again
	// here because it is the baseline the rest of this test's boundary
	// assertions depend on.
	assert.Equal(t, int64(1), firstA1, "statement 1 (tblA) must report its first inserted id 1, got %d", firstA1)

	// Statement 2: 2 rows into tblB (a different table).
	rowsB1, firstB1, err := session.ExecuteStatement(ctx, "INSERT INTO tblB(v) VALUES('b1'),('b2')", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), rowsB1)
	_, rowsDataB1, err := session.Query(ctx, "SELECT id FROM tblB WHERE id > 999 ORDER BY id", nil)
	require.NoError(t, err)
	idsB1 := extractSortedInt64Column(t, rowsDataB1, "id")
	require.Equal(t, []int64{1000, 1001}, idsB1, "sanity: tblB's newly inserted rows must be 1000,1001 given the id=999 seed, or this test's own fixture is wrong")
	// Reddens if statement 2 reports tblA's id (1 or 3) instead of its own
	// table's first id (1000) - proves the report is scoped per statement,
	// not "whatever the session's shared register/first table happened to
	// hold".
	assert.Equal(t, int64(1000), firstB1, "statement 2 (tblB) must report its own first inserted id 1000, not tblA's, got %d", firstB1)

	// Statement 3: back to tblA, 1 more row.
	rowsA2, firstA2, err := session.ExecuteStatement(ctx, "INSERT INTO tblA(v) VALUES('a4')", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rowsA2)
	_, rowsDataA2, err := session.Query(ctx, "SELECT id FROM tblA WHERE id > 3 ORDER BY id", nil)
	require.NoError(t, err)
	idsA2 := extractSortedInt64Column(t, rowsDataA2, "id")
	require.Equal(t, []int64{4}, idsA2, "sanity: tblA's new row must be 4, or this test's own fixture is wrong")
	// Reddens if the implementation latches onto the last table it touched
	// (tblB, ids ~1000) permanently instead of resolving per-statement, so
	// statement 3 - back on tblA - never recovers tblA's own first id.
	assert.Equal(t, int64(4), firstA2, "statement 3 (back to tblA) must report its own first inserted id 4, got %d", firstA2)
}

// TestSharedRegisterNoLeakAcrossNoOpStatements is OBSERVABLE 4, the only test
// that proves the shared-register leak (db/db_integration.go hookDB capped
// at SetMaxOpenConns(1)) is fixed. It is written as a SAFETY assertion
// (checked repeatedly past the register's first successful, non-zero value),
// not a liveness one.
//
// It asserts 0 from the db layer, not "unchanged", because the session field
// above this layer drops a zero rather than storing it. That rule has exactly
// one home, protocol.ConnectionSession.RecordInsertId, and two call paths reach
// it: the coordinator's OK-packet paths (protocol/server.go) and a replica's
// forwarded responses (replica/handler.go applyForwardedSessionState). Returning
// 0 here is what preserves the client's own prior LAST_INSERT_ID() - a non-zero
// value, even the correct one from a DIFFERENT earlier statement, would
// overwrite it.
func TestSharedRegisterNoLeakAcrossNoOpStatements(t *testing.T) {
	const iterations = 3

	t.Run("autocommit", func(t *testing.T) {
		source := newRowidTestDatabase(t, 1)
		require.NoError(t, execAndReload(source, `CREATE TABLE t4a (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)`))

		ctx := context.Background()
		var txnID uint64 = 63000
		ignoreChecks, updateChecks := 0, 0

		for i := 0; i < iterations; i++ {
			v := fmt.Sprintf("shared-reg-%d", i)

			// (0) Prime the register with a real insert.
			txnID++
			primeReq := coordinator.ExecutionRequest{SQL: fmt.Sprintf("INSERT INTO t4a(v) VALUES('%s')", v)}
			primePending, err := source.ExecuteLocalWithHooks(ctx, txnID, primeReq)
			require.NoError(t, err)
			primeID := primePending.GetLastInsertId()
			// Discriminator: without a genuinely non-zero primed id, a 0 from
			// the no-op below would prove nothing (the register might never
			// have held a real value in the first place).
			require.NotZero(t, primeID, "iteration %d: priming insert reported id=0, so this iteration cannot discriminate a fixed register from a never-primed one", i)
			require.NoError(t, primePending.Commit())

			// (i) INSERT OR IGNORE on the just-primed duplicate key inserts
			// nothing.
			txnID++
			ignoreReq := coordinator.ExecutionRequest{SQL: fmt.Sprintf("INSERT OR IGNORE INTO t4a(v) VALUES('%s')", v)}
			ignorePending, err := source.ExecuteLocalWithHooks(ctx, txnID, ignoreReq)
			require.NoError(t, err)
			ignoreID := ignorePending.GetLastInsertId()
			// Reddens if ExecContext keeps reading the hookDB connection's
			// single shared sqlite3_last_insert_rowid() register (still
			// holding primeID here, from a completely different Go-level
			// session/txnID that happened to reuse the same underlying
			// SQLite connection) instead of recognizing zero rows were
			// inserted by THIS statement.
			assert.Equalf(t, int64(0), ignoreID, "iteration %d: INSERT OR IGNORE on duplicate key must report 0, got %d (primed id was %d)", i, ignoreID, primeID)
			require.NoError(t, ignorePending.Commit())
			ignoreChecks++

			// (ii) An upsert that updates the existing row rather than
			// inserting.
			txnID++
			upsertReq := coordinator.ExecutionRequest{SQL: fmt.Sprintf("INSERT INTO t4a(v) VALUES('%s') ON CONFLICT(v) DO UPDATE SET v = excluded.v", v)}
			upsertPending, err := source.ExecuteLocalWithHooks(ctx, txnID, upsertReq)
			require.NoError(t, err)
			upsertID := upsertPending.GetLastInsertId()
			assert.Equalf(t, int64(0), upsertID, "iteration %d: ON CONFLICT DO UPDATE must report 0 (it updated, not inserted), got %d", i, upsertID)
			require.NoError(t, upsertPending.Commit())
			updateChecks++
		}

		// Guards against the loop silently degrading to fewer samples (e.g.
		// a stray `continue`/early return) - the safety property must have
		// actually been checked `iterations` times, not just once.
		require.Equal(t, iterations, ignoreChecks, "autocommit: INSERT OR IGNORE no-op path must have been exercised %d times", iterations)
		require.Equal(t, iterations, updateChecks, "autocommit: ON CONFLICT DO UPDATE no-op path must have been exercised %d times", iterations)
	})

	t.Run("pinned", func(t *testing.T) {
		source := newRowidTestDatabase(t, 1)
		require.NoError(t, execAndReload(source, `CREATE TABLE t4b (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)`))

		ctx := context.Background()
		session, err := source.BeginPinnedSession(ctx, 64001)
		require.NoError(t, err)
		defer session.Release()

		ignoreChecks, updateChecks := 0, 0

		for i := 0; i < iterations; i++ {
			v := fmt.Sprintf("pinned-shared-reg-%d", i)

			_, primeID, err := session.ExecuteStatement(ctx, fmt.Sprintf("INSERT INTO t4b(v) VALUES('%s')", v), nil)
			require.NoError(t, err)
			require.NotZero(t, primeID, "iteration %d: priming insert reported id=0, so this iteration cannot discriminate a fixed register from a never-primed one", i)

			// (i) no rows inserted.
			_, ignoreID, err := session.ExecuteStatement(ctx, fmt.Sprintf("INSERT OR IGNORE INTO t4b(v) VALUES('%s')", v), nil)
			require.NoError(t, err)
			// Reddens for the same reason as the autocommit case above:
			// pinnedHookSession.ExecuteStatement returns
			// p.session.GetLastInsertId(), the raw shared register, never
			// touching CDC entries (0 entries here) at all.
			assert.Equalf(t, int64(0), ignoreID, "iteration %d: pinned INSERT OR IGNORE on duplicate key must report 0, got %d (primed id was %d)", i, ignoreID, primeID)
			ignoreChecks++

			// (ii) an existing row is updated, not inserted.
			_, upsertID, err := session.ExecuteStatement(ctx, fmt.Sprintf("INSERT INTO t4b(v) VALUES('%s') ON CONFLICT(v) DO UPDATE SET v = excluded.v", v), nil)
			require.NoError(t, err)
			assert.Equalf(t, int64(0), upsertID, "iteration %d: pinned ON CONFLICT DO UPDATE must report 0, got %d", i, upsertID)
			updateChecks++
		}

		require.Equal(t, iterations, ignoreChecks, "pinned: INSERT OR IGNORE no-op path must have been exercised %d times", iterations)
		require.Equal(t, iterations, updateChecks, "pinned: ON CONFLICT DO UPDATE no-op path must have been exercised %d times", iterations)
	})
}

// TestNoAutoIncrementColumnReportsZero is OBSERVABLE 5: the reported id must
// come from the table's auto-increment column, not "the pk" or SQLite's
// hidden rowid. db/schema_cache.go loadSchema only ever sets
// TableSchema.AutoIncrementCol inside `if len(pkColumns) == 1 { ... upperType
// == "INTEGER" || upperType == "BIGINT" ... }` (lines ~330-344): a single
// INTEGER/BIGINT PRIMARY KEY column. In that model, whenever
// AutoIncrementCol is set it is always equal to the table's sole PK column -
// this codebase's schema cache has no representation for "an auto-increment
// column that differs from the PK", so the strongest test the schema model
// allows is the negative case: a PK that is NOT eligible (a TEXT PRIMARY
// KEY), which must report 0, not SQLite's implicit hidden rowid (which
// exists and is non-zero for this table, since it is not WITHOUT ROWID).
func TestNoAutoIncrementColumnReportsZero(t *testing.T) {
	t.Run("autocommit", func(t *testing.T) {
		source := newRowidTestDatabase(t, 1)
		require.NoError(t, execAndReload(source, `CREATE TABLE t5a (id TEXT PRIMARY KEY, v TEXT)`))

		schema, err := source.GetCachedTableSchema("t5a")
		require.NoError(t, err)
		require.Empty(t, schema.AutoIncrementCol, "sanity: loadSchema must leave AutoIncrementCol empty for a TEXT PRIMARY KEY, or this test's own premise is wrong")

		ctx := context.Background()
		req := coordinator.ExecutionRequest{SQL: "INSERT INTO t5a (id, v) VALUES ('abc', 'x')"}
		pending, err := source.ExecuteLocalWithHooks(ctx, 65001, req)
		require.NoError(t, err)

		// Reddens if ExecContext still surfaces SQLite's implicit hidden
		// rowid (sqlite3_last_insert_rowid(), non-zero here even though the
		// declared PK is TEXT) instead of looking up a named auto-increment
		// column that does not exist on this table.
		assert.Equal(t, int64(0), pending.GetLastInsertId(), "table with a TEXT PRIMARY KEY (no auto-increment column) must report insert id 0, got %d", pending.GetLastInsertId())
		require.NoError(t, pending.Commit())
	})

	t.Run("pinned", func(t *testing.T) {
		source := newRowidTestDatabase(t, 1)
		require.NoError(t, execAndReload(source, `CREATE TABLE t5b (id TEXT PRIMARY KEY, v TEXT)`))

		schema, err := source.GetCachedTableSchema("t5b")
		require.NoError(t, err)
		require.Empty(t, schema.AutoIncrementCol, "sanity: loadSchema must leave AutoIncrementCol empty for a TEXT PRIMARY KEY, or this test's own premise is wrong")

		ctx := context.Background()
		session, err := source.BeginPinnedSession(ctx, 66001)
		require.NoError(t, err)
		defer session.Release()

		_, lastInsertId, err := session.ExecuteStatement(ctx, "INSERT INTO t5b (id, v) VALUES ('abc', 'x')", nil)
		require.NoError(t, err)
		// Same defect as the autocommit case, on the explicit-transaction
		// path: pinnedHookSession.ExecuteStatement returns
		// p.session.GetLastInsertId(), the raw hidden-rowid register.
		assert.Equal(t, int64(0), lastInsertId, "pinned INSERT into a TEXT PRIMARY KEY table (no auto-increment column) must report insert id 0, got %d", lastInsertId)
	})
}
