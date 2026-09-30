//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// int32Max is the ceiling of a signed MySQL INT.
const int32Max = 2147483647

// narrowIDs reads every id of a table, in id order.
func narrowIDs(t *testing.T, s *noopDMLSetup, query string) []int64 {
	t.Helper()
	rows, err := s.conn.Query(query)
	require.NoError(t, err)
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}

// TestNarrowAutoIncrementLastInsertIDAutocommit pins D5 on the autocommit
// path: a multi-row INSERT into an INT AUTO_INCREMENT table gets contiguous
// ids that fit 32 bits, and LAST_INSERT_ID is the FIRST of them, as MySQL
// reports it.
//
// Mutation: source LastInsertId from sqlite3_last_insert_rowid instead of the
// statement's first insert. It reports the third id and this fires.
func TestNarrowAutoIncrementLastInsertIDAutocommit(t *testing.T) {
	s := setupNoopDML(t)
	_, err := s.handler.HandleQuery(s.session, "CREATE TABLE g (group_id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)", nil)
	require.NoError(t, err)

	res, err := s.handler.HandleQuery(s.session, "INSERT INTO g (name) VALUES ('a'), ('b'), ('c')", nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), res.RowsAffected)

	ids := narrowIDs(t, s, "SELECT group_id FROM g ORDER BY group_id")
	require.Len(t, ids, 3)
	for i, id := range ids {
		require.Positive(t, id)
		require.LessOrEqual(t, id, int64(int32Max), "id %d does not fit the INT column", id)
		require.Equal(t, ids[0]+int64(i), id, "a multi-row INSERT's ids must be contiguous: %v", ids)
	}
	require.Equal(t, ids[0], res.LastInsertId, "LAST_INSERT_ID must be the statement's first id")
}

// TestNarrowAutoIncrementFirstWriteInTransactionShape pins D5 on the pinned
// explicit-transaction path, in the shape an ORM-style application boots
// with: one transaction inserting into two narrow tables, both of which need their first claim.
// It must complete, every id must fit 32 bits, and each statement must report
// its own first id - not the transaction's first insert.
//
// Mutation: report the transaction's first captured insert instead of the
// statement's. The groups INSERT then reports the users id and this fires.
func TestNarrowAutoIncrementFirstWriteInTransactionShape(t *testing.T) {
	s := setupNoopDML(t)
	for _, ddl := range []string{
		"CREATE TABLE users (user_id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)",
		"CREATE TABLE `groups` (group_id INT AUTO_INCREMENT PRIMARY KEY, display_name TEXT)",
	} {
		_, err := s.handler.HandleQuery(s.session, ddl, nil)
		require.NoError(t, err)
	}

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)
	usersRes, err := s.handler.HandleQuery(s.session, "INSERT INTO users (name) VALUES ('admin')", nil)
	require.NoError(t, err)
	groupsRes, err := s.handler.HandleQuery(s.session,
		"INSERT INTO `groups` (display_name) VALUES (?), (?)", []interface{}{"app_admin", "app_editor"})
	require.NoError(t, err)
	_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err)

	userIDs := narrowIDs(t, s, "SELECT user_id FROM users")
	groupIDs := narrowIDs(t, s, "SELECT group_id FROM `groups` ORDER BY group_id")
	require.Len(t, userIDs, 1)
	require.Len(t, groupIDs, 2)
	for _, id := range append(userIDs, groupIDs...) {
		require.LessOrEqual(t, id, int64(int32Max), "id %d does not fit the INT column", id)
	}
	require.Equal(t, userIDs[0], usersRes.LastInsertId)
	require.Equal(t, groupIDs[0], groupsRes.LastInsertId, "the second statement must report its own first id")
	require.Equal(t, groupIDs[0]+1, groupIDs[1], "a multi-row INSERT's ids must be contiguous")
}

// TestNarrowAutoIncrementExhaustionReturnsDupEntry pins D4 end to end: a
// TINYINT AUTO_INCREMENT table filled to 127 refuses the next INSERT with
// ER_DUP_ENTRY on 127, which clients do not retry - never the retryable 1205.
//
// Mutation: map an exhausted claim to the lock-wait timeout. This fires on
// the error code.
func TestNarrowAutoIncrementExhaustionReturnsDupEntry(t *testing.T) {
	s := setupNoopDML(t)
	_, err := s.handler.HandleQuery(s.session, "CREATE TABLE tiny (id TINYINT AUTO_INCREMENT PRIMARY KEY, v TEXT)", nil)
	require.NoError(t, err)

	for i := 1; i <= 127; i++ {
		res, err := s.handler.HandleQuery(s.session, "INSERT INTO tiny (v) VALUES ('x')", nil)
		require.NoError(t, err, "insert %d", i)
		require.Equal(t, int64(i), res.LastInsertId, "a single writer must mint 1, 2, 3 with no gap")
	}

	_, err = s.handler.HandleQuery(s.session, "INSERT INTO tiny (v) VALUES ('overflow')", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "1062")
	require.Contains(t, err.Error(), "Duplicate entry '127'")
}

// TestNarrowPreparedInsertWithNullIDIsGenerated is F3 end to end: a prepared
// INSERT binding NULL (or 0) to a narrow id gets a generated id every time,
// as the literal NULL does. Before, SQLite assigned its own rowid (MAX+1),
// which the CDC admission refuses with 1235 whenever it lies at or below the
// cluster's base in no range of this node - here, after a restart, in the
// range the previous process claimed. 500 runs, no refusal, every id distinct
// and 32-bit, and each LAST_INSERT_ID is the row's own id.
//
// Mutation: do not pass the bound values to the parser
// (ParseOptions.BoundParams). SQLite assigns rowid 4, the admission refuses
// it, and "a prepared insert with a NULL id was refused" fires.
func TestNarrowPreparedInsertWithNullIDIsGenerated(t *testing.T) {
	s := setupNoopDML(t)
	_, err := s.handler.HandleQuery(s.session, "CREATE TABLE g (group_id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)", nil)
	require.NoError(t, err)
	// The first process claims a range and issues 1..3; the restarted one owns
	// nothing below the base.
	for i := 0; i < 3; i++ {
		_, err = s.handler.HandleQuery(s.session, "INSERT INTO g (name) VALUES ('first')", nil)
		require.NoError(t, err)
	}
	s.handler = s.restartHandler()

	const runs = 500
	seen := make(map[int64]bool, runs)
	for i := 0; i < runs; i++ {
		var bound interface{}
		if i%2 == 1 {
			bound = int64(0)
		}
		res, err := s.handler.HandleQuery(s.session, "INSERT INTO g (group_id, name) VALUES (?, ?)", []interface{}{bound, "n"})
		require.NoError(t, err, "a prepared insert with a NULL id was refused (run %d)", i)
		id := res.LastInsertId
		require.Positive(t, id, "a prepared insert with a NULL id reported no generated id")
		require.LessOrEqual(t, id, int64(int32Max))
		require.False(t, seen[id], "id %d issued twice", id)
		seen[id] = true
	}
	ids := narrowIDs(t, s, "SELECT group_id FROM g WHERE name = 'n' ORDER BY group_id")
	require.Len(t, ids, runs)
	for _, id := range ids {
		require.True(t, seen[id], "stored id %d is not a LAST_INSERT_ID the client saw", id)
	}
}
