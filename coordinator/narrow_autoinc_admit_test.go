//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"strings"
	"testing"

	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// narrowTable creates g (INT AUTO_INCREMENT) and plain (a BIGINT source) on s.
func narrowTable(t *testing.T, s *noopDMLSetup) {
	t.Helper()
	for _, ddl := range []string{
		"CREATE TABLE g (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)",
		"CREATE TABLE plain (id BIGINT PRIMARY KEY, name TEXT)",
	} {
		_, err := s.handler.HandleQuery(s.session, ddl, nil)
		require.NoError(t, err)
	}
}

// generatedID inserts one row with a generated id through h and returns it.
func generatedID(t *testing.T, s *noopDMLSetup) int64 {
	t.Helper()
	res, err := s.handler.HandleQuery(s.session, "INSERT INTO g (name) VALUES ('generated')", nil)
	require.NoError(t, err)
	return res.LastInsertId
}

// TestNarrowIDsWrittenByEveryShapeRaiseTheBase pins R3c-1: whatever shape
// writes an id into a narrow AUTO_INCREMENT column - a literal, a bound
// parameter, INSERT ... SELECT, REPLACE, an UPDATE of the column, LOAD DATA -
// the allocator admits it before the write replicates, so the next generated
// id lands above it instead of colliding with it.
//
// Mutation: make admitNarrowIDs return nil without admitting. Every shape but
// the literal one then generates an id at or below the one it wrote, and
// "generated id ... is not above" fires (or the generated INSERT collides).
func TestNarrowIDsWrittenByEveryShapeRaiseTheBase(t *testing.T) {
	cases := []struct {
		name    string
		write   func(t *testing.T, s *noopDMLSetup)
		highest int64
	}{
		{"literal", func(t *testing.T, s *noopDMLSetup) {
			_, err := s.handler.HandleQuery(s.session, "INSERT INTO g (id, name) VALUES (5000, 'x')", nil)
			require.NoError(t, err)
		}, 5000},
		{"bound parameter", func(t *testing.T, s *noopDMLSetup) {
			_, err := s.handler.HandleQuery(s.session, "INSERT INTO g (id, name) VALUES (?, ?)", []interface{}{int64(6000), "x"})
			require.NoError(t, err)
		}, 6000},
		{"insert select", func(t *testing.T, s *noopDMLSetup) {
			for _, id := range []int{7000, 7100, 7200} {
				_, err := s.handler.HandleQuery(s.session, "INSERT INTO plain (id, name) VALUES (?, 'x')", []interface{}{id})
				require.NoError(t, err)
			}
			_, err := s.handler.HandleQuery(s.session, "INSERT INTO g (id, name) SELECT id, name FROM plain", nil)
			require.NoError(t, err)
		}, 7200},
		{"replace", func(t *testing.T, s *noopDMLSetup) {
			_, err := s.handler.HandleQuery(s.session, "REPLACE INTO g (id, name) VALUES (?, ?)", []interface{}{int64(8000), "x"})
			require.NoError(t, err)
		}, 8000},
		{"update of the column", func(t *testing.T, s *noopDMLSetup) {
			generatedID(t, s)
			_, err := s.handler.HandleQuery(s.session, "UPDATE g SET id = ? WHERE name = 'generated'", []interface{}{int64(9000)})
			require.NoError(t, err)
		}, 9000},
		{"load data", func(t *testing.T, s *noopDMLSetup) {
			_, err := s.handler.HandleLoadData(s.session,
				"LOAD DATA LOCAL INFILE 'rows.csv' INTO TABLE g FIELDS TERMINATED BY ',' (id, name)",
				[]byte("9500,a\n9600,b\n"))
			require.NoError(t, err)
		}, 9600},
		{"explicit transaction", func(t *testing.T, s *noopDMLSetup) {
			_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
			require.NoError(t, err)
			_, err = s.handler.HandleQuery(s.session, "INSERT INTO g (id, name) VALUES (?, ?)", []interface{}{int64(9900), "x"})
			require.NoError(t, err)
			_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
			require.NoError(t, err)
		}, 9900},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupNoopDML(t)
			narrowTable(t, s)
			tc.write(t, s)
			for range 3 {
				id := generatedID(t, s)
				require.Greater(t, id, tc.highest, "generated id %d is not above the id %d the %s wrote", id, tc.highest, tc.name)
				require.LessOrEqual(t, id, int64(int32Max))
			}
		})
	}
}

// TestNarrowIDsBelowTheBaseAreRefused pins R3c-2 on one node whose allocator
// state is gone (a restarted process): an id at or below the allocation base
// that lies in no range this process claimed may lie in another node's
// unissued range, so every shape that writes one is refused with 1235 and
// writes nothing, and a generated INSERT still succeeds afterwards.
//
// Mutation: admit any id at or below the base. The writes succeed and "was
// written" fires.
func TestNarrowIDsBelowTheBaseAreRefused(t *testing.T) {
	s := setupNoopDML(t)
	narrowTable(t, s)
	first := generatedID(t, s) // claims 1..64 and issues 1
	require.Equal(t, int64(1), first)
	_, err := s.handler.HandleQuery(s.session, "INSERT INTO plain (id, name) VALUES (30, 'x')", nil)
	require.NoError(t, err)

	s.handler = s.restartHandler()
	writes := map[string]func() error{
		"literal": func() error {
			_, err := s.handler.HandleQuery(s.session, "INSERT INTO g (id, name) VALUES (10, 'x')", nil)
			return err
		},
		"bound parameter": func() error {
			_, err := s.handler.HandleQuery(s.session, "INSERT INTO g (id, name) VALUES (?, ?)", []interface{}{int64(20), "x"})
			return err
		},
		"insert select": func() error {
			_, err := s.handler.HandleQuery(s.session, "INSERT INTO g (id, name) SELECT id, name FROM plain", nil)
			return err
		},
		"update of the column": func() error {
			_, err := s.handler.HandleQuery(s.session, "UPDATE g SET id = 40 WHERE id = 1", nil)
			return err
		},
		"load data": func() error {
			_, err := s.handler.HandleLoadData(s.session,
				"LOAD DATA LOCAL INFILE 'rows.csv' INTO TABLE g FIELDS TERMINATED BY ',' (id, name)", []byte("50,x\n"))
			return err
		},
	}
	for name, write := range writes {
		err := write()
		require.Error(t, err, "%s: an id below the base was written", name)
		require.True(t, strings.Contains(err.Error(), "1235"), "%s: err = %v, want MySQL 1235", name, err)
	}

	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM g WHERE id IN (10, 20, 30, 40, 50)").Scan(&count))
	require.Zero(t, count, "a refused id was written")

	id := generatedID(t, s)
	require.Greater(t, id, int64(64), "the restarted node must claim a fresh range")
}

// TestNarrowLoadDataWithoutTheColumnGetsGeneratedIDs pins that LOAD DATA into
// a narrow table whose rows omit the id gets ids from the allocator - fitting
// the column and contiguous - rather than a key each node's SQLite assigns on
// its own.
//
// Mutation: replicate LOAD DATA into a narrow table as a statement again.
// The ids then come from SQLite and the first generated INSERT afterwards
// collides with one, and this fails.
func TestNarrowLoadDataWithoutTheColumnGetsGeneratedIDs(t *testing.T) {
	s := setupNoopDML(t)
	narrowTable(t, s)
	_, err := s.handler.HandleLoadData(s.session,
		"LOAD DATA LOCAL INFILE 'rows.csv' INTO TABLE g FIELDS TERMINATED BY ',' (name)", []byte("a\nb\nc\n"))
	require.NoError(t, err)
	ids := narrowIDs(t, s, "SELECT id FROM g ORDER BY id")
	require.Len(t, ids, 3)
	next := generatedID(t, s)
	require.Greater(t, next, ids[2], "a generated id after LOAD DATA must lie above the ids it wrote")
}

// TestNarrowRewritesOfExistingRowsAreAdmitted pins that R3c-2's refusal is
// about NEW ids only: after a restart, when no range below the base is this
// node's, statements that rewrite a row whose id already exists - an UPDATE
// of another column, an INSERT ... ON DUPLICATE KEY UPDATE - still succeed.
// The id was issued once already, so it lies in no node's unissued range.
//
// Mutation: admit the id of an UPDATE that left it unchanged. "rewriting an
// existing row was refused" fires on the UPDATE.
func TestNarrowRewritesOfExistingRowsAreAdmitted(t *testing.T) {
	s := setupNoopDML(t)
	narrowTable(t, s)
	id := generatedID(t, s)
	s.handler = s.restartHandler()

	for _, q := range []string{
		"UPDATE g SET name = 'updated' WHERE id = ?",
		"INSERT INTO g (id, name) VALUES (?, 'upserted') ON DUPLICATE KEY UPDATE name = 'upserted'",
	} {
		_, err := s.handler.HandleQuery(s.session, q, []interface{}{id})
		require.NoError(t, err, "rewriting an existing row was refused: %s", q)
	}
	var name string
	require.NoError(t, s.conn.QueryRow("SELECT name FROM g WHERE id = ?", id).Scan(&name))
	require.Equal(t, "upserted", name)
}

// TestNarrowQualifiedTargetsUseTheirOwnDatabase is F4 and L3: an INSERT or a
// LOAD DATA naming its table as db.t, from a session whose current database
// is another one, looks the table up, allocates and admits in db - not in the
// session's database, where the table does not exist. Every form gets
// generated narrow ids and none is refused.
//
// The writes run after a restart, when this node owns no range below the
// base: an id the allocator did not generate (a SQLite rowid) is then refused
// with 1235, so the test sees whether the id was generated.
//
// Mutation: look tables up in the session's database only (drop the
// statement's qualifier in the handler's schema lookup). No id is generated,
// SQLite assigns rowid 4, the admission in db refuses it, and "a qualified
// INSERT was refused" fires.
func TestNarrowQualifiedTargetsUseTheirOwnDatabase(t *testing.T) {
	s := setupNoopDML(t)
	_, err := s.handler.HandleQuery(s.session, "CREATE DATABASE other", nil)
	require.NoError(t, err)
	other := &protocol.ConnectionSession{ConnID: 2, CurrentDatabase: "other", TranspilationEnabled: true}
	_, err = s.handler.HandleQuery(other, "CREATE TABLE g (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)", nil)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err = s.handler.HandleQuery(other, "INSERT INTO g (name) VALUES ('first')", nil)
		require.NoError(t, err)
	}
	s.handler = s.restartHandler()

	for _, q := range []struct {
		sql    string
		params []interface{}
	}{
		{"INSERT INTO other.g (name) VALUES ('text')", nil},
		{"INSERT INTO `other`.`g` (name) VALUES (?)", []interface{}{"bound"}},
		{"INSERT INTO other.g (id, name) VALUES (?, ?)", []interface{}{nil, "bound-null"}},
	} {
		res, err := s.handler.HandleQuery(s.session, q.sql, q.params)
		require.NoError(t, err, "a qualified INSERT was refused: %s", q.sql)
		require.Greater(t, res.LastInsertId, int64(3), "%s did not get a generated id", q.sql)
		require.LessOrEqual(t, res.LastInsertId, int64(int32Max), "%s got a wide id", q.sql)
	}
	_, err = s.handler.HandleLoadData(s.session,
		"LOAD DATA LOCAL INFILE 'rows.csv' INTO TABLE `other`.g FIELDS TERMINATED BY ',' (name)", []byte("a\nb\n"))
	require.NoError(t, err, "a qualified LOAD DATA into a narrow table was refused")

	conn, err := s.dbMgr.GetDatabaseConnection("other")
	require.NoError(t, err)
	var rows, distinct, wide int
	require.NoError(t, conn.QueryRow("SELECT COUNT(*), COUNT(DISTINCT id), SUM(id > ?) FROM g", int32Max).Scan(&rows, &distinct, &wide))
	require.Equal(t, 8, rows, "rows landed outside other.g")
	require.Equal(t, rows, distinct)
	require.Zero(t, wide, "a qualified write got an id wider than the INT column")
	var stray int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name = 'g'").Scan(&stray))
	require.Zero(t, stray, "a qualified write created or used a table in the session's database")
}
