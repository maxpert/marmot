package test

// Cluster tests for the AUTO_INCREMENT feature's DDL-time seeding
// (db/autoinc_seed.go) and for the hidden claim table's invisibility and
// write-protection, driven over the MySQL wire protocol against a 3-node
// cluster. The claim protocol's own scenarios are covered in-process by
// autoinc_claim_inprocess_test.go and autoinc_membership_inprocess_test.go;
// inserts that claim over the wire by autoinc_narrow_cluster_test.go.

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// asMySQLError returns err as a *mysql.MySQLError, failing the test if err
// is nil or another kind of error: a transport error is a different failure
// than the coded refusal these tests assert.
func asMySQLError(t *testing.T, err error, what string) *mysql.MySQLError {
	t.Helper()
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		t.Fatalf("%s: err = %v, want a MySQL error", what, err)
	}
	return mysqlErr
}

// columnValues runs query through node id and returns every row's value of
// the named column (matched case-insensitively, as MySQL identifiers are).
func columnValues(c *cluster, id int, query, column string) ([]string, error) {
	rs, err := c.db(id, db.DefaultDatabaseName).Query(query)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	cols, err := rs.Columns()
	if err != nil {
		return nil, err
	}
	idx := -1
	for i, name := range cols {
		if strings.EqualFold(name, column) {
			idx = i
		}
	}
	if idx < 0 {
		return nil, fmt.Errorf("column %q not in %v", column, cols)
	}
	var out []string
	for rs.Next() {
		raw := make([]any, len(cols))
		vals := make([][]byte, len(cols))
		for i := range raw {
			raw[i] = &vals[i]
		}
		if err := rs.Scan(raw...); err != nil {
			return nil, err
		}
		out = append(out, string(vals[idx]))
	}
	return out, rs.Err()
}

// TestAutoIncSeed_CreateTableWithFloorOption: CREATE TABLE ...
// AUTO_INCREMENT=5000 seeds a claim base of at least 4999 on every node,
// each from its own DDL apply.
func TestAutoIncSeed_CreateTableWithFloorOption(t *testing.T) {
	c := newCluster(t)
	c.start()
	const table = "autoinc_seed_floor"
	c.createTable(1, "marmot", table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT) AUTO_INCREMENT=5000")
	c.waitAutoIncBaseAtLeast("marmot", table, 4999, 1, 2, 3)
}

// TestAutoIncSeed_AlterSeedsFromExistingMax: explicit ids 1..1000 raise the
// claim base to at least 1000 as they are inserted, and an unrelated ALTER
// TABLE, which seeds again from MAX(id), leaves it there on every node.
func TestAutoIncSeed_AlterSeedsFromExistingMax(t *testing.T) {
	c := newCluster(t)
	c.start()
	const table, rowCount = "autoinc_seed_altermax", 1000
	c.createTable(1, "marmot", table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(20))")
	for start := 1; start <= rowCount; start += 100 {
		values, _ := idValueRows(start, start+99, "row")
		c.mustExec(1, "marmot", "INSERT INTO "+table+" (id, v) VALUES "+values)
	}
	c.waitRows("marmot", "SELECT COUNT(*), COUNT(DISTINCT id), MAX(id) FROM "+table, []string{"1000|1000|1000"}, 1, 2, 3)
	base, found, err := c.autoIncBase(1, "marmot", table)
	if err != nil || !found || base < rowCount {
		t.Fatalf("before ALTER: node 1 base %d (found %v, err %v), want >= %d: explicit ids must raise it as they are inserted", base, found, err, rowCount)
	}
	c.mustExec(1, "marmot", "ALTER TABLE "+table+" ADD COLUMN w INT")
	c.waitAutoIncBaseAtLeast("marmot", table, rowCount, 1, 2, 3)
}

// TestAutoIncSeed_AlterFloorWithoutAColumnIsRefused: an ALTER carrying an
// AUTO_INCREMENT=N option with no narrow AUTO_INCREMENT column definition has
// nowhere to store the floor, so it is refused with 1235 / SQLSTATE 42000,
// alone or beside a column add, and leaves no column behind.
func TestAutoIncSeed_AlterFloorWithoutAColumnIsRefused(t *testing.T) {
	c := newCluster(t)
	c.start()
	const table = "autoinc_seed_floorrefused"
	c.createTable(1, "marmot", table, "CREATE TABLE "+table+" (id INT PRIMARY KEY, v TEXT)")
	for label, stmt := range map[string]string{
		"ADD COLUMN alongside floor option": "ALTER TABLE " + table + " ADD COLUMN w INT, AUTO_INCREMENT=5000",
		"floor option alone":                "ALTER TABLE " + table + " AUTO_INCREMENT=5000",
	} {
		_, err := c.exec(1, "marmot", stmt)
		mysqlErr := asMySQLError(t, err, label)
		if mysqlErr.Number != mysqlcode.ErrCodeNotSupportedYet || string(mysqlErr.SQLState[:]) != mysqlcode.SQLStateSyntax {
			t.Errorf("%s: %d/%s, want %d/%s", label, mysqlErr.Number, mysqlErr.SQLState[:], mysqlcode.ErrCodeNotSupportedYet, mysqlcode.SQLStateSyntax)
		}
	}
	if _, err := c.exec(1, "marmot", "SELECT w FROM "+table+" LIMIT 1"); err == nil {
		t.Errorf("column w exists on %s after both ALTERs were refused", table)
	}
}

// TestAutoIncSeed_RefusesAnIDBeyondTheWidth: an explicit id above a TINYINT
// AUTO_INCREMENT column's ceiling is refused at INSERT with 1264 / SQLSTATE
// 22003, as MySQL does in strict mode, and nothing is written.
func TestAutoIncSeed_RefusesAnIDBeyondTheWidth(t *testing.T) {
	c := newCluster(t)
	c.start()
	const table = "autoinc_width_ceiling"
	c.createTable(1, "marmot", table, "CREATE TABLE "+table+" (id TINYINT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	_, err := c.exec(1, "marmot", "INSERT INTO "+table+" (id, v) VALUES (200, 'beyond_tinyint')")
	mysqlErr := asMySQLError(t, err, "explicit id beyond the width")
	if mysqlErr.Number != mysqlcode.ErrCodeDataOutOfRange || string(mysqlErr.SQLState[:]) != mysqlcode.SQLStateDataOutOfRange {
		t.Errorf("%d/%s, want %d/%s", mysqlErr.Number, mysqlErr.SQLState[:], mysqlcode.ErrCodeDataOutOfRange, mysqlcode.SQLStateDataOutOfRange)
	}
	c.waitRows("marmot", "SELECT COUNT(*) FROM "+table, []string{"0"}, 1)
}

// TestAutoIncHiddenTable_AbsentFromListings: the claim table never appears in
// SHOW TABLES or information_schema.tables on any node, even once it holds a
// row for a seeded table.
func TestAutoIncHiddenTable_AbsentFromListings(t *testing.T) {
	c := newCluster(t)
	c.start()
	const table = "autoinc_hidden_listing"
	c.createTable(1, "marmot", table, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	c.waitAutoIncBaseAtLeast("marmot", table, 0, 1, 2, 3)
	for id := 1; id <= 3; id++ {
		if listed, err := c.hasTable(id, "marmot", common.AutoIncClaimTableName); err != nil || listed {
			t.Errorf("node %d: SHOW TABLES lists %s (err %v)", id, common.AutoIncClaimTableName, err)
		}
		// Formatted rather than bound: a bound parameter takes the prepared
		// statement path, which does not route INFORMATION_SCHEMA queries.
		names, err := columnValues(c, id, fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'",
			db.DefaultDatabaseName), "table_name")
		if err != nil {
			t.Fatalf("node %d: information_schema.tables: %v", id, err)
		}
		if !containsFold(names, table) {
			t.Errorf("node %d: information_schema.tables omits %s: %v", id, table, names)
		}
		if containsFold(names, common.AutoIncClaimTableName) {
			t.Errorf("node %d: information_schema.tables lists %s", id, common.AutoIncClaimTableName)
		}
	}
}

// containsFold reports whether names holds name, ignoring case.
func containsFold(names []string, name string) bool {
	for _, n := range names {
		if strings.EqualFold(n, name) {
			return true
		}
	}
	return false
}

// TestAutoIncHiddenTable_RejectsClientWrites: INSERT, UPDATE, DELETE and DROP
// TABLE against the claim table are refused with 1142.
func TestAutoIncHiddenTable_RejectsClientWrites(t *testing.T) {
	c := newCluster(t)
	c.start()
	table := common.AutoIncClaimTableName
	for label, stmt := range map[string]string{
		"INSERT": "INSERT INTO " + table + " (db, tbl, committed, owner, granted_at) VALUES ('marmot', 'x', 1, 1, 1)",
		"UPDATE": "UPDATE " + table + " SET committed = 999 WHERE tbl = 'x'",
		"DELETE": "DELETE FROM " + table + " WHERE tbl = 'x'",
		"DROP":   "DROP TABLE " + table,
	} {
		_, err := c.exec(1, "marmot", stmt)
		if code := asMySQLError(t, err, label).Number; code != mysqlcode.ErrCodeTableAccessDenied {
			t.Errorf("%s: error %d, want %d", label, code, mysqlcode.ErrCodeTableAccessDenied)
		}
	}
}
