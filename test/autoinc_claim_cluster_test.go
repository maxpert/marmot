package test

// Cluster tests for the AUTO_INCREMENT feature's DDL-time seeding
// (coordinator.ClaimRange, db.AutoIncClaimStore, db/autoinc_seed.go) and for
// the hidden claim table's invisibility and write-protection, all driven
// over the real MySQL wire protocol against a 3-node ClusterHarness
// (test/crash_recovery_test.go).
//
// The claim protocol's own scenarios (sequential claim safety, concurrent
// boundary claims, restart and crash recovery, meta-store wipe, full-cluster
// snapshot restore, membership growth) are covered in
// test/autoinc_claim_inprocess_test.go and
// test/autoinc_membership_inprocess_test.go, which drive the real coordinator
// and participant engines in-process, replacing only the gRPC transport, so
// they can script which node sees which round. Inserts that reach
// ClaimRange over the MySQL wire protocol are covered in
// test/autoinc_narrow_cluster_test.go.

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/protocol/mysqlcode"
)

// systemDBFile returns the on-disk path of a node's system database, the
// file db.SystemDatabaseName resolves to under its data directory
// (db/database_manager.go: filepath.Join(dataDir, SystemDatabaseName+".db")).
// __marmot__autoinc lives inside this file, never inside a user database
// (db/autoinc_claim.go's AutoIncClaimTable doc).
func systemDBFile(dataDir string) string {
	return filepath.Join(dataDir, db.SystemDatabaseName+".db")
}

// readAutoIncBase opens a node's system database file directly, outside the
// MySQL wire protocol, and reads the base for (database, table) - the
// largest of its committed, seed and merged floors - from __marmot__autoinc. This is the only channel available to a cluster
// test: the table is deliberately hidden from client SQL (see
// common.IsInternalTableName and protocol/query/rules/claim_table_guard.go),
// and nothing on the wire path reveals it. found is false, with no error,
// when the row is absent - not yet seeded, or the table itself does not yet
// exist - matching db.AutoIncClaimStore.ReadBase's own "absent row, absent
// table" equivalence.
func readAutoIncBase(dataDir, database, table string) (base uint64, found bool, err error) {
	dsn := fmt.Sprintf("file:%s?mode=ro&_busy_timeout=5000", systemDBFile(dataDir))
	conn, openErr := sql.Open("sqlite3", dsn)
	if openErr != nil {
		return 0, false, fmt.Errorf("open system db under %s: %w", dataDir, openErr)
	}
	defer conn.Close()

	var got int64
	scanErr := conn.QueryRow(
		"SELECT MAX(committed, seed, merged) FROM "+common.AutoIncClaimTableName+" WHERE db = ? AND tbl = ?",
		database, table).Scan(&got)
	switch {
	case scanErr == sql.ErrNoRows:
		return 0, false, nil
	case scanErr != nil && strings.Contains(scanErr.Error(), "no such table"):
		return 0, false, nil
	case scanErr != nil:
		return 0, false, fmt.Errorf("read %s under %s: %w", common.AutoIncClaimTableName, dataDir, scanErr)
	}
	return uint64(got), true, nil
}

// waitForAutoIncBaseAtLeast polls a node's system database until the
// committed base for (database, table) is >= min, or timeout elapses. DDL
// seeding commits the claim row atomically with the DDL's own COMMIT, before
// that COMMIT is ACKed (db/autoinc_seed.go, db/autoinc_claim.go Seed), so by
// the time WaitForTableExists reports a table visible on a node the seed row
// should already be durable there; this still polls rather than reading
// once, so the assertion is not coupled to that timing guarantee holding
// exactly.
func waitForAutoIncBaseAtLeast(t *testing.T, dataDir, database, table string, min uint64, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var base uint64
	var found bool
	var lastErr error
	for time.Now().Before(deadline) {
		base, found, lastErr = readAutoIncBase(dataDir, database, table)
		if lastErr == nil && found && base >= min {
			return base
		}
		time.Sleep(200 * time.Millisecond)
	}
	switch {
	case lastErr != nil:
		t.Fatalf("reading auto-increment base for %s.%s under %s: %v", database, table, dataDir, lastErr)
	case !found:
		t.Fatalf("no auto-increment claim row for %s.%s under %s after %v", database, table, dataDir, timeout)
	default:
		t.Fatalf("auto-increment base for %s.%s under %s = %d after %v, want >= %d",
			database, table, dataDir, base, timeout, min)
	}
	return 0
}

// queryStringColumn runs query against db and returns every row's value for
// the named column (matched case-insensitively, since MySQL identifiers are
// case-insensitive), scanning generically so it works regardless of how many
// other columns the result set carries - information_schema.tables returns a
// fixed wide row shape unrelated to the client's SELECT list.
func queryStringColumn(conn *sql.DB, query, colName string, args ...interface{}) ([]string, error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	idx := -1
	for i, c := range cols {
		if strings.EqualFold(c, colName) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, fmt.Errorf("column %q not found in result columns %v", colName, cols)
	}

	var out []string
	for rows.Next() {
		dest := make([]interface{}, len(cols))
		raw := make([]sql.NullString, len(cols))
		for i := range dest {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		out = append(out, raw[idx].String)
	}
	return out, rows.Err()
}

// asMySQLError extracts *mysql.MySQLError from err via errors.As, failing
// the test if err is not one - a raw driver/transport error is a different
// failure than the coded rejection these tests assert on.
func asMySQLError(t *testing.T, err error, context string) *mysql.MySQLError {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected an error, got nil", context)
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		t.Fatalf("%s: error %v is not a *mysql.MySQLError", context, err)
	}
	return mysqlErr
}

// ===========================================================================
// Item 6: DDL-time seeding
// ===========================================================================

// TestAutoIncSeed_CreateTableWithFloorOption exercises the shape DDL-time
// seeding actually supports: ALTER ... MODIFY/CHANGE to retroactively tag an
// existing BIGINT column AUTO_INCREMENT cannot land, because vitess emits
// MySQL ALTER syntax SQLite cannot apply. CREATE TABLE carrying a
// table-level AUTO_INCREMENT=N option is unaffected by that limitation and
// is the shape production DDL actually uses, so it is what this test
// drives: CREATE TABLE ... AUTO_INCREMENT=5000 must seed base >= 4999 (N-1:
// protocol/query/transform/table_utils.go autoIncFloorFromOptions) on every
// node, since each node seeds from its own DDL apply independently
// (db/autoinc_seed.go).
func TestAutoIncSeed_CreateTableWithFloorOption(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}

	const table = "autoinc_seed_floor"
	allNodes := []int{1, 2, 3}

	if _, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT) AUTO_INCREMENT=5000", table)); err != nil {
		t.Fatalf("CREATE TABLE ... AUTO_INCREMENT=5000: %v", err)
	}
	if err := harness.WaitForTableExists(table, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("autoinc_seed_floor_ddl")
		t.Fatalf("DDL did not replicate: %v", err)
	}

	for _, nodeID := range allNodes {
		dataDir := harness.Nodes[nodeID-1].DataDir
		base := waitForAutoIncBaseAtLeast(t, dataDir, db.DefaultDatabaseName, table, 4999, 15*time.Second)
		t.Logf("node %d: seeded base = %d (want >= 4999)", nodeID, base)
	}
}

// TestAutoIncSeed_AlterSeedsFromExistingMax: a table filled with explicit
// ids 1..rowCount holds a base at or above rowCount on every node, both
// before and after an unrelated ALTER TABLE ADD COLUMN re-triggers DDL-time
// seeding. The explicit ids raise the base as they are inserted (the range
// allocator observes every explicit id), and the ALTER's seed from MAX(id)
// can only raise it further. That the seed itself derives from MAX(id) is
// pinned without the allocator in the way by
// db/autoinc_seed_test.go TestSeedAutoIncBasesForDDL_ExistingRowsRaiseTheFloor.
func TestAutoIncSeed_AlterSeedsFromExistingMax(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}

	const table = "autoinc_seed_altermax"
	const rowCount = 1000
	const batchSize = 100
	allNodes := []int{1, 2, 3}

	if _, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(20))", table)); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := harness.WaitForTableExists(table, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("autoinc_seed_altermax_ddl")
		t.Fatalf("DDL did not replicate: %v", err)
	}

	for start := 1; start <= rowCount; start += batchSize {
		var values strings.Builder
		for i := start; i < start+batchSize; i++ {
			if i > start {
				values.WriteString(", ")
			}
			fmt.Fprintf(&values, "(%d, 'row_%d')", i, i)
		}
		if _, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, v) VALUES %s", table, values.String())); err != nil {
			t.Fatalf("explicit-id batch insert starting at %d: %v", start, err)
		}
	}
	if err := harness.WaitForRowCount(table, allNodes, rowCount, 15*time.Second); err != nil {
		t.Fatalf("explicit-id rows did not replicate: %v", err)
	}

	// Before the ALTER: explicit ids above the base raise it through the
	// claim protocol as they are inserted (id.RangeAllocator.Observe), so no
	// node can later issue one of them.
	preDataDir := harness.Nodes[0].DataDir
	preBase, found, err := readAutoIncBase(preDataDir, db.DefaultDatabaseName, table)
	if err != nil {
		t.Fatalf("read pre-ALTER base: %v", err)
	}
	if !found {
		t.Fatalf("no claim row for %s before ALTER", table)
	}
	if preBase < rowCount {
		t.Fatalf("pre-ALTER base = %d, want >= %d: explicit ids must raise the base as they are inserted", preBase, rowCount)
	}

	if _, err := harness.ExecNode(1, fmt.Sprintf("ALTER TABLE %s ADD COLUMN w INT", table)); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN w: %v", err)
	}

	for _, nodeID := range allNodes {
		dataDir := harness.Nodes[nodeID-1].DataDir
		base := waitForAutoIncBaseAtLeast(t, dataDir, db.DefaultDatabaseName, table, rowCount, 15*time.Second)
		t.Logf("node %d: base after ALTER = %d (want >= %d)", nodeID, base, rowCount)
	}
}

// TestAutoIncSeed_AlterFloorWithoutAColumnIsRefused asserts the refusal
// AlterTableColumnTypeRule raises (protocol/query/transform/alter_table_column_type.go):
// an ALTER carrying an AUTO_INCREMENT=N option with no narrow AUTO_INCREMENT
// column definition in the same statement has nowhere to store that floor
// (it belongs in the marker of the column it types), so it is refused with
// MySQL error 1235 (ER_NOT_SUPPORTED_YET), SQLSTATE 42000, rather than
// silently dropped. This covers both shapes the rule refuses: the
// option riding alongside an unrelated column add, and the bare
// option-only form.
func TestAutoIncSeed_AlterFloorWithoutAColumnIsRefused(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}

	const table = "autoinc_seed_floorrefused"
	allNodes := []int{1, 2, 3}

	if _, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, v TEXT)", table)); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := harness.WaitForTableExists(table, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("autoinc_seed_floorrefused_ddl")
		t.Fatalf("DDL did not replicate: %v", err)
	}

	statements := map[string]string{
		"ADD COLUMN alongside floor option": fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN w INT, AUTO_INCREMENT=5000", table),
		"floor option alone": fmt.Sprintf(
			"ALTER TABLE %s AUTO_INCREMENT=5000", table),
	}

	for label, stmt := range statements {
		_, err := harness.ExecNode(1, stmt)
		mysqlErr := asMySQLError(t, err, label)
		if mysqlErr.Number != mysqlcode.ErrCodeNotSupportedYet {
			t.Errorf("%s: error code = %d, want %d (ER_NOT_SUPPORTED_YET)",
				label, mysqlErr.Number, mysqlcode.ErrCodeNotSupportedYet)
		}
		if got := string(mysqlErr.SQLState[:]); got != mysqlcode.SQLStateSyntax {
			t.Errorf("%s: SQLSTATE = %q, want %q", label, got, mysqlcode.SQLStateSyntax)
		}
	}

	// Neither refused statement may have left column w behind.
	if _, err := harness.ExecNode(1, fmt.Sprintf("SELECT w FROM %s LIMIT 1", table)); err == nil {
		t.Errorf("column w exists on %s after both ALTERs were refused", table)
	}
}

// TestAutoIncSeed_RefusesAnIDBeyondTheWidth: a TINYINT AUTO_INCREMENT column
// (widthMax = 127, intmarker.Attributes.WidthMax) refuses an explicit id
// above its ceiling at INSERT, with MySQL error 1264
// (ER_WARN_DATA_OUT_OF_RANGE) / SQLSTATE 22003, as MySQL does in strict
// mode. SQLite would store it; letting it in would leave the table holding an
// id its declared width cannot, so it is refused before it reaches SQLite
// (protocol/query/rules/autoincrement_id.go). A DDL that declares a width the
// existing data does not fit is refused with the same code at PREPARE
// (db/ddl_prepare_validate.go checkAutoIncWidthCeilings), pinned by
// db/ddl_prepare_validate_autoinc_code_test.go and
// grpc/autoinc_claim_handler_test.go.
func TestAutoIncSeed_RefusesAnIDBeyondTheWidth(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}

	const table = "autoinc_width_ceiling"
	allNodes := []int{1, 2, 3}

	if _, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id TINYINT AUTO_INCREMENT PRIMARY KEY, v TEXT)", table)); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := harness.WaitForTableExists(table, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("autoinc_width_ceiling_ddl")
		t.Fatalf("DDL did not replicate: %v", err)
	}

	_, err := harness.ExecNode(1, fmt.Sprintf("INSERT INTO %s (id, v) VALUES (200, 'beyond_tinyint')", table))
	mysqlErr := asMySQLError(t, err, "explicit-id INSERT beyond widthMax")
	if mysqlErr.Number != mysqlcode.ErrCodeDataOutOfRange {
		t.Errorf("error code = %d, want %d (ER_WARN_DATA_OUT_OF_RANGE)", mysqlErr.Number, mysqlcode.ErrCodeDataOutOfRange)
	}
	if got := string(mysqlErr.SQLState[:]); got != mysqlcode.SQLStateDataOutOfRange {
		t.Errorf("SQLSTATE = %q, want %q", got, mysqlcode.SQLStateDataOutOfRange)
	}
	if n := harness.getRowCount(1, table); n != 0 {
		t.Errorf("the refused row reached the table: %d rows", n)
	}
	t.Logf("refused as required: %v", mysqlErr)
}

// ===========================================================================
// Item 7: the hidden table is invisible and write-protected
// ===========================================================================

// TestAutoIncHiddenTable_AbsentFromListings asserts __marmot__autoinc never
// appears in SHOW TABLES or information_schema.tables on any node, for two
// independent reasons that both hold: it lives in the system database, which
// DatabaseManager never lists as a user database at all
// (db/autoinc_claim.go's placement doc), and the listing queries themselves
// filter the "__marmot__" prefix
// (protocol/handlers/metadata.go, protocol/handlers/information_schema.go).
func TestAutoIncHiddenTable_AbsentFromListings(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}

	const table = "autoinc_hidden_listing"
	allNodes := []int{1, 2, 3}

	// A table that DOES exist and must be seeded, so the system database
	// definitely holds a __marmot__autoinc row by the time listings are
	// checked - a table with no row at all would make this assertion trivial.
	if _, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)", table)); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := harness.WaitForTableExists(table, allNodes, 15*time.Second); err != nil {
		t.Fatalf("DDL did not replicate: %v", err)
	}
	for _, nodeID := range allNodes {
		dataDir := harness.Nodes[nodeID-1].DataDir
		waitForAutoIncBaseAtLeast(t, dataDir, db.DefaultDatabaseName, table, 0, 15*time.Second)
	}

	for _, nodeID := range allNodes {
		if harness.tableExistsOnNode(nodeID, common.AutoIncClaimTableName) {
			t.Errorf("node %d: SHOW TABLES lists %s", nodeID, common.AutoIncClaimTableName)
		}

		conn, err := harness.ConnectToNode(nodeID)
		if err != nil {
			t.Fatalf("connect to node %d: %v", nodeID, err)
		}
		// The schema name is formatted into the statement rather than bound as
		// a parameter on purpose. A bound parameter makes the client use a
		// prepared statement, and the prepared path answers COM_STMT_EXECUTE
		// by re-running the TRANSPILED text (protocol/server.go:1411), which
		// no longer parses as an INFORMATION_SCHEMA query - so the query lands
		// on SQLite as "no such table: tables". That is a pre-existing gap in
		// prepared-statement routing for every statement Marmot answers above
		// the database, not a property of the claim table, and it is recorded
		// as such rather than worked around silently. The value is a constant
		// in this test, so formatting it injects nothing.
		names, err := queryStringColumn(conn, fmt.Sprintf(
			"SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'",
			db.DefaultDatabaseName), "table_name")
		if err != nil {
			t.Fatalf("node %d: information_schema.tables: %v", nodeID, err)
		}
		for _, name := range names {
			if strings.EqualFold(name, common.AutoIncClaimTableName) {
				t.Errorf("node %d: information_schema.tables lists %s", nodeID, common.AutoIncClaimTableName)
			}
		}
	}
}

// TestAutoIncHiddenTable_RejectsClientWrites asserts INSERT, UPDATE, DELETE
// and DROP TABLE against __marmot__autoinc are all refused with MySQL error
// 1142 (ER_TABLEACCESS_DENIED_ERROR), the code
// protocol/query/rules/claim_table_guard.go's ClaimTableGuardRule uses. The
// table need not exist in the user database for the rule to fire - it is a
// structural match on the parsed statement's target table name, evaluated
// before the statement ever reaches SQLite (ClaimTableGuardRule.Priority()
// == 0, runs before every other transform rule).
func TestAutoIncHiddenTable_RejectsClientWrites(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}

	table := common.AutoIncClaimTableName
	statements := map[string]string{
		"INSERT": fmt.Sprintf(
			"INSERT INTO %s (db, tbl, committed, owner, granted_at) VALUES ('marmot', 'x', 1, 1, 1)", table),
		"UPDATE": fmt.Sprintf("UPDATE %s SET committed = 999 WHERE tbl = 'x'", table),
		"DELETE": fmt.Sprintf("DELETE FROM %s WHERE tbl = 'x'", table),
		"DROP":   fmt.Sprintf("DROP TABLE %s", table),
	}

	for label, stmt := range statements {
		_, err := harness.ExecNode(1, stmt)
		mysqlErr := asMySQLError(t, err, label+" against "+table)
		if mysqlErr.Number != mysqlcode.ErrCodeTableAccessDenied {
			t.Errorf("%s: error code = %d, want %d (ER_TABLEACCESS_DENIED_ERROR)",
				label, mysqlErr.Number, mysqlcode.ErrCodeTableAccessDenied)
		}
		t.Logf("%s correctly refused: %v", label, mysqlErr)
	}
}
