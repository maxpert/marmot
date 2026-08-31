//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func openDDLValidationDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`CREATE TABLE groups (group_id INTEGER PRIMARY KEY, display_name TEXT, creation_date datetime)`)
	require.NoError(t, err)
	return db
}

func columnNames(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM pragma_table_info(?)`, table)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

// A column that already exists must be rejected during PREPARE, not at COMMIT.
func TestValidateDDLStatements_DuplicateColumn(t *testing.T) {
	t.Parallel()
	db := openDDLValidationDB(t)

	err := ValidateDDLStatements(context.Background(), db, []string{
		`ALTER TABLE groups ADD COLUMN creation_date datetime NOT NULL DEFAULT '2026-08-10 19:38:56.952152'`,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate column name: creation_date")
}

func TestValidateDDLStatements_MissingTable(t *testing.T) {
	t.Parallel()
	db := openDDLValidationDB(t)

	err := ValidateDDLStatements(context.Background(), db, []string{
		`ALTER TABLE missing_table ADD COLUMN x TEXT`,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "no such table")
}

func TestValidateDDLStatements_ValidDDLLeavesNoSideEffects(t *testing.T) {
	t.Parallel()
	db := openDDLValidationDB(t)

	require.NoError(t, ValidateDDLStatements(context.Background(), db, []string{
		`ALTER TABLE groups ADD COLUMN uuid TEXT`,
		`CREATE INDEX idx_groups_display_name ON groups(display_name)`,
	}))

	require.NotContains(t, columnNames(t, db, "groups"), "uuid")

	var indexCount int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_groups_display_name'`).Scan(&indexCount))
	require.Equal(t, 0, indexCount, "validation must roll back every statement it executes")
}

// Later statements must observe the schema produced by earlier ones so a
// transaction that creates a table and then indexes it is not falsely rejected.
func TestValidateDDLStatements_DependentStatements(t *testing.T) {
	t.Parallel()
	db := openDDLValidationDB(t)

	require.NoError(t, ValidateDDLStatements(context.Background(), db, []string{
		`CREATE TABLE memberships (id INTEGER PRIMARY KEY, group_id INTEGER)`,
		`CREATE INDEX idx_memberships_group ON memberships(group_id)`,
	}))
}

func TestValidateDDLStatements_FailureLeavesNoPartialSchema(t *testing.T) {
	t.Parallel()
	db := openDDLValidationDB(t)

	err := ValidateDDLStatements(context.Background(), db, []string{
		`ALTER TABLE groups ADD COLUMN uuid TEXT`,
		`ALTER TABLE groups ADD COLUMN creation_date TEXT`,
	})

	require.Error(t, err)
	require.NotContains(t, columnNames(t, db, "groups"), "uuid")
}

func TestValidateDDLStatements_NoStatements(t *testing.T) {
	t.Parallel()
	db := openDDLValidationDB(t)

	require.NoError(t, ValidateDDLStatements(context.Background(), db, nil))
	require.NoError(t, ValidateDDLStatements(context.Background(), db, []string{""}))
	require.NoError(t, ValidateDDLStatements(context.Background(), nil, []string{`ALTER TABLE groups ADD COLUMN x TEXT`}))
}

// Validation must not hold the SQLite write connection once it returns, or the
// COMMIT that follows PREPARE would block on a single-connection write pool.
func TestValidateDDLStatements_ReleasesWriteConnection(t *testing.T) {
	t.Parallel()
	db := openDDLValidationDB(t)

	err := ValidateDDLStatements(context.Background(), db, []string{
		`ALTER TABLE groups ADD COLUMN creation_date TEXT`,
	})
	require.Error(t, err)

	_, err = db.Exec(`ALTER TABLE groups ADD COLUMN uuid TEXT`)
	require.NoError(t, err)
	require.Contains(t, columnNames(t, db, "groups"), "uuid")
}

// A genuine SQLite verdict on the statement - duplicate column - must be a
// final rejection: retrying it can never succeed.
func TestIsDDLRejection_DuplicateColumnIsRejected(t *testing.T) {
	t.Parallel()
	dbc := openDDLValidationDB(t)

	err := ValidateDDLStatements(context.Background(), dbc, []string{
		`ALTER TABLE groups ADD COLUMN creation_date datetime NOT NULL DEFAULT '2026-08-10 19:38:56.952152'`,
	})
	require.Error(t, err)

	var sqliteErr sqlite3.Error
	require.True(t, errors.As(err, &sqliteErr), "expected a typed sqlite3.Error, got %T: %v", err, err)
	require.True(t, isDDLRejection(context.Background(), err))
}

// A genuine SQLite verdict on the statement - missing table - must be a final
// rejection.
func TestIsDDLRejection_MissingTableIsRejected(t *testing.T) {
	t.Parallel()
	dbc := openDDLValidationDB(t)

	err := ValidateDDLStatements(context.Background(), dbc, []string{
		`ALTER TABLE missing_table ADD COLUMN x TEXT`,
	})
	require.Error(t, err)
	require.True(t, isDDLRejection(context.Background(), err))
}

// A cancelled validation says nothing about the DDL and must never be a
// rejection.
func TestIsDDLRejection_ContextCanceledIsNotRejected(t *testing.T) {
	t.Parallel()
	dbc := openDDLValidationDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ValidateDDLStatements(ctx, dbc, []string{`ALTER TABLE groups ADD COLUMN x TEXT`})
	require.Error(t, err)
	// Premise: this really is a context error, not some other failure that
	// happens to occur after cancellation.
	require.True(t, isContextError(err), "premise failed: err is not a context error: %v", err)

	require.False(t, isDDLRejection(ctx, err))
}

// A validation that times out says nothing about the DDL and must never be a
// rejection - this is the case the added DDL validation timeout exists to make
// survivable rather than a hard PREPARE failure.
func TestIsDDLRejection_DeadlineExceededIsNotRejected(t *testing.T) {
	t.Parallel()
	dbc := openDDLValidationDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	err := ValidateDDLStatements(ctx, dbc, []string{`ALTER TABLE groups ADD COLUMN x TEXT`})
	require.Error(t, err)
	require.True(t, isContextError(err), "premise failed: err is not a context error: %v", err)

	require.False(t, isDDLRejection(ctx, err))
}

// An error that is not a typed SQLite error - for example connection-pool
// exhaustion wrapped around BeginTx - carries no verdict on the statement and
// must stay retryable rather than being assumed to be a rejection.
func TestIsDDLRejection_NonSQLiteErrorIsNotRejected(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("failed to begin DDL validation transaction: %w", errors.New("pool exhausted"))

	var sqliteErr sqlite3.Error
	require.False(t, errors.As(err, &sqliteErr), "premise failed: err unexpectedly is a typed sqlite3.Error")

	require.False(t, isDDLRejection(context.Background(), err))
}

// SQLite table locks are the reachable, real-world case a transient
// classification protects: with cache=shared, hookDB and writeDB are separate
// connections in the same process (see db_integration.go), and a table lock
// hookDB holds surfaces to writeDB as SQLITE_LOCKED immediately - busy_timeout
// does not apply to it. This must stay a retryable missing ACK, not a
// rejection: DDL colliding with an in-flight DML on the same table is routine,
// not a verdict that the DDL can never be applied.
func TestIsDDLRejection_SharedCacheLockedIsNotRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "lock.db") + "?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate&cache=shared"

	// connA mimics hookDB: a separate connection on the same shared cache that
	// holds a write lock on the table the DDL below targets.
	connA, err := sql.Open(SQLiteDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { connA.Close() })
	connA.SetMaxOpenConns(1)

	// connB mimics writeDB: the connection ValidateDDLStatements runs on.
	connB, err := sql.Open(SQLiteDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { connB.Close() })
	connB.SetMaxOpenConns(1)

	_, err = connB.Exec(`CREATE TABLE locked_t (id INTEGER PRIMARY KEY, val TEXT)`)
	require.NoError(t, err)

	tx, err := connA.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.ExecContext(context.Background(), `INSERT INTO locked_t (id, val) VALUES (1, 'x')`)
	require.NoError(t, err)

	start := time.Now()
	verr := ValidateDDLStatements(context.Background(), connB, []string{`ALTER TABLE locked_t ADD COLUMN extra TEXT`})
	elapsed := time.Since(start)

	require.Error(t, verr)
	// Premise: SQLITE_LOCKED fails immediately and does not wait out
	// _busy_timeout=5000ms - if it did, this scenario would be indistinguishable
	// from SQLITE_BUSY and the whole rationale for a typed-code classification
	// over blanket "not a context error" would not hold.
	require.Less(t, elapsed, 2*time.Second, "premise failed: SQLITE_LOCKED waited out busy_timeout")

	var sqliteErr sqlite3.Error
	require.True(t, errors.As(verr, &sqliteErr), "expected a typed sqlite3.Error, got %T: %v", verr, verr)
	require.Equal(t, sqlite3.ErrLocked, sqliteErr.Code,
		"expected SQLITE_LOCKED - if this fires as SQLITE_BUSY instead, the shared-cache premise needs re-checking")

	require.False(t, isDDLRejection(context.Background(), verr),
		"a shared-cache lock is a transient condition on this node, not a verdict on the DDL")
}

// A read-only database is a condition on this node - e.g. this replica's file
// is mounted read-only, or a prior connection put it in query-only mode - not
// a verdict on the DDL statement itself: the same statement applies cleanly on
// a writable node. This must stay a retryable missing ACK, not a rejection.
func TestIsDDLRejection_ReadonlyIsNotRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "readonly.db") + "?_journal_mode=WAL"

	setup, err := sql.Open(SQLiteDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { setup.Close() })
	_, err = setup.Exec(`CREATE TABLE groups (group_id INTEGER PRIMARY KEY, display_name TEXT)`)
	require.NoError(t, err)
	require.NoError(t, setup.Close())

	roConn, err := sql.Open(SQLiteDriverName, dsn+"&mode=ro")
	require.NoError(t, err)
	t.Cleanup(func() { roConn.Close() })
	roConn.SetMaxOpenConns(1)

	verr := ValidateDDLStatements(context.Background(), roConn, []string{`ALTER TABLE groups ADD COLUMN extra TEXT`})
	require.Error(t, verr)

	var sqliteErr sqlite3.Error
	require.True(t, errors.As(verr, &sqliteErr), "expected a typed sqlite3.Error, got %T: %v", verr, verr)
	require.Equal(t, sqlite3.ErrReadonly, sqliteErr.Code,
		"expected SQLITE_READONLY - if this fires as a different code, the mode=ro premise needs re-checking")

	require.False(t, isDDLRejection(context.Background(), verr),
		"a read-only database is a condition on this node, not a verdict on the DDL")
}
