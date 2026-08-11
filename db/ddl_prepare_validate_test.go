//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"testing"

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
