//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func openInMemorySQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestValidateBaseTableForVectorIndex_IntegerPK(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)
	_, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	require.NoError(t, ValidateBaseTableForVectorIndex(db, "docs"))
}

func TestValidateBaseTableForVectorIndex_NoPK(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)
	_, err := db.Exec(`CREATE TABLE docs (embed BLOB)`)
	require.NoError(t, err)

	err = ValidateBaseTableForVectorIndex(db, "docs")
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-011")
}

func TestValidateBaseTableForVectorIndex_TextPK(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)
	_, err := db.Exec(`CREATE TABLE docs (id TEXT PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	err = ValidateBaseTableForVectorIndex(db, "docs")
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-011")
}

func TestValidateBaseTableForVectorIndex_CompositePK(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)
	_, err := db.Exec(`CREATE TABLE docs (a INTEGER, b INTEGER, embed BLOB, PRIMARY KEY(a, b))`)
	require.NoError(t, err)

	// Composite PK is not an alias for rowid — must be rejected.
	err = ValidateBaseTableForVectorIndex(db, "docs")
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-011")
}

func TestValidateBaseTableForVectorIndex_TableNotExist(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)

	err := ValidateBaseTableForVectorIndex(db, "nonexistent")
	require.Error(t, err)
}

func TestValidateBaseTableForVectorIndex_IntAlias(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)
	// "INT" is NOT the same as "INTEGER" in SQLite's rowid alias rule.
	// Only exact "INTEGER PRIMARY KEY" aliases rowid.
	_, err := db.Exec(`CREATE TABLE docs (id INT PRIMARY KEY, embed BLOB)`)
	require.NoError(t, err)

	err = ValidateBaseTableForVectorIndex(db, "docs")
	require.Error(t, err)
	require.Contains(t, err.Error(), "MARMOT-VEC-011")
}
