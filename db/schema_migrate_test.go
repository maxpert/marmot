//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrateVectorIndexesSchema_CreatesFresh(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)

	require.NoError(t, MigrateVectorIndexesSchema(db))

	// Table must exist with all expected columns.
	cols, err := currentVecIndexColumns(db)
	require.NoError(t, err)

	required := []string{"index_name", "table_name", "column_name", "database_name",
		"metric", "dim", "nlist", "nprobe", "max_norm", "status", "created_at"}
	for _, col := range required {
		_, ok := cols[col]
		require.True(t, ok, "missing column: %s", col)
	}
}

func TestMigrateVectorIndexesSchema_Idempotent(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)

	require.NoError(t, MigrateVectorIndexesSchema(db))
	require.NoError(t, MigrateVectorIndexesSchema(db)) // second run must not error
}

func TestMigrateVectorIndexesSchema_AddsNewColumn(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)

	// Create old-style table missing nlist/nprobe/max_norm.
	_, err := db.Exec(`CREATE TABLE __marmot_vector_indexes (
		index_name    TEXT PRIMARY KEY,
		table_name    TEXT NOT NULL,
		column_name   TEXT NOT NULL,
		database_name TEXT NOT NULL,
		metric        TEXT NOT NULL,
		dim           INTEGER NOT NULL,
		status        TEXT NOT NULL DEFAULT 'building',
		created_at    INTEGER NOT NULL
	)`)
	require.NoError(t, err)

	require.NoError(t, MigrateVectorIndexesSchema(db))

	cols, err := currentVecIndexColumns(db)
	require.NoError(t, err)
	require.Contains(t, cols, "nlist")
	require.Contains(t, cols, "nprobe")
	require.Contains(t, cols, "max_norm")
}

func TestMigrateVectorIndexesSchema_IncompatibleType(t *testing.T) {
	t.Parallel()
	db := openInMemorySQLite(t)

	// Create table with dim as TEXT — incompatible with expected INTEGER.
	_, err := db.Exec(`CREATE TABLE __marmot_vector_indexes (
		index_name    TEXT PRIMARY KEY,
		table_name    TEXT NOT NULL,
		column_name   TEXT NOT NULL,
		database_name TEXT NOT NULL,
		metric        TEXT NOT NULL,
		dim           TEXT NOT NULL,
		status        TEXT NOT NULL DEFAULT 'building',
		created_at    INTEGER NOT NULL
	)`)
	require.NoError(t, err)

	err = MigrateVectorIndexesSchema(db)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dim")
}
