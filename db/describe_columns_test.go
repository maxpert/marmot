package db

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// setupDescribeDB builds a real database with a typed table so column
// description runs against SQLite rather than a mock.
func setupDescribeDB(t *testing.T) *ReplicatedDatabase {
	t.Helper()

	mgr, err := NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	require.NoError(t, err)
	t.Cleanup(func() { mgr.Close() })

	const dbName = "describe"
	require.NoError(t, mgr.CreateDatabase(dbName))

	conn, err := mgr.GetDatabaseConnection(dbName)
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE metadata (
		version  smallint,
		label    varchar(255),
		ratio    REAL,
		created  datetime
	)`)
	require.NoError(t, err)

	rdb, err := mgr.GetDatabase(dbName)
	require.NoError(t, err)
	return rdb
}

func TestDescribeResultColumns(t *testing.T) {
	rdb := setupDescribeDB(t)
	ctx := context.Background()

	cols, err := rdb.DescribeResultColumns(ctx, "SELECT version, label, ratio, created FROM metadata")
	require.NoError(t, err)
	require.Len(t, cols, 4)
	require.Equal(t, "version", cols[0].Name)
	require.Equal(t, "smallint", cols[0].DeclType)
	require.Equal(t, "label", cols[1].Name)
	require.Equal(t, "varchar(255)", cols[1].DeclType)
	require.Equal(t, "ratio", cols[2].Name)
	require.Equal(t, "created", cols[3].Name)
}

// TestDescribeResultColumnsDoesNotRun proves description has no side effects:
// it reads metadata at prepare time and never steps the statement.
func TestDescribeResultColumnsDoesNotRun(t *testing.T) {
	rdb := setupDescribeDB(t)
	ctx := context.Background()

	// An INSERT describes as producing no columns and must not insert a row.
	cols, err := rdb.DescribeResultColumns(ctx, "INSERT INTO metadata (version) VALUES (1)")
	require.NoError(t, err)
	require.Empty(t, cols)

	_, rows, err := rdb.ExecuteSnapshotRead(ctx, "SELECT COUNT(*) AS c FROM metadata")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.EqualValues(t, 0, rows[0]["c"], "describing a statement must not execute it")
}

// TestDescribeResultColumnsWithPlaceholders covers the parameterised case: the
// statement binds NULL for each placeholder and is never stepped.
func TestDescribeResultColumnsWithPlaceholders(t *testing.T) {
	rdb := setupDescribeDB(t)

	cols, err := rdb.DescribeResultColumns(context.Background(),
		"SELECT version, label FROM metadata WHERE version = ? AND label = ?")
	require.NoError(t, err)
	require.Len(t, cols, 2)
	require.Equal(t, "version", cols[0].Name)
	require.Equal(t, "label", cols[1].Name)
}

// TestDescribeResultColumnsUnknownTable pins that an unpreparable statement
// reports an error rather than silently describing nothing. Reporting no
// columns would let a client cache empty metadata for a table that later exists.
func TestDescribeResultColumnsUnknownTable(t *testing.T) {
	rdb := setupDescribeDB(t)

	_, err := rdb.DescribeResultColumns(context.Background(), "SELECT x FROM nosuchtable")
	require.Error(t, err)
	require.Contains(t, err.Error(), "nosuchtable")
}

// TestDescribeResultColumnsExpression covers computed columns, where SQLite
// reports a label but no declared type.
func TestDescribeResultColumnsExpression(t *testing.T) {
	rdb := setupDescribeDB(t)

	cols, err := rdb.DescribeResultColumns(context.Background(), "SELECT COUNT(*) AS total FROM metadata")
	require.NoError(t, err)
	require.Len(t, cols, 1)
	require.Equal(t, "total", cols[0].Name)
	require.Empty(t, cols[0].DeclType, "an expression has no declared type")
}
