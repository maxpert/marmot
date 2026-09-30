package handlers_test

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/handlers"
	"github.com/stretchr/testify/require"
)

// TestMetadataAnswersWhileTheWriterIsBusy pins the other half of F-P1 on a
// real DatabaseManager: metadata is read through the read pool, so it answers
// while the database's single write connection is taken by an open write
// transaction, as it is during every DDL and every 2PC apply.
//
// Mutation: DatabaseManager.GetDatabaseReadConnection returns GetDB() (the
// write handle); the first call misses its deadline.
func TestMetadataAnswersWhileTheWriterIsBusy(t *testing.T) {
	mgr, err := db.NewDatabaseManager(t.TempDir(), 1, hlc.NewClock(1))
	require.NoError(t, err)
	t.Cleanup(func() { mgr.Close() })

	const dbName = "busy"
	require.NoError(t, mgr.CreateDatabase(dbName))
	writer, err := mgr.GetDatabaseConnection(dbName)
	require.NoError(t, err)
	_, err = writer.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	require.NoError(t, err)

	// Take the write handle's only connection and keep it.
	tx, err := writer.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.Exec("INSERT INTO t (v) VALUES ('held')")
	require.NoError(t, err)

	h := handlers.NewMetadataHandler(mgr, db.SystemDatabaseName)
	calls := []struct {
		name string
		call func() (*protocol.ResultSet, error)
	}{
		{"TABLES", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema(dbName, protocol.Statement{ISTableType: protocol.ISTableTables})
		}},
		{"COLUMNS", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema(dbName, protocol.Statement{ISTableType: protocol.ISTableColumns})
		}},
		{"SHOW INDEX", func() (*protocol.ResultSet, error) { return h.HandleShowIndexes(dbName, "t") }},
		{"SHOW CREATE TABLE", func() (*protocol.ResultSet, error) { return h.HandleShowCreateTable(dbName, "t") }},
	}
	for _, c := range calls {
		done := make(chan error, 1)
		go func() {
			_, err := c.call()
			done <- err
		}()
		select {
		case err := <-done:
			require.NoError(t, err, c.name)
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not answer while the write connection was held: metadata is waiting on the writer", c.name)
		}
	}
}
