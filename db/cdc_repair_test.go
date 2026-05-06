//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

type iterateErrorMetaStore struct {
	MetaStore
	err error
}

func (s *iterateErrorMetaStore) IterateCapturedRows(txnID uint64) (CapturedRowCursor, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.MetaStore.IterateCapturedRows(txnID)
}

func TestExecuteLocalWithHooksPropagatesIntentEntryErrors(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "app.db")

	baseStore, err := NewPebbleMetaStore(filepath.Join(tmpDir, "meta"), DefaultPebbleOptions())
	require.NoError(t, err)

	metaStore := &iterateErrorMetaStore{MetaStore: baseStore}
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, hlc.NewClock(1), metaStore)
	if err != nil {
		require.NoError(t, metaStore.Close())
		t.Fatalf("NewReplicatedDatabase failed: %v", err)
	}
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	require.NoError(t, replicatedDB.ReloadSchema())

	metaStore.err = errors.New("captured row iterator unavailable")
	_, err = replicatedDB.ExecuteLocalWithHooks(context.Background(), 1001, []coordinator.ExecutionRequest{
		{SQL: "INSERT INTO users (id, name) VALUES (?, ?)", Params: []interface{}{int64(1), "alice"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get CDC intent entries")
	require.Contains(t, err.Error(), "captured row iterator unavailable")
}

func TestRepairAppliedTxnMetadataPreservesDatabaseName(t *testing.T) {
	tmpDir := t.TempDir()
	dbDir := filepath.Join(tmpDir, "databases")
	require.NoError(t, os.MkdirAll(dbDir, 0755))
	dbPath := filepath.Join(dbDir, "app.db")

	sqlDB, err := sql.Open(SQLiteDriverName, dbPath)
	require.NoError(t, err)
	require.NoError(t, ensureAppliedTxnTable(sqlDB))

	txnID := uint64(2001)
	commitTS := hlc.Timestamp{WallTime: 123, Logical: 4, NodeID: 9}
	tx, err := sqlDB.Begin()
	require.NoError(t, err)
	require.NoError(t, MarkSQLiteTxnApplied(tx, txnID, commitTS))
	require.NoError(t, tx.Commit())
	require.NoError(t, sqlDB.Close())

	metaStore, err := NewPebbleMetaStore(filepath.Join(tmpDir, "meta"), DefaultPebbleOptions())
	require.NoError(t, err)
	require.NoError(t, metaStore.WriteIntentEntry(
		txnID,
		1,
		uint8(OpTypeInsert),
		"users",
		"users:1",
		nil,
		map[string][]byte{"id": []byte{1}},
	))

	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, hlc.NewClock(1), metaStore)
	if err != nil {
		require.NoError(t, metaStore.Close())
		t.Fatalf("NewReplicatedDatabase failed: %v", err)
	}
	defer replicatedDB.Close()

	rec, err := metaStore.GetTransaction(txnID)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, TxnStatusCommitted, rec.Status)
	require.Equal(t, "app", rec.DatabaseName)

	var streamed []*TransactionRecord
	err = metaStore.StreamCommittedTransactions(0, func(rec *TransactionRecord) error {
		streamed = append(streamed, rec)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, streamed, 1)
	require.Equal(t, "app", streamed[0].DatabaseName)
}
