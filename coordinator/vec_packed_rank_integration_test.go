//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	"github.com/stretchr/testify/require"
)

func TestPackedRankMatchesSQLNoCache(t *testing.T) {
	opts := sharedScanOptions{
		dim:        32,
		rows:       2048,
		clusters:   64,
		nlist:      64,
		nprobe:     8,
		cacheBytes: 0,
	}
	s := setupSharedScanFixture(t, opts)
	queryBlob := buildSharedScanWorkload(t, s, sharedScanWorkloadOverlap, 8)[0].queryBlob

	sqlIDs := runSharedScanQuery(t, s, queryBlob, false)

	state, ok := s.engine.Lookup(sharedScanIndexName)
	require.True(t, ok)
	dbPath, err := s.dbMgr.GetDatabasePath(sharedScanDBName)
	require.NoError(t, err)
	store, err := db.RebuildPackedPartitionStore(context.Background(), s.conn, dbPath, common.VectorIndexMeta{
		IndexName: sharedScanIndexName,
	}, state.Spec())
	require.NoError(t, err)
	require.NotNil(t, store)
	defer state.ClearPackedStore()
	state.StorePackedStore(store)

	packedIDs := runSharedScanQuery(t, s, queryBlob, false)
	require.Equal(t, sqlIDs, packedIDs)
}
