//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/protocol/query/transform"
	"github.com/stretchr/testify/require"
)

type directPKDBManagerStub struct {
	pk string
}

func (s directPKDBManagerStub) ListDatabases() []string                            { return nil }
func (s directPKDBManagerStub) DatabaseExists(name string) bool                    { return false }
func (s directPKDBManagerStub) CreateDatabase(name string) error                   { return nil }
func (s directPKDBManagerStub) DropDatabase(name string) error                     { return nil }
func (s directPKDBManagerStub) GetDatabaseConnection(name string) (*sql.DB, error) { return nil, nil }
func (s directPKDBManagerStub) GetDatabaseReadConnection(name string) (*sql.DB, error) {
	return nil, nil
}
func (s directPKDBManagerStub) GetReplicatedDatabase(name string) (ReplicatedDatabaseProvider, error) {
	return nil, nil
}
func (s directPKDBManagerStub) GetAutoIncrementColumn(database, table string) (string, error) {
	return s.pk, nil
}
func (s directPKDBManagerStub) GetTranspilerSchema(database, table string) (*transform.SchemaInfo, error) {
	return nil, nil
}
func (s directPKDBManagerStub) GetVectorIndexManager() VectorIndexManagerProvider { return nil }

type packedRankDBManagerStub struct {
	directPKDBManagerStub
	readErr error
}

func (s packedRankDBManagerStub) GetDatabaseReadConnection(name string) (*sql.DB, error) {
	return nil, s.readErr
}

func openPackedStoreForTest(t *testing.T, dim int, rows ...struct {
	clusterID int64
	rowid     int64
	vec       []float32
}) *vecindex.PackedPartitionStore {
	t.Helper()

	path := filepath.Join(t.TempDir(), "test.vecpack")
	maxCluster := 0
	for _, row := range rows {
		if int(row.clusterID) > maxCluster {
			maxCluster = int(row.clusterID)
		}
	}
	writer, err := vecindex.CreatePackedPartitionStoreWriter(path, dim, maxCluster)
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, writer.Append(row.clusterID, row.rowid, vecindex.Float32ToBytes(row.vec)))
	}
	store, err := writer.Close()
	require.NoError(t, err)
	return store
}

func TestTryDirectPKResult(t *testing.T) {
	t.Parallel()

	h := &CoordinatorHandler{dbManager: directPKDBManagerStub{pk: "id"}}
	plan := &GoRankPlan{
		Database:       "dbpedia",
		BaseTable:      "docs",
		DirectPKColumn: "id",
		DirectPKLabel:  "id",
	}
	topK := []rankItem{
		{rowid: 7, dist: 0.1},
		{rowid: 2, dist: 0.2},
		{rowid: 9, dist: 0.3},
	}

	rs, ok, err := h.tryDirectPKResult(plan, topK)
	if err != nil {
		t.Fatalf("tryDirectPKResult returned error: %v", err)
	}
	if !ok {
		t.Fatalf("tryDirectPKResult should match autoincrement primary key")
	}
	if len(rs.Columns) != 1 || rs.Columns[0].Name != "id" {
		t.Fatalf("unexpected columns: %#v", rs.Columns)
	}
	if got, want := rs.Rows, [][]interface{}{{int64(7)}, {int64(2)}, {int64(9)}}; len(got) != len(want) {
		t.Fatalf("row count mismatch: got=%d want=%d", len(got), len(want))
	} else {
		for i := range want {
			if got[i][0] != want[i][0] {
				t.Fatalf("row %d mismatch: got=%v want=%v", i, got[i], want[i])
			}
		}
	}
}

func TestTryDirectPKResult_SkipsNonPKProjection(t *testing.T) {
	t.Parallel()

	h := &CoordinatorHandler{dbManager: directPKDBManagerStub{pk: "id"}}
	plan := &GoRankPlan{
		Database:       "dbpedia",
		BaseTable:      "docs",
		DirectPKColumn: "other_id",
		DirectPKLabel:  "other_id",
	}

	rs, ok, err := h.tryDirectPKResult(plan, []rankItem{{rowid: 1, dist: 0}})
	if err != nil {
		t.Fatalf("tryDirectPKResult returned error: %v", err)
	}
	if ok || rs != nil {
		t.Fatalf("tryDirectPKResult should skip non-primary-key projections, got ok=%v rs=%#v", ok, rs)
	}
}

func TestPackedRankUsesResidentDeltaWithoutSQLite(t *testing.T) {
	t.Parallel()

	cs, err := kmeans.NewCentroidSet(7, [][]float32{{1, 0}})
	require.NoError(t, err)
	state := vecindex.NewIndexState(vecindex.IVFSpec{
		ID:     "emb",
		Dim:    2,
		Metric: vecindex.MetricCosine,
		Nlist:  1,
		Nprobe: 1,
	}, cs)
	store := openPackedStoreForTest(t, 2, struct {
		clusterID int64
		rowid     int64
		vec       []float32
	}{clusterID: 1, rowid: 10, vec: []float32{1, 0}})
	t.Cleanup(func() { _ = store.Close() })
	state.StorePackedStore(store)

	delta := vecindex.NewDeltaBuffer()
	delta.Append(vecindex.CachedVector{RowID: 20, Vec: []float32{0, 1}})
	state.StoreResidentDelta(delta)

	engine := vecindex.NewEngine()
	engine.Register("emb", state)

	h := &CoordinatorHandler{
		dbManager: packedRankDBManagerStub{
			readErr: errors.New("packed rank unexpectedly touched SQLite"),
		},
	}
	h.SetVectorEngine(engine)

	topK, ok, err := h.packedRank(&GoRankPlan{
		Database:   "db",
		IndexName:  "emb",
		K:          2,
		QueryVec:   []float32{1, 0},
		ClusterIDs: []int64{1},
		RankMetric: metric.MetricCosine,
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, topK, 2)
	require.Equal(t, int64(10), topK[0].rowid)
	require.Equal(t, int64(20), topK[1].rowid)
}
