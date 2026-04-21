//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator

import (
	"database/sql"
	"testing"

	"github.com/maxpert/marmot/protocol/query/transform"
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
