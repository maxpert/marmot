package coordinator

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/query/transform"
)

// stubVectorIndexManager is a test double for VectorIndexManagerProvider.
type stubVectorIndexManager struct {
	createErr error
	dropErr   error
	created   []common.VectorIndexMeta
	dropped   []string
}

func (s *stubVectorIndexManager) CreateIndex(_ context.Context, meta common.VectorIndexMeta) error {
	if s.createErr != nil {
		return s.createErr
	}
	s.created = append(s.created, meta)
	return nil
}

func (s *stubVectorIndexManager) DropIndex(_ context.Context, indexName, _ string) error {
	if s.dropErr != nil {
		return s.dropErr
	}
	s.dropped = append(s.dropped, indexName)
	return nil
}

// stubDatabaseManagerWithVec satisfies coordinator.DatabaseManager and exposes a VectorIndexManagerProvider.
type stubDatabaseManagerWithVec struct {
	vecMgr VectorIndexManagerProvider
}

func (s *stubDatabaseManagerWithVec) ListDatabases() []string       { return nil }
func (s *stubDatabaseManagerWithVec) DatabaseExists(_ string) bool  { return false }
func (s *stubDatabaseManagerWithVec) CreateDatabase(_ string) error { return nil }
func (s *stubDatabaseManagerWithVec) DropDatabase(_ string) error   { return nil }
func (s *stubDatabaseManagerWithVec) GetDatabaseConnection(_ string) (*sql.DB, error) {
	return nil, nil
}
func (s *stubDatabaseManagerWithVec) GetReplicatedDatabase(_ string) (ReplicatedDatabaseProvider, error) {
	return nil, nil
}
func (s *stubDatabaseManagerWithVec) GetAutoIncrementColumn(_, _ string) (string, error) {
	return "", nil
}
func (s *stubDatabaseManagerWithVec) GetTranspilerSchema(_, _ string) (*transform.SchemaInfo, error) {
	return nil, nil
}
func (s *stubDatabaseManagerWithVec) GetVectorIndexManager() VectorIndexManagerProvider {
	return s.vecMgr
}

func newHandlerWithVecMgr(t *testing.T, vecMgr VectorIndexManagerProvider) *CoordinatorHandler {
	t.Helper()
	clock := hlc.NewClock(1)
	dbMgr := &stubDatabaseManagerWithVec{vecMgr: vecMgr}
	return NewCoordinatorHandler(1, nil, nil, clock, dbMgr, nil, nil, &stubNodeRegistry{localNodeID: 1})
}

// TestHandleVectorDDL_NilManager verifies that handleVectorDDL returns a clear error
// when no VectorIndexManager is configured.
func TestHandleVectorDDL_NilManager(t *testing.T) {
	t.Parallel()

	h := newHandlerWithVecMgr(t, nil)

	stmt := protocol.Statement{
		Type:             protocol.StatementCreateVectorIndex,
		VectorIndexName:  "idx",
		TableName:        "t",
		VectorColumnName: "vec",
		Database:         "testdb",
		VectorMetric:     "cosine",
		VectorDim:        128,
	}

	_, err := h.handleVectorDDL(stmt)
	if err == nil {
		t.Fatal("expected error when VectorIndexManager is nil, got nil")
	}
	if err.Error() != "vector index support not enabled" {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

// TestHandleVectorDDL_NilDbManager verifies that handleVectorDDL returns a clear error
// when the database manager itself is nil.
func TestHandleVectorDDL_NilDbManager(t *testing.T) {
	t.Parallel()

	clock := hlc.NewClock(1)
	h := NewCoordinatorHandler(1, nil, nil, clock, nil, nil, nil, &stubNodeRegistry{localNodeID: 1})

	stmt := protocol.Statement{
		Type:            protocol.StatementCreateVectorIndex,
		VectorIndexName: "idx",
		Database:        "testdb",
	}

	_, err := h.handleVectorDDL(stmt)
	if err == nil {
		t.Fatal("expected error when dbManager is nil, got nil")
	}
}

// TestHandleVectorDDL_CreateIndex verifies that CREATE VECTOR INDEX is routed to
// VectorIndexManager.CreateIndex with correct metadata.
func TestHandleVectorDDL_CreateIndex(t *testing.T) {
	t.Parallel()

	stub := &stubVectorIndexManager{}
	h := newHandlerWithVecMgr(t, stub)

	stmt := protocol.Statement{
		Type:             protocol.StatementCreateVectorIndex,
		VectorIndexName:  "idx_embed",
		TableName:        "articles",
		VectorColumnName: "embedding",
		Database:         "mydb",
		VectorMetric:     "cosine",
		VectorDim:        768,
	}

	rs, err := h.handleVectorDDL(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rs == nil {
		t.Fatal("expected non-nil result set")
	}

	if len(stub.created) != 1 {
		t.Fatalf("expected 1 CreateIndex call, got %d", len(stub.created))
	}
	meta := stub.created[0]
	if meta.IndexName != "idx_embed" {
		t.Errorf("IndexName = %q, want %q", meta.IndexName, "idx_embed")
	}
	if meta.TableName != "articles" {
		t.Errorf("TableName = %q, want %q", meta.TableName, "articles")
	}
	if meta.ColumnName != "embedding" {
		t.Errorf("ColumnName = %q, want %q", meta.ColumnName, "embedding")
	}
	if meta.Database != "mydb" {
		t.Errorf("Database = %q, want %q", meta.Database, "mydb")
	}
	if meta.Metric != "cosine" {
		t.Errorf("Metric = %q, want %q", meta.Metric, "cosine")
	}
	if meta.Dim != 768 {
		t.Errorf("Dim = %d, want 768", meta.Dim)
	}
	if meta.Status != "building" {
		t.Errorf("Status = %q, want %q", meta.Status, "building")
	}
	if meta.CreatedAt == 0 {
		t.Error("CreatedAt must be set")
	}
}

// TestHandleVectorDDL_DropIndex verifies that DROP VECTOR INDEX is routed to
// VectorIndexManager.DropIndex with the correct index name.
func TestHandleVectorDDL_DropIndex(t *testing.T) {
	t.Parallel()

	stub := &stubVectorIndexManager{}
	h := newHandlerWithVecMgr(t, stub)

	stmt := protocol.Statement{
		Type:            protocol.StatementDropVectorIndex,
		VectorIndexName: "idx_embed",
		TableName:       "articles",
		Database:        "mydb",
	}

	rs, err := h.handleVectorDDL(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rs == nil {
		t.Fatal("expected non-nil result set")
	}

	if len(stub.dropped) != 1 {
		t.Fatalf("expected 1 DropIndex call, got %d", len(stub.dropped))
	}
	if stub.dropped[0] != "idx_embed" {
		t.Errorf("dropped index name = %q, want %q", stub.dropped[0], "idx_embed")
	}
}

// TestHandleVectorDDL_CreateError verifies that errors from VectorIndexManager.CreateIndex
// are propagated correctly.
func TestHandleVectorDDL_CreateError(t *testing.T) {
	t.Parallel()

	stub := &stubVectorIndexManager{createErr: errors.New("engine failure")}
	h := newHandlerWithVecMgr(t, stub)

	stmt := protocol.Statement{
		Type:            protocol.StatementCreateVectorIndex,
		VectorIndexName: "idx",
		Database:        "mydb",
	}

	_, err := h.handleVectorDDL(stmt)
	if err == nil {
		t.Fatal("expected error from CreateIndex, got nil")
	}
}

// TestHandleVectorDDL_DropError verifies that errors from VectorIndexManager.DropIndex
// are propagated correctly.
func TestHandleVectorDDL_DropError(t *testing.T) {
	t.Parallel()

	stub := &stubVectorIndexManager{dropErr: errors.New("not found")}
	h := newHandlerWithVecMgr(t, stub)

	stmt := protocol.Statement{
		Type:            protocol.StatementDropVectorIndex,
		VectorIndexName: "idx",
		Database:        "mydb",
	}

	_, err := h.handleVectorDDL(stmt)
	if err == nil {
		t.Fatal("expected error from DropIndex, got nil")
	}
}

// TestVectorDDL_IsMutation verifies that vector DDL statement types are classified
// as mutations by protocol.IsMutation, ensuring they flow through handleMutation.
func TestVectorDDL_IsMutation(t *testing.T) {
	t.Parallel()

	for _, st := range []protocol.StatementCode{
		protocol.StatementCreateVectorIndex,
		protocol.StatementDropVectorIndex,
	} {
		stmt := protocol.Statement{Type: st}
		if !protocol.IsMutation(stmt) {
			t.Errorf("statement type %d should be classified as a mutation", st)
		}
	}
}
