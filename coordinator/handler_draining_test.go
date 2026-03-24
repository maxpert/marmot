package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// stubNodeRegistry satisfies coordinator.NodeRegistry with configurable leaving state.
type stubNodeRegistry struct {
	localNodeID uint64
	leaving     bool
}

func (s *stubNodeRegistry) UpdateSchemaVersions(_ map[string]uint64) {}
func (s *stubNodeRegistry) CountAlive() int                          { return 1 }
func (s *stubNodeRegistry) GetAll() []any                            { return []any{} }
func (s *stubNodeRegistry) IsLeaving(nodeID uint64) bool             { return s.leaving && nodeID == s.localNodeID }
func (s *stubNodeRegistry) GetLocalNodeID() uint64                   { return s.localNodeID }

// newDrainingHandler builds a CoordinatorHandler with no real coordinators,
// suitable for testing the draining/mutation-rejection path only.
func newDrainingHandler(t *testing.T) *CoordinatorHandler {
	t.Helper()
	clock := hlc.NewClock(1)
	return NewCoordinatorHandler(1, nil, nil, clock, nil, nil, nil, &stubNodeRegistry{localNodeID: 1})
}

// TestCoordinatorHandler_SetDraining verifies the draining flag toggling.
func TestCoordinatorHandler_SetDraining(t *testing.T) {
	t.Parallel()

	h := newDrainingHandler(t)

	if h.IsDraining() {
		t.Fatal("handler should not be draining initially")
	}

	h.SetDraining(true)
	if !h.IsDraining() {
		t.Fatal("handler should be draining after SetDraining(true)")
	}

	h.SetDraining(false)
	if h.IsDraining() {
		t.Fatal("handler should not be draining after SetDraining(false)")
	}
}

// TestCoordinatorHandler_RejectsWriteQueriesWhenDraining verifies that mutation
// queries return ER_SERVER_SHUTDOWN (1053) when the handler is draining.
func TestCoordinatorHandler_RejectsWriteQueriesWhenDraining(t *testing.T) {
	t.Parallel()

	h := newDrainingHandler(t)
	h.SetDraining(true)

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "test",
	}

	mutationQueries := []string{
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET x=1",
		"DELETE FROM t WHERE id=1",
	}

	for _, sql := range mutationQueries {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			rs, err := h.HandleQuery(session, sql, nil)
			if rs != nil {
				t.Errorf("expected nil result set, got non-nil")
			}
			if err == nil {
				t.Fatal("expected error for mutation while draining, got nil")
			}
			mysqlErr, ok := err.(*protocol.MySQLError)
			if !ok {
				t.Fatalf("expected *protocol.MySQLError, got %T: %v", err, err)
			}
			if mysqlErr.Code != protocol.ErrCodeServerShutdown {
				t.Errorf("error code = %d, want %d (ER_SERVER_SHUTDOWN)", mysqlErr.Code, protocol.ErrCodeServerShutdown)
			}
		})
	}
}

// TestCoordinatorHandler_DrainingDoesNotAffectIsDraining verifies that IsDraining
// accurately reflects the flag state after multiple toggles (atomic correctness).
func TestCoordinatorHandler_DrainingDoesNotAffectIsDraining(t *testing.T) {
	t.Parallel()

	h := newDrainingHandler(t)

	for i := 0; i < 100; i++ {
		h.SetDraining(true)
		if !h.IsDraining() {
			t.Fatalf("iteration %d: expected draining=true", i)
		}
		h.SetDraining(false)
		if h.IsDraining() {
			t.Fatalf("iteration %d: expected draining=false", i)
		}
	}
}

// TestCoordinatorHandler_DrainingOnlyRejectsMutations verifies that the draining
// guard only fires for mutation statement types, not reads.
// It exercises this via IsMutation on statement types directly.
func TestCoordinatorHandler_DrainingOnlyRejectsMutations(t *testing.T) {
	t.Parallel()

	// SELECT (StatementSelect) must not be a mutation — the draining guard uses
	// IsMutation as its gate, so this documents and enforces the expected contract.
	selectStmt := protocol.Statement{Type: protocol.StatementSelect}
	if protocol.IsMutation(selectStmt) {
		t.Fatal("SELECT classified as mutation; draining guard would incorrectly block reads")
	}

	// INSERT/UPDATE/DELETE must be mutations.
	for _, st := range []protocol.StatementCode{
		protocol.StatementInsert,
		protocol.StatementUpdate,
		protocol.StatementDelete,
	} {
		if !protocol.IsMutation(protocol.Statement{Type: st}) {
			t.Errorf("statement type %v should be a mutation", st)
		}
	}
}

// TestCoordinatorHandler_HandleLoadData_RejectsWhenDraining verifies that LOAD DATA
// is rejected with ER_SERVER_SHUTDOWN when the handler is draining.
func TestCoordinatorHandler_HandleLoadData_RejectsWhenDraining(t *testing.T) {
	t.Parallel()

	h := newDrainingHandler(t)
	h.SetDraining(true)

	session := &protocol.ConnectionSession{
		ConnID:          1,
		CurrentDatabase: "test",
	}

	rs, err := h.HandleLoadData(session, "LOAD DATA LOCAL INFILE 't.csv' INTO TABLE t", []byte("1,a"))
	if rs != nil {
		t.Errorf("expected nil result set, got non-nil")
	}
	if err == nil {
		t.Fatal("expected error for LOAD DATA while draining, got nil")
	}
	mysqlErr, ok := err.(*protocol.MySQLError)
	if !ok {
		t.Fatalf("expected *protocol.MySQLError, got %T: %v", err, err)
	}
	if mysqlErr.Code != protocol.ErrCodeServerShutdown {
		t.Errorf("error code = %d, want %d (ER_SERVER_SHUTDOWN)", mysqlErr.Code, protocol.ErrCodeServerShutdown)
	}
}
