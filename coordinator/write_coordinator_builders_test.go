package coordinator

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// createTestCoordinator creates a WriteCoordinator for testing builder methods
func createTestCoordinator(nodeID uint64) *WriteCoordinator {
	nodeProvider := newMockNodeProvider([]uint64{nodeID})
	replicator := newMockReplicator()
	clock := hlc.NewClock(nodeID)

	return NewWriteCoordinator(nodeID, nodeProvider, replicator, replicator, 5*time.Second, clock)
}

// Method 1: validateStatements

func TestValidateStatements_ValidCDC(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	err := wc.validateStatements(txn)
	if err != nil {
		t.Errorf("expected no error for valid CDC statement, got: %v", err)
	}
}

func TestValidateStatements_EmptyStatements(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().
		WithStatements([]protocol.Statement{}).
		Build()

	err := wc.validateStatements(txn)
	if err != nil {
		t.Errorf("expected no error for empty statements, got: %v", err)
	}
}

func TestValidateStatements_NilStatements(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().
		WithStatements(nil).
		Build()

	err := wc.validateStatements(txn)
	if err != nil {
		t.Errorf("expected no error for nil statements, got: %v", err)
	}
}

func TestValidateStatements_CDCMissingTableName(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().Build()
	txn.Statements = []protocol.Statement{
		{
			TableName: "",
			OldValues: map[string][]byte{"id": {1}},
			NewValues: map[string][]byte{"id": {2}},
		},
	}

	err := wc.validateStatements(txn)
	if err == nil {
		t.Error("expected error for CDC statement missing TableName, got nil")
	}
}

func TestValidateStatements_CDCNilOldValues(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().Build()
	txn.Statements = []protocol.Statement{
		{
			TableName: "",
			OldValues: nil,
			NewValues: map[string][]byte{"id": {2}},
		},
	}

	err := wc.validateStatements(txn)
	if err == nil {
		t.Error("expected error for CDC statement with nil OldValues and no TableName, got nil")
	}
}

func TestValidateStatements_CDCNilNewValues(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().Build()
	txn.Statements = []protocol.Statement{
		{
			TableName: "",
			OldValues: map[string][]byte{"id": {1}},
			NewValues: nil,
		},
	}

	err := wc.validateStatements(txn)
	if err == nil {
		t.Error("expected error for CDC statement with nil NewValues and no TableName, got nil")
	}
}

func TestValidateStatements_MixedCDCAndSQL(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().Build()
	txn.Statements = []protocol.Statement{
		{
			TableName: "users",
			OldValues: map[string][]byte{"id": {1}},
			NewValues: map[string][]byte{"id": {2}},
		},
		{
			SQL: "CREATE TABLE test (id INT)",
		},
	}

	err := wc.validateStatements(txn)
	if err != nil {
		t.Errorf("expected no error for mixed CDC and DDL statements, got: %v", err)
	}
}

func TestValidateStatements_LargeStatementCount(t *testing.T) {
	wc := createTestCoordinator(1)
	stmts := CreateCDCStatements(10000, "users")
	txn := NewTxnBuilder().
		WithStatements(stmts).
		Build()

	err := wc.validateStatements(txn)
	if err != nil {
		t.Errorf("expected no error for 10,000 statements, got: %v", err)
	}
}

func TestValidateStatements_DDLStatements(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().
		WithDDLStatement("CREATE TABLE test (id INT)").
		WithDDLStatement("ALTER TABLE test ADD COLUMN name TEXT").
		WithDDLStatement("DROP TABLE old_table").
		Build()

	err := wc.validateStatements(txn)
	if err != nil {
		t.Errorf("expected no error for DDL statements, got: %v", err)
	}
}

// Method 2: buildPrepareRequest

func TestBuildPrepareRequest_Basic(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().
		WithID(123).
		WithNodeID(1).
		WithDatabase("testdb").
		WithCDCStatement("users", map[string][]byte{"id": {1}}, map[string][]byte{"id": {2}}).
		Build()

	req := wc.buildPrepareRequest(txn)

	AssertPrepareRequest(t, req, expectedPrepareReq{
		TxnID:                 123,
		NodeID:                1,
		Phase:                 PhasePrep,
		Database:              "testdb",
		RequiredSchemaVersion: 0,
		StatementCount:        1,
	})

	if req.StartTS != txn.StartTS {
		t.Errorf("StartTS: got %+v, want %+v", req.StartTS, txn.StartTS)
	}
}

func TestBuildPrepareRequest_SchemaVersionPropagation(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().
		WithID(456).
		WithRequiredSchemaVersion(42).
		Build()

	req := wc.buildPrepareRequest(txn)

	AssertPrepareRequest(t, req, expectedPrepareReq{
		TxnID:                 456,
		NodeID:                1,
		Phase:                 PhasePrep,
		Database:              "test",
		RequiredSchemaVersion: 42,
		StatementCount:        0,
	})
}

func TestBuildPrepareRequest_EmptyDatabase(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().
		WithDatabase("").
		Build()

	req := wc.buildPrepareRequest(txn)

	if req.Database != "" {
		t.Errorf("Database: got %s, want empty string", req.Database)
	}
}

func TestBuildPrepareRequest_LargeStatementArray(t *testing.T) {
	wc := createTestCoordinator(1)
	stmts := CreateCDCStatements(1000, "users")
	txn := NewTxnBuilder().
		WithStatements(stmts).
		Build()

	req := wc.buildPrepareRequest(txn)

	if len(req.Statements) != 1000 {
		t.Errorf("expected 1000 statements, got %d", len(req.Statements))
	}

	for i := 0; i < 1000; i++ {
		if req.Statements[i].TableName != "users" {
			t.Errorf("statement %d: TableName got %s, want users", i, req.Statements[i].TableName)
		}
	}
}

func TestBuildPrepareRequest_PhaseIsAlwaysPrep(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().Build()

	req := wc.buildPrepareRequest(txn)

	if req.Phase != PhasePrep {
		t.Errorf("Phase: got %v, want %v", req.Phase, PhasePrep)
	}
}

func TestBuildPrepareRequest_StartTSCopied(t *testing.T) {
	wc := createTestCoordinator(1)
	customTS := hlc.Timestamp{WallTime: 5000, Logical: 3, NodeID: 2}
	txn := NewTxnBuilder().
		WithStartTS(customTS).
		Build()

	req := wc.buildPrepareRequest(txn)

	if req.StartTS.WallTime != customTS.WallTime ||
		req.StartTS.Logical != customTS.Logical ||
		req.StartTS.NodeID != customTS.NodeID {
		t.Errorf("StartTS: got %+v, want %+v", req.StartTS, customTS)
	}
}

// Method 3: buildCommitRequest

func TestBuildCommitRequest_Basic(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().
		WithID(789).
		WithNodeID(1).
		WithDatabase("testdb").
		Build()

	req := wc.buildCommitRequest(txn)

	AssertCommitRequest(t, req, expectedCommitReq{
		TxnID:          789,
		Phase:          PhaseCommit,
		Database:       "testdb",
		StatementCount: 0,
	})

	if req.NodeID != 1 {
		t.Errorf("NodeID: got %d, want 1", req.NodeID)
	}
}

func TestBuildCommitRequest_PhaseIsAlwaysCommit(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().Build()

	req := wc.buildCommitRequest(txn)

	if req.Phase != PhaseCommit {
		t.Errorf("Phase: got %v, want %v", req.Phase, PhaseCommit)
	}
}

func TestBuildCommitRequest_StatementsExcluded(t *testing.T) {
	wc := createTestCoordinator(1)
	stmts := CreateCDCStatements(5, "users")
	txn := NewTxnBuilder().
		WithStatements(stmts).
		Build()

	req := wc.buildCommitRequest(txn)

	// Statements should be nil or empty in commit request (payload reduction optimization)
	if len(req.Statements) != 0 {
		t.Errorf("expected 0 statements in commit request, got %d", len(req.Statements))
	}
}

func TestBuildCommitRequest_EmptyDatabase(t *testing.T) {
	wc := createTestCoordinator(1)
	txn := NewTxnBuilder().
		WithDatabase("").
		Build()

	req := wc.buildCommitRequest(txn)

	if req.Database != "" {
		t.Errorf("Database: got %s, want empty string", req.Database)
	}
}
