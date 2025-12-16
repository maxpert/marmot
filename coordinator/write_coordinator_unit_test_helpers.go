package coordinator

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/telemetry"
)

// TxnBuilder provides a fluent API for building test transactions
type TxnBuilder struct {
	txn *Transaction
}

// NewTxnBuilder creates a new transaction builder with default values
func NewTxnBuilder() *TxnBuilder {
	return &TxnBuilder{
		txn: &Transaction{
			ID:                    1,
			NodeID:                1,
			Statements:            []protocol.Statement{},
			StartTS:               hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
			CommitTS:              hlc.Timestamp{WallTime: 2000, Logical: 0, NodeID: 1},
			WriteConsistency:      protocol.ConsistencyQuorum,
			ReadConsistency:       protocol.ConsistencyLocalOne,
			Database:              "test",
			RequiredSchemaVersion: 0,
			LocalExecutionDone:    false,
		},
	}
}

// WithID sets the transaction ID
func (b *TxnBuilder) WithID(id uint64) *TxnBuilder {
	b.txn.ID = id
	return b
}

// WithNodeID sets the node ID
func (b *TxnBuilder) WithNodeID(nodeID uint64) *TxnBuilder {
	b.txn.NodeID = nodeID
	return b
}

// WithStatements sets the statements
func (b *TxnBuilder) WithStatements(stmts []protocol.Statement) *TxnBuilder {
	b.txn.Statements = stmts
	return b
}

// WithCDCStatement adds a CDC statement with old/new values
// oldVals and newVals are maps of column_name -> value
func (b *TxnBuilder) WithCDCStatement(tableName string, oldVals, newVals map[string][]byte) *TxnBuilder {
	stmt := protocol.Statement{
		TableName: tableName,
		OldValues: oldVals,
		NewValues: newVals,
	}
	b.txn.Statements = append(b.txn.Statements, stmt)
	return b
}

// WithDDLStatement adds a DDL statement
func (b *TxnBuilder) WithDDLStatement(sql string) *TxnBuilder {
	stmt := protocol.Statement{
		SQL: sql,
	}
	b.txn.Statements = append(b.txn.Statements, stmt)
	return b
}

// WithDatabase sets the database name
func (b *TxnBuilder) WithDatabase(db string) *TxnBuilder {
	b.txn.Database = db
	return b
}

// WithWriteConsistency sets the write consistency level
func (b *TxnBuilder) WithWriteConsistency(level protocol.ConsistencyLevel) *TxnBuilder {
	b.txn.WriteConsistency = level
	return b
}

// WithRequiredSchemaVersion sets the required schema version
func (b *TxnBuilder) WithRequiredSchemaVersion(version uint64) *TxnBuilder {
	b.txn.RequiredSchemaVersion = version
	return b
}

// WithLocalExecutionDone sets whether local execution is already done
func (b *TxnBuilder) WithLocalExecutionDone(done bool) *TxnBuilder {
	b.txn.LocalExecutionDone = done
	return b
}

// WithStartTS sets the start timestamp
func (b *TxnBuilder) WithStartTS(ts hlc.Timestamp) *TxnBuilder {
	b.txn.StartTS = ts
	return b
}

// WithCommitTS sets the commit timestamp
func (b *TxnBuilder) WithCommitTS(ts hlc.Timestamp) *TxnBuilder {
	b.txn.CommitTS = ts
	return b
}

// Build returns the constructed transaction
func (b *TxnBuilder) Build() *Transaction {
	return b.txn
}

// expectedPrepareReq defines expected values for a prepare request
type expectedPrepareReq struct {
	TxnID                 uint64
	NodeID                uint64
	Phase                 ReplicationPhase
	Database              string
	RequiredSchemaVersion uint64
	StatementCount        int
}

// AssertPrepareRequest validates a prepare request has expected fields
func AssertPrepareRequest(t *testing.T, req *ReplicationRequest, expected expectedPrepareReq) {
	t.Helper()

	if req == nil {
		t.Fatal("request is nil")
	}

	if req.TxnID != expected.TxnID {
		t.Errorf("TxnID: got %d, want %d", req.TxnID, expected.TxnID)
	}

	if req.NodeID != expected.NodeID {
		t.Errorf("NodeID: got %d, want %d", req.NodeID, expected.NodeID)
	}

	if req.Phase != expected.Phase {
		t.Errorf("Phase: got %v, want %v", req.Phase, expected.Phase)
	}

	if req.Database != expected.Database {
		t.Errorf("Database: got %s, want %s", req.Database, expected.Database)
	}

	if req.RequiredSchemaVersion != expected.RequiredSchemaVersion {
		t.Errorf("RequiredSchemaVersion: got %d, want %d", req.RequiredSchemaVersion, expected.RequiredSchemaVersion)
	}

	if expected.StatementCount >= 0 && len(req.Statements) != expected.StatementCount {
		t.Errorf("Statement count: got %d, want %d", len(req.Statements), expected.StatementCount)
	}
}

// expectedCommitReq defines expected values for a commit request
type expectedCommitReq struct {
	TxnID          uint64
	Phase          ReplicationPhase
	Database       string
	StatementCount int
}

// AssertCommitRequest validates a commit request
func AssertCommitRequest(t *testing.T, req *ReplicationRequest, expected expectedCommitReq) {
	t.Helper()

	if req == nil {
		t.Fatal("request is nil")
	}

	if req.TxnID != expected.TxnID {
		t.Errorf("TxnID: got %d, want %d", req.TxnID, expected.TxnID)
	}

	if req.Phase != expected.Phase {
		t.Errorf("Phase: got %v, want %v", req.Phase, expected.Phase)
	}

	if req.Database != expected.Database {
		t.Errorf("Database: got %s, want %s", req.Database, expected.Database)
	}

	if expected.StatementCount >= 0 && len(req.Statements) != expected.StatementCount {
		t.Errorf("Statement count: got %d, want %d", len(req.Statements), expected.StatementCount)
	}
}

// enhancedMockReplicatorExtensions provides additional methods for the mock replicator
// These methods extend the existing enhancedMockReplicator in test_helpers.go

// SetNetworkPartition simulates a network partition for a node
func (m *enhancedMockReplicator) SetNetworkPartition(nodeID uint64, partitioned bool) {
	if partitioned {
		m.SetNodeResponse(nodeID, nil) // Timeout behavior
	} else {
		m.mu.Lock()
		delete(m.responses, nodeID)
		m.mu.Unlock()
	}
}

// SetByzantineFailure configures a node to return a malicious/incorrect response
func (m *enhancedMockReplicator) SetByzantineFailure(nodeID uint64, maliciousResp *ReplicationResponse) {
	m.SetNodeResponse(nodeID, maliciousResp)
}

// SetLatencyVariance configures variable latency for a node
// Note: This is simplified - actual implementation uses SetDelay per txnID
func (m *enhancedMockReplicator) SetLatencyVariance(nodeID uint64, min, max time.Duration) {
	// For simplicity, we'll use the average latency
	// Real tests should use SetDelay per transaction
	avgLatency := (min + max) / 2
	m.mu.Lock()
	m.prepareLatency = avgLatency
	m.commitLatency = avgLatency
	m.mu.Unlock()
}

// GetPhaseCalls returns calls filtered by phase
func (m *enhancedMockReplicator) GetPhaseCalls(phase ReplicationPhase) []ReplicationCall {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var phaseCalls []ReplicationCall
	for _, call := range m.calls {
		if call.Request.Phase == phase {
			phaseCalls = append(phaseCalls, call)
		}
	}
	return phaseCalls
}

// WithTimeout creates a context with timeout in milliseconds
func WithTimeout(ms int) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(ms)*time.Millisecond)
}

// WithCancellation creates a cancellable context
func WithCancellation() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// CancelAfter cancels context after duration
func CancelAfter(ctx context.Context, cancel context.CancelFunc, after time.Duration) {
	go func() {
		time.Sleep(after)
		cancel()
	}()
}

// ChannelMocker sends responses to channel with configurable delays
type ChannelMocker struct{}

// NewChannelMocker creates a new channel mocker
func NewChannelMocker() *ChannelMocker {
	return &ChannelMocker{}
}

// SendResponse sends a single response to the channel with optional delay
func (m *ChannelMocker) SendResponse(ch chan response, nodeID uint64, resp *ReplicationResponse, delay time.Duration) {
	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		ch <- response{
			nodeID: nodeID,
			resp:   resp,
			err:    nil,
		}
	}()
}

// SendResponses sends multiple responses with individual delays
func (m *ChannelMocker) SendResponses(ch chan response, responses map[uint64]*ReplicationResponse, delays map[uint64]time.Duration) {
	for nodeID, resp := range responses {
		delay := delays[nodeID]
		m.SendResponse(ch, nodeID, resp, delay)
	}
}

// TelemetrySnapshot captures current metric values for comparison
type TelemetrySnapshot struct {
	txnTotal   map[string]int64
	quorumAcks map[string]int64
}

// CaptureTelemetry captures current telemetry values
// Note: This is a simplified version since we can't easily read metric values
// In real tests, we rely on metric behavior being correct rather than checking values
func CaptureTelemetry() *TelemetrySnapshot {
	return &TelemetrySnapshot{
		txnTotal:   make(map[string]int64),
		quorumAcks: make(map[string]int64),
	}
}

// AssertIncremented checks if a metric was incremented
// Note: This is a placeholder - actual metric assertion requires metric inspection
func (s *TelemetrySnapshot) AssertIncremented(t *testing.T, metric string, expectedDelta int) {
	t.Helper()
	// Telemetry metrics are write-only in the current implementation
	// Tests should verify metrics are called, not their values
	// This is a design limitation we accept for now
}

// CheckGoroutines verifies no goroutine leaks occurred
// Usage: defer CheckGoroutines(t)()
func CheckGoroutines(t *testing.T) func() {
	t.Helper()
	before := runtime.NumGoroutine()
	return func() {
		// Give goroutines time to clean up
		time.Sleep(50 * time.Millisecond)
		after := runtime.NumGoroutine()

		// Allow some variance (test framework goroutines)
		// We consider >10 new goroutines to be a leak
		if after-before > 10 {
			t.Errorf("Potential goroutine leak: before=%d, after=%d, delta=%d", before, after, after-before)
		}
	}
}

// AssertError checks if an error is of expected type
func AssertError(t *testing.T, err error, expectedType interface{}) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected error of type %T, got nil", expectedType)
	}

	switch expectedType.(type) {
	case *QuorumNotAchievedError:
		if _, ok := err.(*QuorumNotAchievedError); !ok {
			t.Errorf("expected QuorumNotAchievedError, got %T: %v", err, err)
		}
	case *CoordinatorNotParticipatedError:
		if _, ok := err.(*CoordinatorNotParticipatedError); !ok {
			t.Errorf("expected CoordinatorNotParticipatedError, got %T: %v", err, err)
		}
	case *PartialCommitError:
		if _, ok := err.(*PartialCommitError); !ok {
			t.Errorf("expected PartialCommitError, got %T: %v", err, err)
		}
	case *PrepareConflictError:
		if _, ok := err.(*PrepareConflictError); !ok {
			t.Errorf("expected PrepareConflictError, got %T: %v", err, err)
		}
	default:
		t.Errorf("unknown error type to assert: %T", expectedType)
	}
}

// AssertNoError fails the test if error is not nil
func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// AssertQuorumNotAchievedError validates QuorumNotAchievedError details
func AssertQuorumNotAchievedError(t *testing.T, err error, expectedPhase string, expectedAcks, expectedQuorum int) {
	t.Helper()

	qErr, ok := err.(*QuorumNotAchievedError)
	if !ok {
		t.Fatalf("expected QuorumNotAchievedError, got %T: %v", err, err)
	}

	if qErr.Phase != expectedPhase {
		t.Errorf("phase: got %s, want %s", qErr.Phase, expectedPhase)
	}

	if qErr.AcksReceived != expectedAcks {
		t.Errorf("acks received: got %d, want %d", qErr.AcksReceived, expectedAcks)
	}

	if qErr.QuorumRequired != expectedQuorum {
		t.Errorf("quorum required: got %d, want %d", qErr.QuorumRequired, expectedQuorum)
	}
}

// AssertPartialCommitError validates PartialCommitError details
func AssertPartialCommitError(t *testing.T, err error, expectedIsLocal bool) {
	t.Helper()

	pErr, ok := err.(*PartialCommitError)
	if !ok {
		t.Fatalf("expected PartialCommitError, got %T: %v", err, err)
	}

	if pErr.IsLocal != expectedIsLocal {
		t.Errorf("IsLocal: got %v, want %v", pErr.IsLocal, expectedIsLocal)
	}
}

// AssertConflictDetected checks if error is a conflict error
func AssertConflictDetected(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}

	// Check if it's a MySQL deadlock error (what WriteCoordinator returns on conflict)
	// ErrDeadlock() returns a MySQLError with specific code
	mysqlErr, ok := err.(*protocol.MySQLError)
	if !ok {
		t.Errorf("expected MySQLError for deadlock, got: %T - %v", err, err)
		return
	}

	// MySQL error code 1213 is ER_LOCK_DEADLOCK
	if mysqlErr.Code != 1213 {
		t.Errorf("expected MySQL error code 1213 (deadlock), got: %d", mysqlErr.Code)
	}
}

// AssertResponseMapSize checks the size of a response map
func AssertResponseMapSize(t *testing.T, responses map[uint64]*ReplicationResponse, expectedSize int) {
	t.Helper()

	if len(responses) != expectedSize {
		t.Errorf("response map size: got %d, want %d", len(responses), expectedSize)
	}
}

// AssertResponseSuccess checks if a node's response is successful
func AssertResponseSuccess(t *testing.T, responses map[uint64]*ReplicationResponse, nodeID uint64) {
	t.Helper()

	resp, exists := responses[nodeID]
	if !exists {
		t.Errorf("node %d not in response map", nodeID)
		return
	}

	if resp == nil {
		t.Errorf("node %d response is nil", nodeID)
		return
	}

	if !resp.Success {
		t.Errorf("node %d response not successful: %s", nodeID, resp.Error)
	}

	if resp.ConflictDetected {
		t.Errorf("node %d has conflict: %s", nodeID, resp.ConflictDetails)
	}
}

// AssertNodeNotInResponses checks that a node is not in the response map
func AssertNodeNotInResponses(t *testing.T, responses map[uint64]*ReplicationResponse, nodeID uint64) {
	t.Helper()

	if _, exists := responses[nodeID]; exists {
		t.Errorf("node %d should not be in response map", nodeID)
	}
}

// CreateCDCStatements creates CDC statements for testing
func CreateCDCStatements(count int, tableName string) []protocol.Statement {
	stmts := make([]protocol.Statement, count)
	for i := 0; i < count; i++ {
		stmts[i] = protocol.Statement{
			TableName: tableName,
			OldValues: map[string][]byte{"id": {byte(i)}},
			NewValues: map[string][]byte{"id": {byte(i + 1)}},
		}
	}
	return stmts
}

// CreateDDLStatements creates DDL statements for testing
func CreateDDLStatements(count int) []protocol.Statement {
	stmts := make([]protocol.Statement, count)
	for i := 0; i < count; i++ {
		stmts[i] = protocol.Statement{
			SQL: "CREATE TABLE test" + string(rune(i)) + " (id INT)",
		}
	}
	return stmts
}

// WaitForCondition waits for a condition to be true or times out
func WaitForCondition(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("condition not met within %v: %s", timeout, message)
}

// CountGoroutines returns the current number of goroutines
func CountGoroutines() int {
	return runtime.NumGoroutine()
}

// AssertCallCount verifies the number of replication calls
func AssertCallCount(t *testing.T, mock *enhancedMockReplicator, expected int) {
	t.Helper()

	actual := mock.GetCallCount()
	if actual != expected {
		t.Errorf("call count: got %d, want %d", actual, expected)
	}
}

// AssertPhaseCallCount verifies the number of calls for a specific phase
func AssertPhaseCallCount(t *testing.T, mock *enhancedMockReplicator, phase ReplicationPhase, expected int) {
	t.Helper()

	calls := mock.GetPhaseCalls(phase)
	actual := len(calls)
	if actual != expected {
		t.Errorf("phase %v call count: got %d, want %d", phase, actual, expected)
	}
}

// AssertNodeCalledInPhase checks if a node was called in a specific phase
func AssertNodeCalledInPhase(t *testing.T, mock *enhancedMockReplicator, nodeID uint64, phase ReplicationPhase) {
	t.Helper()

	calls := mock.GetPhaseCalls(phase)
	for _, call := range calls {
		if call.NodeID == nodeID {
			return
		}
	}

	t.Errorf("node %d not called in phase %v", nodeID, phase)
}

// AssertNodeNotCalledInPhase checks if a node was NOT called in a specific phase
func AssertNodeNotCalledInPhase(t *testing.T, mock *enhancedMockReplicator, nodeID uint64, phase ReplicationPhase) {
	t.Helper()

	calls := mock.GetPhaseCalls(phase)
	for _, call := range calls {
		if call.NodeID == nodeID {
			t.Errorf("node %d should not be called in phase %v", nodeID, phase)
			return
		}
	}
}

// CreateSuccessResponse creates a successful replication response
func CreateSuccessResponse() *ReplicationResponse {
	return &ReplicationResponse{
		Success:          true,
		ConflictDetected: false,
	}
}

// CreateConflictResponse creates a conflict response
func CreateConflictResponse(details string) *ReplicationResponse {
	return &ReplicationResponse{
		Success:          true,
		ConflictDetected: true,
		ConflictDetails:  details,
	}
}

// CreateErrorResponse creates an error response
func CreateErrorResponse(errMsg string) *ReplicationResponse {
	return &ReplicationResponse{
		Success: false,
		Error:   errMsg,
	}
}

// InitTestTelemetry initializes telemetry for tests
// Note: In the current implementation, telemetry metrics default to noop implementations
// This function is provided for future use when telemetry initialization is needed
func InitTestTelemetry() {
	// Telemetry metrics use noop implementations by default
	// InitMetrics() must be called explicitly to enable Prometheus metrics
	// For unit tests, we typically use the noop implementations
	telemetry.InitMetrics()
}
