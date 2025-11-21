package coordinator

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// Mock Reader for testing (old tests)
type mockReaderOld struct {
	responses map[uint64]*ReadResponse
}

func (m *mockReaderOld) ReadSnapshot(ctx context.Context, nodeID uint64, req *ReadRequest) (*ReadResponse, error) {
	if resp, ok := m.responses[nodeID]; ok {
		return resp, nil
	}
	return &ReadResponse{Success: false, Error: "node not found"}, nil
}

// Mock NodeProvider for testing (old tests)
type mockNodeProviderOld struct {
	nodes []uint64
}

func (m *mockNodeProviderOld) GetAliveNodes() ([]uint64, error) {
	return m.nodes, nil
}

func (m *mockNodeProviderOld) GetClusterSize() int {
	return len(m.nodes)
}

func TestReadCoordinator_ConsistencyOne(t *testing.T) {
	clock := hlc.NewClock(1)
	snapshotTS := clock.Now()

	// Setup mock reader with responses from 3 nodes
	reader := &mockReaderOld{
		responses: map[uint64]*ReadResponse{
			1: {Success: true, Rows: []map[string]interface{}{{"id": 1, "name": "Alice"}}, RowCount: 1},
			2: {Success: true, Rows: []map[string]interface{}{{"id": 1, "name": "Alice"}}, RowCount: 1},
			3: {Success: true, Rows: []map[string]interface{}{{"id": 1, "name": "Alice"}}, RowCount: 1},
		},
	}

	nodeProvider := &mockNodeProviderOld{nodes: []uint64{1, 2, 3}}

	rc := NewReadCoordinator(1, nodeProvider, reader, 5*time.Second)

	req := &ReadRequest{
		Query:       "SELECT * FROM users WHERE id = ?",
		Args:        []interface{}{1},
		SnapshotTS:  snapshotTS,
		Consistency: protocol.ConsistencyOne,
		TableName:   "users",
	}

	resp, err := rc.ReadTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Read not successful")
	}

	if resp.RowCount != 1 {
		t.Fatalf("Expected 1 row, got %d", resp.RowCount)
	}

	t.Logf("✓ CONSISTENCY_ONE read successful: %d rows", resp.RowCount)
}

func TestReadCoordinator_ConsistencyQuorum(t *testing.T) {
	clock := hlc.NewClock(1)
	snapshotTS := clock.Now()

	// Setup mock reader - 2 out of 3 nodes respond
	reader := &mockReaderOld{
		responses: map[uint64]*ReadResponse{
			1: {Success: true, Rows: []map[string]interface{}{{"id": 1, "name": "Alice"}}, RowCount: 1},
			2: {Success: true, Rows: []map[string]interface{}{{"id": 1, "name": "Alice"}}, RowCount: 1},
			// Node 3 doesn't respond
		},
	}

	nodeProvider := &mockNodeProviderOld{nodes: []uint64{1, 2, 3}}

	rc := NewReadCoordinator(1, nodeProvider, reader, 5*time.Second)

	req := &ReadRequest{
		Query:       "SELECT * FROM users WHERE id = ?",
		Args:        []interface{}{1},
		SnapshotTS:  snapshotTS,
		Consistency: protocol.ConsistencyQuorum,
		TableName:   "users",
	}

	resp, err := rc.ReadTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Read not successful")
	}

	t.Logf("✓ CONSISTENCY_QUORUM read successful with 2/3 nodes")
}

func TestReadCoordinator_QuorumNotAchieved(t *testing.T) {
	clock := hlc.NewClock(1)
	snapshotTS := clock.Now()

	// Setup mock reader - only 1 out of 3 nodes responds (quorum = 2)
	reader := &mockReaderOld{
		responses: map[uint64]*ReadResponse{
			1: {Success: true, Rows: []map[string]interface{}{{"id": 1, "name": "Alice"}}, RowCount: 1},
			// Nodes 2 and 3 don't respond
		},
	}

	nodeProvider := &mockNodeProviderOld{nodes: []uint64{1, 2, 3}}

	rc := NewReadCoordinator(1, nodeProvider, reader, 100*time.Millisecond)

	req := &ReadRequest{
		Query:       "SELECT * FROM users WHERE id = ?",
		Args:        []interface{}{1},
		SnapshotTS:  snapshotTS,
		Consistency: protocol.ConsistencyQuorum,
		TableName:   "users",
	}

	_, err := rc.ReadTransaction(context.Background(), req)
	if err == nil {
		t.Fatal("Expected quorum failure, got success")
	}

	t.Logf("✓ Quorum not achieved (1/3 nodes): %v", err)
}

func TestLocalSnapshotRead(t *testing.T) {
	// Create test database
	dbPath := "/tmp/test_snapshot_read.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			email TEXT,
			balance INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (id, name, email, balance) VALUES (?, ?, ?, ?)`,
		1, "Alice", "alice@example.com", 100)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	clock := hlc.NewClock(1)
	snapshotTS := clock.Now()

	// Execute snapshot read
	resp, err := LocalSnapshotRead(db, snapshotTS, "users",
		"SELECT id, name, email, balance FROM users WHERE id = ?",
		[]interface{}{1})

	if err != nil {
		t.Fatalf("Snapshot read failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Snapshot read not successful: %s", resp.Error)
	}

	if resp.RowCount != 1 {
		t.Fatalf("Expected 1 row, got %d", resp.RowCount)
	}

	row := resp.Rows[0]
	if row["name"] != "Alice" {
		t.Fatalf("Expected name=Alice, got %v", row["name"])
	}

	t.Logf("✓ Local snapshot read successful: %+v", row)
}

func TestLocalSnapshotRead_NoRows(t *testing.T) {
	// Create test database
	dbPath := "/tmp/test_snapshot_read_empty.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table (empty)
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			email TEXT,
			balance INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	clock := hlc.NewClock(1)
	snapshotTS := clock.Now()

	// Execute snapshot read on empty table
	resp, err := LocalSnapshotRead(db, snapshotTS, "users",
		"SELECT id, name, email, balance FROM users WHERE id = ?",
		[]interface{}{999})

	if err != nil {
		t.Fatalf("Snapshot read failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Snapshot read not successful: %s", resp.Error)
	}

	if resp.RowCount != 0 {
		t.Fatalf("Expected 0 rows, got %d", resp.RowCount)
	}

	t.Logf("✓ Local snapshot read returns empty result correctly")
}
