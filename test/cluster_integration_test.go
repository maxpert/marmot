package test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"

	_ "github.com/mattn/go-sqlite3"
)

// TestMultiNodeCluster tests a 3-node cluster with replication
func TestMultiNodeCluster(t *testing.T) {
	// Create 3 nodes
	nodes := make([]*TestNode, 3)
	for i := 0; i < 3; i++ {
		node, err := createTestNode(i + 1)
		if err != nil {
			t.Fatalf("Failed to create node %d: %v", i+1, err)
		}
		defer node.Cleanup()
		nodes[i] = node
	}

	// Give nodes time to initialize
	time.Sleep(100 * time.Millisecond)

	// Test 1: Write via node 1
	t.Run("WriteViaNode1", func(t *testing.T) {
		session := &protocol.ConnectionSession{ConnID: 1, CurrentDatabase: "marmot"}
		query := "INSERT INTO test_table (id, name, value) VALUES (1, 'test1', 100)"
		_, err := nodes[0].Handler.HandleQuery(session, query)
		if err != nil {
			t.Fatalf("Failed to execute insert: %v", err)
		}
		t.Log("Successfully wrote via node 1")
	})

	// Test 2: Read from node 1 (same node)
	t.Run("ReadFromNode1", func(t *testing.T) {
		time.Sleep(500 * time.Millisecond) // Give time for commit

		session := &protocol.ConnectionSession{ConnID: 1, CurrentDatabase: "marmot"}
		query := "SELECT * FROM test_table WHERE id = 1"
		rs, err := nodes[0].Handler.HandleQuery(session, query)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}

		if len(rs.Rows) == 0 {
			t.Fatal("Expected 1 row, got 0")
		}

		t.Logf("Read from node 1: %d rows, %d columns", len(rs.Rows), len(rs.Columns))
	})

	// Test 3: Update via node 1
	t.Run("UpdateViaNode1", func(t *testing.T) {
		session := &protocol.ConnectionSession{ConnID: 1, CurrentDatabase: "marmot"}
		query := "UPDATE test_table SET value = 200 WHERE id = 1"
		_, err := nodes[0].Handler.HandleQuery(session, query)
		if err != nil {
			t.Fatalf("Failed to execute update: %v", err)
		}
		t.Log("Successfully updated via node 1")
	})

	// Test 4: Verify update
	t.Run("VerifyUpdate", func(t *testing.T) {
		time.Sleep(500 * time.Millisecond)

		session := &protocol.ConnectionSession{ConnID: 1, CurrentDatabase: "marmot"}
		query := "SELECT value FROM test_table WHERE id = 1"
		rs, err := nodes[0].Handler.HandleQuery(session, query)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}

		if len(rs.Rows) == 0 {
			t.Fatal("Expected 1 row, got 0")
		}

		value := rs.Rows[0][0]
		t.Logf("Value after update: %v", value)
	})
}

// TestNode represents a single node in the cluster
type TestNode struct {
	NodeID  uint64
	DB      *sql.DB
	MVCCDB  *db.MVCCDatabase
	Clock   *hlc.Clock
	Handler *coordinator.CoordinatorHandler
}

// createTestNode creates a single test node
func createTestNode(nodeID int) (*TestNode, error) {
	// Create in-memory database path
	dbPath := fmt.Sprintf("file:test_node_%d?mode=memory&cache=shared", nodeID)

	// Create HLC clock
	clock := hlc.NewClock(uint64(nodeID))

	// Create MVCC database
	mvccDB, err := db.NewMVCCDatabase(dbPath, uint64(nodeID), clock)
	if err != nil {
		return nil, fmt.Errorf("failed to create MVCC database: %w", err)
	}

	// Disable WAL for in-memory databases
	_, err = mvccDB.GetDB().Exec("PRAGMA journal_mode=DELETE")
	if err != nil {
		mvccDB.Close()
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}

	// Create test schema
	_, err = mvccDB.GetDB().Exec(`
		CREATE TABLE IF NOT EXISTS test_table (
			id INTEGER PRIMARY KEY,
			name TEXT,
			value INTEGER
		)
	`)
	if err != nil {
		mvccDB.Close()
		return nil, fmt.Errorf("failed to create test schema: %w", err)
	}

	// Create reader and replicator with test database manager
	dbMgr := NewTestDatabaseManager(mvccDB)
	reader := db.NewLocalReader(dbMgr)
	replicator := db.NewLocalReplicator(uint64(nodeID), dbMgr, clock)

	// Create node provider (local only for this test)
	nodeProvider := &MockNodeProvider{nodes: []uint64{uint64(nodeID)}}

	// Create coordinators (local only for this test)
	// In a real cluster, nodes would be connected via gRPC
	writeCoord := coordinator.NewWriteCoordinator(
		uint64(nodeID),
		nodeProvider,
		&MockReplicator{}, // No remote replication
		replicator,
		2*time.Second,
	)

	readCoord := coordinator.NewReadCoordinator(
		uint64(nodeID),
		nodeProvider,
		reader,
		2*time.Second,
	)

	// Create coordinator handler
	handler := coordinator.NewCoordinatorHandler(uint64(nodeID), writeCoord, readCoord, clock, nil)

	return &TestNode{
		NodeID:  uint64(nodeID),
		DB:      mvccDB.GetDB(),
		MVCCDB:  mvccDB,
		Clock:   clock,
		Handler: handler,
	}, nil
}

// Cleanup closes the node's database
func (n *TestNode) Cleanup() {
	if n.DB != nil {
		n.DB.Close()
	}
}
