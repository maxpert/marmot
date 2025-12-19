package grpc

import (
	"os"
	"testing"

	"github.com/maxpert/marmot/hlc"
)

// TestCheckPromotionCriteria_NoAliveNodes verifies promotion succeeds when no ALIVE nodes exist (seed case)
func TestCheckPromotionCriteria_NoAliveNodes(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_no_alive")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	// Create a test database so we have at least one DB
	err := dbMgr.CreateDatabase("test_db")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	err = schemaVersionMgr.SetSchemaVersion("test_db", 1, "CREATE TABLE test (id INT)", 1)
	if err != nil {
		t.Fatalf("Failed to set schema version: %v", err)
	}

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	// No ALIVE nodes exist - should return true (we're the seed)
	result := server.checkPromotionCriteria()
	if !result {
		t.Error("Expected promotion to succeed when no ALIVE nodes exist (seed node case)")
	}
}

// TestCheckPromotionCriteria_SchemaMatches verifies promotion succeeds when schema versions match
func TestCheckPromotionCriteria_SchemaMatches(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_match")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	err := dbMgr.CreateDatabase("test_db")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	err = schemaVersionMgr.SetSchemaVersion("test_db", 5, "CREATE TABLE test (id INT)", 1)
	if err != nil {
		t.Fatalf("Failed to set schema version: %v", err)
	}

	// Add an ALIVE peer with same schema version
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"test_db": 5,
		},
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	result := server.checkPromotionCriteria()
	if !result {
		t.Error("Expected promotion to succeed when schema versions match peer")
	}
}

// TestCheckPromotionCriteria_SchemaAhead verifies promotion succeeds when local schema is ahead
func TestCheckPromotionCriteria_SchemaAhead(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_ahead")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	err := dbMgr.CreateDatabase("test_db")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	err = schemaVersionMgr.SetSchemaVersion("test_db", 5, "CREATE TABLE test (id INT)", 1)
	if err != nil {
		t.Fatalf("Failed to set schema version: %v", err)
	}

	// Add an ALIVE peer with lower schema version
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"test_db": 3,
		},
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	result := server.checkPromotionCriteria()
	if !result {
		t.Error("Expected promotion to succeed when local schema is ahead of peer")
	}
}

// TestCheckPromotionCriteria_SchemaBehind verifies promotion fails when local schema is behind
func TestCheckPromotionCriteria_SchemaBehind(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_behind")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	err := dbMgr.CreateDatabase("test_db")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	err = schemaVersionMgr.SetSchemaVersion("test_db", 5, "CREATE TABLE test (id INT)", 1)
	if err != nil {
		t.Fatalf("Failed to set schema version: %v", err)
	}

	// Add an ALIVE peer with higher schema version
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"test_db": 10,
		},
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	result := server.checkPromotionCriteria()
	if result {
		t.Error("Expected promotion to fail when local schema is behind peer")
	}
}

// TestCheckPromotionCriteria_PeerHasNewDatabase verifies promotion succeeds when peer has a DB we don't
func TestCheckPromotionCriteria_PeerHasNewDatabase(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_new_db")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	err := dbMgr.CreateDatabase("test_db")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	err = schemaVersionMgr.SetSchemaVersion("test_db", 5, "CREATE TABLE test (id INT)", 1)
	if err != nil {
		t.Fatalf("Failed to set schema version: %v", err)
	}

	// Add an ALIVE peer with an additional database we don't have
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"test_db":  5,
			"other_db": 3,
		},
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	result := server.checkPromotionCriteria()
	if !result {
		t.Error("Expected promotion to succeed when peer has database we don't have yet")
	}
}

// TestCheckPromotionCriteria_MultipleDatabase verifies correct behavior with multiple databases
func TestCheckPromotionCriteria_MultipleDatabase(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_multi")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	err := dbMgr.CreateDatabase("db1")
	if err != nil {
		t.Fatalf("Failed to create db1: %v", err)
	}
	err = dbMgr.CreateDatabase("db2")
	if err != nil {
		t.Fatalf("Failed to create db2: %v", err)
	}

	err = schemaVersionMgr.SetSchemaVersion("db1", 5, "CREATE TABLE test (id INT)", 1)
	if err != nil {
		t.Fatalf("Failed to set schema version for db1: %v", err)
	}
	err = schemaVersionMgr.SetSchemaVersion("db2", 3, "CREATE TABLE test (id INT)", 2)
	if err != nil {
		t.Fatalf("Failed to set schema version for db2: %v", err)
	}

	// Add an ALIVE peer with multiple databases
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"db1": 5,
			"db2": 3,
		},
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	result := server.checkPromotionCriteria()
	if !result {
		t.Error("Expected promotion to succeed when all databases match peer schema versions")
	}
}

// TestCheckPromotionCriteria_MultipleDatabaseOneBehind verifies promotion fails when one DB is behind
func TestCheckPromotionCriteria_MultipleDatabaseOneBehind(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_multi_behind")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	err := dbMgr.CreateDatabase("db1")
	if err != nil {
		t.Fatalf("Failed to create db1: %v", err)
	}
	err = dbMgr.CreateDatabase("db2")
	if err != nil {
		t.Fatalf("Failed to create db2: %v", err)
	}

	err = schemaVersionMgr.SetSchemaVersion("db1", 5, "CREATE TABLE test (id INT)", 1)
	if err != nil {
		t.Fatalf("Failed to set schema version for db1: %v", err)
	}
	err = schemaVersionMgr.SetSchemaVersion("db2", 3, "CREATE TABLE test (id INT)", 2)
	if err != nil {
		t.Fatalf("Failed to set schema version for db2: %v", err)
	}

	// Add an ALIVE peer where one database has higher schema version
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"db1": 5,
			"db2": 10,
		},
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	result := server.checkPromotionCriteria()
	if result {
		t.Error("Expected promotion to fail when one database is behind peer")
	}
}

// TestCheckPromotionCriteria_NoReplicationHandler verifies graceful handling of nil replicationHandler
func TestCheckPromotionCriteria_NoReplicationHandler(t *testing.T) {
	tmpDir, dbMgr, _ := setupTestEnvironment(t, "test_promotion_no_handler")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	err := dbMgr.CreateDatabase("test_db")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Add an ALIVE peer
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"test_db": 5,
		},
	})

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: nil,
	}

	result := server.checkPromotionCriteria()
	if result {
		t.Error("Expected promotion to fail when replicationHandler is nil")
	}
}

// TestCheckPromotionCriteria_NoDatabase verifies promotion succeeds when only system database exists
func TestCheckPromotionCriteria_NoDatabase(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_no_db")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	// DatabaseManager creates a default "marmot" database, so promotion should succeed
	result := server.checkPromotionCriteria()
	if !result {
		t.Error("Expected promotion to succeed when default database exists")
	}
}

// TestCheckPromotionCriteria_NilDBManager verifies graceful handling of nil dbManager
func TestCheckPromotionCriteria_NilDBManager(t *testing.T) {
	registry := NewNodeRegistry(1, "localhost:8081")

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          nil,
		replicationHandler: nil,
	}

	result := server.checkPromotionCriteria()
	if result {
		t.Error("Expected promotion to fail when dbManager is nil")
	}
}

// TestCheckPromotionCriteria_PeerNilSchemaVersions verifies handling of peers with nil schema versions
func TestCheckPromotionCriteria_PeerNilSchemaVersions(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_nil_peer_schema")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	err := dbMgr.CreateDatabase("test_db")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	err = schemaVersionMgr.SetSchemaVersion("test_db", 5, "CREATE TABLE test (id INT)", 1)
	if err != nil {
		t.Fatalf("Failed to set schema version: %v", err)
	}

	// Add an ALIVE peer with nil schema versions
	registry.Add(&NodeState{
		NodeId:                 2,
		Address:                "peer:8082",
		Status:                 NodeStatus_ALIVE,
		Incarnation:            1,
		DatabaseSchemaVersions: nil,
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	result := server.checkPromotionCriteria()
	if !result {
		t.Error("Expected promotion to succeed when peer has nil schema versions")
	}
}

// TestPromotionCriteria_FreshNodeJoiningCluster verifies the EXACT bug scenario:
// Node 1 starts fresh with schema version 0 for "marmot" database (never set).
// ALIVE peer (Node 2) has schema version 2 for "marmot" database.
// Node 1 MUST NOT be allowed to become ALIVE until it catches up.
func TestPromotionCriteria_FreshNodeJoiningCluster(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_fresh_node")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	// "marmot" database already exists (created by DatabaseManager)
	// Schema version is 0 because we never called SetSchemaVersion
	// This simulates a fresh node joining an existing cluster

	// Get current schema version for marmot (should be 0)
	localVersion, err := schemaVersionMgr.GetSchemaVersion("marmot")
	if err != nil {
		t.Fatalf("Failed to get schema version: %v", err)
	}
	if localVersion != 0 {
		t.Fatalf("Expected local schema version to be 0, got %d", localVersion)
	}

	// Add an ALIVE peer with schema version 2 for marmot
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"marmot": 2,
		},
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	// This MUST return false - we're behind the peer
	result := server.checkPromotionCriteria()
	if result {
		t.Error("Expected promotion to FAIL when local schema version (0) is behind peer (2)")
	}
}

// TestPromotionCriteria_ThreeNodeClusterScenario verifies multi-peer scenario:
// Local node has schema version 0 for "marmot"
// Peer 2 has schema version 2 for "marmot"
// Peer 3 has schema version 2 for "marmot"
// Promotion MUST be blocked until we catch up
func TestPromotionCriteria_ThreeNodeClusterScenario(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_three_node")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	// Verify local schema version is 0
	localVersion, err := schemaVersionMgr.GetSchemaVersion("marmot")
	if err != nil {
		t.Fatalf("Failed to get schema version: %v", err)
	}
	if localVersion != 0 {
		t.Fatalf("Expected local schema version to be 0, got %d", localVersion)
	}

	// Add two ALIVE peers, both with schema version 2
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer2:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"marmot": 2,
		},
	})

	registry.Add(&NodeState{
		NodeId:      3,
		Address:     "peer3:8083",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"marmot": 2,
		},
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	// This MUST return false - we're behind both peers
	result := server.checkPromotionCriteria()
	if result {
		t.Error("Expected promotion to FAIL when local schema version (0) is behind multiple peers (2)")
	}
}

// TestPromotionCriteria_CatchUpThenPromote verifies catch-up scenario:
// Start with local schema=0, peer schema=2 (promotion blocked)
// Update local schema to 2
// Now promotion should succeed
func TestPromotionCriteria_CatchUpThenPromote(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_promotion_catchup")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	registry := NewNodeRegistry(1, "localhost:8081")

	// Verify local schema version is 0
	localVersion, err := schemaVersionMgr.GetSchemaVersion("marmot")
	if err != nil {
		t.Fatalf("Failed to get schema version: %v", err)
	}
	if localVersion != 0 {
		t.Fatalf("Expected local schema version to be 0, got %d", localVersion)
	}

	// Add ALIVE peer with schema version 2
	registry.Add(&NodeState{
		NodeId:      2,
		Address:     "peer:8082",
		Status:      NodeStatus_ALIVE,
		Incarnation: 1,
		DatabaseSchemaVersions: map[string]uint64{
			"marmot": 2,
		},
	})

	clock := hlc.NewClock(1)
	replicationHandler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	server := &Server{
		nodeID:             1,
		registry:           registry,
		dbManager:          dbMgr,
		replicationHandler: replicationHandler,
	}

	// First check: should FAIL because we're behind
	result := server.checkPromotionCriteria()
	if result {
		t.Error("Expected promotion to FAIL when local schema version (0) is behind peer (2)")
	}

	// Now simulate catching up by setting our schema version to 2
	err = schemaVersionMgr.SetSchemaVersion("marmot", 2, "CREATE TABLE test (id INT)", 2)
	if err != nil {
		t.Fatalf("Failed to set schema version: %v", err)
	}

	// Second check: should SUCCEED because we've caught up
	result = server.checkPromotionCriteria()
	if !result {
		t.Error("Expected promotion to SUCCEED after catching up to peer schema version")
	}
}
