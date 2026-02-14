package replica

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/encoding"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
)

// setupStreamClientTest creates test environment
func setupStreamClientTest(t *testing.T) (*db.DatabaseManager, *hlc.Clock, string, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "marmot-stream-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create database manager: %v", err)
	}

	cleanup := func() {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
	}

	return dbMgr, clock, tmpDir, cleanup
}

// testSchemaAdapter provides PK info for tests using cached schema
type testSchemaAdapter struct {
	mdb *db.ReplicatedDatabase
}

func (a *testSchemaAdapter) GetPrimaryKeys(tableName string) ([]string, error) {
	schema, err := a.mdb.GetCachedTableSchema(tableName)
	if err != nil {
		return nil, err
	}
	return schema.PrimaryKeys, nil
}

// TestNewStreamClient tests client creation
func TestNewStreamClient(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	followAddrs := []string{"localhost:8080", "localhost:8081"}
	client := NewStreamClient(followAddrs, 1, dbMgr, clock, replica)

	if client == nil {
		t.Fatal("Expected client to be created, got nil")
	}

	if client.currentAddr != "localhost:8080" {
		t.Errorf("Expected currentAddr localhost:8080, got %s", client.currentAddr)
	}

	if client.nodeID != 1 {
		t.Errorf("Expected nodeID 1, got %d", client.nodeID)
	}

	if client.lastTxnID == nil {
		t.Error("Expected lastTxnID map to be initialized")
	}

	if len(client.clusterNodes) != 2 {
		t.Errorf("Expected 2 cluster nodes, got %d", len(client.clusterNodes))
	}
}

// TestStreamClient_ApplyCDCInsert tests CDC insert application
func TestStreamClient_ApplyCDCInsert(t *testing.T) {
	dbMgr, _, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Create test database and table
	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()

	_, err := sqlDB.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test INSERT using unified CDC applier
	tx, err := sqlDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	newValues := map[string][]byte{
		"id":    msgpackMarshal(1),
		"name":  msgpackMarshal("Alice"),
		"email": msgpackMarshal("alice@example.com"),
	}

	err = db.ApplyCDCInsert(tx, "users", newValues)
	if err != nil {
		tx.Rollback()
		t.Fatalf("ApplyCDCInsert failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify data
	var id int
	var name, email string
	err = sqlDB.QueryRow("SELECT id, name, email FROM users WHERE id = 1").Scan(&id, &name, &email)
	if err != nil {
		t.Fatalf("Failed to read inserted row: %v", err)
	}

	if id != 1 || name != "Alice" || email != "alice@example.com" {
		t.Errorf("Unexpected values: id=%d, name=%s, email=%s", id, name, email)
	}
}

// TestStreamClient_ApplyCDCUpdate tests CDC update via INSERT OR REPLACE
func TestStreamClient_ApplyCDCUpdate(t *testing.T) {
	dbMgr, _, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Create test database and table with initial data
	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()

	_, err := sqlDB.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = sqlDB.Exec(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@old.com')`)
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Test UPDATE via INSERT OR REPLACE using unified CDC applier
	tx, err := sqlDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	newValues := map[string][]byte{
		"id":    msgpackMarshal(1),
		"name":  msgpackMarshal("Alice Updated"),
		"email": msgpackMarshal("alice@new.com"),
	}

	err = db.ApplyCDCInsert(tx, "users", newValues)
	if err != nil {
		tx.Rollback()
		t.Fatalf("ApplyCDCInsert (update) failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify update
	var name, email string
	err = sqlDB.QueryRow("SELECT name, email FROM users WHERE id = 1").Scan(&name, &email)
	if err != nil {
		t.Fatalf("Failed to read updated row: %v", err)
	}

	if name != "Alice Updated" || email != "alice@new.com" {
		t.Errorf("Unexpected values after update: name=%s, email=%s", name, email)
	}
}

// TestStreamClient_ApplyCDCDelete tests CDC delete application
func TestStreamClient_ApplyCDCDelete(t *testing.T) {
	dbMgr, _, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Create test database and table with initial data
	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()

	_, err := sqlDB.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Reload schema cache after DDL
	if err := mdb.ReloadSchema(); err != nil {
		t.Fatalf("Failed to reload schema: %v", err)
	}

	// Insert initial data
	_, err = sqlDB.Exec(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Test DELETE using unified CDC applier with schema adapter
	schemaAdapter := &testSchemaAdapter{mdb: mdb}

	tx, err := sqlDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	oldValues := map[string][]byte{
		"id":    msgpackMarshal(1),
		"name":  msgpackMarshal("Alice"),
		"email": msgpackMarshal("alice@example.com"),
	}

	err = db.ApplyCDCDelete(tx, schemaAdapter, "users", oldValues)
	if err != nil {
		tx.Rollback()
		t.Fatalf("ApplyCDCDelete failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify deletion
	var count int
	err = sqlDB.QueryRow("SELECT COUNT(*) FROM users WHERE id = 1").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 rows after delete, got %d", count)
	}
}

// TestStreamClient_ApplyCDCDeleteWithOldValues tests CDC delete using oldValues for PK extraction
func TestStreamClient_ApplyCDCDeleteWithOldValues(t *testing.T) {
	dbMgr, _, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Create test database and table with initial data
	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()

	_, err := sqlDB.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Reload schema cache after DDL
	if err := mdb.ReloadSchema(); err != nil {
		t.Fatalf("Failed to reload schema: %v", err)
	}

	// Insert initial data
	_, err = sqlDB.Exec(`INSERT INTO users (id, name) VALUES (42, 'Bob')`)
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Test DELETE with oldValues using unified CDC applier
	schemaAdapter := &testSchemaAdapter{mdb: mdb}

	tx, err := sqlDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	oldValues := map[string][]byte{
		"id":   msgpackMarshal(42),
		"name": msgpackMarshal("Bob"),
	}

	err = db.ApplyCDCDelete(tx, schemaAdapter, "users", oldValues)
	if err != nil {
		tx.Rollback()
		t.Fatalf("ApplyCDCDelete failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify deletion
	var count int
	err = sqlDB.QueryRow("SELECT COUNT(*) FROM users WHERE id = 42").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 rows after delete, got %d", count)
	}
}

// TestStreamClient_ApplyChangeEvent tests full change event application
func TestStreamClient_ApplyChangeEvent(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Create test database and table
	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()

	_, err := sqlDB.Exec(`CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, nil)

	// Create a change event with CDC data
	event := &marmotgrpc.ChangeEvent{
		TxnId:    100,
		Database: "testdb",
		Statements: []*marmotgrpc.Statement{
			{
				TableName: "events",
				Type:      pb.StatementType_INSERT,
				Payload: &marmotgrpc.Statement_RowChange{
					RowChange: &marmotgrpc.RowChange{
						NewValues: map[string][]byte{
							"id":   msgpackMarshal(1),
							"data": msgpackMarshal("test event"),
						},
					},
				},
			},
		},
	}

	ctx := context.Background()
	err = client.applyChangeEvent(ctx, event)
	if err != nil {
		t.Fatalf("applyChangeEvent failed: %v", err)
	}

	// Verify data
	var id int
	var data string
	err = sqlDB.QueryRow("SELECT id, data FROM events WHERE id = 1").Scan(&id, &data)
	if err != nil {
		t.Fatalf("Failed to read inserted row: %v", err)
	}

	if id != 1 || data != "test event" {
		t.Errorf("Unexpected values: id=%d, data=%s", id, data)
	}
}

// TestStreamClient_ApplyChangeEvent_DDL tests DDL change event application
func TestStreamClient_ApplyChangeEvent_DDL(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Create test database
	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, nil)

	// Create a DDL change event
	event := &marmotgrpc.ChangeEvent{
		TxnId:    200,
		Database: "testdb",
		Statements: []*marmotgrpc.Statement{
			{
				Type: pb.StatementType_DDL,
				Payload: &marmotgrpc.Statement_DdlChange{
					DdlChange: &marmotgrpc.DDLChange{
						Sql: "CREATE TABLE new_table (id INTEGER PRIMARY KEY, value TEXT)",
					},
				},
			},
		},
	}

	ctx := context.Background()
	err := client.applyChangeEvent(ctx, event)
	if err != nil {
		t.Fatalf("applyChangeEvent (DDL) failed: %v", err)
	}

	// Verify table was created
	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()

	var tableName string
	err = sqlDB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='new_table'").Scan(&tableName)
	if err != nil {
		t.Fatalf("DDL table not created: %v", err)
	}

	if tableName != "new_table" {
		t.Errorf("Expected table 'new_table', got '%s'", tableName)
	}
}

// TestStreamClient_ApplyChangeEvent_LoadData ensures stream apply handles
// LOAD DATA payloads instead of silently skipping them.
func TestStreamClient_ApplyChangeEvent_LoadData(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()
	_, err := sqlDB.Exec(`CREATE TABLE bulk_users (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, nil)

	loadSQL := "LOAD DATA LOCAL INFILE 'users.csv' INTO TABLE bulk_users FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 LINES (id,name)"
	loadBytes := []byte("id,name\n1,Alice\n2,Bob\n")

	event := &marmotgrpc.ChangeEvent{
		TxnId:    201,
		Database: "testdb",
		Statements: []*marmotgrpc.Statement{
			{
				Type:      pb.StatementType_LOAD_DATA,
				TableName: "bulk_users",
				Payload: &marmotgrpc.Statement_LoadDataChange{
					LoadDataChange: &marmotgrpc.LoadDataChange{
						Sql:  loadSQL,
						Data: loadBytes,
					},
				},
			},
		},
	}

	err = client.applyChangeEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("applyChangeEvent (LOAD DATA) failed: %v", err)
	}

	var count int
	if err := sqlDB.QueryRow("SELECT COUNT(*) FROM bulk_users").Scan(&count); err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected 2 rows after LOAD DATA, got %d", count)
	}
}

// TestStreamClient_GetLocalMaxTxnIDs tests local txn ID retrieval
func TestStreamClient_GetLocalMaxTxnIDs(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, nil)

	// Get local txn IDs (should be empty/zero for new databases)
	txnIDs, err := client.getLocalMaxTxnIDs()
	if err != nil {
		t.Fatalf("getLocalMaxTxnIDs failed: %v", err)
	}

	// Should have entries for existing databases (marmot is created by default)
	if len(txnIDs) < 1 {
		t.Errorf("Expected at least 1 database in txn IDs")
	}
}

// TestStreamClient_Stop tests graceful stop
func TestStreamClient_Stop(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, replica)

	// Start a goroutine that will be stopped
	done := make(chan bool)
	go func() {
		// Simulate some work
		select {
		case <-client.ctx.Done():
			done <- true
		case <-time.After(5 * time.Second):
			done <- false
		}
	}()

	// Stop should cancel context
	client.Stop()

	select {
	case stopped := <-done:
		if !stopped {
			t.Error("Expected client to stop")
		}
	case <-time.After(2 * time.Second):
		t.Error("Stop timed out")
	}
}

// TestStreamClient_LastTxnIDTracking tests txn ID tracking
func TestStreamClient_LastTxnIDTracking(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, nil)

	// Initially empty
	client.mu.RLock()
	if len(client.lastTxnID) != 0 {
		t.Error("Expected lastTxnID to be empty initially")
	}
	client.mu.RUnlock()

	// Set a txn ID
	client.mu.Lock()
	client.lastTxnID["testdb"] = 100
	client.mu.Unlock()

	// Verify
	client.mu.RLock()
	if client.lastTxnID["testdb"] != 100 {
		t.Errorf("Expected lastTxnID[testdb]=100, got %d", client.lastTxnID["testdb"])
	}
	client.mu.RUnlock()
}

// TestStreamClient_ReconnectConfig tests reconnect configuration
func TestStreamClient_ReconnectConfig(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Set config values
	originalReconnectSec := cfg.Config.Replica.ReconnectIntervalSec
	originalMaxBackoffSec := cfg.Config.Replica.ReconnectMaxBackoffSec
	defer func() {
		cfg.Config.Replica.ReconnectIntervalSec = originalReconnectSec
		cfg.Config.Replica.ReconnectMaxBackoffSec = originalMaxBackoffSec
	}()

	cfg.Config.Replica.ReconnectIntervalSec = 10
	cfg.Config.Replica.ReconnectMaxBackoffSec = 60

	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, nil)

	if client.reconnectInterval != 10*time.Second {
		t.Errorf("Expected reconnectInterval=10s, got %v", client.reconnectInterval)
	}

	if client.maxBackoff != 60*time.Second {
		t.Errorf("Expected maxBackoff=60s, got %v", client.maxBackoff)
	}
}

// TestApplyCDCInsert_EmptyValues tests error handling
func TestApplyCDCInsert_EmptyValues(t *testing.T) {
	dbMgr, _, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Create test database
	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()

	_, err := sqlDB.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, _ := sqlDB.BeginTx(context.Background(), nil)
	defer tx.Rollback()

	// Empty values should return error
	err = db.ApplyCDCInsert(tx, "users", map[string][]byte{})
	if err == nil {
		t.Error("Expected error for empty values")
	}
}

// TestApplyCDCDelete_NoKeyOrValues tests error handling
func TestApplyCDCDelete_NoKeyOrValues(t *testing.T) {
	dbMgr, _, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	// Create test database
	if err := dbMgr.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()

	_, err := sqlDB.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	schemaAdapter := &testSchemaAdapter{mdb: mdb}

	tx, _ := sqlDB.BeginTx(context.Background(), nil)
	defer tx.Rollback()

	// No old values should return error
	err = db.ApplyCDCDelete(tx, schemaAdapter, "users", nil)
	if err == nil {
		t.Error("Expected error for delete with no old values")
	}
}

// Helper to msgpack marshal values
func msgpackMarshal(v interface{}) []byte {
	data, _ := encoding.Marshal(v)
	return data
}

// Benchmark CDC insert using unified applier
func BenchmarkStreamClient_ApplyCDCInsert(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "marmot-stream-bench-*")
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, _ := db.NewDatabaseManager(tmpDir, 1, clock)
	defer dbMgr.Close()

	dbMgr.CreateDatabase("testdb")
	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()
	sqlDB.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := sqlDB.BeginTx(context.Background(), nil)
		newValues := map[string][]byte{
			"id":   msgpackMarshal(i),
			"name": msgpackMarshal("test"),
		}
		db.ApplyCDCInsert(tx, "users", newValues)
		tx.Commit()
	}
}

// Benchmark change event application
func BenchmarkStreamClient_ApplyChangeEvent(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "marmot-stream-bench-*")
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, _ := db.NewDatabaseManager(tmpDir, 1, clock)
	defer dbMgr.Close()

	dbMgr.CreateDatabase("testdb")
	mdb, _ := dbMgr.GetDatabase("testdb")
	sqlDB := mdb.GetDB()
	sqlDB.Exec(`CREATE TABLE events (id INTEGER PRIMARY KEY, data TEXT)`)

	client := NewStreamClient([]string{"localhost:8080"}, 1, dbMgr, clock, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event := &marmotgrpc.ChangeEvent{
			TxnId:    uint64(i),
			Database: "testdb",
			Statements: []*marmotgrpc.Statement{
				{
					TableName: "events",
					Type:      pb.StatementType_INSERT,
					Payload: &marmotgrpc.Statement_RowChange{
						RowChange: &marmotgrpc.RowChange{
							NewValues: map[string][]byte{
								"id":   msgpackMarshal(i),
								"data": msgpackMarshal("test"),
							},
						},
					},
				},
			},
		}
		client.applyChangeEvent(context.Background(), event)
	}
}

// TestSelectAliveNode_FiltersDeadNodes tests that dead nodes are excluded from selection
func TestSelectAliveNode_FiltersDeadNodes(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	followAddrs := []string{"node1:8080", "node2:8080", "node3:8080"}
	client := NewStreamClient(followAddrs, 1, dbMgr, clock, replica)

	// Populate cluster nodes with mixed states
	client.clusterMu.Lock()
	client.clusterNodes = map[uint64]*marmotgrpc.NodeState{
		100: {NodeId: 100, Address: "node1:8080", Status: marmotgrpc.NodeStatus_ALIVE, Incarnation: 1},
		200: {NodeId: 200, Address: "node2:8080", Status: marmotgrpc.NodeStatus_DEAD, Incarnation: 1},
		300: {NodeId: 300, Address: "node3:8080", Status: marmotgrpc.NodeStatus_ALIVE, Incarnation: 1},
	}
	client.currentAddr = "node1:8080"
	client.clusterMu.Unlock()

	// selectAliveNode should only return node3 (node1 is current, node2 is dead)
	addr, nodeID, err := client.selectAliveNode()
	if err != nil {
		t.Fatalf("selectAliveNode failed: %v", err)
	}

	if addr != "node3:8080" {
		t.Errorf("Expected node3:8080, got %s", addr)
	}
	if nodeID != 300 {
		t.Errorf("Expected nodeID 300, got %d", nodeID)
	}
}

// TestSelectAliveNode_SkipsCurrentNode tests that current node is excluded from selection
func TestSelectAliveNode_SkipsCurrentNode(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	followAddrs := []string{"node1:8080", "node2:8080"}
	client := NewStreamClient(followAddrs, 1, dbMgr, clock, replica)

	// Only one alive node (current)
	client.clusterMu.Lock()
	client.clusterNodes = map[uint64]*marmotgrpc.NodeState{
		100: {NodeId: 100, Address: "node1:8080", Status: marmotgrpc.NodeStatus_ALIVE, Incarnation: 1},
	}
	client.currentAddr = "node1:8080"
	client.clusterMu.Unlock()

	// Should return error - no alive nodes other than current
	_, _, err := client.selectAliveNode()
	if err == nil {
		t.Error("Expected error when no alive nodes available")
	}
}

// TestMergeClusterView_UpdatesOnHigherIncarnation tests cluster view merging
func TestMergeClusterView_UpdatesOnHigherIncarnation(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	followAddrs := []string{"node1:8080"}
	client := NewStreamClient(followAddrs, 1, dbMgr, clock, replica)

	// Initial cluster state
	client.clusterMu.Lock()
	client.clusterNodes = map[uint64]*marmotgrpc.NodeState{
		100: {NodeId: 100, Address: "node1:8080", Status: marmotgrpc.NodeStatus_ALIVE, Incarnation: 1},
	}
	client.clusterMu.Unlock()

	// Merge with higher incarnation
	newNodes := []*marmotgrpc.NodeState{
		{NodeId: 100, Address: "node1:8080", Status: marmotgrpc.NodeStatus_SUSPECT, Incarnation: 2},
		{NodeId: 200, Address: "node2:8080", Status: marmotgrpc.NodeStatus_ALIVE, Incarnation: 1},
	}
	client.mergeClusterView(newNodes)

	// Verify node 100 was updated
	client.clusterMu.RLock()
	defer client.clusterMu.RUnlock()

	node100 := client.clusterNodes[100]
	if node100.Incarnation != 2 {
		t.Errorf("Expected incarnation 2, got %d", node100.Incarnation)
	}
	if node100.Status != marmotgrpc.NodeStatus_SUSPECT {
		t.Errorf("Expected status SUSPECT, got %v", node100.Status)
	}

	// Verify node 200 was added
	node200 := client.clusterNodes[200]
	if node200 == nil {
		t.Fatal("Expected node 200 to be added")
	}
	if node200.Address != "node2:8080" {
		t.Errorf("Expected address node2:8080, got %s", node200.Address)
	}
}

// TestMergeClusterView_IgnoresLowerIncarnation tests that lower incarnations are ignored
func TestMergeClusterView_IgnoresLowerIncarnation(t *testing.T) {
	dbMgr, clock, _, cleanup := setupStreamClientTest(t)
	defer cleanup()

	replica := &Replica{}
	followAddrs := []string{"node1:8080"}
	client := NewStreamClient(followAddrs, 1, dbMgr, clock, replica)

	// Initial cluster state with higher incarnation
	client.clusterMu.Lock()
	client.clusterNodes = map[uint64]*marmotgrpc.NodeState{
		100: {NodeId: 100, Address: "node1:8080", Status: marmotgrpc.NodeStatus_ALIVE, Incarnation: 5},
	}
	client.clusterMu.Unlock()

	// Try to merge with lower incarnation
	newNodes := []*marmotgrpc.NodeState{
		{NodeId: 100, Address: "node1:8080", Status: marmotgrpc.NodeStatus_DEAD, Incarnation: 3},
	}
	client.mergeClusterView(newNodes)

	// Verify node 100 was NOT updated
	client.clusterMu.RLock()
	defer client.clusterMu.RUnlock()

	node100 := client.clusterNodes[100]
	if node100.Incarnation != 5 {
		t.Errorf("Expected incarnation to remain 5, got %d", node100.Incarnation)
	}
	if node100.Status != marmotgrpc.NodeStatus_ALIVE {
		t.Errorf("Expected status to remain ALIVE, got %v", node100.Status)
	}
}

// Ensure sql import is used
var _ sql.DB
