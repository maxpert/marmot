package test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

const (
	propagationTimeout = 10 * time.Second
	startupDelay       = 500 * time.Millisecond
)

type testNode struct {
	nodeID        uint64
	dataDir       string
	grpcPort      int
	mysqlPort     int
	dbMgr         *db.DatabaseManager
	grpcServer    *marmotgrpc.Server
	mysqlServer   *protocol.MySQLServer
	client        *marmotgrpc.Client
	gossip        *marmotgrpc.GossipProtocol
	clock         *hlc.Clock
	mysqlConn     *sql.DB
	antiEntropy   *marmotgrpc.AntiEntropyService
	writeCoord    *coordinator.WriteCoordinator
	readCoord     *coordinator.ReadCoordinator
	deltaSync     *marmotgrpc.DeltaSyncClient
	catchUpClient *marmotgrpc.CatchUpClient
}

func (n *testNode) cleanup() {
	if n.mysqlConn != nil {
		n.mysqlConn.Close()
	}
	if n.mysqlServer != nil {
		n.mysqlServer.Stop()
	}
	if n.antiEntropy != nil {
		n.antiEntropy.Stop()
	}
	if n.grpcServer != nil {
		n.grpcServer.Stop()
	}
	if n.dbMgr != nil {
		n.dbMgr.Close()
	}
	if n.dataDir != "" {
		os.RemoveAll(n.dataDir)
	}
}

func TestClusterReplication(t *testing.T) {
	// Skip if short mode (this is an integration test)
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create 3 nodes
	nodes := make([]*testNode, 3)

	// Setup and start nodes
	for i := 0; i < 3; i++ {
		nodeID := uint64(i + 1)
		node := &testNode{
			nodeID:    nodeID,
			dataDir:   filepath.Join(os.TempDir(), fmt.Sprintf("marmot-test-node-%d-%d", nodeID, time.Now().UnixNano())),
			grpcPort:  18081 + i,
			mysqlPort: 13307 + i,
		}
		nodes[i] = node

		// Ensure cleanup
		t.Cleanup(node.cleanup)

		// Create data directory
		require.NoError(t, os.MkdirAll(node.dataDir, 0755))
	}

	// Start nodes sequentially (node 1 first as seed)
	for i, node := range nodes {
		var seedNodes []string
		if i > 0 {
			// Non-seed nodes point to node 1
			seedNodes = []string{fmt.Sprintf("localhost:%d", nodes[0].grpcPort)}
		}

		startNode(t, node, seedNodes)

		// Give node time to start up
		time.Sleep(startupDelay)
	}

	// Wait for cluster to stabilize
	t.Log("Waiting for cluster to stabilize...")
	time.Sleep(2 * time.Second)

	// Verify all nodes see each other
	for _, node := range nodes {
		aliveNodes := node.gossip.GetNodeRegistry().GetAlive()
		require.GreaterOrEqual(t, len(aliveNodes), 3, "Node %d should see at least 3 nodes", node.nodeID)
	}

	// Test 1: CREATE TABLE on node 1, verify on all nodes
	t.Log("Test 1: CREATE TABLE replication")
	_, err := nodes[0].mysqlConn.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, balance INT)")
	require.NoError(t, err, "Failed to create table on node 1")

	// Wait for replication
	waitForReplication(t, "Table creation")

	// Verify table exists on all nodes
	for _, node := range nodes {
		verifyTableExists(t, node, "users")
	}

	// Test 2: INSERT 10 rows on node 1, verify on all nodes
	t.Log("Test 2: INSERT replication (10 rows)")
	for i := 1; i <= 10; i++ {
		_, err := nodes[0].mysqlConn.Exec(
			"INSERT INTO users (id, name, balance) VALUES (?, ?, ?)",
			i, fmt.Sprintf("user%d", i), i*100,
		)
		require.NoError(t, err, "Failed to insert row %d on node 1", i)
	}

	// Wait for replication
	waitForReplication(t, "10 INSERTs")

	// Verify all 10 rows on all nodes
	for _, node := range nodes {
		verifyRowCount(t, node, "users", 10)
		verifyRowData(t, node, "users", 1, "user1", 100)
		verifyRowData(t, node, "users", 5, "user5", 500)
		verifyRowData(t, node, "users", 10, "user10", 1000)
	}

	// Test 3: UPDATE 5 rows on node 2, verify on all nodes
	t.Log("Test 3: UPDATE replication (5 rows)")
	for i := 1; i <= 5; i++ {
		_, err := nodes[1].mysqlConn.Exec(
			"UPDATE users SET balance = ? WHERE id = ?",
			i*200, i,
		)
		require.NoError(t, err, "Failed to update row %d on node 2", i)
	}

	// Wait for replication
	waitForReplication(t, "5 UPDATEs")

	// Verify updates on all nodes
	for _, node := range nodes {
		verifyRowCount(t, node, "users", 10)
		verifyRowData(t, node, "users", 1, "user1", 200)
		verifyRowData(t, node, "users", 3, "user3", 600)
		verifyRowData(t, node, "users", 5, "user5", 1000)
		// Unchanged rows
		verifyRowData(t, node, "users", 6, "user6", 600)
		verifyRowData(t, node, "users", 10, "user10", 1000)
	}

	// Test 4: DELETE 3 rows on node 3, verify on all nodes
	t.Log("Test 4: DELETE replication (3 rows)")
	for i := 8; i <= 10; i++ {
		_, err := nodes[2].mysqlConn.Exec("DELETE FROM users WHERE id = ?", i)
		require.NoError(t, err, "Failed to delete row %d on node 3", i)
	}

	// Wait for replication
	waitForReplication(t, "3 DELETEs")

	// Verify deletes on all nodes
	for _, node := range nodes {
		verifyRowCount(t, node, "users", 7)
		// Verify deleted rows are gone
		verifyRowNotExists(t, node, "users", 8)
		verifyRowNotExists(t, node, "users", 9)
		verifyRowNotExists(t, node, "users", 10)
		// Verify remaining rows
		verifyRowData(t, node, "users", 1, "user1", 200)
		verifyRowData(t, node, "users", 7, "user7", 700)
	}

	// Test 5: Final verification - exact data match across all nodes
	t.Log("Test 5: Final verification - data consistency")
	for _, node := range nodes {
		rows, err := node.mysqlConn.Query("SELECT id, name, balance FROM users ORDER BY id")
		require.NoError(t, err, "Failed to query users on node %d", node.nodeID)

		var results []struct {
			id      int
			name    string
			balance int
		}
		for rows.Next() {
			var id, balance int
			var name string
			require.NoError(t, rows.Scan(&id, &name, &balance))
			results = append(results, struct {
				id      int
				name    string
				balance int
			}{id, name, balance})
		}
		rows.Close()

		require.Equal(t, 7, len(results), "Node %d should have exactly 7 rows", node.nodeID)

		// Verify exact data
		expected := []struct {
			id      int
			name    string
			balance int
		}{
			{1, "user1", 200},
			{2, "user2", 400},
			{3, "user3", 600},
			{4, "user4", 800},
			{5, "user5", 1000},
			{6, "user6", 600},
			{7, "user7", 700},
		}

		for i, exp := range expected {
			require.Equal(t, exp.id, results[i].id, "Node %d row %d: ID mismatch", node.nodeID, i)
			require.Equal(t, exp.name, results[i].name, "Node %d row %d: Name mismatch", node.nodeID, i)
			require.Equal(t, exp.balance, results[i].balance, "Node %d row %d: Balance mismatch", node.nodeID, i)
		}

		t.Logf("Node %d: All data verified ✓", node.nodeID)
	}

	t.Log("All cluster replication tests passed ✓")
}

func startNode(t *testing.T, node *testNode, seedNodes []string) {
	// Initialize HLC clock
	node.clock = hlc.NewClock(node.nodeID)

	// Initialize gRPC server
	grpcConfig := marmotgrpc.ServerConfig{
		NodeID:           node.nodeID,
		Address:          "0.0.0.0",
		Port:             node.grpcPort,
		AdvertiseAddress: fmt.Sprintf("localhost:%d", node.grpcPort),
	}

	grpcServer, err := marmotgrpc.NewServer(grpcConfig)
	require.NoError(t, err, "Failed to create gRPC server for node %d", node.nodeID)
	node.grpcServer = grpcServer

	require.NoError(t, grpcServer.Start(), "Failed to start gRPC server for node %d", node.nodeID)

	// Initialize gossip
	gossipConfig := marmotgrpc.DefaultGossipConfig()
	gossip := grpcServer.GetGossipProtocol()
	node.gossip = gossip

	client := marmotgrpc.NewClient(node.nodeID)
	node.client = client
	gossip.SetClient(client)

	// Join cluster if seed nodes provided
	if len(seedNodes) > 0 {
		err := gossip.JoinCluster(seedNodes, grpcConfig.AdvertiseAddress)
		require.NoError(t, err, "Failed to join cluster for node %d", node.nodeID)
	}

	gossip.Start(gossipConfig)

	// Initialize database manager
	dbMgr, err := db.NewDatabaseManager(node.dataDir, node.nodeID, node.clock)
	require.NoError(t, err, "Failed to create database manager for node %d", node.nodeID)
	node.dbMgr = dbMgr

	// Get system database for schema versioning
	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	require.NoError(t, err, "Failed to get system database for node %d", node.nodeID)
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetMetaStore())

	// Wire up replication handlers
	replicationHandler := marmotgrpc.NewReplicationHandler(
		node.nodeID,
		dbMgr,
		node.clock,
		schemaVersionMgr,
	)
	grpcServer.SetReplicationHandler(replicationHandler)
	grpcServer.SetDatabaseManager(dbMgr)

	// Setup anti-entropy
	node.catchUpClient = marmotgrpc.NewCatchUpClient(
		node.nodeID,
		node.dataDir,
		grpcServer.GetNodeRegistry(),
		seedNodes,
	)

	node.deltaSync = marmotgrpc.NewDeltaSyncClient(marmotgrpc.DeltaSyncConfig{
		NodeID:           node.nodeID,
		Client:           client,
		DBManager:        dbMgr,
		Clock:            node.clock,
		ApplyTxnsFn:      replicationHandler.HandleReplicateTransaction,
		SchemaVersionMgr: schemaVersionMgr,
	})

	snapshotFunc := func(ctx context.Context, peerNodeID uint64, peerAddr string, database string) error {
		if err := node.catchUpClient.CatchUpFromPeer(ctx, peerNodeID, peerAddr, database); err != nil {
			return err
		}
		if err := dbMgr.ReopenDatabase(database); err != nil {
			return fmt.Errorf("database reload failed after snapshot: %w", err)
		}
		return nil
	}

	schemaVersionMgr = db.NewSchemaVersionManager(systemDB.GetMetaStore())
	node.antiEntropy = marmotgrpc.NewAntiEntropyServiceFromConfig(
		node.nodeID,
		grpcServer.GetNodeRegistry(),
		client,
		dbMgr,
		node.deltaSync,
		node.clock,
		snapshotFunc,
		schemaVersionMgr,
	)
	node.antiEntropy.Start()

	// Setup coordinators
	nodeProvider := marmotgrpc.NewGossipNodeProvider(gossip.GetNodeRegistry())
	replicator := marmotgrpc.NewGRPCReplicator(client)

	localReplicator := db.NewLocalReplicator(node.nodeID, dbMgr, node.clock)
	node.writeCoord = coordinator.NewWriteCoordinator(
		node.nodeID,
		nodeProvider,
		replicator,
		localReplicator,
		5*time.Second,
		node.clock,
	)

	localReader := db.NewLocalReader(dbMgr)
	node.readCoord = coordinator.NewReadCoordinator(
		node.nodeID,
		nodeProvider,
		localReader,
		2*time.Second,
	)

	// Setup MySQL server
	ddlLockMgr := coordinator.NewDDLLockManager(30 * time.Second)
	registryAdapter := marmotgrpc.NewNodeRegistryAdapter(gossip.GetNodeRegistry())

	handler := coordinator.NewCoordinatorHandler(
		node.nodeID,
		node.writeCoord,
		node.readCoord,
		node.clock,
		dbMgr,
		ddlLockMgr,
		schemaVersionMgr,
		registryAdapter,
	)

	mysqlServer := protocol.NewMySQLServer(
		fmt.Sprintf("127.0.0.1:%d", node.mysqlPort),
		"",
		0,
		handler,
	)
	require.NoError(t, mysqlServer.Start(), "Failed to start MySQL server for node %d", node.nodeID)
	node.mysqlServer = mysqlServer

	// Mark node as ALIVE (seed node behavior)
	if len(seedNodes) == 0 {
		grpcServer.GetNodeRegistry().MarkAlive(node.nodeID)
	}

	// Connect MySQL client
	time.Sleep(100 * time.Millisecond) // Let server start

	dsn := fmt.Sprintf("root:@tcp(127.0.0.1:%d)/marmot", node.mysqlPort)
	mysqlConn, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "Failed to open MySQL connection for node %d", node.nodeID)
	require.NoError(t, mysqlConn.Ping(), "Failed to ping MySQL server for node %d", node.nodeID)
	node.mysqlConn = mysqlConn

	t.Logf("Node %d started (gRPC: %d, MySQL: %d)", node.nodeID, node.grpcPort, node.mysqlPort)
}

func waitForReplication(t *testing.T, description string) {
	t.Logf("Waiting for replication: %s", description)
	time.Sleep(2 * time.Second) // Give time for replication + anti-entropy
}

func verifyTableExists(t *testing.T, node *testNode, tableName string) {
	var name string
	err := node.mysqlConn.QueryRow(
		"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
		tableName,
	).Scan(&name)
	require.NoError(t, err, "Table %s should exist on node %d", tableName, node.nodeID)
	require.Equal(t, tableName, name)
}

func verifyRowCount(t *testing.T, node *testNode, tableName string, expectedCount int) {
	var count int
	err := node.mysqlConn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	require.NoError(t, err, "Failed to count rows in %s on node %d", tableName, node.nodeID)
	require.Equal(t, expectedCount, count, "Node %d should have %d rows in %s", node.nodeID, expectedCount, tableName)
}

func verifyRowData(t *testing.T, node *testNode, tableName string, id int, expectedName string, expectedBalance int) {
	var name string
	var balance int
	err := node.mysqlConn.QueryRow(
		fmt.Sprintf("SELECT name, balance FROM %s WHERE id = ?", tableName),
		id,
	).Scan(&name, &balance)
	require.NoError(t, err, "Failed to query row %d from %s on node %d", id, tableName, node.nodeID)
	require.Equal(t, expectedName, name, "Node %d row %d: name mismatch", node.nodeID, id)
	require.Equal(t, expectedBalance, balance, "Node %d row %d: balance mismatch", node.nodeID, id)
}

func verifyRowNotExists(t *testing.T, node *testNode, tableName string, id int) {
	var count int
	err := node.mysqlConn.QueryRow(
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = ?", tableName),
		id,
	).Scan(&count)
	require.NoError(t, err, "Failed to check row %d in %s on node %d", id, tableName, node.nodeID)
	require.Equal(t, 0, count, "Node %d should not have row %d in %s", node.nodeID, id, tableName)
}
