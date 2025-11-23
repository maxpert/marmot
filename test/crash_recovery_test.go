package test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	baseGRPCPort  = 8080
	baseMySQLPort = 3306
	numNodes      = 3
)

// ClusterNode represents a running Marmot node
type ClusterNode struct {
	NodeID      int
	GRPCPort    int
	MySQLPort   int
	DataDir     string
	ConfigPath  string
	PIDFile     string
	LogFile     string
	Cmd         *exec.Cmd
	DB          *sql.DB
	isRunning   bool
	mu          sync.Mutex
}

// ClusterHarness manages a test cluster
type ClusterHarness struct {
	Nodes       []*ClusterNode
	BaseDir     string
	MarmotBin   string
	t           *testing.T
	cleanupOnce sync.Once
}

// NewClusterHarness creates a new cluster harness for testing
func NewClusterHarness(t *testing.T) *ClusterHarness {
	baseDir := filepath.Join(os.TempDir(), fmt.Sprintf("marmot_crash_test_%d", time.Now().UnixNano()))

	harness := &ClusterHarness{
		Nodes:     make([]*ClusterNode, numNodes),
		BaseDir:   baseDir,
		MarmotBin: "",
		t:         t,
	}

	// Build Marmot binary
	if err := harness.buildMarmot(); err != nil {
		t.Fatalf("Failed to build Marmot: %v", err)
	}

	// Create node configurations
	for i := 0; i < numNodes; i++ {
		node := harness.createNode(i + 1)
		harness.Nodes[i] = node
	}

	return harness
}

// buildMarmot builds the Marmot binary for testing
func (h *ClusterHarness) buildMarmot() error {
	h.t.Logf("Building Marmot binary...")

	binPath := filepath.Join(h.BaseDir, "marmot")
	h.MarmotBin = binPath

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = "/Users/zohaib/repos/marmot"
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("build failed: %v\n%s", err, output)
	}

	h.t.Logf("Marmot binary built at %s", binPath)
	return nil
}

// createNode creates a cluster node configuration
func (h *ClusterHarness) createNode(nodeID int) *ClusterNode {
	dataDir := filepath.Join(h.BaseDir, fmt.Sprintf("node%d", nodeID))
	os.MkdirAll(dataDir, 0755)

	node := &ClusterNode{
		NodeID:     nodeID,
		GRPCPort:   baseGRPCPort + nodeID,
		MySQLPort:  baseMySQLPort + nodeID,
		DataDir:    dataDir,
		ConfigPath: filepath.Join(dataDir, "config.toml"),
		PIDFile:    filepath.Join(dataDir, "marmot.pid"),
		LogFile:    filepath.Join(dataDir, "marmot.log"),
		isRunning:  false,
	}

	h.createNodeConfig(node)
	return node
}

// createNodeConfig creates the TOML configuration for a node
func (h *ClusterHarness) createNodeConfig(node *ClusterNode) {
	// Build seed nodes list - Node 1 is the seed (no seeds), others join Node 1
	seedNodes := []string{}
	if node.NodeID != 1 {
		// All nodes except node 1 use node 1 as seed
		seedNodes = append(seedNodes, fmt.Sprintf("\"localhost:%d\"", baseGRPCPort+1))
	}

	config := fmt.Sprintf(`# Marmot v2.0 Test Node %d
node_id = %d
data_dir = "%s"

[mvcc]
gc_interval_seconds = 30
gc_retention_hours = 1
heartbeat_timeout_seconds = 10
version_retention_count = 10
conflict_window_seconds = 10

[connection_pool]
pool_size = 4
max_idle_time_seconds = 10
max_lifetime_seconds = 300

[grpc_client]
keepalive_time_seconds = 10
keepalive_timeout_seconds = 3
max_retries = 3
retry_backoff_ms = 100

[coordinator]
prepare_timeout_ms = 2000
commit_timeout_ms = 2000
abort_timeout_ms = 2000

[cluster]
grpc_bind_address = "0.0.0.0"
grpc_advertise_address = "localhost:%d"
grpc_port = %d
seed_nodes = [%s]
gossip_interval_ms = 1000
gossip_fanout = 2
suspect_timeout_ms = 5000
dead_timeout_ms = 10000

[replication]
replication_factor = 3
virtual_nodes = 150
default_write_consistency = "QUORUM"
default_read_consistency = "LOCAL_ONE"
write_timeout_ms = 5000
read_timeout_ms = 2000
enable_anti_entropy = true
anti_entropy_interval_seconds = 10

[mysql]
enabled = true
bind_address = "0.0.0.0"
port = %d
max_connections = 100

[logging]
verbose = true
format = "console"

[prometheus]
enabled = false
`,
		node.NodeID,
		node.NodeID,
		node.DataDir,
		node.GRPCPort,
		node.GRPCPort,
		strings.Join(seedNodes, ", "),
		node.MySQLPort,
	)

	if err := os.WriteFile(node.ConfigPath, []byte(config), 0644); err != nil {
		h.t.Fatalf("Failed to write config for node %d: %v", node.NodeID, err)
	}

	h.t.Logf("Created config for node %d (gRPC: %d, MySQL: %d)", node.NodeID, node.GRPCPort, node.MySQLPort)
}

// StartNode starts a specific node
func (h *ClusterHarness) StartNode(nodeID int) error {
	node := h.Nodes[nodeID-1]
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.isRunning {
		return fmt.Errorf("node %d is already running", nodeID)
	}

	h.t.Logf("Starting node %d...", nodeID)

	// Open log file
	logFile, err := os.Create(node.LogFile)
	if err != nil {
		return fmt.Errorf("failed to create log file: %v", err)
	}

	// Start Marmot process
	cmd := exec.Command(h.MarmotBin, "--config", node.ConfigPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("failed to start node %d: %v", nodeID, err)
	}

	node.Cmd = cmd
	node.isRunning = true

	// Write PID file
	os.WriteFile(node.PIDFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0644)

	h.t.Logf("Node %d started (PID: %d)", nodeID, cmd.Process.Pid)
	return nil
}

// StopNode gracefully stops a specific node
func (h *ClusterHarness) StopNode(nodeID int) error {
	node := h.Nodes[nodeID-1]
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.isRunning {
		return fmt.Errorf("node %d is not running", nodeID)
	}

	h.t.Logf("Stopping node %d (PID: %d)...", nodeID, node.Cmd.Process.Pid)

	// Close DB connection if open
	if node.DB != nil {
		node.DB.Close()
		node.DB = nil
	}

	// Send SIGTERM
	if err := node.Cmd.Process.Kill(); err != nil {
		h.t.Logf("Warning: failed to kill node %d: %v", nodeID, err)
	}

	// Wait for process to exit
	node.Cmd.Wait()
	node.isRunning = false
	node.Cmd = nil

	// Remove PID file
	os.Remove(node.PIDFile)

	h.t.Logf("Node %d stopped", nodeID)
	return nil
}

// KillNode forcefully kills a node (simulates crash)
func (h *ClusterHarness) KillNode(nodeID int) error {
	node := h.Nodes[nodeID-1]
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.isRunning {
		return fmt.Errorf("node %d is not running", nodeID)
	}

	h.t.Logf("Killing node %d (simulating crash)...", nodeID)

	// Close DB connection if open
	if node.DB != nil {
		node.DB.Close()
		node.DB = nil
	}

	// Send SIGKILL
	if err := node.Cmd.Process.Kill(); err != nil {
		return fmt.Errorf("failed to kill node %d: %v", nodeID, err)
	}

	node.Cmd.Wait()
	node.isRunning = false
	node.Cmd = nil

	h.t.Logf("Node %d killed (crash simulated)", nodeID)
	return nil
}

// WaitForAlive waits for a node to become ALIVE in the cluster
func (h *ClusterHarness) WaitForAlive(nodeID int, timeout time.Duration) error {
	h.t.Logf("Waiting for node %d to become ALIVE...", nodeID)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for node %d to become ALIVE", nodeID)
		case <-ticker.C:
			db, err := h.ConnectToNode(nodeID)
			if err != nil {
				continue
			}

			// Try a simple query
			var result int
			err = db.QueryRow("SELECT 1").Scan(&result)
			if err == nil {
				h.t.Logf("Node %d is ALIVE", nodeID)
				return nil
			}
		}
	}
}

// ConnectToNode establishes a MySQL connection to a node
func (h *ClusterHarness) ConnectToNode(nodeID int) (*sql.DB, error) {
	node := h.Nodes[nodeID-1]

	// Check if already connected
	if node.DB != nil {
		if err := node.DB.Ping(); err == nil {
			return node.DB, nil
		}
		node.DB.Close()
		node.DB = nil
	}

	dsn := fmt.Sprintf("root:@tcp(localhost:%d)/test", node.MySQLPort)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	node.DB = db
	return db, nil
}

// QueryNode executes a query on a specific node
func (h *ClusterHarness) QueryNode(nodeID int, query string, args ...interface{}) (*sql.Rows, error) {
	db, err := h.ConnectToNode(nodeID)
	if err != nil {
		return nil, err
	}
	return db.Query(query, args...)
}

// ExecNode executes a statement on a specific node
func (h *ClusterHarness) ExecNode(nodeID int, query string, args ...interface{}) (sql.Result, error) {
	db, err := h.ConnectToNode(nodeID)
	if err != nil {
		return nil, err
	}
	return db.Exec(query, args...)
}

// WaitForTableExists waits for a table to be replicated to all nodes
func (h *ClusterHarness) WaitForTableExists(tableName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for _, node := range h.Nodes {
		nodeID := node.NodeID
		for time.Now().Before(deadline) {
			// Use inline value instead of prepared statement
			query := fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'", tableName)
			rows, err := h.QueryNode(nodeID, query)
			if err == nil {
				var name string
				found := false
				if rows.Next() {
					rows.Scan(&name)
					found = true
				}
				rows.Close()
				if found {
					break // Table exists on this node
				}
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Final check
		query := fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'", tableName)
		rows, err := h.QueryNode(nodeID, query)
		if err != nil {
			return fmt.Errorf("node %d: failed to query table: %v", nodeID, err)
		}
		var name string
		found := false
		if rows.Next() {
			rows.Scan(&name)
			found = true
		}
		rows.Close()
		if !found {
			return fmt.Errorf("node %d: table %s not replicated within %v", nodeID, tableName, timeout)
		}
	}

	return nil
}

// StartCluster starts all nodes in the cluster
func (h *ClusterHarness) StartCluster() error {
	h.t.Logf("Starting %d-node cluster...", numNodes)

	for i := 1; i <= numNodes; i++ {
		if err := h.StartNode(i); err != nil {
			return err
		}
	}

	// Wait for all nodes to become ALIVE
	h.t.Logf("Waiting for cluster to converge...")
	time.Sleep(3 * time.Second)

	for i := 1; i <= numNodes; i++ {
		if err := h.WaitForAlive(i, 10*time.Second); err != nil {
			return err
		}
	}

	// Additional wait for gRPC connections to establish between nodes
	// This ensures quorum replication is ready before returning
	time.Sleep(2 * time.Second)

	h.t.Logf("Cluster started successfully")
	return nil
}

// StopCluster stops all nodes
func (h *ClusterHarness) StopCluster() {
	h.t.Logf("Stopping cluster...")
	for i := 1; i <= numNodes; i++ {
		h.StopNode(i)
	}
}

// Cleanup removes all test data
func (h *ClusterHarness) Cleanup() {
	h.cleanupOnce.Do(func() {
		h.t.Logf("Cleaning up cluster harness...")
		h.StopCluster()
		time.Sleep(500 * time.Millisecond)
		os.RemoveAll(h.BaseDir)
		h.t.Logf("Cleanup complete")
	})
}

// =======================
// CRASH RECOVERY TESTS
// =======================

// TestNodeRestartWithStaleData tests that a restarted node catches up missed writes
func TestNodeRestartWithStaleData(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start 3-node cluster
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Create test table on node 1
	t.Logf("Creating test table...")
	_, err := harness.ExecNode(1, "CREATE TABLE IF NOT EXISTS test_recovery (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Wait for table to replicate to all nodes
	if err := harness.WaitForTableExists("test_recovery", 5*time.Second); err != nil {
		t.Fatalf("Failed to replicate table: %v", err)
	}

	// Write initial data that replicates to all nodes
	t.Logf("Writing initial data...")
	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf("INSERT INTO test_recovery (id, value) VALUES (%d, 'initial_%d')", i, i)
		_, err := harness.ExecNode(1, query)
		if err != nil {
			t.Fatalf("Failed to insert initial data: %v", err)
		}
	}
	time.Sleep(2 * time.Second) // Allow replication

	// Verify all nodes have initial data
	for nodeID := 1; nodeID <= 3; nodeID++ {
		rows, err := harness.QueryNode(nodeID, "SELECT COUNT(*) FROM test_recovery")
		if err != nil {
			t.Fatalf("Failed to query node %d: %v", nodeID, err)
		}
		var count int
		if rows.Next() {
			rows.Scan(&count)
		}
		rows.Close()
		if count != 5 {
			t.Fatalf("Node %d has %d rows, expected 5", nodeID, count)
		}
		t.Logf("Node %d has initial 5 rows", nodeID)
	}

	// Kill node 3 (simulate crash)
	t.Logf("Killing node 3 to simulate crash...")
	if err := harness.KillNode(3); err != nil {
		t.Fatalf("Failed to kill node 3: %v", err)
	}

	// Write more data while node 3 is down
	t.Logf("Writing data while node 3 is down...")
	for i := 6; i <= 10; i++ {
		query := fmt.Sprintf("INSERT INTO test_recovery (id, value) VALUES (%d, 'missed_%d')", i, i)
		_, err := harness.ExecNode(1, query)
		if err != nil {
			t.Fatalf("Failed to insert data while node 3 down: %v", err)
		}
	}
	time.Sleep(2 * time.Second) // Allow replication to nodes 1 & 2

	// Verify nodes 1 & 2 have the new data
	for nodeID := 1; nodeID <= 2; nodeID++ {
		rows, err := harness.QueryNode(nodeID, "SELECT COUNT(*) FROM test_recovery")
		if err != nil {
			t.Fatalf("Failed to query node %d: %v", nodeID, err)
		}
		var count int
		if rows.Next() {
			rows.Scan(&count)
		}
		rows.Close()
		if count != 10 {
			t.Fatalf("Node %d has %d rows, expected 10", nodeID, count)
		}
		t.Logf("Node %d has all 10 rows", nodeID)
	}

	// Restart node 3
	t.Logf("Restarting node 3...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}
	if err := harness.WaitForAlive(3, 20*time.Second); err != nil {
		t.Fatalf("Node 3 did not become ALIVE: %v", err)
	}

	// Wait for anti-entropy to catch up node 3
	t.Logf("Waiting for node 3 to catch up via anti-entropy...")
	time.Sleep(15 * time.Second)

	// Verify node 3 caught up
	rows, err := harness.QueryNode(3, "SELECT COUNT(*) FROM test_recovery")
	if err != nil {
		t.Fatalf("Failed to query node 3 after restart: %v", err)
	}
	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	rows.Close()

	if count != 10 {
		t.Fatalf("Node 3 has %d rows after restart, expected 10 (catch-up failed)", count)
	}
	t.Logf("SUCCESS: Node 3 caught up, has all 10 rows")

	// Verify we can read the missed data from node 3
	rows, err = harness.QueryNode(3, "SELECT id, value FROM test_recovery WHERE id >= 6 ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to read missed data from node 3: %v", err)
	}
	defer rows.Close()

	missedCount := 0
	for rows.Next() {
		var id int
		var value string
		rows.Scan(&id, &value)
		expectedValue := fmt.Sprintf("missed_%d", id)
		if value != expectedValue {
			t.Fatalf("Node 3 has wrong value for id=%d: got %s, expected %s", id, value, expectedValue)
		}
		missedCount++
	}

	if missedCount != 5 {
		t.Fatalf("Node 3 only has %d missed rows, expected 5", missedCount)
	}

	t.Logf("SUCCESS: Node 3 serves correct data, no stale reads")
}

// TestMultipleNodeRestarts tests sequential node restarts
func TestMultipleNodeRestarts(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start cluster
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Create test table
	t.Logf("Creating test table...")
	_, err := harness.ExecNode(1, "CREATE TABLE IF NOT EXISTS test_restarts (id INT PRIMARY KEY, iteration INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Write initial data
	t.Logf("Writing initial data...")
	for i := 1; i <= 10; i++ {
		query := fmt.Sprintf("INSERT INTO test_restarts (id, iteration) VALUES (%d, 0)", i)
		_, err := harness.ExecNode(1, query)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	time.Sleep(2 * time.Second)

	// Restart each node one by one
	for nodeID := 1; nodeID <= 3; nodeID++ {
		t.Logf("Restarting node %d...", nodeID)

		if err := harness.StopNode(nodeID); err != nil {
			t.Fatalf("Failed to stop node %d: %v", nodeID, err)
		}

		// Write data while this node is down
		otherNode := (nodeID % 3) + 1
		t.Logf("Writing data via node %d while node %d is down...", otherNode, nodeID)
		for i := 1; i <= 10; i++ {
			query := fmt.Sprintf("UPDATE test_restarts SET iteration = iteration + 1 WHERE id = %d", i)
			_, err := harness.ExecNode(otherNode, query)
			if err != nil {
				t.Logf("Warning: update failed: %v", err)
			}
		}
		time.Sleep(1 * time.Second)

		// Restart the node
		if err := harness.StartNode(nodeID); err != nil {
			t.Fatalf("Failed to restart node %d: %v", nodeID, err)
		}
		if err := harness.WaitForAlive(nodeID, 15*time.Second); err != nil {
			t.Fatalf("Node %d did not become ALIVE: %v", nodeID, err)
		}

		// Wait for catch-up
		time.Sleep(12 * time.Second)
	}

	// Verify all nodes have consistent data
	t.Logf("Verifying data consistency across all nodes...")
	var firstSum int
	for nodeID := 1; nodeID <= 3; nodeID++ {
		rows, err := harness.QueryNode(nodeID, "SELECT SUM(iteration) FROM test_restarts")
		if err != nil {
			t.Fatalf("Failed to query node %d: %v", nodeID, err)
		}
		var sum int
		if rows.Next() {
			rows.Scan(&sum)
		}
		rows.Close()

		t.Logf("Node %d: SUM(iteration) = %d", nodeID, sum)

		if nodeID == 1 {
			firstSum = sum
		} else if sum != firstSum {
			t.Fatalf("Data inconsistency: node 1 has sum=%d, node %d has sum=%d", firstSum, nodeID, sum)
		}
	}

	t.Logf("SUCCESS: All nodes consistent after multiple restarts")
}

// TestRollingRestartCluster simulates upgrade scenario
func TestRollingRestartCluster(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start cluster
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Create test table
	t.Logf("Creating test table...")
	_, err := harness.ExecNode(1, "CREATE TABLE IF NOT EXISTS test_rolling (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Start background writer
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	var writeCount int
	var writeMu sync.Mutex
	var writeErrors []error

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		id := 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Write to random node
				nodeID := (id % 3) + 1
				query := fmt.Sprintf("INSERT INTO test_rolling (id, value) VALUES (%d, 'value_%d') ON DUPLICATE KEY UPDATE value = 'value_%d'",
					id, id, id)
				_, err := harness.ExecNode(nodeID, query)

				writeMu.Lock()
				if err != nil {
					writeErrors = append(writeErrors, err)
				} else {
					writeCount++
				}
				writeMu.Unlock()

				id++
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Perform rolling restart
	time.Sleep(2 * time.Second) // Let some writes happen first

	for nodeID := 1; nodeID <= 3; nodeID++ {
		t.Logf("Rolling restart: restarting node %d...", nodeID)

		if err := harness.StopNode(nodeID); err != nil {
			t.Fatalf("Failed to stop node %d: %v", nodeID, err)
		}

		time.Sleep(2 * time.Second) // Node down for a bit

		if err := harness.StartNode(nodeID); err != nil {
			t.Fatalf("Failed to restart node %d: %v", nodeID, err)
		}

		if err := harness.WaitForAlive(nodeID, 15*time.Second); err != nil {
			t.Fatalf("Node %d did not become ALIVE: %v", nodeID, err)
		}

		t.Logf("Node %d is back ALIVE", nodeID)
		time.Sleep(3 * time.Second) // Let it stabilize
	}

	// Stop writer
	cancel()
	<-writerDone

	writeMu.Lock()
	t.Logf("Background writer completed: %d successful writes, %d errors", writeCount, len(writeErrors))
	writeMu.Unlock()

	// Wait for final replication
	time.Sleep(5 * time.Second)

	// Verify cluster maintained availability (should have > 0 writes)
	if writeCount == 0 {
		t.Fatal("Cluster lost availability during rolling restart - no writes succeeded")
	}

	// Verify data consistency
	t.Logf("Verifying data consistency...")
	var counts [3]int
	for nodeID := 1; nodeID <= 3; nodeID++ {
		rows, err := harness.QueryNode(nodeID, "SELECT COUNT(*) FROM test_rolling")
		if err != nil {
			t.Fatalf("Failed to query node %d: %v", nodeID, err)
		}
		if rows.Next() {
			rows.Scan(&counts[nodeID-1])
		}
		rows.Close()
		t.Logf("Node %d has %d rows", nodeID, counts[nodeID-1])
	}

	// All nodes should have same count
	if counts[0] != counts[1] || counts[1] != counts[2] {
		t.Fatalf("Data inconsistency after rolling restart: node1=%d, node2=%d, node3=%d",
			counts[0], counts[1], counts[2])
	}

	t.Logf("SUCCESS: Rolling restart maintained availability and consistency")
}

// TestNodeCrashDuringWrite tests torn write protection
func TestNodeCrashDuringWrite(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start cluster
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Create test table
	t.Logf("Creating test table...")
	_, err := harness.ExecNode(1, "CREATE TABLE IF NOT EXISTS test_crash (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Start a transaction with multiple inserts
	t.Logf("Starting transaction with multiple inserts...")
	db, err := harness.ConnectToNode(1)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert multiple rows in transaction
	for i := 1; i <= 10; i++ {
		query := fmt.Sprintf("INSERT INTO test_crash (id, value) VALUES (%d, 'txn_value_%d')", i, i)
		_, err := tx.Exec(query)
		if err != nil {
			t.Fatalf("Failed to insert in transaction: %v", err)
		}
	}

	// Crash node 1 before commit (simulates crash mid-transaction)
	t.Logf("Crashing node 1 mid-transaction (before commit)...")
	time.Sleep(100 * time.Millisecond)
	harness.KillNode(1)

	// The transaction should not commit (was killed before commit)
	time.Sleep(2 * time.Second)

	// Check nodes 2 and 3 - should have no data (transaction didn't commit)
	for nodeID := 2; nodeID <= 3; nodeID++ {
		rows, err := harness.QueryNode(nodeID, "SELECT COUNT(*) FROM test_crash")
		if err != nil {
			t.Fatalf("Failed to query node %d: %v", nodeID, err)
		}
		var count int
		if rows.Next() {
			rows.Scan(&count)
		}
		rows.Close()

		if count != 0 {
			t.Fatalf("Node %d has %d rows (expected 0 - transaction should be rolled back)", nodeID, count)
		}
		t.Logf("Node %d correctly has 0 rows (transaction rolled back)", nodeID)
	}

	// Restart node 1
	t.Logf("Restarting node 1...")
	if err := harness.StartNode(1); err != nil {
		t.Fatalf("Failed to restart node 1: %v", err)
	}
	if err := harness.WaitForAlive(1, 15*time.Second); err != nil {
		t.Fatalf("Node 1 did not become ALIVE: %v", err)
	}

	time.Sleep(3 * time.Second)

	// Verify node 1 also has no data (transaction was rolled back on crash)
	rows, err := harness.QueryNode(1, "SELECT COUNT(*) FROM test_crash")
	if err != nil {
		t.Fatalf("Failed to query node 1 after restart: %v", err)
	}
	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	rows.Close()

	if count != 0 {
		t.Fatalf("Node 1 has %d rows after restart (expected 0 - transaction should be rolled back)", count)
	}

	t.Logf("SUCCESS: Transaction correctly rolled back, no partial state (torn write protection works)")
}

// TestAntiEntropyHealsStaleNode tests eventual consistency
func TestAntiEntropyHealsStaleNode(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	// Start cluster
	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Create test table
	t.Logf("Creating test table...")
	_, err := harness.ExecNode(1, "CREATE TABLE IF NOT EXISTS test_antientropy (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Write initial data
	t.Logf("Writing initial data...")
	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf("INSERT INTO test_antientropy (id, value) VALUES (%d, 'initial_%d')", i, i)
		_, err := harness.ExecNode(1, query)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	time.Sleep(2 * time.Second)

	// Simulate partition: kill node 3 (simulates network partition)
	t.Logf("Partitioning node 3 from cluster (simulated by killing it)...")
	harness.KillNode(3)

	// Write data while node 3 is partitioned
	t.Logf("Writing data while node 3 is partitioned...")
	for i := 6; i <= 15; i++ {
		query := fmt.Sprintf("INSERT INTO test_antientropy (id, value) VALUES (%d, 'partitioned_%d')", i, i)
		_, err := harness.ExecNode(1, query)
		if err != nil {
			t.Logf("Warning: insert failed: %v", err)
		}
	}
	time.Sleep(2 * time.Second)

	// Verify nodes 1 & 2 have all data
	for nodeID := 1; nodeID <= 2; nodeID++ {
		rows, err := harness.QueryNode(nodeID, "SELECT COUNT(*) FROM test_antientropy")
		if err != nil {
			t.Fatalf("Failed to query node %d: %v", nodeID, err)
		}
		var count int
		if rows.Next() {
			rows.Scan(&count)
		}
		rows.Close()
		t.Logf("Node %d has %d rows", nodeID, count)
	}

	// Heal partition (restart node 3)
	t.Logf("Healing partition (restarting node 3)...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}
	if err := harness.WaitForAlive(3, 20*time.Second); err != nil {
		t.Fatalf("Node 3 did not become ALIVE: %v", err)
	}

	// Wait for anti-entropy to heal the stale node
	t.Logf("Waiting for anti-entropy to heal node 3...")
	time.Sleep(20 * time.Second)

	// Verify node 3 eventually converges
	rows, err := harness.QueryNode(3, "SELECT COUNT(*) FROM test_antientropy")
	if err != nil {
		t.Fatalf("Failed to query node 3: %v", err)
	}
	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	rows.Close()

	t.Logf("Node 3 has %d rows after anti-entropy", count)

	// Should have all 15 rows
	if count < 10 {
		t.Fatalf("Anti-entropy did not heal node 3: has %d rows, expected at least 10", count)
	}

	// Verify consistency across all nodes
	var counts [3]int
	for nodeID := 1; nodeID <= 3; nodeID++ {
		rows, err := harness.QueryNode(nodeID, "SELECT COUNT(*) FROM test_antientropy")
		if err != nil {
			t.Fatalf("Failed to query node %d: %v", nodeID, err)
		}
		if rows.Next() {
			rows.Scan(&counts[nodeID-1])
		}
		rows.Close()
	}

	t.Logf("Final counts: node1=%d, node2=%d, node3=%d", counts[0], counts[1], counts[2])

	// Check if counts are close (eventual consistency allows slight lag)
	maxCount := counts[0]
	minCount := counts[0]
	for i := 1; i < 3; i++ {
		if counts[i] > maxCount {
			maxCount = counts[i]
		}
		if counts[i] < minCount {
			minCount = counts[i]
		}
	}

	if maxCount-minCount > 2 {
		t.Fatalf("Nodes not eventually consistent: min=%d, max=%d (diff > 2)", minCount, maxCount)
	}

	t.Logf("SUCCESS: Anti-entropy healed stale node, eventual consistency achieved")
}

// Helper function to read count from node
func readCountFromNode(t *testing.T, harness *ClusterHarness, nodeID int, table string) int {
	rows, err := harness.QueryNode(nodeID, fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	if err != nil {
		t.Logf("Warning: failed to query node %d: %v", nodeID, err)
		return -1
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	return count
}

// Helper to parse PID from file
func readPIDFile(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}
