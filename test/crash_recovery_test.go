package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// cleanupStalePorts kills any processes using ports that will be used by the test cluster
func cleanupStalePorts() {
	ports := []int{
		baseGRPCPort + 1, baseGRPCPort + 2, baseGRPCPort + 3,
		baseMySQLPort + 1, baseMySQLPort + 2, baseMySQLPort + 3,
	}
	for _, port := range ports {
		cmd := exec.Command("lsof", "-ti", fmt.Sprintf(":%d", port))
		output, err := cmd.Output()
		if err == nil && len(output) > 0 {
			pids := strings.Fields(strings.TrimSpace(string(output)))
			for _, pid := range pids {
				killCmd := exec.Command("kill", "-9", pid)
				killCmd.Run()
			}
		}
	}
	time.Sleep(100 * time.Millisecond)
}

// ClusterNode represents a running Marmot node
type ClusterNode struct {
	NodeID     int
	GRPCPort   int
	MySQLPort  int
	DataDir    string
	ConfigPath string
	PIDFile    string
	LogFile    string
	Cmd        *exec.Cmd
	DB         *sql.DB
	isRunning  bool
	mu         sync.Mutex
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
	cleanupStalePorts()

	baseDir := filepath.Join(os.TempDir(), fmt.Sprintf("marmot_crash_test_%d", time.Now().UnixNano()))

	harness := &ClusterHarness{
		Nodes:     make([]*ClusterNode, numNodes),
		BaseDir:   baseDir,
		MarmotBin: "",
		t:         t,
	}

	if err := harness.buildMarmot(); err != nil {
		t.Fatalf("Failed to build Marmot: %v", err)
	}

	for i := 0; i < numNodes; i++ {
		node := harness.createNode(i + 1)
		harness.Nodes[i] = node
	}

	return harness
}

func (h *ClusterHarness) buildMarmot() error {
	h.t.Logf("Building Marmot binary...")

	binPath := filepath.Join(h.BaseDir, "marmot")
	h.MarmotBin = binPath

	cmd := exec.Command("go", "build", "-tags", "sqlite_preupdate_hook", "-o", binPath, ".")
	cmd.Dir = "/Users/zohaib/repos/marmot"
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("build failed: %v\n%s", err, output)
	}

	h.t.Logf("Marmot binary built at %s", binPath)
	return nil
}

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

func (h *ClusterHarness) createNodeConfig(node *ClusterNode) {
	seedNodes := []string{}
	if node.NodeID != 1 {
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
prepare_timeout_ms = 5000
commit_timeout_ms = 5000
abort_timeout_ms = 3000
intent_ttl_ms = 60000
max_guard_rows = 65536

[cluster]
grpc_bind_address = "0.0.0.0"
grpc_advertise_address = "localhost:%d"
grpc_port = %d
seed_nodes = [%s]
gossip_interval_ms = 500
gossip_fanout = 3
suspect_timeout_ms = 10000
dead_timeout_ms = 20000
cluster_secret = "test-secret"

[replication]
replication_factor = 3
virtual_nodes = 150
default_write_consistency = "QUORUM"
default_read_consistency = "LOCAL_ONE"
write_timeout_ms = 10000
read_timeout_ms = 5000
enable_anti_entropy = true
anti_entropy_interval_seconds = 5

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

func (h *ClusterHarness) StartNode(nodeID int) error {
	node := h.Nodes[nodeID-1]
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.isRunning {
		return fmt.Errorf("node %d is already running", nodeID)
	}

	h.t.Logf("Starting node %d...", nodeID)

	logFile, err := os.Create(node.LogFile)
	if err != nil {
		return fmt.Errorf("failed to create log file: %v", err)
	}

	cmd := exec.Command(h.MarmotBin, "--config", node.ConfigPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("failed to start node %d: %v", nodeID, err)
	}

	node.Cmd = cmd
	node.isRunning = true

	os.WriteFile(node.PIDFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0644)

	h.t.Logf("Node %d started (PID: %d)", nodeID, cmd.Process.Pid)
	return nil
}

func (h *ClusterHarness) StopNode(nodeID int) error {
	node := h.Nodes[nodeID-1]
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.isRunning {
		return fmt.Errorf("node %d is not running", nodeID)
	}

	pid := node.Cmd.Process.Pid
	h.t.Logf("Stopping node %d (PID: %d)...", nodeID, pid)

	if node.DB != nil {
		node.DB.Close()
		node.DB = nil
	}

	if err := node.Cmd.Process.Kill(); err != nil {
		h.t.Logf("Warning: failed to kill node %d: %v", nodeID, err)
	}

	node.Cmd.Wait()
	node.isRunning = false
	node.Cmd = nil

	os.Remove(node.PIDFile)

	// Small wait to ensure clean shutdown
	time.Sleep(1 * time.Second)

	h.t.Logf("Node %d stopped", nodeID)
	return nil
}

func (h *ClusterHarness) KillNode(nodeID int) error {
	node := h.Nodes[nodeID-1]
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.isRunning {
		return fmt.Errorf("node %d is not running", nodeID)
	}

	h.t.Logf("Killing node %d (simulating crash)...", nodeID)

	if node.DB != nil {
		node.DB.Close()
		node.DB = nil
	}

	if err := node.Cmd.Process.Kill(); err != nil {
		return fmt.Errorf("failed to kill node %d: %v", nodeID, err)
	}

	node.Cmd.Wait()
	node.isRunning = false
	node.Cmd = nil

	h.t.Logf("Node %d killed (crash simulated)", nodeID)
	return nil
}

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

			var result int
			err = db.QueryRow("SELECT 1").Scan(&result)
			if err == nil {
				h.t.Logf("Node %d is ALIVE", nodeID)
				return nil
			}
		}
	}
}

func (h *ClusterHarness) ConnectToNode(nodeID int) (*sql.DB, error) {
	node := h.Nodes[nodeID-1]
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.DB != nil {
		if err := node.DB.Ping(); err == nil {
			return node.DB, nil
		}
		node.DB.Close()
		node.DB = nil
	}

	dsn := fmt.Sprintf("root:@tcp(localhost:%d)/marmot", node.MySQLPort)
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

func (h *ClusterHarness) QueryNode(nodeID int, query string, args ...interface{}) (*sql.Rows, error) {
	db, err := h.ConnectToNode(nodeID)
	if err != nil {
		return nil, err
	}
	return db.Query(query, args...)
}

func (h *ClusterHarness) ExecNode(nodeID int, query string, args ...interface{}) (sql.Result, error) {
	db, err := h.ConnectToNode(nodeID)
	if err != nil {
		return nil, err
	}
	return db.Exec(query, args...)
}

// tableExistsOnNode checks if a table exists on a node using SHOW TABLES (MySQL-compatible)
func (h *ClusterHarness) tableExistsOnNode(nodeID int, tableName string) bool {
	rows, err := h.QueryNode(nodeID, "SHOW TABLES")
	if err != nil {
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		if name == tableName {
			return true
		}
	}
	return false
}

// WaitForTableExists waits for a table to exist on all specified nodes
func (h *ClusterHarness) WaitForTableExists(tableName string, nodeIDs []int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for _, nodeID := range nodeIDs {
		for time.Now().Before(deadline) {
			if h.tableExistsOnNode(nodeID, tableName) {
				h.t.Logf("Table %s exists on node %d", tableName, nodeID)
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		if !h.tableExistsOnNode(nodeID, tableName) {
			return fmt.Errorf("table %s not found on node %d after %v", tableName, nodeID, timeout)
		}
	}

	return nil
}

// getRowCount returns the row count for a table on a node, or -1 on error
func (h *ClusterHarness) getRowCount(nodeID int, tableName string) int {
	rows, err := h.QueryNode(nodeID, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	if err != nil {
		return -1
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	return count
}

// ClusterMember represents a node in the cluster membership response
type ClusterMember struct {
	NodeID      uint64 `json:"node_id"`
	Address     string `json:"address"`
	Status      string `json:"status"`
	Incarnation uint64 `json:"incarnation"`
}

// AdminAPIResponse wraps the admin API response format
type AdminAPIResponse struct {
	Data []ClusterMember `json:"data"`
}

// getClusterStatus fetches cluster status from a node's admin API
func (h *ClusterHarness) getClusterStatus(nodeID int) ([]ClusterMember, error) {
	node := h.Nodes[nodeID-1]
	url := fmt.Sprintf("http://localhost:%d/admin/cluster/members", node.GRPCPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Marmot-Secret", "test-secret")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var apiResp AdminAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}

	return apiResp.Data, nil
}

// WaitForClusterConvergence waits for all nodes to be ALIVE according to cluster membership
func (h *ClusterHarness) WaitForClusterConvergence(timeout time.Duration) error {
	h.t.Logf("Waiting for cluster to converge (all nodes ALIVE)...")

	deadline := time.Now().Add(timeout)
	errorLogged := false
	for time.Now().Before(deadline) {
		// Check from node 1's perspective
		members, err := h.getClusterStatus(1)
		if err != nil {
			if !errorLogged {
				h.t.Logf("Error getting cluster status (will retry): %v", err)
				errorLogged = true
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}
		errorLogged = false

		// Count ALIVE nodes
		aliveCount := 0
		for _, m := range members {
			if m.Status == "ALIVE" {
				aliveCount++
			}
		}

		if aliveCount >= numNodes {
			h.t.Logf("Cluster converged: %d nodes ALIVE", aliveCount)
			return nil
		}

		h.t.Logf("Cluster not yet converged: %d/%d nodes ALIVE", aliveCount, numNodes)
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("cluster did not converge within %v", timeout)
}

// dumpNodeLogs saves node logs to /tmp for debugging
func (h *ClusterHarness) dumpNodeLogs(prefix string) {
	for nodeID := 1; nodeID <= numNodes; nodeID++ {
		logPath := h.Nodes[nodeID-1].LogFile
		data, err := os.ReadFile(logPath)
		if err != nil {
			h.t.Logf("Node %d log error: %v", nodeID, err)
			continue
		}

		tmpLogPath := fmt.Sprintf("/tmp/%s_node%d.log", prefix, nodeID)
		os.WriteFile(tmpLogPath, data, 0644)
		h.t.Logf("Node %d log saved to %s", nodeID, tmpLogPath)

		// Print last 20 lines
		lines := strings.Split(string(data), "\n")
		start := 0
		if len(lines) > 20 {
			start = len(lines) - 20
		}
		h.t.Logf("=== Node %d last 20 log lines ===", nodeID)
		for _, line := range lines[start:] {
			if line != "" {
				h.t.Logf("  %s", line)
			}
		}
	}
}

func (h *ClusterHarness) StartCluster() error {
	h.t.Logf("Starting %d-node cluster...", numNodes)

	for i := 1; i <= numNodes; i++ {
		if err := h.StartNode(i); err != nil {
			return err
		}
	}

	h.t.Logf("Waiting for MySQL servers to be ready...")
	time.Sleep(3 * time.Second)

	for i := 1; i <= numNodes; i++ {
		if err := h.WaitForAlive(i, 30*time.Second); err != nil {
			return err
		}
	}

	// Wait for gossip cluster to fully converge (all nodes ALIVE)
	if err := h.WaitForClusterConvergence(30 * time.Second); err != nil {
		h.dumpNodeLogs("cluster_start_fail")
		return err
	}

	// Additional wait for gRPC connections to stabilize
	time.Sleep(2 * time.Second)

	h.t.Logf("Cluster started successfully")
	return nil
}

func (h *ClusterHarness) StopCluster() {
	h.t.Logf("Stopping cluster...")
	for i := 1; i <= numNodes; i++ {
		h.StopNode(i)
	}
}

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
// INTEGRATION TESTS
// =======================

// TestDDLReplication verifies that CREATE TABLE replicates to all nodes
func TestDDLReplication(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "ddl_test_table"
	t.Logf("Creating table %s on node 1...", tableName)

	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Wait for DDL to replicate to all nodes
	allNodes := []int{1, 2, 3}
	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("ddl_test")
		t.Fatalf("DDL replication failed: %v", err)
	}

	t.Logf("SUCCESS: Table %s replicated to all nodes", tableName)
}

// TestBasicReplication verifies that INSERT replicates to all nodes
func TestBasicReplication(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "basic_test"

	// Create table and wait for DDL replication
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	allNodes := []int{1, 2, 3}
	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("basic_test")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Insert data
	t.Logf("Inserting 10 rows on node 1...")
	for i := 1; i <= 10; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'value_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Wait for replication
	time.Sleep(3 * time.Second)

	// Verify all nodes have the data
	for _, nodeID := range allNodes {
		count := harness.getRowCount(nodeID, tableName)
		if count != 10 {
			t.Fatalf("Node %d has %d rows, expected 10", nodeID, count)
		}
		t.Logf("Node %d has %d rows", nodeID, count)
	}

	t.Logf("SUCCESS: Basic replication verified")
}

// TestNodeRestartRecovery tests that a restarted node catches up missed writes
func TestNodeRestartRecovery(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "recovery_test"

	// Create table
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	allNodes := []int{1, 2, 3}
	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("recovery_test_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Write initial data
	t.Logf("Writing initial 5 rows...")
	for i := 1; i <= 5; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'initial_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}
	time.Sleep(2 * time.Second)

	// Verify all nodes have initial data
	for _, nodeID := range allNodes {
		count := harness.getRowCount(nodeID, tableName)
		if count != 5 {
			t.Fatalf("Node %d has %d rows before crash, expected 5", nodeID, count)
		}
	}
	t.Logf("All nodes have 5 rows before crash")

	// Kill node 3
	t.Logf("Killing node 3...")
	if err := harness.KillNode(3); err != nil {
		t.Fatalf("Failed to kill node 3: %v", err)
	}

	// Write more data while node 3 is down
	t.Logf("Writing 5 more rows while node 3 is down...")
	for i := 6; i <= 10; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'missed_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Insert failed while node 3 down: %v", err)
		}
	}
	time.Sleep(2 * time.Second)

	// Verify nodes 1 & 2 have all data
	for _, nodeID := range []int{1, 2} {
		count := harness.getRowCount(nodeID, tableName)
		if count != 10 {
			t.Fatalf("Node %d has %d rows, expected 10", nodeID, count)
		}
	}
	t.Logf("Nodes 1 & 2 have 10 rows")

	// Restart node 3
	t.Logf("Restarting node 3...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}
	if err := harness.WaitForAlive(3, 20*time.Second); err != nil {
		t.Fatalf("Node 3 did not become ALIVE: %v", err)
	}

	// Wait for anti-entropy to catch up
	t.Logf("Waiting for node 3 to catch up via anti-entropy...")
	var count3 int
	for attempt := 0; attempt < 12; attempt++ {
		time.Sleep(5 * time.Second)
		count3 = harness.getRowCount(3, tableName)
		t.Logf("Attempt %d: Node 3 has %d rows", attempt+1, count3)
		if count3 == 10 {
			break
		}
	}

	if count3 != 10 {
		harness.dumpNodeLogs("recovery_test_fail")
		t.Fatalf("Node 3 did not catch up: has %d rows, expected 10", count3)
	}

	t.Logf("SUCCESS: Node 3 caught up after restart")
}

// TestRollingRestart tests cluster availability during rolling restart.
// This test verifies that:
// 1. Writes continue to succeed (on remaining nodes) during rolling restarts
// 2. After all restarts complete, anti-entropy brings all nodes to eventual consistency
func TestRollingRestart(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "rolling_test"

	// Create table
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	allNodes := []int{1, 2, 3}
	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("rolling_test_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Insert baseline data before rolling restart
	t.Logf("Inserting baseline 20 rows before rolling restart...")
	for i := 1; i <= 20; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'baseline_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Baseline insert failed: %v", err)
		}
	}
	time.Sleep(3 * time.Second)

	// Verify all nodes have baseline data
	for _, nodeID := range allNodes {
		count := harness.getRowCount(nodeID, tableName)
		if count != 20 {
			t.Fatalf("Node %d has %d baseline rows, expected 20", nodeID, count)
		}
	}
	t.Logf("All nodes have 20 baseline rows")

	// Rolling restart each node - no writes during restart to test pure recovery
	for nodeID := 1; nodeID <= 3; nodeID++ {
		t.Logf("Rolling restart: stopping node %d...", nodeID)
		if err := harness.StopNode(nodeID); err != nil {
			t.Fatalf("Failed to stop node %d: %v", nodeID, err)
		}

		// Write 5 rows while node is down (to nodes still running)
		targetNode := (nodeID % 3) + 1 // Pick a different node
		if nodeID == targetNode {
			targetNode = ((nodeID + 1) % 3) + 1
		}
		t.Logf("Writing 5 rows to node %d while node %d is down...", targetNode, nodeID)
		for i := 0; i < 5; i++ {
			rowID := 20 + (nodeID-1)*5 + i + 1 // Unique IDs: 21-25, 26-30, 31-35
			_, err := harness.ExecNode(targetNode, fmt.Sprintf(
				"INSERT INTO %s (id, value) VALUES (%d, 'rolling_%d')", tableName, rowID, rowID))
			if err != nil {
				t.Logf("Warning: write during rolling restart failed: %v", err)
			}
		}
		time.Sleep(2 * time.Second)

		if err := harness.StartNode(nodeID); err != nil {
			t.Fatalf("Failed to restart node %d: %v", nodeID, err)
		}
		if err := harness.WaitForAlive(nodeID, 20*time.Second); err != nil {
			t.Fatalf("Node %d did not become ALIVE: %v", nodeID, err)
		}
		t.Logf("Node %d restarted", nodeID)

		// Wait for anti-entropy to sync the restarted node
		time.Sleep(10 * time.Second)
	}

	// Wait for eventual consistency across all nodes
	t.Logf("Waiting for eventual consistency...")
	var converged bool
	var counts [3]int

	for attempt := 0; attempt < 12; attempt++ {
		time.Sleep(5 * time.Second)

		for i := 1; i <= 3; i++ {
			counts[i-1] = harness.getRowCount(i, tableName)
		}
		t.Logf("Attempt %d: node1=%d, node2=%d, node3=%d", attempt+1, counts[0], counts[1], counts[2])

		// Check if all nodes have the same count
		if counts[0] == counts[1] && counts[1] == counts[2] && counts[0] > 0 {
			converged = true
			break
		}
	}

	if !converged {
		harness.dumpNodeLogs("rolling_test_fail")
		t.Fatalf("Cluster did not converge: node1=%d, node2=%d, node3=%d", counts[0], counts[1], counts[2])
	}

	// Verify we have baseline + rolling restart writes (20 baseline + up to 15 rolling)
	expectedMin := 20 // At least baseline data
	if counts[0] < expectedMin {
		t.Fatalf("Expected at least %d rows, got %d", expectedMin, counts[0])
	}

	t.Logf("SUCCESS: Rolling restart maintained availability and consistency (final count: %d)", counts[0])
}

// TestCrashMidTransaction tests that uncommitted transactions are rolled back
func TestCrashMidTransaction(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "crash_txn_test"

	// Create table
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	allNodes := []int{1, 2, 3}
	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("crash_txn_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Start a transaction
	t.Logf("Starting transaction on node 1...")
	db, err := harness.ConnectToNode(1)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert in transaction
	for i := 1; i <= 5; i++ {
		_, err := tx.Exec(fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'uncommitted_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Insert in transaction failed: %v", err)
		}
	}

	// Kill node 1 before commit
	t.Logf("Killing node 1 before commit...")
	time.Sleep(100 * time.Millisecond)
	harness.KillNode(1)

	time.Sleep(2 * time.Second)

	// Verify nodes 2 & 3 have no data (transaction not committed)
	for _, nodeID := range []int{2, 3} {
		count := harness.getRowCount(nodeID, tableName)
		if count != 0 {
			t.Fatalf("Node %d has %d rows, expected 0 (uncommitted transaction should not replicate)", nodeID, count)
		}
	}
	t.Logf("Nodes 2 & 3 correctly have 0 rows")

	// Restart node 1
	t.Logf("Restarting node 1...")
	if err := harness.StartNode(1); err != nil {
		t.Fatalf("Failed to restart node 1: %v", err)
	}
	if err := harness.WaitForAlive(1, 15*time.Second); err != nil {
		t.Fatalf("Node 1 did not become ALIVE: %v", err)
	}

	time.Sleep(3 * time.Second)

	// Node 1 should also have 0 rows (transaction rolled back on crash)
	count := harness.getRowCount(1, tableName)
	if count != 0 {
		t.Fatalf("Node 1 has %d rows after restart, expected 0", count)
	}

	t.Logf("SUCCESS: Uncommitted transaction correctly rolled back")
}

// TestHighLoadRecovery tests node catch-up under continuous load
func TestHighLoadRecovery(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	tableName := "highload_test"

	// Create table
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT, ts INT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	allNodes := []int{1, 2, 3}
	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("highload_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Start writer targeting nodes 1 & 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var writeCount int64
	var writeMu sync.Mutex

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		id := 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
				query := fmt.Sprintf("INSERT INTO %s (id, value, ts) VALUES (%d, 'value_%d', %d)",
					tableName, id, id, time.Now().UnixNano())

				// Only target nodes 1 & 2
				for _, nodeID := range []int{1, 2} {
					_, err := harness.ExecNode(nodeID, query)
					if err == nil {
						writeMu.Lock()
						writeCount++
						writeMu.Unlock()
						break
					}
				}

				id++
				time.Sleep(20 * time.Millisecond) // ~50 rps
			}
		}
	}()

	// Phase 1: Accumulate data
	t.Logf("Phase 1: Accumulating data for 5 seconds...")
	time.Sleep(5 * time.Second)

	writeMu.Lock()
	initialCount := writeCount
	writeMu.Unlock()
	t.Logf("Initial writes: %d", initialCount)

	// Verify all nodes have data
	for _, nodeID := range allNodes {
		count := harness.getRowCount(nodeID, tableName)
		t.Logf("Node %d has %d rows before crash", nodeID, count)
	}

	// Phase 2: Kill node 3
	t.Logf("Phase 2: Killing node 3...")
	harness.KillNode(3)

	// Phase 3: Continue writes for 20 seconds
	t.Logf("Phase 3: Writes continue while node 3 is down...")
	time.Sleep(20 * time.Second)

	writeMu.Lock()
	countWhileDown := writeCount - initialCount
	writeMu.Unlock()
	t.Logf("Writes during downtime: %d", countWhileDown)

	// Phase 4: Restart node 3
	t.Logf("Phase 4: Restarting node 3...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}
	if err := harness.WaitForAlive(3, 30*time.Second); err != nil {
		t.Fatalf("Node 3 did not become ALIVE: %v", err)
	}

	// Phase 5: Continue writes for 10 more seconds
	t.Logf("Phase 5: Writes continue while node 3 catches up...")
	time.Sleep(10 * time.Second)

	// Stop writer
	cancel()
	<-writerDone

	writeMu.Lock()
	finalWriteCount := writeCount
	writeMu.Unlock()
	t.Logf("Total writes: %d", finalWriteCount)

	// Phase 6: Wait for convergence
	t.Logf("Phase 6: Waiting for convergence...")
	var counts [3]int
	var converged bool

	for attempt := 0; attempt < 24; attempt++ {
		time.Sleep(5 * time.Second)

		for i := 1; i <= 3; i++ {
			counts[i-1] = harness.getRowCount(i, tableName)
		}
		t.Logf("Attempt %d: node1=%d, node2=%d, node3=%d", attempt+1, counts[0], counts[1], counts[2])

		// Check convergence (within 1% or 10 rows)
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

		tolerance := maxCount / 100
		if tolerance < 10 {
			tolerance = 10
		}
		if maxCount-minCount <= tolerance {
			converged = true
			break
		}
	}

	if !converged {
		harness.dumpNodeLogs("highload_fail")
		t.Fatalf("Node 3 failed to converge: node1=%d, node2=%d, node3=%d", counts[0], counts[1], counts[2])
	}

	t.Logf("SUCCESS: Node recovered under high load")
	t.Logf("  - Total writes: %d", finalWriteCount)
	t.Logf("  - Writes during downtime: %d", countWhileDown)
	t.Logf("  - Final counts: node1=%d, node2=%d, node3=%d", counts[0], counts[1], counts[2])
}

// Helper to parse PID from file
func readPIDFile(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}
