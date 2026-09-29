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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/maxpert/marmot/test/stallwatch"
)

const (
	baseGRPCPort  = 8080
	baseMySQLPort = 3306
	numNodes      = 3

	// clusterStallTimeout bounds how long a cluster may go with no progress:
	// no file written under any node's directory (logs, SQLite, meta store),
	// no client call returned (noteClientCall) and no progress reported by
	// the test (ClusterHarness.Progress).
	clusterStallTimeout = 20 * time.Second
	// clusterStallPoll is how often the stall watchdog looks.
	clusterStallPoll = 250 * time.Millisecond
	// clusterReadyDeadline bounds StartCluster's wait for each node to be
	// ready for writes (WaitForReady).
	clusterReadyDeadline = 20 * time.Second
	// clusterConvergencePoll is how often WaitForClusterConvergence asks a
	// node for its membership.
	clusterConvergencePoll = 250 * time.Millisecond
	// stallLogTailLines is how much of each node's log a stall report shows.
	stallLogTailLines = 30
	// stallStackWait bounds the wait for a node to write its goroutine dump.
	stallStackWait = 3 * time.Second

	// clusterTestBudget is the hard wall-clock budget for any test built on
	// ClusterHarness (user rule: every cluster test finishes in <60s). It is
	// enforced independently of the stall watchdog: a test can keep making
	// progress (writes, restarts) and still blow well past a sane budget.
	clusterTestBudget = 60 * time.Second

	// defaultGCIntervalSeconds is the harness's [replication] gc_interval_seconds.
	defaultGCIntervalSeconds = 30
	// defaultHeartbeatTimeoutSeconds is the harness's [transaction]
	// heartbeat_timeout_seconds.
	defaultHeartbeatTimeoutSeconds = 10
	// unsetOverride marks a ClusterHarness int override field as "use the
	// harness default" (0 is a real value for some of these fields, so it
	// cannot double as the sentinel).
	unsetOverride = -1
)

var (
	crashHarnessBuildOnce sync.Once
	crashHarnessBinPath   string
	crashHarnessBuildErr  error
)

// cleanupStalePorts kills any processes using ports that will be used by the test cluster
func cleanupStalePorts() {
	selfPID := os.Getpid()
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
				parsedPID, parseErr := strconv.Atoi(pid)
				if parseErr != nil {
					continue
				}
				if parsedPID == selfPID {
					continue
				}
				if !isMarmotProcess(parsedPID) {
					continue
				}
				killCmd := exec.Command("kill", "-9", pid)
				killCmd.Run()
			}
		}
	}
	time.Sleep(100 * time.Millisecond)
}

func isMarmotProcess(pid int) bool {
	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	commandLine := strings.ToLower(strings.TrimSpace(string(output)))
	if commandLine == "" {
		return false
	}
	if strings.Contains(commandLine, "go test") || strings.Contains(commandLine, "___go_build") {
		return false
	}
	return strings.Contains(commandLine, "marmot")
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
	watchdog    *stallwatch.Watchdog

	// NodeBin overrides MarmotBin for one node id, so a test can run a mix of
	// binaries (e.g. a rolling-upgrade test). Set it before StartNode.
	NodeBin map[int]string

	// GCIntervalSeconds overrides [replication] gc_interval_seconds on every
	// node's config. unsetOverride (the default) keeps defaultGCIntervalSeconds.
	GCIntervalSeconds int
	// GCMinRetentionHours overrides [replication] gc_min_retention_hours.
	// unsetOverride keeps the config's own default (2h).
	GCMinRetentionHours int
	// DeltaSyncThresholdSeconds overrides [replication] delta_sync_threshold_seconds.
	// unsetOverride keeps the config's own default (3600s). A live-GC test
	// needs this lowered together with GCMinRetentionHours: cfg.Validate
	// requires gc_min_retention_hours >= delta_sync_threshold_seconds in hours.
	DeltaSyncThresholdSeconds int
	// HeartbeatTimeoutSeconds overrides [transaction] heartbeat_timeout_seconds,
	// after which the stale-transaction GC aborts a PENDING transaction.
	// unsetOverride keeps defaultHeartbeatTimeoutSeconds.
	HeartbeatTimeoutSeconds int

	// budgetStart is when the harness (and so the test using it) began.
	budgetStart time.Time
	budgetDone  chan struct{}
	budgetOnce  sync.Once

	phaseMu   sync.Mutex
	lastPhase time.Time
	phases    []phaseRecord
}

// phaseRecord is one named phase of a cluster test and how long it took
// since the previous phase (or since the harness was created).
type phaseRecord struct {
	name    string
	elapsed time.Duration
}

// NewClusterHarness creates a new cluster harness for testing. Options run
// before any node config is written, so they can set GCIntervalSeconds and
// friends.
func NewClusterHarness(t *testing.T, opts ...func(*ClusterHarness)) *ClusterHarness {
	cleanupStalePorts()

	baseDir := filepath.Join(testDataRoot, fmt.Sprintf("marmot_crash_test_%d", time.Now().UnixNano()))
	marmotBin, err := getCrashHarnessBinary(t)
	if err != nil {
		t.Fatalf("Failed to build Marmot: %v", err)
	}

	now := time.Now()
	harness := &ClusterHarness{
		Nodes:                     make([]*ClusterNode, numNodes),
		BaseDir:                   baseDir,
		MarmotBin:                 marmotBin,
		t:                         t,
		GCIntervalSeconds:         unsetOverride,
		HeartbeatTimeoutSeconds:   unsetOverride,
		GCMinRetentionHours:       unsetOverride,
		DeltaSyncThresholdSeconds: unsetOverride,
		budgetStart:               now,
		budgetDone:                make(chan struct{}),
		lastPhase:                 now,
	}
	for _, opt := range opts {
		opt(harness)
	}

	for i := 0; i < numNodes; i++ {
		node := harness.createNode(i + 1)
		harness.Nodes[i] = node
	}
	harness.watchdog = stallwatch.Start(clusterStallTimeout, clusterStallPoll, harness.activity, harness.reportStall)
	go harness.watchBudget()

	return harness
}

// Progress tells the stall watchdog the test saw the cluster make progress,
// such as an ACKed write.
func (h *ClusterHarness) Progress() {
	h.watchdog.Progress()
}

// Phase logs how long this phase of the test took (since the previous phase,
// or since the harness was created) and records it for the budget report.
// Call it at each phase of a cluster test: start, load, kill/restart,
// converge, verify, ...
func (h *ClusterHarness) Phase(name string) {
	h.phaseMu.Lock()
	now := time.Now()
	elapsed := now.Sub(h.lastPhase)
	h.lastPhase = now
	h.phases = append(h.phases, phaseRecord{name: name, elapsed: elapsed})
	h.phaseMu.Unlock()
	h.t.Logf("phase=%s elapsed=%s", name, elapsed.Round(time.Millisecond))
}

// watchBudget fails the test once the harness has lived past clusterTestBudget,
// independently of the stall watchdog: a test can keep making progress and
// still blow well past the budget the user rule sets.
func (h *ClusterHarness) watchBudget() {
	timer := time.NewTimer(clusterTestBudget)
	defer timer.Stop()
	select {
	case <-timer.C:
		h.reportBudgetExceeded()
	case <-h.budgetDone:
	}
}

// reportBudgetExceeded fails the test with every recorded phase's elapsed
// time and every node's log tail, then stops the nodes so the test's own
// calls fail at once. A goroutine cannot call t.Fatalf, so this mirrors
// reportStall's pattern (t.Errorf + StopCluster).
func (h *ClusterHarness) reportBudgetExceeded() {
	h.t.Errorf("cluster test exceeded its %s budget", clusterTestBudget)
	h.phaseMu.Lock()
	for _, p := range h.phases {
		h.t.Logf("phase=%s elapsed=%s", p.name, p.elapsed.Round(time.Millisecond))
	}
	h.phaseMu.Unlock()
	for i := range h.Nodes {
		nodeID := i + 1
		h.t.Logf("=== node %d: last %d log lines ===\n%s", nodeID, stallLogTailLines, h.getNodeLogTail(nodeID, stallLogTailLines))
	}
	h.StopCluster()
}

// clientCalls counts client calls the tests' shared helpers completed
// (noteClientCall); the stall watchdog counts each as progress.
var clientCalls atomic.Int64

// noteClientCall records that one client call to a node returned, with or
// without an error: a cluster answering its clients is not stalled.
func noteClientCall() {
	clientCalls.Add(1)
}

// activity digests everything the stall watchdog counts as progress: every
// file write under the harness's directory (size and modification time, so
// a write into a preallocated file counts too) and every returned client
// call.
func (h *ClusterHarness) activity() int64 {
	total := clientCalls.Load()
	_ = filepath.WalkDir(h.BaseDir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size() + info.ModTime().UnixNano()
		}
		return nil
	})
	return total
}

// reportStall fails the test for a cluster that made no progress for
// clusterStallTimeout: it shows each node's log tail, has every running node
// dump its goroutines (SIGQUIT, into its log) and shows them, and stops the
// nodes so the test's own calls fail at once.
func (h *ClusterHarness) reportStall(idle time.Duration) {
	h.t.Errorf("cluster stalled: no node wrote anything, no client call returned and the test reported no progress for %s", idle.Round(time.Second))
	for i, node := range h.Nodes {
		nodeID := i + 1
		h.t.Logf("=== node %d: last %d log lines ===\n%s", nodeID, stallLogTailLines, h.getNodeLogTail(nodeID, stallLogTailLines))
		node.mu.Lock()
		running, cmd := node.isRunning, node.Cmd
		node.mu.Unlock()
		if !running || cmd == nil || cmd.Process == nil {
			continue
		}
		before, err := os.Stat(node.LogFile)
		if err != nil {
			continue
		}
		if err := cmd.Process.Signal(syscall.SIGQUIT); err != nil {
			h.t.Logf("node %d: SIGQUIT: %v", nodeID, err)
			continue
		}
		deadline := time.Now().Add(stallStackWait)
		for time.Now().Before(deadline) {
			if after, err := os.Stat(node.LogFile); err == nil && after.Size() > before.Size() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
		data, err := os.ReadFile(node.LogFile)
		if err != nil || int64(len(data)) <= before.Size() {
			h.t.Logf("node %d wrote no goroutine dump", nodeID)
			continue
		}
		stackPath := filepath.Join(node.DataDir, "stall-stacks.txt")
		if err := os.WriteFile(stackPath, data[before.Size():], 0o644); err == nil {
			h.t.Logf("node %d goroutine dump: %s", nodeID, stackPath)
		}
	}
	h.StopCluster()
}

// testDataRoot holds the integration tests' node data and binaries: Marmot's
// runtime files live under /tmp/marmot.
const testDataRoot = "/tmp/marmot/test"

// testDataDir creates a fresh directory under testDataRoot.
func testDataDir(pattern string) (string, error) {
	if err := os.MkdirAll(testDataRoot, 0o755); err != nil {
		return "", err
	}
	return os.MkdirTemp(testDataRoot, pattern)
}

// repoRoot is the module root: the directory above this test package, taken
// from this file's own path so the harness builds the tree it is compiled from.
func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(file))
}

func getCrashHarnessBinary(t *testing.T) (string, error) {
	t.Helper()

	crashHarnessBuildOnce.Do(func() {
		t.Logf("Building shared Marmot binary for crash-recovery tests...")

		buildDir, err := testDataDir("marmot_crash_bin_")
		if err != nil {
			crashHarnessBuildErr = fmt.Errorf("failed to create build dir: %w", err)
			return
		}

		binPath := filepath.Join(buildDir, "marmot")
		cmd := exec.Command("go", "build", "-tags", "sqlite_preupdate_hook sqlite_fts5 sqlite_json sqlite_math_functions sqlite_foreign_keys sqlite_stat4 sqlite_vacuum_incr", "-o", binPath, ".")
		cmd.Dir = repoRoot()
		output, err := cmd.CombinedOutput()
		if err != nil {
			crashHarnessBuildErr = fmt.Errorf("build failed: %v\n%s", err, output)
			return
		}

		crashHarnessBinPath = binPath
		t.Logf("Shared Marmot binary built at %s", crashHarnessBinPath)
	})

	return crashHarnessBinPath, crashHarnessBuildErr
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

	gcInterval := defaultGCIntervalSeconds
	if h.GCIntervalSeconds != unsetOverride {
		gcInterval = h.GCIntervalSeconds
	}
	heartbeatTimeout := defaultHeartbeatTimeoutSeconds
	if h.HeartbeatTimeoutSeconds != unsetOverride {
		heartbeatTimeout = h.HeartbeatTimeoutSeconds
	}
	gcMinRetentionLine, deltaSyncThresholdLine := "", ""
	if h.GCMinRetentionHours != unsetOverride {
		gcMinRetentionLine = fmt.Sprintf("gc_min_retention_hours = %d\n", h.GCMinRetentionHours)
	}
	if h.DeltaSyncThresholdSeconds != unsetOverride {
		deltaSyncThresholdLine = fmt.Sprintf("delta_sync_threshold_seconds = %d\n", h.DeltaSyncThresholdSeconds)
	}

	config := fmt.Sprintf(`# Marmot v2.9.16-beta Test Node %d
node_id = %d
data_dir = "%s"

[transaction]
heartbeat_timeout_seconds = %d
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
gc_interval_seconds = %d
%s%s
[mysql]
enabled = true
bind_address = "0.0.0.0"
port = %d
max_connections = 100

[logging]
verbose = false
format = "console"

[prometheus]
enabled = false
`,
		node.NodeID,
		node.NodeID,
		node.DataDir,
		heartbeatTimeout,
		node.GRPCPort,
		node.GRPCPort,
		strings.Join(seedNodes, ", "),
		gcInterval,
		gcMinRetentionLine,
		deltaSyncThresholdLine,
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

	// Append across restarts: a rolling-restart or kill/restart test needs
	// every incarnation's log, not just the last one's.
	logFile, err := os.OpenFile(node.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create log file: %v", err)
	}
	fmt.Fprintf(logFile, "=== node %d start %s ===\n", nodeID, time.Now().Format(time.RFC3339Nano))

	bin := h.MarmotBin
	if b, ok := h.NodeBin[nodeID]; ok {
		bin = b
	}
	cmd := exec.Command(bin, "--config", node.ConfigPath)
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

	h.t.Logf("Node %d stopped", nodeID)
	return nil
}

// GracefulStopNode sends SIGTERM and waits for the node's own shutdown - the
// path that announces LEAVING to its peers - unlike StopNode, which kills it.
func (h *ClusterHarness) GracefulStopNode(nodeID int, timeout time.Duration) error {
	node := h.Nodes[nodeID-1]
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.isRunning {
		return fmt.Errorf("node %d is not running", nodeID)
	}
	if node.DB != nil {
		node.DB.Close()
		node.DB = nil
	}
	if err := node.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("SIGTERM node %d: %w", nodeID, err)
	}

	exited := make(chan error, 1)
	go func() { exited <- node.Cmd.Wait() }()
	var err error
	select {
	case waitErr := <-exited:
		if waitErr != nil {
			err = fmt.Errorf("node %d exited with an error after SIGTERM: %w", nodeID, waitErr)
		} else {
			h.t.Logf("Node %d stopped gracefully", nodeID)
		}
	case <-time.After(timeout):
		err = fmt.Errorf("node %d did not shut down within %v of SIGTERM", nodeID, timeout)
		if killErr := node.Cmd.Process.Kill(); killErr != nil {
			err = fmt.Errorf("%w; kill: %v", err, killErr)
		}
		<-exited
	}
	node.isRunning = false
	node.Cmd = nil
	os.Remove(node.PIDFile)
	return err
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
			return fmt.Errorf("timeout waiting for node %d to become ALIVE; log tail:\n%s", nodeID, h.getNodeLogTail(nodeID, 40))
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

func (h *ClusterHarness) getNodeLogTail(nodeID int, lines int) string {
	if nodeID < 1 || nodeID > len(h.Nodes) {
		return "invalid node id"
	}
	data, err := os.ReadFile(h.Nodes[nodeID-1].LogFile)
	if err != nil {
		return fmt.Sprintf("failed to read log: %v", err)
	}
	all := strings.Split(string(data), "\n")
	if lines <= 0 || len(all) <= lines {
		return strings.Join(all, "\n")
	}
	return strings.Join(all[len(all)-lines:], "\n")
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
	defer noteClientCall()
	return db.Query(query, args...)
}

func (h *ClusterHarness) ExecNode(nodeID int, query string, args ...interface{}) (sql.Result, error) {
	db, err := h.ConnectToNode(nodeID)
	if err != nil {
		return nil, err
	}
	defer noteClientCall()
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

// WaitForRowCount waits for all specified nodes to have at least expectedCount rows
func (h *ClusterHarness) WaitForRowCount(tableName string, nodes []int, expectedCount int, timeout time.Duration) error {
	if timeout < 15*time.Second {
		timeout = 15 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allMatch := true
		for _, nodeID := range nodes {
			count := h.getRowCount(nodeID, tableName)
			if count < expectedCount {
				allMatch = false
				break
			}
		}
		if allMatch {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %d rows on nodes %v", expectedCount, nodes)
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

// votesHeld asks node's admin API whether its AUTO_INCREMENT claim votes are
// held (GET /admin/cluster/autoinc/votes).
func (h *ClusterHarness) votesHeld(nodeID int) (bool, error) {
	url := fmt.Sprintf("http://localhost:%d/admin/cluster/autoinc/votes", h.Nodes[nodeID-1].GRPCPort)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("X-Marmot-Secret", "test-secret")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	noteClientCall()
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	var apiResp struct {
		Data struct {
			VotesHeld bool `json:"votes_held"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return false, err
	}
	return apiResp.Data.VotesHeld, nil
}

// WaitForVotesReleased polls until node's AUTO_INCREMENT claim votes are not
// held: a node whose system database is new declines every claim at PREPARE
// until it merged claim bases from enough members, so a narrow
// AUTO_INCREMENT insert right after a cluster's first start can return 1205
// until then.
func (h *ClusterHarness) WaitForVotesReleased(nodeID int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		h.Progress()
		held, err := h.votesHeld(nodeID)
		if err == nil && !held {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("node %d claim votes still held after %v (err=%v); log tail:\n%s", nodeID, timeout, err, h.getNodeLogTail(nodeID, 40))
		}
		time.Sleep(clusterConvergencePoll)
	}
}

// WaitForReady polls until node is ready for any write: it reports itself
// ALIVE (a JOINING node is still catching up) and its claim votes are not
// held (WaitForVotesReleased), both within one named deadline.
func (h *ClusterHarness) WaitForReady(nodeID int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		h.Progress()
		if status, err := h.selfStatus(nodeID); err == nil && status == "ALIVE" {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("node %d not ALIVE within %v; log tail:\n%s", nodeID, timeout, h.getNodeLogTail(nodeID, 40))
		}
		time.Sleep(clusterConvergencePoll)
	}
	return h.WaitForVotesReleased(nodeID, time.Until(deadline))
}

// selfStatus is node's own membership status as its admin API reports it.
func (h *ClusterHarness) selfStatus(nodeID int) (string, error) {
	members, err := h.getClusterStatus(nodeID)
	if err != nil {
		return "", err
	}
	for _, m := range members {
		if m.NodeID == uint64(nodeID) {
			return m.Status, nil
		}
	}
	return "", fmt.Errorf("node %d absent from its own membership", nodeID)
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
	noteClientCall()
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

// WaitForClusterConvergence waits until every node sees every node ALIVE
// in its cluster membership, polling each node's admin API until timeout.
func (h *ClusterHarness) WaitForClusterConvergence(timeout time.Duration) error {
	h.t.Logf("Waiting for cluster to converge (all nodes ALIVE on every node)...")

	deadline := time.Now().Add(timeout)
	for node := 1; node <= numNodes; node++ {
		for {
			alive, err := h.aliveCount(node)
			if err == nil && alive >= numNodes {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("cluster did not converge on node %d within %v (alive=%d, err=%v)", node, timeout, alive, err)
			}
			time.Sleep(clusterConvergencePoll)
		}
	}
	h.t.Logf("Cluster converged: every node sees %d nodes ALIVE", numNodes)
	return nil
}

// aliveCount returns how many members node's cluster membership reports
// ALIVE.
func (h *ClusterHarness) aliveCount(node int) (int, error) {
	members, err := h.getClusterStatus(node)
	if err != nil {
		return 0, err
	}
	alive := 0
	for _, m := range members {
		if m.Status == "ALIVE" {
			alive++
		}
	}
	return alive, nil
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
	for i := 1; i <= numNodes; i++ {
		if err := h.WaitForAlive(i, 30*time.Second); err != nil {
			return err
		}
	}

	// Wait for gossip cluster to fully converge (all nodes ALIVE, as seen
	// by every node).
	if err := h.WaitForClusterConvergence(30 * time.Second); err != nil {
		h.dumpNodeLogs("cluster_start_fail")
		return err
	}

	// Every node on the harness's own binary must be ready for writes. A
	// node on an overridden binary (NodeBin) may predate the readiness
	// endpoint; a test mixing binaries waits for readiness itself.
	for i := 1; i <= numNodes; i++ {
		if _, overridden := h.NodeBin[i]; overridden {
			continue
		}
		if err := h.WaitForReady(i, clusterReadyDeadline); err != nil {
			h.dumpNodeLogs("cluster_start_not_ready")
			return err
		}
	}

	h.t.Logf("Cluster started successfully")
	return nil
}

func (h *ClusterHarness) StopCluster() {
	h.t.Logf("Stopping cluster...")
	for i := 1; i <= len(h.Nodes); i++ {
		h.StopNode(i)
	}
}

func (h *ClusterHarness) Cleanup() {
	h.cleanupOnce.Do(func() {
		h.watchdog.Stop()
		h.budgetOnce.Do(func() { close(h.budgetDone) })
		h.t.Logf("Cleaning up cluster harness...")
		h.StopCluster()
		time.Sleep(500 * time.Millisecond)
		if h.t.Failed() {
			h.t.Logf("Preserving test artifacts for debugging: %s", h.BaseDir)
		} else {
			os.RemoveAll(h.BaseDir)
		}
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
	harness.Phase("converged")

	t.Logf("SUCCESS: Table %s replicated to all nodes", tableName)
}

// TestBasicReplication verifies that INSERT replicates to all nodes
func TestBasicReplication(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}
	harness.Phase("start")

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
	if err := harness.WaitForRowCount(tableName, allNodes, 10, 15*time.Second); err != nil {
		harness.dumpNodeLogs("basic_replication_fail")
		t.Fatalf("rows did not replicate: %v", err)
	}
	harness.Phase("converged")

	for _, nodeID := range allNodes {
		t.Logf("Node %d has %d rows", nodeID, harness.getRowCount(nodeID, tableName))
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
	harness.Phase("start")

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
	if err := harness.WaitForRowCount(tableName, allNodes, 5, 15*time.Second); err != nil {
		t.Fatalf("initial rows did not replicate: %v", err)
	}
	t.Logf("All nodes have 5 rows before crash")
	harness.Phase("initial-load")

	// Kill node 3
	t.Logf("Killing node 3...")
	if err := harness.KillNode(3); err != nil {
		t.Fatalf("Failed to kill node 3: %v", err)
	}
	harness.Phase("node3-killed")

	// Write more data while node 3 is down
	t.Logf("Writing 5 more rows while node 3 is down...")
	for i := 6; i <= 10; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'missed_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Insert failed while node 3 down: %v", err)
		}
	}
	if err := harness.WaitForRowCount(tableName, []int{1, 2}, 10, 15*time.Second); err != nil {
		t.Fatalf("rows did not replicate to nodes 1&2: %v", err)
	}
	t.Logf("Nodes 1 & 2 have 10 rows")
	harness.Phase("missed-load")

	// Restart node 3
	t.Logf("Restarting node 3...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}
	if err := harness.WaitForAlive(3, 20*time.Second); err != nil {
		t.Fatalf("Node 3 did not become ALIVE: %v", err)
	}
	harness.Phase("node3-alive")

	// Wait for anti-entropy to catch up
	t.Logf("Waiting for node 3 to catch up via anti-entropy...")
	if err := harness.WaitForRowCount(tableName, []int{3}, 10, 20*time.Second); err != nil {
		harness.dumpNodeLogs("recovery_test_fail")
		t.Fatalf("Node 3 did not catch up: %v", err)
	}
	harness.Phase("caught-up")

	t.Logf("SUCCESS: Node 3 caught up after restart")
}

// TestRollingRestart tests cluster availability during rolling restart.
// This test verifies that:
// 1. Writes continue to succeed (on remaining nodes) during rolling restarts
// 2. After all restarts complete, anti-entropy brings all nodes to eventual consistency
//
// Every wait here is a poll on a named deadline (WaitForRowCount / WaitForAlive),
// not a flat sleep, so the test finishes as soon as the cluster actually
// converges, keeping it well under the 60 s cluster test budget.
func TestRollingRestart(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}
	harness.Phase("start")

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
	const baseline = 20
	t.Logf("Inserting baseline %d rows before rolling restart...", baseline)
	for i := 1; i <= baseline; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'baseline_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Baseline insert failed: %v", err)
		}
	}
	if err := harness.WaitForRowCount(tableName, allNodes, baseline, 15*time.Second); err != nil {
		t.Fatalf("baseline rows did not replicate: %v", err)
	}
	t.Logf("All nodes have %d baseline rows", baseline)
	harness.Phase("baseline-load")

	// Rolling restart each node - writes continue against the nodes still up.
	expected := baseline
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
			rowID := baseline + (nodeID-1)*5 + i + 1 // Unique IDs: 21-25, 26-30, 31-35
			_, err := harness.ExecNode(targetNode, fmt.Sprintf(
				"INSERT INTO %s (id, value) VALUES (%d, 'rolling_%d')", tableName, rowID, rowID))
			if err != nil {
				t.Logf("Warning: write during rolling restart failed: %v", err)
				continue
			}
			expected++
		}

		var stillUp []int
		for _, n := range allNodes {
			if n != nodeID {
				stillUp = append(stillUp, n)
			}
		}
		if err := harness.WaitForRowCount(tableName, stillUp, expected, 10*time.Second); err != nil {
			t.Fatalf("writes during node %d's downtime did not land on the up nodes: %v", nodeID, err)
		}

		if err := harness.StartNode(nodeID); err != nil {
			t.Fatalf("Failed to restart node %d: %v", nodeID, err)
		}
		if err := harness.WaitForAlive(nodeID, 20*time.Second); err != nil {
			t.Fatalf("Node %d did not become ALIVE: %v", nodeID, err)
		}
		t.Logf("Node %d restarted", nodeID)

		if err := harness.WaitForRowCount(tableName, []int{nodeID}, expected, 20*time.Second); err != nil {
			harness.dumpNodeLogs("rolling_test_catchup")
			t.Fatalf("node %d did not catch up via anti-entropy: %v", nodeID, err)
		}
		harness.Phase(fmt.Sprintf("node%d-rolled", nodeID))
	}

	// Final verification - all nodes converge on the same count.
	if err := harness.WaitForRowCount(tableName, allNodes, expected, 15*time.Second); err != nil {
		harness.dumpNodeLogs("rolling_test_fail")
		t.Fatalf("cluster did not converge: %v", err)
	}
	harness.Phase("converged")

	finalCount := harness.getRowCount(1, tableName)
	t.Logf("SUCCESS: Rolling restart maintained availability and consistency (final count: %d)", finalCount)
}

// TestCrashMidTransaction tests that uncommitted transactions are rolled back
func TestCrashMidTransaction(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}
	harness.Phase("start")

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
	harness.Phase("node1-alive")

	time.Sleep(3 * time.Second)

	// Node 1 should also have 0 rows (transaction rolled back on crash)
	count := harness.getRowCount(1, tableName)
	if count != 0 {
		t.Fatalf("Node 1 has %d rows after restart, expected 0", count)
	}

	t.Logf("SUCCESS: Uncommitted transaction correctly rolled back")
}

// TestHighLoadRecovery tests node catch-up after missing writes during downtime
func TestHighLoadRecovery(t *testing.T) {
	harness := NewClusterHarness(t)
	defer harness.Cleanup()

	if err := harness.StartCluster(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}
	harness.Phase("start")

	tableName := "highload_test"
	allNodes := []int{1, 2, 3}

	// Create table
	_, err := harness.ExecNode(1, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := harness.WaitForTableExists(tableName, allNodes, 15*time.Second); err != nil {
		harness.dumpNodeLogs("highload_ddl")
		t.Fatalf("DDL replication failed: %v", err)
	}

	// Phase 1: Insert initial batch with all nodes up
	initialRows := 20
	t.Logf("Phase 1: Inserting %d rows with all nodes up...", initialRows)
	for i := 1; i <= initialRows; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'initial_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Wait for all nodes to have initial rows
	if err := harness.WaitForRowCount(tableName, allNodes, initialRows, 30*time.Second); err != nil {
		harness.dumpNodeLogs("highload_initial")
		t.Fatalf("Initial replication failed: %v", err)
	}
	t.Logf("Phase 1 complete: All nodes have %d rows", initialRows)
	harness.Phase("initial-load")

	// Phase 2: Kill node 3
	t.Logf("Phase 2: Killing node 3...")
	harness.KillNode(3)
	harness.Phase("node3-killed")

	// Phase 3: Insert more rows while node 3 is down
	additionalRows := 30
	t.Logf("Phase 3: Inserting %d rows while node 3 is down...", additionalRows)
	for i := initialRows + 1; i <= initialRows+additionalRows; i++ {
		_, err := harness.ExecNode(1, fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES (%d, 'during_down_%d')", tableName, i, i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Verify nodes 1 & 2 have all rows
	expectedTotal := initialRows + additionalRows
	if err := harness.WaitForRowCount(tableName, []int{1, 2}, expectedTotal, 30*time.Second); err != nil {
		harness.dumpNodeLogs("highload_replication")
		t.Fatalf("Replication to nodes 1&2 failed: %v", err)
	}
	t.Logf("Phase 3 complete: Nodes 1 & 2 have %d rows", expectedTotal)
	harness.Phase("missed-load")

	// Phase 4: Restart node 3
	t.Logf("Phase 4: Restarting node 3...")
	if err := harness.StartNode(3); err != nil {
		t.Fatalf("Failed to restart node 3: %v", err)
	}
	if err := harness.WaitForAlive(3, 30*time.Second); err != nil {
		harness.dumpNodeLogs("highload_restart_alive")
		t.Fatalf("Node 3 did not become ALIVE: %v", err)
	}
	harness.Phase("node3-alive")

	// Phase 5: Wait for node 3 to catch up
	t.Logf("Phase 5: Waiting for node 3 to catch up to %d rows...", expectedTotal)
	if err := harness.WaitForRowCount(tableName, []int{3}, expectedTotal, 25*time.Second); err != nil {
		harness.dumpNodeLogs("highload_catchup")
		t.Fatalf("Node 3 failed to catch up: %v", err)
	}
	harness.Phase("caught-up")

	// Final verification - all nodes have exact same count
	for _, nodeID := range allNodes {
		count := harness.getRowCount(nodeID, tableName)
		if count != expectedTotal {
			harness.dumpNodeLogs("highload_final")
			t.Fatalf("Node %d has %d rows, expected %d", nodeID, count, expectedTotal)
		}
		t.Logf("Node %d has %d rows", nodeID, count)
	}

	t.Logf("SUCCESS: Node 3 recovered and caught up to %d rows", expectedTotal)
}
