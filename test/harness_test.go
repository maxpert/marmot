package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	marmotgrpc "github.com/maxpert/marmot/grpc"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/maxpert/marmot/test/stallwatch"
)

const (
	// testBudget is the wall-clock budget of every cluster test: the harness
	// fails a test whose cluster lives longer and stops its nodes.
	testBudget = 10 * time.Second
	// stallTimeout fails a cluster that wrote nothing, answered no client
	// call and reported no progress for this long.
	stallTimeout = 5 * time.Second
	// pollInterval is how often waitFor re-checks its condition.
	pollInterval = 20 * time.Millisecond
	// readyDeadline bounds a node's wait from start to ready for writes.
	readyDeadline = 6 * time.Second
	// replicationDeadline bounds a wait for a write to reach every node.
	replicationDeadline = 5 * time.Second
	// reconnectDeadline bounds execAcrossReconnect's retries: grpc's default
	// reconnect backoff starts at one second, with jitter.
	reconnectDeadline = 3 * time.Second
	// stopDeadline bounds a graceful stop (SIGTERM until exit).
	stopDeadline = 5 * time.Second
	// clientTimeout bounds every client call a test makes to a node.
	clientTimeout = 3 * time.Second
	// stallDumpWait bounds the wait for stalled nodes to write their
	// goroutine dumps and exit.
	stallDumpWait = 2 * time.Second
	// logTailLines is how much of each node's log a failure report shows.
	logTailLines = 30
	// clusterSecret is every test cluster's cluster_secret.
	clusterSecret = "test-secret"
)

// waitFor polls cond every pollInterval until it reports done, failing the
// test once deadline passes. what names the wait; cond's state describes what
// it last saw, and is what the failure prints. It is the only place a test
// sleeps.
func waitFor(t testing.TB, what string, deadline time.Duration, cond func() (done bool, state string)) {
	t.Helper()
	if state, ok := poll(deadline, cond); !ok {
		t.Fatalf("%s: not reached within %s; last state: %s", what, deadline, state)
	}
}

// poll is waitFor without the test: it reports whether cond was done within
// deadline and the state it last described.
func poll(deadline time.Duration, cond func() (bool, string)) (string, bool) {
	end := time.Now().Add(deadline)
	for {
		done, state := cond()
		if done {
			return state, true
		}
		if time.Now().After(end) {
			return state, false
		}
		time.Sleep(pollInterval)
	}
}

const (
	// retryBackoffBase and retryBackoffCap bound retryBackoff's waits.
	retryBackoffBase = 5 * time.Millisecond
	retryBackoffCap  = 250 * time.Millisecond
)

// retryBackoff retries attempt the way a real client retries a refused
// write: after each failure it waits a random time up to an exponentially
// growing bound (full jitter), until attempt succeeds or deadline passes.
// Two clients refused by the same conflict so retry at different times,
// where retrying on one clock would make them collide again. It reports the
// state attempt last described and whether it succeeded.
func retryBackoff(deadline time.Duration, attempt func() (bool, string)) (string, bool) {
	end := time.Now().Add(deadline)
	bound := retryBackoffBase
	for {
		done, state := attempt()
		if done {
			return state, true
		}
		if time.Now().After(end) {
			return state, false
		}
		time.Sleep(rand.N(bound) + time.Millisecond)
		bound = min(2*bound, retryBackoffCap)
	}
}

// node is one Marmot process of a test cluster.
type node struct {
	id        int
	grpcPort  int
	mysqlPort int
	dir       string
	config    string
	log       string
	// bin is the binary the node runs; "" means the tree under test.
	bin string

	mu     sync.Mutex
	cmd    *exec.Cmd
	exited chan struct{}
	// waitErr is the process's exit error, set before exited closes.
	waitErr error
	conns   map[string]*sql.DB
}

// clusterConfig holds the per-test settings a node's config file is written
// from. Every field is written explicitly: the defaults below are small so a
// cluster starts, releases its claim votes and catches up within about a
// second.
type clusterConfig struct {
	size                   int
	gossipIntervalMS       int
	suspectTimeoutMS       int
	deadTimeoutMS          int
	antiEntropySeconds     int
	gcIntervalSeconds      int
	gcMinRetentionHours    int
	deltaSyncThresholdSecs int
	heartbeatTimeoutSecs   int
	lockWaitTimeoutSecs    int
	shutdownGraceMS        int
	autoIncMergeMS         int
	autoIncBaseSyncMS      int
	// strictPrepareSync makes a PREPARE durable before it is ACKed.
	strictPrepareSync bool
}

func defaultClusterConfig() clusterConfig {
	return clusterConfig{
		size:                   3,
		gossipIntervalMS:       100,
		suspectTimeoutMS:       1500,
		deadTimeoutMS:          3000,
		antiEntropySeconds:     1,
		gcIntervalSeconds:      30,
		gcMinRetentionHours:    2,
		deltaSyncThresholdSecs: 3600,
		heartbeatTimeoutSecs:   10,
		lockWaitTimeoutSecs:    5,
		shutdownGraceMS:        1000,
		autoIncMergeMS:         50,
		autoIncBaseSyncMS:      1000,
	}
}

// cluster is one test's Marmot cluster: its own ports, its own directory
// under the run directory, and nodes that are always stopped when the test
// ends, however it ends.
type cluster struct {
	t     *testing.T
	dir   string
	cfg   clusterConfig
	nodes []*node

	watchdog *stallwatch.Watchdog
	budget   *time.Timer
	// lastPhase is when the previous phase ended (phase).
	lastPhase time.Time
}

// phase logs how long the test's phase that just ended took.
func (c *cluster) phase(name string) {
	now := time.Now()
	c.t.Logf("phase=%s elapsed=%s", name, now.Sub(c.lastPhase).Round(time.Millisecond))
	c.lastPhase = now
	c.progress()
}

// newCluster writes a cluster's node configs and registers its cleanup; it
// starts nothing. Options adjust the config before it is written.
func newCluster(t *testing.T, opts ...func(*clusterConfig)) *cluster {
	t.Helper()
	cfg := defaultClusterConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	dir := testDir(t)
	c := &cluster{t: t, dir: dir, cfg: cfg, lastPhase: time.Now()}
	ports := freePorts(t, 2*cfg.size)
	for i := 0; i < cfg.size; i++ {
		c.nodes = append(c.nodes, &node{
			id:        i + 1,
			grpcPort:  ports[2*i],
			mysqlPort: ports[2*i+1],
			dir:       filepath.Join(dir, fmt.Sprintf("node%d", i+1)),
			conns:     map[string]*sql.DB{},
		})
	}
	for _, n := range c.nodes {
		c.writeConfig(n)
	}
	c.watchdog = stallwatch.Start(stallTimeout, pollInterval*10, c.activity, c.reportStall)
	c.budget = time.AfterFunc(testBudget, c.reportBudgetExceeded)
	t.Cleanup(c.cleanup)
	return c
}

// testDir creates and returns t's own directory under runDir.
func testDir(t testing.TB) string {
	t.Helper()
	dir := filepath.Join(runDir, strings.ReplaceAll(t.Name(), "/", "_"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// freshDir creates and returns a new, empty directory under t's own
// directory.
func freshDir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp(testDir(t), "dir-")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// freePorts reserves n distinct free TCP ports by listening on each until all
// are chosen, so no two nodes of one cluster are handed the same port.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, 0, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve a port: %v", err)
		}
		defer l.Close()
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports
}

// seeds is node's seed list: every node but the first joins through node 1.
func (c *cluster) seeds(n *node) string {
	if n.id == 1 {
		return ""
	}
	return fmt.Sprintf("%q", fmt.Sprintf("127.0.0.1:%d", c.nodes[0].grpcPort))
}

// writeConfig writes node's config file from the cluster's config.
func (c *cluster) writeConfig(n *node) {
	if err := os.MkdirAll(n.dir, 0o755); err != nil {
		c.t.Fatal(err)
	}
	n.config = filepath.Join(n.dir, "config.toml")
	n.log = filepath.Join(n.dir, "marmot.log")
	cfg := c.cfg
	body := fmt.Sprintf(`node_id = %d
data_dir = %q

[transaction]
heartbeat_timeout_seconds = %d
lock_wait_timeout_seconds = %d

[coordinator]
prepare_timeout_ms = 2000
commit_timeout_ms = 2000
abort_timeout_ms = 2000

[cluster]
grpc_bind_address = "127.0.0.1"
grpc_advertise_address = "127.0.0.1:%d"
grpc_port = %d
seed_nodes = [%s]
gossip_interval_ms = %d
suspect_timeout_ms = %d
dead_timeout_ms = %d
shutdown_grace_period_ms = %d
autoinc_merge_interval_ms = %d
autoinc_base_sync_interval_ms = %d
cluster_secret = %q

[cluster.promotion]
check_interval_seconds = 1
min_healthy_duration_sec = 0

[replication]
anti_entropy_interval_seconds = %d
gc_interval_seconds = %d
gc_min_retention_hours = %d
delta_sync_threshold_seconds = %d

[metastore]
strict_prepare_sync = %t

[batch_commit]
# A test client writes one statement at a time: each commit waits out the
# whole flush window.
max_wait_ms = 1

[mysql]
bind_address = "127.0.0.1"
port = %d

[logging]
format = "console"

[prometheus]
enabled = false
`, n.id, n.dir, cfg.heartbeatTimeoutSecs, cfg.lockWaitTimeoutSecs, n.grpcPort, n.grpcPort, c.seeds(n),
		cfg.gossipIntervalMS, cfg.suspectTimeoutMS, cfg.deadTimeoutMS, cfg.shutdownGraceMS,
		cfg.autoIncMergeMS, cfg.autoIncBaseSyncMS, clusterSecret,
		cfg.antiEntropySeconds, cfg.gcIntervalSeconds, cfg.gcMinRetentionHours, cfg.deltaSyncThresholdSecs, cfg.strictPrepareSync, n.mysqlPort)
	if err := os.WriteFile(n.config, []byte(body), 0o644); err != nil {
		c.t.Fatal(err)
	}
}

// node returns node id (1-based).
func (c *cluster) node(id int) *node { return c.nodes[id-1] }

// startNode launches node id's process without waiting for it; its output
// is appended to its log, one header per incarnation.
func (c *cluster) startNode(id int) {
	c.t.Helper()
	n := c.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd != nil {
		c.t.Fatalf("node %d is already running", id)
	}
	logFile, err := os.OpenFile(n.log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		c.t.Fatal(err)
	}
	defer logFile.Close()
	fmt.Fprintf(logFile, "=== node %d start %s ===\n", id, time.Now().Format(time.RFC3339Nano))
	bin := marmotBin
	if n.bin != "" {
		bin = n.bin
	}
	cmd := exec.Command(bin, "--config", n.config)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start node %d: %v", id, err)
	}
	exited := make(chan struct{})
	go func() {
		err := cmd.Wait()
		n.mu.Lock()
		n.waitErr = err
		n.mu.Unlock()
		close(exited)
	}()
	n.cmd, n.exited = cmd, exited
}

// kill SIGKILLs node id and waits for it to exit: a crash, with nothing
// flushed or announced.
func (c *cluster) kill(id int) {
	c.t.Helper()
	cmd, exited := c.detach(id)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		c.t.Fatalf("SIGKILL node %d: %v", id, err)
	}
	<-exited
}

// stop SIGTERMs node id and waits for its own shutdown (the path that
// announces LEAVING to its peers), failing the test unless it exits cleanly
// within stopDeadline.
func (c *cluster) stop(id int) {
	c.t.Helper()
	cmd, exited := c.detach(id)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		c.t.Fatalf("SIGTERM node %d: %v", id, err)
	}
	select {
	case <-exited:
	case <-time.After(stopDeadline):
		_ = cmd.Process.Kill()
		<-exited
		c.t.Fatalf("node %d did not exit within %s of SIGTERM; log tail:\n%s", id, stopDeadline, c.logTail(id))
	}
	n := c.node(id)
	n.mu.Lock()
	err := n.waitErr
	n.mu.Unlock()
	if err != nil {
		c.t.Fatalf("node %d exited with an error after SIGTERM: %v; log tail:\n%s", id, err, c.logTail(id))
	}
}

// detach marks node id stopped and closes its client connections, returning
// its process for the caller to end.
func (c *cluster) detach(id int) (*exec.Cmd, chan struct{}) {
	c.t.Helper()
	n := c.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil {
		c.t.Fatalf("node %d is not running", id)
	}
	for name, conn := range n.conns {
		conn.Close()
		delete(n.conns, name)
	}
	cmd, exited := n.cmd, n.exited
	n.cmd = nil
	return cmd, exited
}

// killAll SIGKILLs every running node. Unlike kill it never fails the test,
// so the watchdogs' goroutines and cleanup can call it.
func (c *cluster) killAll() {
	for _, n := range c.nodes {
		n.mu.Lock()
		cmd, exited := n.cmd, n.exited
		n.cmd = nil
		for name, conn := range n.conns {
			conn.Close()
			delete(n.conns, name)
		}
		n.mu.Unlock()
		if cmd != nil {
			_ = cmd.Process.Signal(syscall.SIGKILL)
			<-exited
		}
	}
}

// cleanup stops every node; it keeps the cluster's directory, and prints
// each node's log tail, when the test failed.
func (c *cluster) cleanup() {
	c.budget.Stop()
	c.watchdog.Stop()
	if c.t.Failed() {
		for _, n := range c.nodes {
			c.t.Logf("=== node %d: last %d log lines ===\n%s", n.id, logTailLines, c.logTail(n.id))
		}
	}
	c.killAll()
	if c.t.Failed() {
		c.t.Logf("cluster directory kept: %s", c.dir)
		return
	}
	if err := os.RemoveAll(c.dir); err != nil {
		c.t.Logf("remove %s: %v", c.dir, err)
	}
}

// logTail is the last logTailLines lines of node id's log.
func (c *cluster) logTail(id int) string {
	data, err := os.ReadFile(c.node(id).log)
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	return strings.Join(lines[max(len(lines)-logTailLines, 0):], "\n")
}

// clientCalls counts client calls to any node that returned, with or without
// an error: a cluster answering its clients is not stalled.
var clientCalls atomic.Int64

// activity digests what the stall watchdog counts as progress: every file
// write under the cluster's directory and every returned client call.
func (c *cluster) activity() int64 {
	total := clientCalls.Load()
	_ = filepath.WalkDir(c.dir, func(_ string, d os.DirEntry, err error) error {
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

// progress tells the stall watchdog the test saw the cluster make progress.
func (c *cluster) progress() { c.watchdog.Progress() }

// reportStall fails a cluster that made no progress for stallTimeout. Each
// running node gets SIGQUIT, so the Go runtime writes every goroutine's stack
// into its log before it exits; the nodes then stop, so the test's own calls
// fail at once.
func (c *cluster) reportStall(idle time.Duration) {
	c.t.Errorf("cluster stalled: nothing written, no client call returned and no progress for %s; goroutine dumps are in each node's log under %s",
		idle.Round(time.Millisecond), c.dir)
	var dumping []chan struct{}
	for _, n := range c.nodes {
		n.mu.Lock()
		if n.cmd != nil && n.cmd.Process.Signal(syscall.SIGQUIT) == nil {
			dumping = append(dumping, n.exited)
		}
		n.mu.Unlock()
	}
	dumpDeadline := time.After(stallDumpWait)
	for _, exited := range dumping {
		select {
		case <-exited:
		case <-dumpDeadline:
		}
	}
	c.killAll()
}

// reportBudgetExceeded fails a test whose cluster outlived testBudget and
// stops its nodes, so the test's own calls fail at once.
func (c *cluster) reportBudgetExceeded() {
	c.t.Errorf("cluster test exceeded its %s budget", testBudget)
	c.killAll()
}

// dsn is the MySQL DSN for database on node id; every call through it is
// bounded by clientTimeout.
func (c *cluster) dsn(id int, database string) string {
	return fmt.Sprintf("root:@tcp(127.0.0.1:%d)/%s?timeout=%s&readTimeout=%s&writeTimeout=%s",
		c.node(id).mysqlPort, database, clientTimeout, clientTimeout, clientTimeout)
}

// db is the connection pool for database on node id, opened on first use and
// closed when the node stops.
func (c *cluster) db(id int, database string) *sql.DB {
	c.t.Helper()
	n := c.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	if conn, ok := n.conns[database]; ok {
		return conn
	}
	conn, err := sql.Open("mysql", c.dsn(id, database))
	if err != nil {
		c.t.Fatalf("open %s on node %d: %v", database, id, err)
	}
	n.conns[database] = conn
	return conn
}

// exec runs q on database through node id.
func (c *cluster) exec(id int, database, q string, args ...any) (sql.Result, error) {
	defer clientCalls.Add(1)
	return c.db(id, database).Exec(q, args...)
}

// execAcrossReconnect runs q through node id like mustExec, but retries while
// it fails with 1105 for up to reconnectDeadline. A coordinator's gRPC
// connection to a peer that just restarted redials only after grpc's
// reconnect backoff, and until then a PREPARE to that peer fails at once, so
// with another node down the write cannot reach a quorum. It reports how many
// attempts failed.
func (c *cluster) execAcrossReconnect(id int, database, q string, args ...any) int {
	c.t.Helper()
	failed := 0
	waitFor(c.t, fmt.Sprintf("node %d accepts %s", id, q), reconnectDeadline, func() (bool, string) {
		_, err := c.exec(id, database, q, args...)
		if err == nil {
			return true, ""
		}
		if mysqlCode(err) != mysqlcode.ErrCodeUnknown {
			c.t.Fatalf("node %d: %s: %v", id, q, err)
		}
		failed++
		return false, err.Error()
	})
	return failed
}

// ping checks that node id answers a MySQL query.
func (c *cluster) ping(id int) error {
	defer clientCalls.Add(1)
	var one int
	return c.db(id, "marmot").QueryRow("SELECT 1").Scan(&one)
}

// waitMySQL waits until node id answers MySQL queries.
func (c *cluster) waitMySQL(id int) {
	c.t.Helper()
	waitFor(c.t, fmt.Sprintf("node %d answers MySQL", id), readyDeadline, func() (bool, string) {
		err := c.ping(id)
		return err == nil, fmt.Sprint(err)
	})
}

// mustExec is exec that fails the test on an error.
func (c *cluster) mustExec(id int, database, q string, args ...any) sql.Result {
	c.t.Helper()
	res, err := c.exec(id, database, q, args...)
	if err != nil {
		c.t.Fatalf("node %d: %s: %v", id, q, err)
	}
	return res
}

// rows runs query on database through node id and returns each row as its
// columns joined by "|" (NULL as "NULL"), in the order the query gives.
func (c *cluster) rows(id int, database, query string, args ...any) ([]string, error) {
	defer clientCalls.Add(1)
	rs, err := c.db(id, database).Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	cols, err := rs.Columns()
	if err != nil {
		return nil, err
	}
	var out []string
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rs.Next() {
		if err := rs.Scan(ptrs...); err != nil {
			return nil, err
		}
		fields := make([]string, len(vals))
		for i, v := range vals {
			fields[i] = "NULL"
			if v.Valid {
				fields[i] = v.String
			}
		}
		out = append(out, strings.Join(fields, "|"))
	}
	return out, rs.Err()
}

// mustRows is rows that fails the test on an error.
func (c *cluster) mustRows(id int, database, query string, args ...any) []string {
	c.t.Helper()
	out, err := c.rows(id, database, query, args...)
	if err != nil {
		c.t.Fatalf("node %d: %s: %v", id, query, err)
	}
	return out
}

// waitRows waits until query (which must order its rows) returns exactly
// want on database through every node in ids.
func (c *cluster) waitRows(database, query string, want []string, ids ...int) {
	c.t.Helper()
	wantText := strings.Join(want, ",")
	waitFor(c.t, fmt.Sprintf("nodes %v: %s = %d rows", ids, query, len(want)), replicationDeadline, func() (bool, string) {
		var state []string
		for _, id := range ids {
			got, err := c.rows(id, database, query)
			if err != nil {
				return false, fmt.Sprintf("node %d: %v", id, err)
			}
			if gotText := strings.Join(got, ","); gotText != wantText {
				return false, fmt.Sprintf("node %d has %d rows: %s", id, len(got), abbreviate(gotText))
			}
			state = append(state, fmt.Sprintf("node %d ok", id))
		}
		return true, strings.Join(state, "; ")
	})
}

// waitSameRows waits until query (which must order its rows) returns the
// same rows on database through every node in ids, and returns them.
func (c *cluster) waitSameRows(database, query string, ids ...int) []string {
	c.t.Helper()
	var agreed []string
	waitFor(c.t, fmt.Sprintf("nodes %v agree on %s", ids, query), replicationDeadline, func() (bool, string) {
		var first string
		var state []string
		for i, id := range ids {
			got, err := c.rows(id, database, query)
			if err != nil {
				return false, fmt.Sprintf("node %d: %v", id, err)
			}
			text := strings.Join(got, ",")
			state = append(state, fmt.Sprintf("node %d: %d rows %s", id, len(got), abbreviate(text)))
			if i == 0 {
				first, agreed = text, got
			} else if text != first {
				return false, strings.Join(state, "; ")
			}
		}
		return true, ""
	})
	return agreed
}

// hasTable reports whether database on node id lists table.
func (c *cluster) hasTable(id int, database, table string) (bool, error) {
	tables, err := c.rows(id, database, "SHOW TABLES")
	if err != nil {
		return false, err
	}
	return slices.Contains(tables, table), nil
}

// waitTable waits until database on every node in ids lists table.
func (c *cluster) waitTable(database, table string, ids ...int) {
	c.t.Helper()
	waitFor(c.t, fmt.Sprintf("table %s.%s on nodes %v", database, table, ids), replicationDeadline, func() (bool, string) {
		for _, id := range ids {
			if ok, err := c.hasTable(id, database, table); !ok {
				return false, fmt.Sprintf("node %d: absent (err=%v)", id, err)
			}
		}
		return true, ""
	})
}

// waitDatabase waits until SHOW DATABASES on every node in ids lists
// database (present) or does not (!present).
func (c *cluster) waitDatabase(database string, present bool, ids ...int) {
	c.t.Helper()
	waitFor(c.t, fmt.Sprintf("database %s present=%v on nodes %v", database, present, ids), replicationDeadline, func() (bool, string) {
		for _, id := range ids {
			if ok, err := c.hasDatabase(id, database); err != nil || ok != present {
				return false, fmt.Sprintf("node %d: present=%v (err=%v)", id, ok, err)
			}
		}
		return true, ""
	})
}

// hasDatabase reports whether SHOW DATABASES on node id lists database.
func (c *cluster) hasDatabase(id int, database string) (bool, error) {
	databases, err := c.rows(id, "marmot", "SHOW DATABASES")
	return slices.Contains(databases, database), err
}

// createDatabase creates database through node id and waits until every
// running node has it.
func (c *cluster) createDatabase(id int, database string) {
	c.t.Helper()
	c.mustExec(id, "marmot", "CREATE DATABASE "+database)
	c.waitDatabase(database, true, c.running()...)
}

// createTable runs ddl, which creates table in database, through node id and
// waits until every running node has the table.
func (c *cluster) createTable(id int, database, table, ddl string) {
	c.t.Helper()
	c.mustExec(id, database, ddl)
	c.waitTable(database, table, c.running()...)
}

// idValueRows is ids from..to of a (id, value) table whose value is tag_id,
// as a VALUES list and as the rows rows() returns for them.
func idValueRows(from, to int, tag string) (values string, want []string) {
	var tuples []string
	for i := from; i <= to; i++ {
		tuples = append(tuples, fmt.Sprintf("(%d, '%s_%d')", i, tag, i))
		want = append(want, fmt.Sprintf("%d|%s_%d", i, tag, i))
	}
	return strings.Join(tuples, ", "), want
}

// mysqlCode is err's MySQL error number, or 0 when err is not a MySQL error.
func mysqlCode(err error) uint16 {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number
	}
	return 0
}

// abbreviate shortens s for a failure message.
func abbreviate(s string) string {
	const limit = 300
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}

// member is one entry of a node's membership, as its admin API reports it.
type member struct {
	NodeID uint64 `json:"node_id"`
	Status string `json:"status"`
}

// admin sends method path to node id's admin API, waiting at most timeout,
// and decodes the response's data field into out.
func (c *cluster) admin(id int, method, path string, timeout time.Duration, out any) error {
	defer clientCalls.Add(1)
	req, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d/admin%s", c.node(id).grpcPort, path), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Marmot-Secret", clusterSecret)
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s on node %d: status %d: %s", method, path, id, resp.StatusCode, body)
	}
	return json.NewDecoder(resp.Body).Decode(&struct {
		Data any `json:"data"`
	}{Data: out})
}

// statuses is node id's view of every member's status, keyed by node id.
func (c *cluster) statuses(id int) (map[uint64]string, error) {
	var members []member
	if err := c.admin(id, http.MethodGet, "/cluster/members", clientTimeout, &members); err != nil {
		return nil, err
	}
	out := make(map[uint64]string, len(members))
	for _, m := range members {
		out[m.NodeID] = m.Status
	}
	return out, nil
}

// autoIncState asks node id over gRPC for its AUTO_INCREMENT claim bases and
// whether its claim votes are held. GetAutoIncBases predates the admin votes
// endpoint, so this also answers for a node on an older release.
func (c *cluster) autoIncState(id int) (*marmotgrpc.AutoIncBasesResponse, error) {
	defer clientCalls.Add(1)
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", c.node(id).grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, marmotgrpc.ClusterSecretHeader, clusterSecret)
	return marmotgrpc.NewMarmotServiceClient(conn).GetAutoIncBases(ctx, &marmotgrpc.AutoIncBasesRequest{})
}

// votesHeld reports whether node id's AUTO_INCREMENT claim votes are held.
func (c *cluster) votesHeld(id int) (bool, error) {
	state, err := c.autoIncState(id)
	if err != nil {
		return false, err
	}
	return state.GetVotesHeld(), nil
}

// autoIncBase is node id's AUTO_INCREMENT claim base for database.table (the
// largest of its committed, seed and merged floors); found is false while
// the node has no claim row for the table.
func (c *cluster) autoIncBase(id int, database, table string) (base uint64, found bool, err error) {
	state, err := c.autoIncState(id)
	if err != nil {
		return 0, false, err
	}
	for _, b := range state.GetBases() {
		if b.GetDatabase() == database && b.GetTable() == table {
			return b.GetBase(), true, nil
		}
	}
	return 0, false, nil
}

// waitAutoIncBaseAtLeast waits until every node in ids holds a claim base of
// at least min for database.table.
func (c *cluster) waitAutoIncBaseAtLeast(database, table string, min uint64, ids ...int) {
	c.t.Helper()
	waitFor(c.t, fmt.Sprintf("claim base of %s.%s >= %d on nodes %v", database, table, min, ids), replicationDeadline, func() (bool, string) {
		for _, id := range ids {
			base, found, err := c.autoIncBase(id, database, table)
			if err != nil || !found || base < min {
				return false, fmt.Sprintf("node %d: base %d found %v err %v", id, base, found, err)
			}
		}
		return true, ""
	})
}

// readiness describes why node id is not ready for writes yet, or returns ""
// once it is: it sees itself and every node in running ALIVE, and its claim
// votes are not held (a node that has not merged claim bases from enough
// members declines every claim, so a narrow AUTO_INCREMENT insert through it
// returns 1205).
func (c *cluster) readiness(id int, running []int) string {
	// The MySQL listener starts last, once the node finished starting: only
	// then is its own status the one it keeps (a restarted node restores its
	// membership before it marks itself JOINING).
	if err := c.ping(id); err != nil {
		return fmt.Sprintf("node %d MySQL: %v", id, err)
	}
	statuses, err := c.statuses(id)
	if err != nil {
		return fmt.Sprintf("node %d: %v", id, err)
	}
	for _, peer := range running {
		if s := statuses[uint64(peer)]; s != "ALIVE" {
			return fmt.Sprintf("node %d sees node %d as %q (all: %v)", id, peer, s, statuses)
		}
	}
	held, err := c.votesHeld(id)
	if err != nil {
		return fmt.Sprintf("node %d votes: %v", id, err)
	}
	if held {
		return fmt.Sprintf("node %d claim votes held", id)
	}
	return ""
}

// running lists the ids of the nodes whose process is running.
func (c *cluster) running() []int {
	var ids []int
	for _, n := range c.nodes {
		n.mu.Lock()
		if n.cmd != nil {
			ids = append(ids, n.id)
		}
		n.mu.Unlock()
	}
	return ids
}

// waitReady waits until every running node is ready for writes (readiness).
func (c *cluster) waitReady() {
	c.t.Helper()
	running := c.running()
	waitFor(c.t, fmt.Sprintf("nodes %v ready", running), readyDeadline, func() (bool, string) {
		for _, id := range running {
			if why := c.readiness(id, running); why != "" {
				return false, why
			}
		}
		return true, ""
	})
}

// start starts every node and waits until the cluster is ready. Node 1, every
// other node's seed, starts first: a joiner that cannot reach its seed at boot
// stays outside the cluster.
func (c *cluster) start() {
	c.t.Helper()
	c.startNodes(c.all()...)
}

// all lists every node id.
func (c *cluster) all() []int {
	ids := make([]int, len(c.nodes))
	for i := range ids {
		ids[i] = i + 1
	}
	return ids
}

// startNodes starts nodes ids, which begin with the seed node 1, as start
// does; the other nodes stay unstarted, unknown to the cluster.
func (c *cluster) startNodes(ids ...int) {
	c.t.Helper()
	if ids[0] != 1 {
		c.t.Fatalf("startNodes(%v): the seed node 1 starts first", ids)
	}
	c.startNode(1)
	waitFor(c.t, "seed node 1 ALIVE", readyDeadline, func() (bool, string) {
		statuses, err := c.statuses(1)
		if err != nil {
			return false, err.Error()
		}
		return statuses[1] == "ALIVE", fmt.Sprint(statuses)
	})
	for _, id := range ids[1:] {
		c.startNode(id)
	}
	c.waitReady()
}

// restart starts node id again and waits until the cluster is ready.
func (c *cluster) restart(id int) {
	c.t.Helper()
	c.startNode(id)
	c.waitReady()
}
