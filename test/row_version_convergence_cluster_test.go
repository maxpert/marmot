package test

// Cluster test for row-value convergence: nodes 1 and 2 keep overwriting the
// same rows while node 3 is down and for a moment after it returns, then
// node 3 writes each row once more. Every node must end with the same value
// for every row. The order in which a node meets older and newer images of
// a row is pinned deterministically by grpc's log puller tests; this checks
// the end-to-end outcome.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// hotRows is how many rows the writers keep overwriting.
	hotRows = 10
	// hotOutage is how long node 3 stays down under load.
	hotOutage = 10 * time.Second
	// hotAfterRestart is how long the load continues once node 3 is back.
	hotAfterRestart = 2 * time.Second
)

// tableChecksum hashes every (id, v) of table on node, in id order.
func tableChecksum(t *testing.T, h *ClusterHarness, node int, database, table string) (string, error) {
	a, err := queryAnswer(openTimedNodeDatabase(t, h, node, database), "SELECT id, v FROM "+table+" ORDER BY id")
	if err != nil {
		return "", fmt.Errorf("node %d: %v", node, err)
	}
	sum := sha256.New()
	for _, r := range a.rows {
		fmt.Fprintf(sum, "%s=%s\n", r[0], r[1])
	}
	return hex.EncodeToString(sum.Sum(nil))[:16], nil
}

// waitChecksumsEqual polls until table's checksum is identical on every
// node, within deadline.
func waitChecksumsEqual(t *testing.T, h *ClusterHarness, database, table string, deadline time.Duration) {
	t.Helper()
	start := time.Now()
	for {
		h.Progress()
		sums := make([]string, 0, numNodes)
		for node := 1; node <= numNodes; node++ {
			sum, err := tableChecksum(t, h, node, database, table)
			if err != nil {
				sum = err.Error()
			}
			sums = append(sums, sum)
		}
		equal := true
		for _, s := range sums[1:] {
			equal = equal && s == sums[0]
		}
		if equal {
			t.Logf("checksums equal on every node %s after load stopped", time.Since(start).Round(time.Millisecond))
			return
		}
		if time.Since(start) > deadline {
			h.dumpNodeLogs("cluster_" + strings.ReplaceAll(t.Name(), "/", "_"))
			t.Fatalf("%s.%s checksums still differ %s after load stopped: %v", database, table, deadline, sums)
		}
		time.Sleep(convergencePoll)
	}
}

// runHotWriters has each node in nodes overwrite rows 1..hotRows in turn
// until stop closes. A statement that fails is retried on the next turn.
func runHotWriters(t *testing.T, h *ClusterHarness, database, table string, nodes []int, stop <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			conn := openTimedNodeDatabase(t, h, n, database)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := i%hotRows + 1
				if _, err := execTimed(conn, "UPDATE "+table+" SET v = ? WHERE id = ?", fmt.Sprintf("n%d#%d", n, i), id); err != nil {
					time.Sleep(convergencePoll)
					continue
				}
				h.Progress()
			}
		}(n)
	}
	return &wg
}

func TestSameRowsOverwrittenAcrossOutageConvergeByValue(t *testing.T) {
	h := NewClusterHarness(t)
	defer h.Cleanup()
	const database, table = "hotrows", "t"
	startWithTables(t, h, map[string][]string{database: {table}})
	conn := openTimedNodeDatabase(t, h, 1, database)
	for id := 1; id <= hotRows; id++ {
		if _, err := execTimed(conn, "INSERT INTO "+table+" (id, v) VALUES (?, 'seed')", id); err != nil {
			t.Fatalf("seed row %d: %v", id, err)
		}
	}
	waitChecksumsEqual(t, h, database, table, convergenceDeadline)
	h.Phase("seeded")

	if err := h.StopNode(3); err != nil {
		t.Fatal(err)
	}
	h.Phase("node3-stopped")
	stop := make(chan struct{})
	wg := runHotWriters(t, h, database, table, []int{1, 2}, stop)
	sleepWithProgress(h, hotOutage)

	if err := h.StartNode(3); err != nil {
		t.Fatal(err)
	}
	if err := h.WaitForAlive(3, restartDeadline); err != nil {
		t.Fatal(err)
	}
	h.Phase("node3-alive")
	sleepWithProgress(h, hotAfterRestart)
	close(stop)
	wg.Wait()
	h.Phase("load-stopped")

	// One last live write per row, coordinated by node 3 itself, while the
	// writes it missed during the outage may still be waiting to be pulled.
	conn3 := openTimedNodeDatabase(t, h, 3, database)
	for id := 1; id <= hotRows; id++ {
		if _, err := execTimed(conn3, "UPDATE "+table+" SET v = ? WHERE id = ?", fmt.Sprintf("final#%d", id), id); err != nil {
			t.Fatalf("final update of row %d: %v", id, err)
		}
	}
	h.Phase("final-writes")

	waitChecksumsEqual(t, h, database, table, convergenceDeadline)
	h.Phase("converged")
}
