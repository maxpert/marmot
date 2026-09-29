package test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// gracefulStopDeadline bounds a node's own shutdown after SIGTERM.
const gracefulStopDeadline = 30 * time.Second

// memberStatus returns nodeID's status in observer's membership view.
func memberStatus(h *ClusterHarness, observer int, nodeID uint64) (string, error) {
	url := fmt.Sprintf("http://localhost:%d/admin/cluster/members", h.Nodes[observer-1].GRPCPort)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Marmot-Secret", "test-secret")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var wrapped struct {
		Data []struct {
			NodeID uint64 `json:"node_id"`
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return "", fmt.Errorf("members on node %d: %w: %s", observer, err, body)
	}
	for _, m := range wrapped.Data {
		if m.NodeID == nodeID {
			return m.Status, nil
		}
	}
	return "", fmt.Errorf("node %d absent from node %d's membership", nodeID, observer)
}

// TestGracefulStopThenRestartRejoins: a node stopped with SIGTERM announces
// LEAVING and its peers keep that record. Restarted, it must rejoin as ALIVE and
// serve writes, not read its own departure as a decommission and shut down.
func TestGracefulStopThenRestartRejoins(t *testing.T) {
	h := NewClusterHarness(t)
	defer h.Cleanup()
	tables := map[string][]string{"marmot": {"t"}}
	startWithTables(t, h, tables)

	if err := h.GracefulStopNode(3, gracefulStopDeadline); err != nil {
		t.Fatal(err)
	}
	h.Phase("node-3-stopped-gracefully")
	if err := h.StartNode(3); err != nil {
		t.Fatal(err)
	}
	if err := h.WaitForAlive(3, restartDeadline); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(restartDeadline)
	for {
		s1, err1 := memberStatus(h, 1, 3)
		s3, err3 := memberStatus(h, 3, 3)
		if err1 == nil && err3 == nil && s1 == "ALIVE" && s3 == "ALIVE" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node 3 not ALIVE within %v of restart: node1 view=%q (%v), own view=%q (%v); log tail:\n%s",
				restartDeadline, s1, err1, s3, err3, h.getNodeLogTail(3, 30))
		}
		time.Sleep(convergencePoll)
	}
	h.Phase("node-3-alive")

	l := newKeyLedger(h)
	insertFromEveryNode(t, h, l, "after-restart")
	waitKeysConverged(t, h, l, tables, convergenceDeadline)
}
