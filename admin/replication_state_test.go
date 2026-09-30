package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maxpert/marmot/db"
)

// newTestMetaStore creates a real db.MetaStore backed by a temp directory,
// for exercising the admin replication handlers directly.
func newTestMetaStore(t *testing.T) db.MetaStore {
	t.Helper()
	ms, err := db.NewMetaStore(t.TempDir() + "/replication_state")
	if err != nil {
		t.Fatalf("NewMetaStore failed: %v", err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}

// TestHandleReplicationByPeerReportsLogPullState: the admin per-peer
// replication endpoint must report the log-pull state - the peer's last-consumed position in this
// node's log, and this node's own pull cursor into the peer's log - not the
// removed MetaStore.ReplicationState table.
func TestHandleReplicationByPeerReportsLogPullState(t *testing.T) {
	metaStore := newTestMetaStore(t)
	if err := metaStore.SetConsumedPosition(2, db.LogPosition{Seq: 7, TxnID: 700}); err != nil {
		t.Fatalf("SetConsumedPosition failed: %v", err)
	}
	if err := metaStore.SetPullCursor(2, db.LogPosition{Seq: 3, TxnID: 300}); err != nil {
		t.Fatalf("SetPullCursor failed: %v", err)
	}

	h := &AdminHandlers{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/replication/state/2", nil)
	h.handleReplicationByPeer(rec, req, metaStore, 2, "app")

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", rec.Code, rec.Body.String())
	}

	var envelope map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("failed to decode response: %v; body: %s", err, rec.Body.String())
	}
	body, ok := envelope["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected a data object, got: %s", rec.Body.String())
	}
	if got, want := body["peer_consumed_seq"], float64(7); got != want {
		t.Errorf("peer_consumed_seq = %v, want %v", got, want)
	}
	if got, want := body["peer_consumed_txn_id"], float64(700); got != want {
		t.Errorf("peer_consumed_txn_id = %v, want %v", got, want)
	}
	if got, want := body["local_pull_cursor_seq"], float64(3); got != want {
		t.Errorf("local_pull_cursor_seq = %v, want %v", got, want)
	}
	if got, want := body["local_pull_cursor_txn_id"], float64(300); got != want {
		t.Errorf("local_pull_cursor_txn_id = %v, want %v", got, want)
	}
}

// TestHandleReplicationAllReportsConsumedPositionsAndTruncation: every peer's last-consumed position, and the database's own GC
// truncation point, in place of the removed replication-state table.
func TestHandleReplicationAllReportsConsumedPositionsAndTruncation(t *testing.T) {
	metaStore := newTestMetaStore(t)
	if err := metaStore.SetConsumedPosition(5, db.LogPosition{Seq: 11, TxnID: 1100}); err != nil {
		t.Fatalf("SetConsumedPosition failed: %v", err)
	}

	h := &AdminHandlers{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/replication/states", nil)
	h.handleReplicationAll(rec, req, metaStore, "app")

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", rec.Code, rec.Body.String())
	}

	var envelope map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("failed to decode response: %v; body: %s", err, rec.Body.String())
	}
	body, ok := envelope["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected a data object, got: %s", rec.Body.String())
	}
	peers, ok := body["peers"].([]interface{})
	if !ok || len(peers) != 1 {
		t.Fatalf("expected exactly one peer in the response, got %v", body["peers"])
	}
	peer := peers[0].(map[string]interface{})
	if got, want := peer["peer_node_id"], float64(5); got != want {
		t.Errorf("peer_node_id = %v, want %v", got, want)
	}
	if got, want := peer["peer_consumed_seq"], float64(11); got != want {
		t.Errorf("peer_consumed_seq = %v, want %v", got, want)
	}
}
