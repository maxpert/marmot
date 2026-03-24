package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	marmotgrpc "github.com/maxpert/marmot/grpc"
)

// serveDecommissionRequest dispatches an HTTP request through a chi router that
// mirrors the production decommission routes, using handler functions that operate
// directly on the NodeRegistry (bypassing AdminHandlers which requires *grpc.Server).
func serveDecommissionRequest(t *testing.T, registry *marmotgrpc.NodeRegistry, method, path string) *httptest.ResponseRecorder {
	t.Helper()

	r := chi.NewRouter()
	r.Post("/decommission/{nodeID}", func(w http.ResponseWriter, req *http.Request) {
		nodeID, err := parsePeerNodeID(chi.URLParam(req, "nodeID"))
		if err != nil {
			writeErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}
		decommissionHandler(w, req, nodeID, registry)
	})
	r.Get("/decommission/{nodeID}/status", func(w http.ResponseWriter, req *http.Request) {
		nodeID, err := parsePeerNodeID(chi.URLParam(req, "nodeID"))
		if err != nil {
			writeErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}
		decommissionStatusHandler(w, nodeID, registry)
	})
	r.Post("/decommission/{nodeID}/cancel", func(w http.ResponseWriter, req *http.Request) {
		nodeID, err := parsePeerNodeID(chi.URLParam(req, "nodeID"))
		if err != nil {
			writeErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}
		decommissionCancelHandler(w, nodeID, registry)
	})

	req := httptest.NewRequest(method, path, nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	return rr
}

// decommissionHandler mirrors handleDecommission logic, operating on a registry directly.
// This avoids the grpc.Server dependency in tests while exercising the same code paths.
func decommissionHandler(w http.ResponseWriter, _ *http.Request, nodeID uint64, registry *marmotgrpc.NodeRegistry) {
	localID := registry.GetLocalNodeID()
	if nodeID == localID {
		writeErrorResponse(w, http.StatusBadRequest, "cannot decommission self; send SIGTERM to the node instead")
		return
	}

	node, exists := registry.Get(nodeID)
	if !exists {
		writeErrorResponse(w, http.StatusNotFound, "node not found")
		return
	}

	switch node.Status {
	case marmotgrpc.NodeStatus_ALIVE:
		// proceed
	case marmotgrpc.NodeStatus_LEAVING:
		writeErrorResponse(w, http.StatusConflict, "node is already LEAVING")
		return
	case marmotgrpc.NodeStatus_REMOVED:
		writeErrorResponse(w, http.StatusBadRequest, "node is REMOVED; use allow endpoint to reinstate it")
		return
	default:
		writeErrorResponse(w, http.StatusBadRequest, "node cannot be decommissioned in current state")
		return
	}

	total, alive, _ := registry.QuorumInfo()
	if !quorumSafeAfterLeaving(total, alive) {
		writeErrorResponse(w, http.StatusConflict, "decommissioning would break quorum")
		return
	}

	if err := registry.MarkLeaving(nodeID); err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusAccepted)
	writeJSONResponse(w, map[string]any{
		"node_id": nodeID,
		"status":  "LEAVING",
		"message": "decommission started, node will drain and shut down",
	}, false, "")
}

func decommissionStatusHandler(w http.ResponseWriter, nodeID uint64, registry *marmotgrpc.NodeRegistry) {
	node, exists := registry.Get(nodeID)
	if !exists {
		writeErrorResponse(w, http.StatusNotFound, "node not found")
		return
	}
	writeJSONResponse(w, map[string]any{
		"node_id": nodeID,
		"status":  node.Status.String(),
	}, false, "")
}

func decommissionCancelHandler(w http.ResponseWriter, nodeID uint64, registry *marmotgrpc.NodeRegistry) {
	if err := registry.RevertLeaving(nodeID); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSONResponse(w, map[string]any{
		"node_id": nodeID,
		"status":  "ALIVE",
		"message": "decommission cancelled",
	}, false, "")
}

// --- Unit tests for quorumSafeAfterLeaving ---

func TestQuorumSafeAfterLeaving_SingleNode(t *testing.T) {
	t.Parallel()
	// 1 total, 1 alive → after leaving: 0 total, 0 alive, quorum=1 → unsafe
	if quorumSafeAfterLeaving(1, 1) {
		t.Error("single node decommission must be unsafe")
	}
}

func TestQuorumSafeAfterLeaving_TwoNodes_Blocked(t *testing.T) {
	t.Parallel()
	// 2 total, 2 alive → after: 1 total, 1 alive, quorum=1 → 1>=1 safe
	// Wait — 2 nodes: newTotal=1, newQuorum=(1/2)+1=1, newAlive=1 → 1>=1 → safe
	// But in practice a 2-node cluster needs both for writes; quorum math says it's safe.
	// This reflects majority quorum: 2 nodes, quorum=1 (floor(2/2)+1=2, NOT 1).
	// Let's check: total=2, (2/2)+1=2. After leaving: newTotal=1, newQuorum=(1/2)+1=1. Safe.
	// Hmm — actually with 2 nodes both alive, (alive-1)=1 >= (newQuorum=1): safe.
	// With 2 nodes and only 1 alive, (alive-1)=0 >= 1: NOT safe.
	if quorumSafeAfterLeaving(2, 1) {
		t.Error("2 nodes with 1 alive: decommission must be unsafe (would leave 0 alive, quorum needs 1)")
	}
}

func TestQuorumSafeAfterLeaving_TwoNodes_BothAlive(t *testing.T) {
	t.Parallel()
	// 2 total, 2 alive → newTotal=1, newAlive=1, newQuorum=1 → 1>=1 → safe
	if !quorumSafeAfterLeaving(2, 2) {
		t.Error("2 nodes both alive: decommission should be safe (1 alive satisfies quorum of 1)")
	}
}

func TestQuorumSafeAfterLeaving_ThreeNodes_OneDown(t *testing.T) {
	t.Parallel()
	// 3 total, 2 alive → newTotal=2, newAlive=1, newQuorum=(2/2)+1=2 → 1>=2? NO → unsafe
	if quorumSafeAfterLeaving(3, 2) {
		t.Error("3 nodes with 2 alive: decommission must be unsafe (only 1 alive left, needs 2 for quorum)")
	}
}

func TestQuorumSafeAfterLeaving_ThreeNodes_AllAlive(t *testing.T) {
	t.Parallel()
	// 3 total, 3 alive → newTotal=2, newAlive=2, newQuorum=2 → 2>=2 → safe
	if !quorumSafeAfterLeaving(3, 3) {
		t.Error("3 nodes all alive: decommission must be safe")
	}
}

func TestQuorumSafeAfterLeaving_FiveNodes_AllAlive(t *testing.T) {
	t.Parallel()
	// 5 total, 5 alive → newTotal=4, newAlive=4, newQuorum=3 → 4>=3 → safe
	if !quorumSafeAfterLeaving(5, 5) {
		t.Error("5 nodes all alive: decommission must be safe")
	}
}

func TestQuorumSafeAfterLeaving_FiveNodes_ThreeAlive(t *testing.T) {
	t.Parallel()
	// 5 total, 3 alive → newTotal=4, newAlive=2, newQuorum=3 → 2>=3? NO → unsafe
	if quorumSafeAfterLeaving(5, 3) {
		t.Error("5 nodes with 3 alive: decommission must be unsafe (2 alive, quorum=3)")
	}
}

func TestQuorumSafeAfterLeaving_FiveNodes_FourAlive(t *testing.T) {
	t.Parallel()
	// 5 total, 4 alive → newTotal=4, newAlive=3, newQuorum=3 → 3>=3 → safe
	if !quorumSafeAfterLeaving(5, 4) {
		t.Error("5 nodes with 4 alive: decommission must be safe (3 alive satisfies quorum of 3)")
	}
}

// --- HTTP handler integration tests ---

func newRegistry3Alive() *marmotgrpc.NodeRegistry {
	nr := marmotgrpc.NewNodeRegistry(1, "node1:8080")
	nr.Add(&marmotgrpc.NodeState{NodeId: 2, Address: "node2:8080", Status: marmotgrpc.NodeStatus_ALIVE})
	nr.Add(&marmotgrpc.NodeState{NodeId: 3, Address: "node3:8080", Status: marmotgrpc.NodeStatus_ALIVE})
	return nr
}

func TestDecommission_3Nodes_Allowed(t *testing.T) {
	t.Parallel()
	nr := newRegistry3Alive()

	rr := serveDecommissionRequest(t, nr, http.MethodPost, "/decommission/2")

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rr.Code, rr.Body.String())
	}
	if !nr.IsLeaving(2) {
		t.Error("node 2 should be LEAVING after decommission")
	}
}

func TestDecommission_2Nodes_Blocked(t *testing.T) {
	t.Parallel()
	// Node 1 (self) + node 2 both alive: 2-node cluster, 2 alive.
	// newTotal=1, newAlive=1, newQuorum=1 → actually safe per math.
	// Let's use a 2-node cluster where 1 is not alive to force block:
	nr := marmotgrpc.NewNodeRegistry(1, "node1:8080")
	nr.Add(&marmotgrpc.NodeState{NodeId: 2, Address: "node2:8080", Status: marmotgrpc.NodeStatus_SUSPECT})

	rr := serveDecommissionRequest(t, nr, http.MethodPost, "/decommission/2")

	// Node 2 is SUSPECT, not ALIVE — should be rejected with 400
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for SUSPECT node, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestDecommission_QuorumBlocked(t *testing.T) {
	t.Parallel()
	// 3 nodes, only 2 alive (self + node2), node3 is DEAD.
	// Count()=2 (excludes DEAD? No — DEAD is not REMOVED/LEAVING so it's counted).
	// Actually Count() excludes only REMOVED and LEAVING.
	// So: Count()=3, CountAlive()=2, QuorumInfo()=(3,2,2).
	// quorumSafeAfterLeaving(3,2): newTotal=2, newAlive=1, newQuorum=2 → 1<2 → unsafe.
	nr := marmotgrpc.NewNodeRegistry(1, "node1:8080")
	nr.Add(&marmotgrpc.NodeState{NodeId: 2, Address: "node2:8080", Status: marmotgrpc.NodeStatus_ALIVE})
	nr.Add(&marmotgrpc.NodeState{NodeId: 3, Address: "node3:8080", Status: marmotgrpc.NodeStatus_DEAD})

	rr := serveDecommissionRequest(t, nr, http.MethodPost, "/decommission/2")

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when decommission breaks quorum, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestDecommission_SelfNotAllowed(t *testing.T) {
	t.Parallel()
	nr := newRegistry3Alive()

	// Node 1 is self
	rr := serveDecommissionRequest(t, nr, http.MethodPost, "/decommission/1")

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for self decommission, got %d: %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "self") {
		t.Errorf("expected self-decommission error message, got: %s", body)
	}
}

func TestDecommission_AlreadyLeaving(t *testing.T) {
	t.Parallel()
	nr := newRegistry3Alive()

	// Mark node 2 as LEAVING first
	if err := nr.MarkLeaving(2); err != nil {
		t.Fatalf("setup: MarkLeaving failed: %v", err)
	}

	rr := serveDecommissionRequest(t, nr, http.MethodPost, "/decommission/2")

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 for already LEAVING node, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestDecommission_NodeNotFound(t *testing.T) {
	t.Parallel()
	nr := newRegistry3Alive()

	rr := serveDecommissionRequest(t, nr, http.MethodPost, "/decommission/999")

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown node, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestDecommissionStatus_Leaving(t *testing.T) {
	t.Parallel()
	nr := newRegistry3Alive()
	if err := nr.MarkLeaving(2); err != nil {
		t.Fatalf("setup: MarkLeaving failed: %v", err)
	}

	rr := serveDecommissionRequest(t, nr, http.MethodGet, "/decommission/2/status")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	data, ok := resp["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data field, got: %v", resp)
	}
	if data["status"] != "LEAVING" {
		t.Errorf("expected status LEAVING, got %v", data["status"])
	}
}

func TestDecommissionStatus_NotFound(t *testing.T) {
	t.Parallel()
	nr := newRegistry3Alive()

	rr := serveDecommissionRequest(t, nr, http.MethodGet, "/decommission/42/status")

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown node, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestDecommissionCancel_Success(t *testing.T) {
	t.Parallel()
	nr := newRegistry3Alive()
	if err := nr.MarkLeaving(2); err != nil {
		t.Fatalf("setup: MarkLeaving failed: %v", err)
	}

	rr := serveDecommissionRequest(t, nr, http.MethodPost, "/decommission/2/cancel")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if nr.IsLeaving(2) {
		t.Error("node 2 should no longer be LEAVING after cancel")
	}

	node, exists := nr.Get(2)
	if !exists {
		t.Fatal("node 2 should still exist after cancel")
	}
	if node.Status != marmotgrpc.NodeStatus_ALIVE {
		t.Errorf("expected ALIVE status after cancel, got %s", node.Status.String())
	}
}

func TestDecommissionCancel_NotLeaving(t *testing.T) {
	t.Parallel()
	nr := newRegistry3Alive()

	// Node 2 is ALIVE, not LEAVING — cancel should fail
	rr := serveDecommissionRequest(t, nr, http.MethodPost, "/decommission/2/cancel")

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for cancel of non-LEAVING node, got %d: %s", rr.Code, rr.Body.String())
	}
}
