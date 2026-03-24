package admin

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	marmotgrpc "github.com/maxpert/marmot/grpc"
)

// quorumSafeAfterLeaving checks whether the cluster can still achieve quorum
// if the given node transitions to LEAVING. It operates on pre-transition counts:
//   - total: current Count() (excludes REMOVED and already-LEAVING nodes)
//   - alive: current CountAlive() (ALIVE nodes only)
//
// After MarkLeaving, Count() and CountAlive() will each drop by 1 for this node.
// Safe when: (alive - 1) >= ((total - 1) / 2) + 1
func quorumSafeAfterLeaving(total, alive int) bool {
	newTotal := total - 1
	newAlive := alive - 1
	newQuorum := (newTotal / 2) + 1
	return newAlive >= newQuorum
}

// resolveRegistryOrError writes an error and returns nil when the registry is unavailable.
func (h *AdminHandlers) resolveRegistryOrError(w http.ResponseWriter) *marmotgrpc.NodeRegistry {
	registry := h.getRegistry()
	if registry == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "node registry unavailable")
		return nil
	}
	return registry
}

// handleDecommission handles POST /admin/cluster/decommission/{nodeID}.
// It marks the target node as LEAVING after verifying quorum safety.
func (h *AdminHandlers) handleDecommission(w http.ResponseWriter, r *http.Request) {
	nodeID, err := parsePeerNodeID(chi.URLParam(r, "nodeID"))
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	registry := h.resolveRegistryOrError(w)
	if registry == nil {
		return
	}

	// Refuse decommissioning self — operator must use SIGTERM for graceful self-shutdown.
	if nodeID == registry.GetLocalNodeID() {
		writeErrorResponse(w, http.StatusBadRequest, "cannot decommission self; send SIGTERM to the node instead")
		return
	}

	node, exists := registry.Get(nodeID)
	if !exists {
		writeErrorResponse(w, http.StatusNotFound, fmt.Sprintf("node %d not found", nodeID))
		return
	}

	// Only ALIVE nodes can be decommissioned via this API.
	switch node.Status {
	case marmotgrpc.NodeStatus_ALIVE:
		// proceed
	case marmotgrpc.NodeStatus_LEAVING:
		writeErrorResponse(w, http.StatusConflict, fmt.Sprintf("node %d is already LEAVING", nodeID))
		return
	case marmotgrpc.NodeStatus_REMOVED:
		writeErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("node %d is REMOVED; use allow endpoint to reinstate it", nodeID))
		return
	default:
		writeErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("node %d cannot be decommissioned in state %s", nodeID, node.Status.String()))
		return
	}

	// Quorum safety check: simulate the post-LEAVING counts.
	total, alive, _ := registry.QuorumInfo()
	if !quorumSafeAfterLeaving(total, alive) {
		writeErrorResponse(w, http.StatusConflict, fmt.Sprintf(
			"decommissioning node %d would break quorum: %d alive nodes cannot satisfy quorum of %d with %d total members",
			nodeID, alive-1, (total-1)/2+1, total-1,
		))
		return
	}

	if err := registry.MarkLeaving(nodeID); err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusAccepted)
	writeJSONResponse(w, map[string]any{
		"node_id": nodeID,
		"status":  marmotgrpc.NodeStatus_LEAVING.String(),
		"message": "decommission started, node will drain and shut down",
	}, false, "")
}

// handleDecommissionStatus handles GET /admin/cluster/decommission/{nodeID}/status.
func (h *AdminHandlers) handleDecommissionStatus(w http.ResponseWriter, r *http.Request) {
	nodeID, err := parsePeerNodeID(chi.URLParam(r, "nodeID"))
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	registry := h.resolveRegistryOrError(w)
	if registry == nil {
		return
	}

	node, exists := registry.Get(nodeID)
	if !exists {
		writeErrorResponse(w, http.StatusNotFound, fmt.Sprintf("node %d not found", nodeID))
		return
	}

	writeJSONResponse(w, map[string]any{
		"node_id": nodeID,
		"status":  node.Status.String(),
	}, false, "")
}

// handleDecommissionCancel handles POST /admin/cluster/decommission/{nodeID}/cancel.
// It reverts a LEAVING node back to ALIVE, cancelling the decommission.
func (h *AdminHandlers) handleDecommissionCancel(w http.ResponseWriter, r *http.Request) {
	nodeID, err := parsePeerNodeID(chi.URLParam(r, "nodeID"))
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	registry := h.resolveRegistryOrError(w)
	if registry == nil {
		return
	}

	if err := registry.RevertLeaving(nodeID); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSONResponse(w, map[string]any{
		"node_id": nodeID,
		"status":  marmotgrpc.NodeStatus_ALIVE.String(),
		"message": "decommission cancelled",
	}, false, "")
}
