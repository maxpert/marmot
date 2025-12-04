package admin

import (
	"net/http"
	"strings"

	marmotgrpc "github.com/maxpert/marmot/grpc"
)

// handleClusterMembers handles GET /admin/cluster/members
func (h *AdminHandlers) handleClusterMembers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.server == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "server not initialized")
		return
	}

	var registry *marmotgrpc.NodeRegistry
	if regGetter, ok := any(h.server).(interface {
		GetRegistry() *marmotgrpc.NodeRegistry
	}); ok {
		registry = regGetter.GetRegistry()
	}
	if registry == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "node registry unavailable")
		return
	}

	members := registry.GetMembershipInfo()
	resp := make([]map[string]interface{}, 0, len(members))
	for _, m := range members {
		resp = append(resp, map[string]interface{}{
			"node_id":     m.NodeID,
			"address":     m.Address,
			"status":      m.Status,
			"incarnation": m.Incarnation,
		})
	}

	writeJSONResponse(w, resp, false, "")
}

// handleClusterRemove handles POST /admin/cluster/remove/{node_id}
func (h *AdminHandlers) handleClusterRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.server == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "server not initialized")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/admin/cluster/remove/")
	nodeID, err := parsePeerNodeID(path)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	var registry *marmotgrpc.NodeRegistry
	if regGetter, ok := any(h.server).(interface {
		GetRegistry() *marmotgrpc.NodeRegistry
	}); ok {
		registry = regGetter.GetRegistry()
	}
	if registry == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "node registry unavailable")
		return
	}

	if err := registry.MarkRemoved(nodeID); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSONResponse(w, map[string]any{"success": true, "node_id": nodeID, "status": "REMOVED"}, false, "")
}

// handleClusterAllow handles POST /admin/cluster/allow/{node_id}
func (h *AdminHandlers) handleClusterAllow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.server == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "server not initialized")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/admin/cluster/allow/")
	nodeID, err := parsePeerNodeID(path)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	var registry *marmotgrpc.NodeRegistry
	if regGetter, ok := any(h.server).(interface {
		GetRegistry() *marmotgrpc.NodeRegistry
	}); ok {
		registry = regGetter.GetRegistry()
	}
	if registry == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "node registry unavailable")
		return
	}

	if err := registry.AllowRejoin(nodeID); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSONResponse(w, map[string]any{"success": true, "node_id": nodeID, "status": "ALLOWED"}, false, "")
}
