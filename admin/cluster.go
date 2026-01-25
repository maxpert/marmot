package admin

import (
	"net/http"
	"strings"

	"github.com/maxpert/marmot/cfg"
	marmotgrpc "github.com/maxpert/marmot/grpc"
)

// getRegistry returns the NodeRegistry from the server
func (h *AdminHandlers) getRegistry() *marmotgrpc.NodeRegistry {
	if h.server == nil {
		return nil
	}
	if regGetter, ok := any(h.server).(interface {
		GetRegistry() *marmotgrpc.NodeRegistry
	}); ok {
		return regGetter.GetRegistry()
	}
	return nil
}

// handleClusterMembers handles GET /admin/cluster/members
func (h *AdminHandlers) handleClusterMembers(w http.ResponseWriter, r *http.Request) {
	registry := h.getRegistry()
	if registry == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "node registry unavailable")
		return
	}

	members := registry.GetMembershipInfo()
	localNodeID := registry.GetLocalNodeID()

	// Build seed node address set for quick lookup
	seedAddrs := make(map[string]bool)
	if cfg.Config != nil && cfg.Config.Cluster.SeedNodes != nil {
		for _, addr := range cfg.Config.Cluster.SeedNodes {
			seedAddrs[addr] = true
		}
	}

	resp := make([]map[string]interface{}, 0, len(members))
	for _, m := range members {
		isSeed := seedAddrs[m.Address]
		role := "member"
		if isSeed {
			role = "seed"
		}

		resp = append(resp, map[string]interface{}{
			"node_id":     m.NodeID,
			"address":     m.Address,
			"status":      m.Status,
			"incarnation": m.Incarnation,
			"is_local":    m.NodeID == localNodeID,
			"is_seed":     isSeed,
			"role":        role,
		})
	}

	writeJSONResponse(w, resp, false, "")
}

// handleClusterHealth handles GET /admin/cluster/health
func (h *AdminHandlers) handleClusterHealth(w http.ResponseWriter, r *http.Request) {
	registry := h.getRegistry()
	if registry == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "node registry unavailable")
		return
	}

	total, alive, quorum := registry.QuorumInfo()
	hasQuorum := alive >= quorum

	status := http.StatusOK
	if !hasQuorum {
		status = http.StatusServiceUnavailable
	}

	w.WriteHeader(status)
	writeJSONResponse(w, map[string]interface{}{
		"healthy":       hasQuorum,
		"total_nodes":   total,
		"alive_nodes":   alive,
		"quorum_size":   quorum,
		"has_quorum":    hasQuorum,
		"local_node_id": registry.GetLocalNodeID(),
	}, false, "")
}

// handleClusterReplication handles GET /admin/cluster/replication
func (h *AdminHandlers) handleClusterReplication(w http.ResponseWriter, r *http.Request) {
	registry := h.getRegistry()
	if registry == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "node registry unavailable")
		return
	}

	members := registry.GetMembershipInfo()
	localNodeID := registry.GetLocalNodeID()
	databases := h.dbManager.ListDatabases()

	result := make([]map[string]interface{}, 0)

	for _, member := range members {
		if member.NodeID == localNodeID {
			continue
		}

		dbStates := make([]map[string]interface{}, 0)

		for _, dbName := range databases {
			mdb, err := h.dbManager.GetDatabase(dbName)
			if err != nil {
				continue
			}

			metaStore := mdb.GetMetaStore()
			state, _ := metaStore.GetReplicationState(member.NodeID, dbName)
			localMaxTxn, _ := metaStore.GetMaxCommittedTxnID()

			var lastAppliedTxnID uint64
			var lastSyncTime string
			var syncStatus string
			if state != nil {
				lastAppliedTxnID = state.LastAppliedTxnID
				lastSyncTime = formatTimestamp(state.LastSyncTime)
				syncStatus = state.SyncStatus.String()
			} else {
				syncStatus = "UNKNOWN"
			}

			lag := uint64(0)
			if localMaxTxn > lastAppliedTxnID {
				lag = localMaxTxn - lastAppliedTxnID
			}

			dbStates = append(dbStates, map[string]interface{}{
				"database":            dbName,
				"last_applied_txn_id": lastAppliedTxnID,
				"last_sync_time":      lastSyncTime,
				"sync_status":         syncStatus,
				"lag_txns":            lag,
			})
		}

		result = append(result, map[string]interface{}{
			"node_id":   member.NodeID,
			"address":   member.Address,
			"status":    member.Status,
			"databases": dbStates,
		})
	}

	writeJSONResponse(w, map[string]interface{}{
		"peers":                 result,
		"cluster_min_watermark": registry.GetClusterMinWatermark(),
		"local_watermark":       registry.GetLocalWatermark(),
	}, false, "")
}

// handleClusterRemove handles POST /admin/cluster/remove/{node_id}
func (h *AdminHandlers) handleClusterRemove(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/cluster/remove/")
	nodeID, err := parsePeerNodeID(path)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	registry := h.getRegistry()
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
	path := strings.TrimPrefix(r.URL.Path, "/admin/cluster/allow/")
	nodeID, err := parsePeerNodeID(path)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	registry := h.getRegistry()
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
