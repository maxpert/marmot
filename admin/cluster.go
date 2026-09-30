package admin

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/maxpert/marmot/cfg"
	marmotgrpc "github.com/maxpert/marmot/grpc"
)

// getRegistry returns the NodeRegistry from the server
func (h *AdminHandlers) getRegistry() *marmotgrpc.NodeRegistry {
	if h.server == nil {
		return nil
	}
	return h.server.GetNodeRegistry()
}

// handleClusterMembers handles GET /admin/cluster/members
func (h *AdminHandlers) handleClusterMembers(w http.ResponseWriter, r *http.Request) {
	registry := h.resolveRegistryOrError(w)
	if registry == nil {
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
	registry := h.resolveRegistryOrError(w)
	if registry == nil {
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

// handleClusterReplication handles GET /admin/cluster/replication. It
// reports the log-pull replication state per peer and per database,
// instead of the removed MetaStore.ReplicationState table: for each peer,
// its last-consumed position in this node's log for the database
// (MetaStore.ConsumedPositions), and, separately, this node's own pull
// cursor into that peer's log (MetaStore.GetPullCursor) - the two directions
// of replication progress between this node and the peer. The per-database
// caught-up/stuck status (h.antiEntropyStatus) is not per peer: it is this
// node's own view of the whole database.
func (h *AdminHandlers) handleClusterReplication(w http.ResponseWriter, r *http.Request) {
	registry := h.resolveRegistryOrError(w)
	if registry == nil {
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

			consumed, err := metaStore.ConsumedPositions()
			if err != nil {
				continue
			}
			cursor, err := metaStore.GetPullCursor(member.NodeID)
			if err != nil {
				continue
			}

			consumedPos, hasConsumed := consumed[member.NodeID]

			dbStates = append(dbStates, map[string]interface{}{
				"database":                 dbName,
				"peer_consumed_seq":        consumedPos.Seq,
				"peer_consumed_txn_id":     consumedPos.TxnID,
				"peer_has_consumed_at_all": hasConsumed,
				"local_pull_cursor_seq":    cursor.Seq,
				"local_pull_cursor_txn_id": cursor.TxnID,
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
		"peers":        result,
		"anti_entropy": h.antiEntropyStatus(databases),
	}, false, "")
}

// antiEntropyStatus reports, per database, anti-entropy's status as of its
// last round: whether every current member's
// log was reachable and fully pulled (CaughtUp), and how many transactions
// are STUCK on a deterministic replay failure (LogPuller.StuckTxns). Empty
// until anti-entropy is wired in (SetAntiEntropy).
func (h *AdminHandlers) antiEntropyStatus(databases []string) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(databases))
	if h.antiEntropy == nil {
		return result
	}
	for _, dbName := range databases {
		result = append(result, map[string]interface{}{
			"database":        dbName,
			"caught_up":       h.antiEntropy.CaughtUp(dbName),
			"promotion_ready": h.antiEntropy.PromotionReady(dbName),
			"stuck_txns":      h.antiEntropy.StuckTxnCount(dbName),
		})
	}
	return result
}

// handleClusterRemove handles POST /admin/cluster/remove/{node_id}
func (h *AdminHandlers) handleClusterRemove(w http.ResponseWriter, r *http.Request) {
	nodeID, err := parsePeerNodeID(chi.URLParam(r, "nodeID"))
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	registry := h.resolveRegistryOrError(w)
	if registry == nil {
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
	nodeID, err := parsePeerNodeID(chi.URLParam(r, "nodeID"))
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	registry := h.resolveRegistryOrError(w)
	if registry == nil {
		return
	}

	if err := registry.AllowRejoin(nodeID); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSONResponse(w, map[string]any{"success": true, "node_id": nodeID, "status": "ALLOWED"}, false, "")
}
