package admin

import (
	"net/http"
	"strings"

	"github.com/maxpert/marmot/db"
)

// handleReplication handles replication-related endpoints
func (h *AdminHandlers) handleReplication(w http.ResponseWriter, r *http.Request, database, path string) {
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	// Parse path: replication/{action}
	pathParts := strings.SplitN(path, "/", 2)
	if len(pathParts) == 0 {
		writeErrorResponse(w, http.StatusNotFound, "not found")
		return
	}

	action := pathParts[0]

	switch action {
	case "all":
		h.handleReplicationAll(w, r, metaStore)
	case "database":
		if len(pathParts) > 1 {
			h.handleReplicationByDatabase(w, r, metaStore, pathParts[1])
		} else {
			writeErrorResponse(w, http.StatusNotFound, "not found")
		}
	default:
		// Check if it's a specific peer node ID
		if peerID, err := parsePeerNodeID(action); err == nil {
			h.handleReplicationByPeer(w, r, metaStore, peerID, database)
		} else {
			writeErrorResponse(w, http.StatusNotFound, "not found")
		}
	}
}

// handleReplicationByPeer returns replication state for specific peer
func (h *AdminHandlers) handleReplicationByPeer(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, peerNodeID uint64, database string) {
	rec, err := metaStore.GetReplicationState(peerNodeID, database)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	if rec == nil {
		writeErrorResponse(w, http.StatusNotFound, "replication state not found")
		return
	}

	response := map[string]interface{}{
		"peer_node_id":            rec.PeerNodeID,
		"database_name":           rec.DatabaseName,
		"last_applied_txn_id":     rec.LastAppliedTxnID,
		"last_applied_ts_wall":    rec.LastAppliedTSWall,
		"last_applied_ts_logical": rec.LastAppliedTSLogical,
		"last_sync_time":          formatTimestamp(rec.LastSyncTime),
		"sync_status":             rec.SyncStatus,
	}

	writeJSONResponse(w, response, false, "")
}

// handleReplicationAll returns all replication states
func (h *AdminHandlers) handleReplicationAll(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	states, err := metaStore.GetAllReplicationStates()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	var response []map[string]interface{}
	for _, rec := range states {
		item := map[string]interface{}{
			"peer_node_id":            rec.PeerNodeID,
			"database_name":           rec.DatabaseName,
			"last_applied_txn_id":     rec.LastAppliedTxnID,
			"last_applied_ts_wall":    rec.LastAppliedTSWall,
			"last_applied_ts_logical": rec.LastAppliedTSLogical,
			"last_sync_time":          formatTimestamp(rec.LastSyncTime),
			"sync_status":             rec.SyncStatus,
		}
		response = append(response, item)
	}

	writeJSONResponse(w, response, false, "")
}

// handleReplicationByDatabase returns replication states for all peers for a database
func (h *AdminHandlers) handleReplicationByDatabase(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, dbName string) {
	states, err := metaStore.GetAllReplicationStates()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	var response []map[string]interface{}
	for _, rec := range states {
		if rec.DatabaseName == dbName {
			item := map[string]interface{}{
				"peer_node_id":            rec.PeerNodeID,
				"database_name":           rec.DatabaseName,
				"last_applied_txn_id":     rec.LastAppliedTxnID,
				"last_applied_ts_wall":    rec.LastAppliedTSWall,
				"last_applied_ts_logical": rec.LastAppliedTSLogical,
				"last_sync_time":          formatTimestamp(rec.LastSyncTime),
				"sync_status":             rec.SyncStatus,
			}
			response = append(response, item)
		}
	}

	writeJSONResponse(w, response, false, "")
}
