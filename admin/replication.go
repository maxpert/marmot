package admin

import (
	"net/http"

	"github.com/maxpert/marmot/db"
)

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
