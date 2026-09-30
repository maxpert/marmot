package admin

import (
	"net/http"

	"github.com/maxpert/marmot/db"
)

// handleReplicationByPeer reports database's log-pull replication state as seen
// from peerNodeID's side: peerNodeID's
// last-consumed position in this node's log (MetaStore.ConsumedPositions),
// and this node's own pull cursor into peerNodeID's log
// (MetaStore.GetPullCursor). It replaces the removed
// MetaStore.ReplicationState table.
func (h *AdminHandlers) handleReplicationByPeer(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, peerNodeID uint64, database string) {
	consumed, err := metaStore.ConsumedPositions()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	cursor, err := metaStore.GetPullCursor(peerNodeID)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	consumedPos, hasConsumed := consumed[peerNodeID]

	response := map[string]interface{}{
		"peer_node_id":             peerNodeID,
		"database_name":            database,
		"peer_consumed_seq":        consumedPos.Seq,
		"peer_consumed_txn_id":     consumedPos.TxnID,
		"peer_has_consumed_at_all": hasConsumed,
		"local_pull_cursor_seq":    cursor.Seq,
		"local_pull_cursor_txn_id": cursor.TxnID,
	}
	writeJSONResponse(w, response, false, "")
}

// handleReplicationAll reports database's log-pull replication state for every
// peer that has ever pulled from this node (MetaStore.ConsumedPositions),
// plus this node's own GC truncation point (MetaStore.TruncatedThrough): the
// highest log position this node's GC has deleted through, which bounds how
// far behind a peer can safely fall before it needs a snapshot restore
// instead of a log pull.
func (h *AdminHandlers) handleReplicationAll(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, database string) {
	consumed, err := metaStore.ConsumedPositions()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	truncated, err := metaStore.TruncatedThrough()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	peers := make([]map[string]interface{}, 0, len(consumed))
	for peerNodeID, pos := range consumed {
		peers = append(peers, map[string]interface{}{
			"peer_node_id":         peerNodeID,
			"peer_consumed_seq":    pos.Seq,
			"peer_consumed_txn_id": pos.TxnID,
		})
	}

	response := map[string]interface{}{
		"database_name":            database,
		"truncated_through_seq":    truncated.Seq,
		"truncated_through_txn_id": truncated.TxnID,
		"peers":                    peers,
	}
	writeJSONResponse(w, response, false, "")
}
