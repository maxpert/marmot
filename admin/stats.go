package admin

import "net/http"

// handleCounters returns all persistent counters
func (h *AdminHandlers) handleCounters(w http.ResponseWriter, r *http.Request, database string) {
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	maxTxnID, err := metaStore.GetMaxCommittedTxnID()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	txnCount, err := metaStore.GetCommittedTxnCount()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]interface{}{
		"max_committed_txn_id": maxTxnID,
		"committed_txn_count":  txnCount,
	}

	writeJSONResponse(w, response, false, "")
}

// handleStats returns MetaStore statistics
func (h *AdminHandlers) handleStats(w http.ResponseWriter, r *http.Request, database string) {
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	// Get statistics from MetaStore
	maxTxnID, err := metaStore.GetMaxCommittedTxnID()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	txnCount, err := metaStore.GetCommittedTxnCount()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	maxSeqNum, err := metaStore.GetMaxSeqNum()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	pendingTxns, err := metaStore.GetPendingTransactions()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]interface{}{
		"max_committed_txn_id": maxTxnID,
		"committed_txn_count":  txnCount,
		"max_seq_num":          maxSeqNum,
		"pending_txns":         len(pendingTxns),
	}

	writeJSONResponse(w, response, false, "")
}

// handleHealth returns MetaStore health check
func (h *AdminHandlers) handleHealth(w http.ResponseWriter, r *http.Request, database string) {
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	maxTxnID, err := metaStore.GetMaxCommittedTxnID()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	pendingTxns, err := metaStore.GetPendingTransactions()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Basic health check - healthy if we can access the metadata
	response := map[string]interface{}{
		"healthy": true,
		"stats": map[string]interface{}{
			"max_committed_txn_id": maxTxnID,
			"pending_txns":         len(pendingTxns),
		},
	}

	writeJSONResponse(w, response, false, "")
}
