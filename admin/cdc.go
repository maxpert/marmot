package admin

import (
	"net/http"
	"strings"

	"github.com/maxpert/marmot/db"
)

// handleCDC handles CDC-related endpoints
func (h *AdminHandlers) handleCDC(w http.ResponseWriter, r *http.Request, database, path string) {
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	// Parse path: cdc/{action}
	pathParts := strings.SplitN(path, "/", 2)
	if len(pathParts) == 0 {
		writeErrorResponse(w, http.StatusNotFound, "not found")
		return
	}

	action := pathParts[0]

	switch action {
	case "range":
		h.handleCDCRange(w, r, metaStore)
	default:
		// Check if it's a specific transaction ID
		if txnID, err := parseTxnID(action); err == nil {
			h.handleCDCByTxn(w, r, metaStore, txnID)
		} else {
			writeErrorResponse(w, http.StatusNotFound, "not found")
		}
	}
}

// handleCDCByTxn returns all CDC entries for a transaction
func (h *AdminHandlers) handleCDCByTxn(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, txnID uint64) {
	entries, err := metaStore.GetIntentEntries(txnID)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	var response []map[string]interface{}
	for _, entry := range entries {
		item := map[string]interface{}{
			"txn_id":     entry.TxnID,
			"seq":        entry.Seq,
			"operation":  entry.Operation,
			"table":      entry.Table,
			"row_key":    entry.RowKey,
			"old_values": encodeBase64Map(entry.OldValues),
			"new_values": encodeBase64Map(entry.NewValues),
			"created_at": formatTimestamp(entry.CreatedAt),
		}
		response = append(response, item)
	}

	writeJSONResponse(w, response, false, "")
}

// handleCDCRange returns CDC entries in transaction range
func (h *AdminHandlers) handleCDCRange(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	// For now, return empty response
	// Full implementation would iterate over /cdc/ prefix with bounds
	response := []map[string]interface{}{}
	writeJSONResponse(w, response, false, "")
}
