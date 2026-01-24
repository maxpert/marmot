package admin

import (
	"net/http"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/protocol/filter"
)

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
			"intent_key": filter.IntentKeyToBase64(entry.IntentKey),
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
