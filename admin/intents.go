package admin

import (
	"fmt"
	"net/http"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/protocol/filter"
)

// handleIntent returns a specific write intent
func (h *AdminHandlers) handleIntent(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, table, intentKey string) {
	rec, err := metaStore.GetIntent(table, intentKey)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to get intent: %v", err))
		return
	}

	if rec == nil {
		writeErrorResponse(w, http.StatusNotFound, "intent not found")
		return
	}

	response := map[string]interface{}{
		"table_name":    rec.TableName,
		"intent_key":    filter.IntentKeyToBase64(rec.IntentKey),
		"txn_id":        rec.TxnID,
		"ts_wall":       rec.TSWall,
		"ts_logical":    rec.TSLogical,
		"node_id":       rec.NodeID,
		"operation":     rec.Operation,
		"sql_statement": rec.SQLStatement,
		"data_snapshot": encodeBase64(rec.DataSnapshot),
		"created_at":    formatTimestamp(rec.CreatedAt),
	}

	writeJSONResponse(w, response, false, "")
}

// handleIntentsByTable returns all intents for a table with pagination
func (h *AdminHandlers) handleIntentsByTable(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, table string) {
	// For now, return empty response
	// Full implementation would iterate over /intent/{table}/ prefix
	response := []map[string]interface{}{}
	writeJSONResponse(w, response, false, "")
}

// handleIntentsByTxn returns all intents for a transaction
func (h *AdminHandlers) handleIntentsByTxn(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, txnID uint64) {
	records, err := metaStore.GetIntentsByTxn(txnID)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to get intents: %v", err))
		return
	}

	var response []map[string]interface{}
	for _, rec := range records {
		item := map[string]interface{}{
			"table_name":    rec.TableName,
			"intent_key":    filter.IntentKeyToBase64(rec.IntentKey),
			"txn_id":        rec.TxnID,
			"ts_wall":       rec.TSWall,
			"ts_logical":    rec.TSLogical,
			"node_id":       rec.NodeID,
			"operation":     rec.Operation,
			"sql_statement": rec.SQLStatement,
			"data_snapshot": encodeBase64(rec.DataSnapshot),
			"created_at":    formatTimestamp(rec.CreatedAt),
		}
		response = append(response, item)
	}

	writeJSONResponse(w, response, false, "")
}

// handleIntentRange returns intents in table/key range
func (h *AdminHandlers) handleIntentRange(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	// For now, return empty response
	// Full implementation would iterate over /intent/ prefix with bounds
	response := []map[string]interface{}{}
	writeJSONResponse(w, response, false, "")
}
