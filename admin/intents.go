package admin

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/protocol/filter"
)

// handleIntents handles intent-related endpoints
func (h *AdminHandlers) handleIntents(w http.ResponseWriter, r *http.Request, database, path string) {
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	// Parse path: intents/{action} - path comes in as "intents/table/users" etc.
	restPath := strings.TrimPrefix(path, "intents/")
	pathParts := strings.SplitN(restPath, "/", 2)
	if len(pathParts) == 0 {
		writeErrorResponse(w, http.StatusNotFound, "not found")
		return
	}

	action := pathParts[0]
	remainder := ""
	if len(pathParts) > 1 {
		remainder = pathParts[1]
	}

	switch action {
	case "filter":
		if remainder == "stats" {
			h.handleIntentFilterStats(w, r, metaStore)
		} else {
			writeErrorResponse(w, http.StatusNotFound, "not found")
		}
	case "table":
		if remainder != "" {
			h.handleIntentsByTable(w, r, metaStore, remainder)
		} else {
			writeErrorResponse(w, http.StatusNotFound, "not found")
		}
	case "txn":
		if remainder != "" {
			if txnID, err := parseTxnID(remainder); err == nil {
				h.handleIntentsByTxn(w, r, metaStore, txnID)
			} else {
				writeErrorResponse(w, http.StatusBadRequest, err.Error())
			}
		} else {
			writeErrorResponse(w, http.StatusNotFound, "not found")
		}
	case "range":
		h.handleIntentRange(w, r, metaStore)
	default:
		// Check if it's table/intentKey pattern
		if remainder != "" {
			h.handleIntent(w, r, metaStore, action, remainder)
		} else {
			writeErrorResponse(w, http.StatusNotFound, "not found")
		}
	}
}

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
		"table_name":         rec.TableName,
		"intent_key":         filter.IntentKeyToBase64(rec.IntentKey),
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

// handleIntentFilterStats returns IntentFilter statistics
func (h *AdminHandlers) handleIntentFilterStats(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	intentFilter := metaStore.GetIntentFilter()
	if intentFilter == nil {
		writeErrorResponse(w, http.StatusInternalServerError, "intent filter not available")
		return
	}

	// Get statistics from intent filter
	// This would require adding methods to IntentFilter to get stats
	response := map[string]interface{}{
		"size":            0, // placeholder
		"false_positives": 0, // placeholder
		"checks":          0, // placeholder
	}

	writeJSONResponse(w, response, false, "")
}
