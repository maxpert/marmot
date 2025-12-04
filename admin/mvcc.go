package admin

import (
	"net/http"
	"strings"

	"github.com/maxpert/marmot/db"
)

// handleMVCC handles MVCC-related endpoints
func (h *AdminHandlers) handleMVCC(w http.ResponseWriter, r *http.Request, database, path string) {
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	// Parse path: mvcc/{action}
	pathParts := strings.SplitN(path, "/", 2)
	if len(pathParts) == 0 {
		writeErrorResponse(w, http.StatusNotFound, "not found")
		return
	}

	action := pathParts[0]

	switch action {
	case "range":
		h.handleMVCCRange(w, r, metaStore)
	default:
		// Check if it's table/rowKey or table/rowKey/history pattern
		if strings.Contains(action, "/") {
			parts := strings.SplitN(action, "/", 3)
			if len(parts) >= 2 {
				table := parts[0]
				rowKey := parts[1]
				if len(parts) == 3 && parts[2] == "history" {
					h.handleMVCCHistory(w, r, metaStore, table, rowKey)
				} else if len(parts) == 2 {
					h.handleMVCCVersion(w, r, metaStore, table, rowKey)
				} else {
					writeErrorResponse(w, http.StatusNotFound, "not found")
				}
			} else {
				writeErrorResponse(w, http.StatusNotFound, "not found")
			}
		} else {
			writeErrorResponse(w, http.StatusNotFound, "not found")
		}
	}
}

// handleMVCCVersion returns latest MVCC version for a row
func (h *AdminHandlers) handleMVCCVersion(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, table, rowKey string) {
	rec, err := metaStore.GetLatestVersion(table, rowKey)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	if rec == nil {
		writeErrorResponse(w, http.StatusNotFound, "MVCC version not found")
		return
	}

	response := map[string]interface{}{
		"table_name":    rec.TableName,
		"row_key":       rec.RowKey,
		"ts_wall":       rec.TSWall,
		"ts_logical":    rec.TSLogical,
		"node_id":       rec.NodeID,
		"txn_id":        rec.TxnID,
		"operation":     rec.Operation,
		"data_snapshot": encodeBase64(rec.DataSnapshot),
		"created_at":    formatTimestamp(rec.CreatedAt),
	}

	writeJSONResponse(w, response, false, "")
}

// handleMVCCHistory returns MVCC version history for a row
func (h *AdminHandlers) handleMVCCHistory(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, table, rowKey string) {
	// MetaStore currently only exposes GetLatestVersion, not full history
	// Return the latest version as a single-item array for now
	rec, err := metaStore.GetLatestVersion(table, rowKey)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	var response []map[string]interface{}
	if rec != nil {
		item := map[string]interface{}{
			"table_name":    rec.TableName,
			"row_key":       rec.RowKey,
			"ts_wall":       rec.TSWall,
			"ts_logical":    rec.TSLogical,
			"node_id":       rec.NodeID,
			"txn_id":        rec.TxnID,
			"operation":     rec.Operation,
			"data_snapshot": encodeBase64(rec.DataSnapshot),
			"created_at":    formatTimestamp(rec.CreatedAt),
		}
		response = append(response, item)
	}

	writeJSONResponse(w, response, false, "")
}

// handleMVCCVersionCount returns count of MVCC versions for a row
func (h *AdminHandlers) handleMVCCVersionCount(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, table, rowKey string) {
	count, err := metaStore.GetMVCCVersionCount(table, rowKey)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]interface{}{
		"table":   table,
		"row_key": rowKey,
		"count":   count,
	}
	writeJSONResponse(w, response, false, "")
}

// handleMVCCRange returns MVCC versions in table/row range
func (h *AdminHandlers) handleMVCCRange(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	// For now, return empty response
	// Full implementation would iterate over /mvcc/{table}/ prefix
	response := []map[string]interface{}{}
	writeJSONResponse(w, response, false, "")
}
