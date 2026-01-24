package admin

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/maxpert/marmot/db"
)

// handleTransaction returns a specific transaction by ID
func (h *AdminHandlers) handleTransaction(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, txnID uint64) {
	rec, err := metaStore.GetTransaction(txnID)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to get transaction: %v", err))
		return
	}

	if rec == nil {
		writeErrorResponse(w, http.StatusNotFound, "transaction not found")
		return
	}

	// Convert to JSON response format
	response := map[string]interface{}{
		"txn_id":            rec.TxnID,
		"node_id":           rec.NodeID,
		"seq_num":           rec.SeqNum,
		"status":            rec.Status,
		"start_ts_wall":     rec.StartTSWall,
		"start_ts_logical":  rec.StartTSLogical,
		"commit_ts_wall":    rec.CommitTSWall,
		"commit_ts_logical": rec.CommitTSLogical,
		"created_at":        formatTimestamp(rec.CreatedAt),
		"committed_at":      formatTimestamp(rec.CommittedAt),
		"last_heartbeat":    formatTimestamp(rec.LastHeartbeat),
		"tables_involved":   rec.TablesInvolved,
		"database_name":     rec.DatabaseName,
	}

	writeJSONResponse(w, response, false, "")
}

// handlePendingTransactions returns all pending transactions
func (h *AdminHandlers) handlePendingTransactions(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	records, err := metaStore.GetPendingTransactions()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to get pending transactions: %v", err))
		return
	}

	// Convert to JSON response format
	var response []map[string]interface{}
	for _, rec := range records {
		item := map[string]interface{}{
			"txn_id":           rec.TxnID,
			"node_id":          rec.NodeID,
			"status":           rec.Status,
			"start_ts_wall":    rec.StartTSWall,
			"start_ts_logical": rec.StartTSLogical,
			"created_at":       formatTimestamp(rec.CreatedAt),
			"last_heartbeat":   formatTimestamp(rec.LastHeartbeat),
		}
		response = append(response, item)
	}

	writeJSONResponse(w, response, false, "")
}

// handleCommittedTransactions returns committed transactions with pagination (newest first)
func (h *AdminHandlers) handleCommittedTransactions(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	limit, err := parseLimit(r)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	// Parse cursor for pagination (txn_id to start before)
	var fromTxnID uint64
	if from := parseFrom(r); from != "" {
		fromTxnID, _ = strconv.ParseUint(from, 10, 64)
	}

	var response []map[string]interface{}
	var lastKey string
	hasMore := false

	// Scan transactions in descending order (newest first)
	err = metaStore.ScanTransactions(fromTxnID, true, func(rec *db.TransactionRecord) error {
		// Only include committed transactions
		if rec.Status != db.TxnStatusCommitted {
			return nil
		}

		if len(response) >= limit {
			hasMore = true
			return db.ErrStopIteration
		}

		item := map[string]interface{}{
			"txn_id":            rec.TxnID,
			"node_id":           rec.NodeID,
			"seq_num":           rec.SeqNum,
			"status":            rec.Status,
			"start_ts_wall":     rec.StartTSWall,
			"start_ts_logical":  rec.StartTSLogical,
			"commit_ts_wall":    rec.CommitTSWall,
			"commit_ts_logical": rec.CommitTSLogical,
			"created_at":        formatTimestamp(rec.CreatedAt),
			"committed_at":      formatTimestamp(rec.CommittedAt),
			"last_heartbeat":    formatTimestamp(rec.LastHeartbeat),
			"tables_involved":   rec.TablesInvolved,
			"database_name":     rec.DatabaseName,
		}
		response = append(response, item)
		lastKey = fmt.Sprintf("%d", rec.TxnID)
		return nil
	})

	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to scan transactions: %v", err))
		return
	}

	writeJSONResponse(w, response, hasMore, lastKey)
}

// handleTransactionRange returns transactions in ID range with optional status filter
func (h *AdminHandlers) handleTransactionRange(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	// For now, implement a simple range scan
	// In a full implementation, this would use PebbleDB iterator with bounds
	h.handleCommittedTransactions(w, r, metaStore)
}
