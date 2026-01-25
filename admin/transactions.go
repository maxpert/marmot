package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
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

	response := formatTransactionRecord(rec)
	writeJSONResponse(w, response, false, "")
}

// handlePendingTransactions returns all pending transactions
func (h *AdminHandlers) handlePendingTransactions(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	records, err := metaStore.GetPendingTransactions()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to get pending transactions: %v", err))
		return
	}

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

	var fromTxnID uint64
	if from := parseFrom(r); from != "" {
		fromTxnID, _ = strconv.ParseUint(from, 10, 64)
	}

	var response []map[string]interface{}
	var lastKey string
	hasMore := false

	err = metaStore.ScanTransactions(fromTxnID, true, func(rec *db.TransactionRecord) error {
		if rec.Status != db.TxnStatusCommitted {
			return nil
		}

		if len(response) >= limit {
			hasMore = true
			return db.ErrStopIteration
		}

		response = append(response, formatTransactionRecord(rec))
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
	h.handleCommittedTransactions(w, r, metaStore)
}

// handleTransactionsByStatus returns transactions filtered by status
func (h *AdminHandlers) handleTransactionsByStatus(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	status := chi.URLParam(r, "status")

	limit, err := parseLimit(r)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	var fromTxnID uint64
	if from := parseFrom(r); from != "" {
		fromTxnID, _ = strconv.ParseUint(from, 10, 64)
	}

	var statusFilter db.TxnStatus
	filterAll := false

	switch status {
	case "pending":
		statusFilter = db.TxnStatusPending
	case "committed":
		statusFilter = db.TxnStatusCommitted
	case "aborted":
		statusFilter = db.TxnStatusAborted
	case "all":
		filterAll = true
	default:
		writeErrorResponse(w, http.StatusBadRequest, "invalid status: use pending, committed, aborted, or all")
		return
	}

	var response []map[string]interface{}
	var lastKey string
	hasMore := false

	err = metaStore.ScanTransactions(fromTxnID, true, func(rec *db.TransactionRecord) error {
		if !filterAll && rec.Status != statusFilter {
			return nil
		}

		if len(response) >= limit {
			hasMore = true
			return db.ErrStopIteration
		}

		response = append(response, formatTransactionRecord(rec))
		lastKey = fmt.Sprintf("%d", rec.TxnID)
		return nil
	})

	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to scan transactions: %v", err))
		return
	}

	writeJSONResponse(w, response, hasMore, lastKey)
}

// handleAbortTransaction handles POST /admin/{database}/transactions/{txnID}/abort
func (h *AdminHandlers) handleAbortTransaction(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	txnIDStr := chi.URLParam(r, "txnID")

	txnID, err := strconv.ParseUint(txnIDStr, 10, 64)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "invalid transaction ID")
		return
	}

	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	txn, err := metaStore.GetTransaction(txnID)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to get transaction: %v", err))
		return
	}

	if txn == nil {
		writeErrorResponse(w, http.StatusNotFound, "transaction not found")
		return
	}

	if txn.Status != db.TxnStatusPending {
		writeErrorResponse(w, http.StatusConflict,
			fmt.Sprintf("cannot abort transaction in status: %s", txn.Status))
		return
	}

	var body struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	log.Warn().
		Uint64("txn_id", txnID).
		Str("database", database).
		Str("reason", body.Reason).
		Msg("Force aborting transaction via admin API")

	if err := metaStore.AbortTransaction(txnID); err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to abort transaction: %v", err))
		return
	}

	writeJSONResponse(w, map[string]interface{}{
		"txn_id":  txnID,
		"status":  "aborted",
		"message": "transaction aborted successfully",
	}, false, "")
}

// formatTransactionRecord converts a TransactionRecord to a JSON-friendly map
func formatTransactionRecord(rec *db.TransactionRecord) map[string]interface{} {
	return map[string]interface{}{
		"txn_id":            rec.TxnID,
		"node_id":           rec.NodeID,
		"seq_num":           rec.SeqNum,
		"status":            rec.Status.String(),
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
}
