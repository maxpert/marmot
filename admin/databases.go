package admin

import (
	"net/http"
	"os"
	"time"
)

// handleListDatabases handles GET /admin/databases
func (h *AdminHandlers) handleListDatabases(w http.ResponseWriter, r *http.Request) {
	databases := h.dbManager.ListDatabases()

	result := make([]map[string]interface{}, 0, len(databases))
	for _, name := range databases {
		dbPath, err := h.dbManager.GetDatabasePath(name)
		if err != nil {
			continue
		}

		stat, err := os.Stat(dbPath)
		if err != nil {
			continue
		}

		mdb, err := h.dbManager.GetDatabase(name)
		if err != nil {
			continue
		}

		metaStore := mdb.GetMetaStore()
		maxTxnID, _ := metaStore.GetMaxCommittedTxnID()
		txnCount, _ := metaStore.GetCommittedTxnCount()

		result = append(result, map[string]interface{}{
			"name":          name,
			"file_size":     stat.Size(),
			"max_txn_id":    maxTxnID,
			"txn_count":     txnCount,
			"last_modified": stat.ModTime().Format(time.RFC3339),
		})
	}

	writeJSONResponse(w, result, false, "")
}
