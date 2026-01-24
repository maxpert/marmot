package admin

import (
	"net/http"

	"github.com/maxpert/marmot/db"
)

// handleSchemaDatabase returns schema version for current database
func (h *AdminHandlers) handleSchemaDatabase(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore, database string) {
	version, err := metaStore.GetSchemaVersion(database)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]interface{}{
		"version": version,
		// Note: Full schema record would require additional methods
	}

	writeJSONResponse(w, response, false, "")
}

// handleSchemaAll returns schema versions for all databases
func (h *AdminHandlers) handleSchemaAll(w http.ResponseWriter, r *http.Request, metaStore db.MetaStore) {
	versions, err := metaStore.GetAllSchemaVersions()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSONResponse(w, versions, false, "")
}
