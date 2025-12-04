package admin

import (
	"net/http"
	"strings"

	"github.com/maxpert/marmot/db"
)

// handleSchema handles schema-related endpoints
func (h *AdminHandlers) handleSchema(w http.ResponseWriter, r *http.Request, database, path string) {
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	// Parse path: schema/{action}
	pathParts := strings.SplitN(path, "/", 2)
	if len(pathParts) == 0 {
		writeErrorResponse(w, http.StatusNotFound, "not found")
		return
	}

	action := pathParts[0]

	switch action {
	case "all":
		h.handleSchemaAll(w, r, metaStore)
	default:
		// Handle specific database schema
		h.handleSchemaDatabase(w, r, metaStore, database)
	}
}

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
