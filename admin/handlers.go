package admin

import (
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/grpc"
	"github.com/rs/zerolog/log"
)

//go:embed ui.html
var uiHTML embed.FS

// AdminHandlers handles admin API endpoints for MetaStore operations
type AdminHandlers struct {
	server    *grpc.Server
	dbManager *db.DatabaseManager
}

// NewAdminHandlers creates a new AdminHandlers instance
func NewAdminHandlers(server *grpc.Server, dbManager *db.DatabaseManager) *AdminHandlers {
	return &AdminHandlers{
		server:    server,
		dbManager: dbManager,
	}
}

// ServeUI serves the admin UI HTML file
func (h *AdminHandlers) ServeUI(w http.ResponseWriter, r *http.Request) {
	// Serve the embedded HTML file
	data, err := uiHTML.ReadFile("ui.html")
	if err != nil {
		http.Error(w, "Failed to load admin UI", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}

// getMetaStore resolves MetaStore for a database
func (h *AdminHandlers) getMetaStore(database string) (db.MetaStore, error) {
	if database == "" {
		return nil, fmt.Errorf("database name is required")
	}

	mdb, err := h.dbManager.GetDatabase(database)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found: %w", database, err)
	}

	return mdb.GetMetaStore(), nil
}

// writeJSONResponse writes a successful JSON response
func writeJSONResponse(w http.ResponseWriter, data interface{}, hasMore bool, lastKey string) {
	response := map[string]interface{}{
		"data": data,
	}

	if hasMore || lastKey != "" {
		response["has_more"] = hasMore
		if lastKey != "" {
			response["last_key"] = lastKey
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Failed to encode JSON response")
	}
}

// writeErrorResponse writes an error JSON response
func writeErrorResponse(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	response := map[string]interface{}{
		"error": message,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Failed to encode error response")
	}
}

// parseLimit parses limit parameter with defaults
func parseLimit(r *http.Request) (int, error) {
	limitStr := r.URL.Query().Get("limit")
	if limitStr == "" {
		return 256, nil // default
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		return 0, fmt.Errorf("invalid limit parameter: %w", err)
	}

	if limit < 1 {
		return 0, fmt.Errorf("limit must be positive")
	}

	if limit > 1024 {
		return 0, fmt.Errorf("limit cannot exceed 1024")
	}

	return limit, nil
}

// parseFrom parses from parameter for pagination
func parseFrom(r *http.Request) string {
	return r.URL.Query().Get("from")
}

// formatTimestamp converts nanoseconds to ISO 8601 string
func formatTimestamp(nanos int64) string {
	if nanos == 0 {
		return ""
	}
	return time.Unix(0, nanos).UTC().Format(time.RFC3339Nano)
}

// encodeBase64 encodes byte slices as base64 strings
func encodeBase64(data []byte) string {
	if data == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

// encodeBase64Map encodes map[string][]byte as map[string]string
func encodeBase64Map(data map[string][]byte) map[string]string {
	if data == nil {
		return nil
	}

	result := make(map[string]string)
	for k, v := range data {
		result[k] = encodeBase64(v)
	}
	return result
}

// parsePeerNodeID parses peer node ID
func parsePeerNodeID(peerIDStr string) (uint64, error) {
	if peerIDStr == "" {
		return 0, fmt.Errorf("peer node ID is required")
	}

	peerID, err := strconv.ParseUint(peerIDStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid peer node ID: %w", err)
	}

	return peerID, nil
}
