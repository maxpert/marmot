package admin

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
)

// RegisterRoutes registers all admin API routes using chi router
func RegisterRoutes(mux *http.ServeMux, handlers *AdminHandlers) {
	r := chi.NewRouter()

	// Admin UI
	r.Get("/", handlers.ServeUI)

	// List all databases (auth required)
	r.With(chiAuthMiddleware).Get("/databases", handlers.handleListDatabases)

	// Cluster management endpoints
	r.Route("/cluster", func(r chi.Router) {
		r.Use(chiAuthMiddleware)
		r.Get("/members", handlers.handleClusterMembers)
		r.Get("/health", handlers.handleClusterHealth)
		r.Get("/replication", handlers.handleClusterReplication)
		r.Post("/remove/{nodeID}", handlers.handleClusterRemove)
		r.Post("/allow/{nodeID}", handlers.handleClusterAllow)
	})

	// Per-database transaction operations (outside metadata path)
	r.Route("/{database}/transactions", func(r chi.Router) {
		r.Use(chiAuthMiddleware)
		r.Post("/{txnID}/abort", handlers.handleAbortTransaction)
	})

	// Database-specific metadata endpoints
	r.Route("/{database}/metadata", func(r chi.Router) {
		r.Use(chiAuthMiddleware)

		// Stats & health
		r.Get("/stats", handlers.wrapWithDB(handlers.handleStats))
		r.Get("/health", handlers.wrapWithDB(handlers.handleHealth))
		r.Get("/counters", handlers.wrapWithDB(handlers.handleCounters))

		// Transactions
		r.Get("/transactions/pending", handlers.wrapWithMeta(handlers.handlePendingTransactions))
		r.Get("/transactions/committed", handlers.wrapWithMeta(handlers.handleCommittedTransactions))
		r.Get("/transactions/range", handlers.wrapWithMeta(handlers.handleTransactionRange))
		r.Get("/transactions/status/{status}", handlers.wrapWithMeta(handlers.handleTransactionsByStatus))
		r.Get("/transactions/{txnID}", handlers.txnByID)

		// Intents
		r.Get("/intents/table/{table}", handlers.intentsByTable)
		r.Get("/intents/txn/{txnID}", handlers.intentsByTxn)
		r.Get("/intents/range", handlers.wrapWithMeta(handlers.handleIntentRange))
		r.Get("/intents/{table}/{intentKey}", handlers.intentByKey)

		// CDC
		r.Get("/cdc/entries/{txnID}", handlers.cdcEntries)
		r.Get("/cdc/range", handlers.wrapWithMeta(handlers.handleCDCRange))

		// Replication
		r.Get("/replication/states", handlers.replicationStates)
		r.Get("/replication/state/{peerID}", handlers.replicationStatePeer)

		// Schema
		r.Get("/schema/version", handlers.schemaVersion)
		r.Get("/schema/versions", handlers.schemaVersions)
	})

	// Mount chi router under /admin
	mux.Handle("/admin", http.RedirectHandler("/admin/", http.StatusMovedPermanently))
	mux.Handle("/admin/", http.StripPrefix("/admin", r))

	log.Info().Msg("Admin endpoints enabled at /admin/{database}/metadata/*")
}

// chiAuthMiddleware adapts AuthMiddleware for chi
func chiAuthMiddleware(next http.Handler) http.Handler {
	return AuthMiddleware(next)
}

// Wrapper helpers that extract URL params and call existing handlers

func (h *AdminHandlers) wrapWithDB(fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		database := chi.URLParam(r, "database")
		if database == "" {
			writeErrorResponse(w, http.StatusBadRequest, "database name is required")
			return
		}
		fn(w, r, database)
	}
}

func (h *AdminHandlers) wrapWithMeta(fn func(http.ResponseWriter, *http.Request, db.MetaStore)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		database := chi.URLParam(r, "database")
		if database == "" {
			writeErrorResponse(w, http.StatusBadRequest, "database name is required")
			return
		}
		metaStore, err := h.getMetaStore(database)
		if err != nil {
			writeErrorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		fn(w, r, metaStore)
	}
}

// Transaction handlers
func (h *AdminHandlers) txnByID(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	txnID, err := strconv.ParseUint(chi.URLParam(r, "txnID"), 10, 64)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "invalid transaction ID")
		return
	}
	h.handleTransaction(w, r, metaStore, txnID)
}

// Intent handlers
func (h *AdminHandlers) intentsByTable(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	table := chi.URLParam(r, "table")
	h.handleIntentsByTable(w, r, metaStore, table)
}

func (h *AdminHandlers) intentsByTxn(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	txnID, err := strconv.ParseUint(chi.URLParam(r, "txnID"), 10, 64)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "invalid transaction ID")
		return
	}
	h.handleIntentsByTxn(w, r, metaStore, txnID)
}

func (h *AdminHandlers) intentByKey(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	table := chi.URLParam(r, "table")
	intentKey := chi.URLParam(r, "intentKey")
	h.handleIntent(w, r, metaStore, table, intentKey)
}

// CDC handlers
func (h *AdminHandlers) cdcEntries(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	txnID, err := strconv.ParseUint(chi.URLParam(r, "txnID"), 10, 64)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "invalid transaction ID")
		return
	}
	h.handleCDCByTxn(w, r, metaStore, txnID)
}

// Replication handlers
func (h *AdminHandlers) replicationStates(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	h.handleReplicationAll(w, r, metaStore)
}

func (h *AdminHandlers) replicationStatePeer(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	peerID, err := strconv.ParseUint(chi.URLParam(r, "peerID"), 10, 64)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "invalid peer ID")
		return
	}
	h.handleReplicationByPeer(w, r, metaStore, peerID, database)
}

// Schema handlers
func (h *AdminHandlers) schemaVersion(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	h.handleSchemaDatabase(w, r, metaStore, database)
}

func (h *AdminHandlers) schemaVersions(w http.ResponseWriter, r *http.Request) {
	database := chi.URLParam(r, "database")
	metaStore, err := h.getMetaStore(database)
	if err != nil {
		writeErrorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	h.handleSchemaAll(w, r, metaStore)
}
