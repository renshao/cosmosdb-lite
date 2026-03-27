package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/shaoren/cosmosdb-lite/internal/store"
)

// Router holds the HTTP multiplexer and backing store for the CosmosDB emulator API.
type Router struct {
	store       store.Store
	authEnabled bool
	mux         *http.ServeMux
}

// NewRouter creates a Router with all CosmosDB REST API routes registered.
func NewRouter(s store.Store, authEnabled bool) *Router {
	rt := &Router{
		store:       s,
		authEnabled: authEnabled,
		mux:         http.NewServeMux(),
	}

	// Wrap an API handler with auth (when enabled) and common-headers middleware.
	wrap := func(h http.HandlerFunc) http.Handler {
		var handler http.Handler = h
		handler = rt.ridResolver(handler)
		handler = rt.commonHeaders(handler)
		if rt.authEnabled {
			handler = rt.authMiddleware(handler)
		}
		return handler
	}

	// Account metadata (SDK initialization)
	rt.mux.Handle("GET /{$}", wrap(rt.handleGetAccount))

	// Database routes
	rt.mux.Handle("POST /dbs", wrap(rt.handleCreateDatabase))
	rt.mux.Handle("GET /dbs", wrap(rt.handleListDatabases))
	rt.mux.Handle("GET /dbs/{dbId}", wrap(rt.handleGetDatabase))
	rt.mux.Handle("DELETE /dbs/{dbId}", wrap(rt.handleDeleteDatabase))

	// Container routes
	rt.mux.Handle("POST /dbs/{dbId}/colls", wrap(rt.handleCreateContainer))
	rt.mux.Handle("GET /dbs/{dbId}/colls", wrap(rt.handleListContainers))
	rt.mux.Handle("GET /dbs/{dbId}/colls/{collId}", wrap(rt.handleGetContainer))
	rt.mux.Handle("DELETE /dbs/{dbId}/colls/{collId}", wrap(rt.handleDeleteContainer))

	// Document routes
	rt.mux.Handle("POST /dbs/{dbId}/colls/{collId}/docs", wrap(rt.handleCreateDocument))
	rt.mux.Handle("GET /dbs/{dbId}/colls/{collId}/docs", wrap(rt.handleListDocuments))
	rt.mux.Handle("GET /dbs/{dbId}/colls/{collId}/docs/{docId}", wrap(rt.handleGetDocument))
	rt.mux.Handle("PUT /dbs/{dbId}/colls/{collId}/docs/{docId}", wrap(rt.handleReplaceDocument))
	rt.mux.Handle("DELETE /dbs/{dbId}/colls/{collId}/docs/{docId}", wrap(rt.handleDeleteDocument))

	// Partition key ranges (no auth — static partition metadata)
	rt.mux.Handle("GET /dbs/{dbId}/colls/{collId}/pkranges", rt.commonHeaders(http.HandlerFunc(rt.handleGetPKRanges)))

	// Web UI (no auth)
	rt.mux.Handle("GET /_explorer/", http.StripPrefix("/_explorer/", http.FileServer(http.Dir("internal/webui/static"))))

	return rt
}

// ServeHTTP implements http.Handler, delegating to the internal mux.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rt.mux.ServeHTTP(w, r)
}

// ListenAndServeTLS starts the HTTPS server.
func (rt *Router) ListenAndServeTLS(addr, certFile, keyFile string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           loggingMiddleware(corsMiddleware(rt.mux)),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return srv.ListenAndServeTLS(certFile, keyFile)
}

// parseResourcePath extracts database, container, and document IDs from the request path.
func parseResourcePath(r *http.Request) (dbID, collID, docID string) {
	dbID = r.PathValue("dbId")
	collID = r.PathValue("collId")
	docID = r.PathValue("docId")
	return
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes a CosmosDB-style error response.
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"code":    code,
		"message": message,
	})
}
