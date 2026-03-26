package api

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/shaoren/cosmosdb-light/internal/auth"
)

// responseRecorder captures the status code for logging.
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rr *responseRecorder) WriteHeader(code int) {
	rr.statusCode = code
	rr.ResponseWriter.WriteHeader(code)
}

// loggingMiddleware logs every request with method, path, status, and duration.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		rec := &responseRecorder{ResponseWriter: w, statusCode: 200}
		next.ServeHTTP(rec, r)

		duration := time.Since(start)
		log.Printf("%-6s %s → %d (%s)", r.Method, r.URL.Path, rec.statusCode, duration)
	})
}

// authMiddleware validates the Authorization header using the CosmosDB token scheme.
func (rt *Router) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		verb := strings.ToLower(r.Method)
		resourceType, resourceLink := parseAuthResource(r.URL.Path)

		date := r.Header.Get("x-ms-date")
		if date == "" {
			date = r.Header.Get("Date")
		}

		authHeader := r.Header.Get("Authorization")

		// Try 1: name-based resource link from the URL path
		err := auth.ValidateAuth(verb, resourceType, resourceLink, date, authHeader)

		if err != nil {
			// Try 2: RID-based — use just the target resource's _rid
			ridLink := parseRIDResourceLink(r.URL.Path)
			if ridLink != "" && ridLink != resourceLink {
				err = auth.ValidateAuth(verb, resourceType, ridLink, date, authHeader)
			}
		}

		if err != nil {
			// Try 3: resolve _rid path to name-based path and re-derive resource link
			if resolved, ok := rt.store.ResolveRIDPath(r.URL.Path); ok {
				_, nameLink := parseAuthResource("/" + resolved)
				if nameLink != resourceLink {
					err = auth.ValidateAuth(verb, resourceType, nameLink, date, authHeader)
				}
			}
		}

		if err != nil {
			writeError(w, http.StatusUnauthorized, "Unauthorized", err.Error())
			return
		}

		next.ServeHTTP(w, r)
	})
}

// parseRIDResourceLink extracts the RID-based resource link from a URL path.
// For RID-based addressing, the resource link in the auth token is just the
// _rid of the target resource (the last resource ID in the path), not the full path.
//
//	/dbs/rid1/colls/rid2/pkranges → "rid2"
//	/dbs/rid1/colls/rid2/docs/rid3 → "rid3"
//	/dbs/rid1/colls/rid2 → "rid2"
//	/dbs/rid1 → "rid1"
func parseRIDResourceLink(path string) string {
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	n := len(parts)
	if n < 2 {
		return ""
	}

	if n%2 == 1 {
		// Odd segments: last is resource type, second-to-last is the parent _rid
		// e.g., ["dbs","rid1","colls","rid2","pkranges"] → "rid2"
		return parts[n-2]
	}
	// Even segments: last is a resource id
	// e.g., ["dbs","rid1","colls","rid2"] → "rid2"
	return parts[n-1]
}

// parseAuthResource derives the CosmosDB resourceType and resourceLink from a URL path.
//
//	/dbs                              → "dbs",  ""
//	/dbs/mydb                         → "dbs",  "dbs/mydb"
//	/dbs/mydb/colls                   → "colls","dbs/mydb"
//	/dbs/mydb/colls/mycoll            → "colls","dbs/mydb/colls/mycoll"
//	/dbs/mydb/colls/mycoll/docs       → "docs", "dbs/mydb/colls/mycoll"
//	/dbs/mydb/colls/mycoll/docs/mydoc → "docs", "dbs/mydb/colls/mycoll/docs/mydoc"
func parseAuthResource(path string) (resourceType, resourceLink string) {
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	n := len(parts)

	if n == 0 || (n == 1 && parts[0] == "") {
		return "", ""
	}

	// Odd number of segments → last segment is a resource type (collection endpoint).
	// Even number of segments → last segment is a resource id (item endpoint).
	if n%2 == 1 {
		// e.g. ["dbs"] or ["dbs","mydb","colls"]
		resourceType = parts[n-1]
		if n > 1 {
			resourceLink = strings.Join(parts[:n-1], "/")
		}
	} else {
		// e.g. ["dbs","mydb"] or ["dbs","mydb","colls","mycoll"]
		resourceType = parts[n-2]
		resourceLink = strings.Join(parts, "/")
	}

	return resourceType, resourceLink
}

// corsMiddleware sets CORS headers and handles OPTIONS preflight requests.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers",
			"Content-Type, Authorization, x-ms-date, x-ms-version, "+
				"x-ms-documentdb-isquery, x-ms-documentdb-partitionkey, "+
				"x-ms-documentdb-query-enablecrosspartition, x-ms-max-item-count, "+
				"If-Match, If-None-Match")
		w.Header().Set("Access-Control-Expose-Headers",
			"x-ms-request-charge, x-ms-activity-id, etag, x-ms-session-token, x-ms-item-count")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// commonHeaders injects standard CosmosDB response headers.
func (rt *Router) commonHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ms-request-charge", "1")
		w.Header().Set("x-ms-activity-id", generateActivityID())
		w.Header().Set("x-ms-session-token", "0:0")
		next.ServeHTTP(w, r)
	})
}

// generateActivityID produces a version-4 UUID for the x-ms-activity-id header.
func generateActivityID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ridResolver resolves _rid-based path values (dbId, collId) to human-readable names.
// The SDK sometimes uses _rid-based URLs for internal requests (e.g., pkranges).
func (rt *Router) ridResolver(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dbID := r.PathValue("dbId")
		if dbID != "" {
			if resolved, ok := rt.store.ResolveDBID(dbID); ok && resolved != dbID {
				r.SetPathValue("dbId", resolved)
			}
		}

		collID := r.PathValue("collId")
		if collID != "" {
			dbName := r.PathValue("dbId") // use potentially resolved db name
			if resolved, ok := rt.store.ResolveContainerID(dbName, collID); ok && resolved != collID {
				r.SetPathValue("collId", resolved)
			}
		}

		next.ServeHTTP(w, r)
	})
}
