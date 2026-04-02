package api

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/shaoren/cosmosdb-lite/internal/store"
)

// generateUUID produces a version-4 UUID using crypto/rand.
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (rt *Router) handleCreateDocument(w http.ResponseWriter, r *http.Request) {
	dbId := r.PathValue("dbId")
	collId := r.PathValue("collId")

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequest", "failed to read body")
		return
	}
	fmt.Printf("DEBUG CreateDocument db=%s coll=%s contentType=%s isQuery=%s body=%s\n",
		dbId, collId, r.Header.Get("Content-Type"), r.Header.Get("x-ms-documentdb-isquery"), string(bodyBytes))

	// Check if this is actually a query request.
	isQuery := strings.EqualFold(r.Header.Get("x-ms-documentdb-isquery"), "true") ||
		strings.Contains(r.Header.Get("Content-Type"), "application/query+json")
	if isQuery {
		r.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))
		rt.handleQueryDocuments(w, r)
		return
	}

	var doc store.Document
	if err := json.Unmarshal(bodyBytes, &doc); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequest", "invalid JSON body")
		return
	}

	if _, ok := doc["id"]; !ok {
		doc["id"] = generateUUID()
	}

	created, err := rt.store.CreateDocument(dbId, collId, doc)
	if err != nil {
		var nf *store.ErrNotFound
		var cf *store.ErrConflict
		switch {
		case errors.As(err, &nf):
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
		case errors.As(err, &cf):
			writeError(w, http.StatusConflict, "Conflict", cf.Error())
		default:
			fmt.Printf("ERROR CreateDocument db=%s coll=%s: %v\n", dbId, collId, err)
			writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		}
		return
	}

	writeJSON(w, http.StatusCreated, created)
}

func (rt *Router) handleListDocuments(w http.ResponseWriter, r *http.Request) {
	dbId := r.PathValue("dbId")
	collId := r.PathValue("collId")

	docs, err := rt.store.ListDocuments(dbId, collId)
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if docs == nil {
		docs = []store.Document{}
	}

	collRid := fmt.Sprintf("%s.%s", dbId, collId)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"_rid":      collRid,
		"_count":    len(docs),
		"Documents": docs,
	})
}

func (rt *Router) handleGetDocument(w http.ResponseWriter, r *http.Request) {
	dbId := r.PathValue("dbId")
	collId := r.PathValue("collId")
	docId := r.PathValue("docId")

	partitionKey := parsePartitionKey(r.Header.Get("x-ms-documentdb-partitionkey"))

	doc, err := rt.store.GetDocument(dbId, collId, docId, partitionKey)
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if etag, ok := doc["_etag"].(string); ok {
		w.Header().Set("ETag", etag)
	}
	writeJSON(w, http.StatusOK, doc)
}

func (rt *Router) handleReplaceDocument(w http.ResponseWriter, r *http.Request) {
	dbId := r.PathValue("dbId")
	collId := r.PathValue("collId")
	docId := r.PathValue("docId")

	var doc store.Document
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequest", "invalid JSON body")
		return
	}

	doc["id"] = docId

	replaced, err := rt.store.ReplaceDocument(dbId, collId, docId, doc)
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, replaced)
}

// patchOperation represents a single CosmosDB patch operation.
type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

// patchRequest represents the CosmosDB patch request body.
type patchRequest struct {
	Operations []patchOperation `json:"operations"`
}

func (rt *Router) handlePatchDocument(w http.ResponseWriter, r *http.Request) {
	dbId := r.PathValue("dbId")
	collId := r.PathValue("collId")
	docId := r.PathValue("docId")

	var req patchRequest
	bodyBytes, _ := io.ReadAll(r.Body)
	fmt.Printf("DEBUG PatchDocument db=%s coll=%s doc=%s body=%s\n", dbId, collId, docId, string(bodyBytes))
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequest", "invalid JSON body")
		return
	}

	partitionKey := parsePartitionKey(r.Header.Get("x-ms-documentdb-partitionkey"))

	// Try to get the existing document; if not found, create a new one.
	doc, err := rt.store.GetDocument(dbId, collId, docId, partitionKey)
	isNew := false
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			doc = store.Document{"id": docId}
			isNew = true
		} else {
			writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}

	// Apply patch operations
	for _, op := range req.Operations {
		field := strings.TrimPrefix(op.Path, "/")
		switch strings.ToLower(op.Op) {
		case "set", "add", "replace":
			doc[field] = op.Value
		case "remove":
			delete(doc, field)
		case "incr", "increment":
			cur, _ := toFloat64(doc[field])
			delta, _ := toFloat64(op.Value)
			doc[field] = cur + delta
		default:
			writeError(w, http.StatusBadRequest, "BadRequest", fmt.Sprintf("unsupported patch op: %s", op.Op))
			return
		}
	}

	var result store.Document
	if isNew {
		result, err = rt.store.CreateDocument(dbId, collId, doc)
	} else {
		result, err = rt.store.ReplaceDocument(dbId, collId, docId, doc)
	}
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	respBytes, _ := json.Marshal(result)
	fmt.Printf("DEBUG PatchDocument response=%s\n", string(respBytes))
	writeJSON(w, http.StatusOK, result)
}

// toFloat64 converts a value to float64 for increment operations.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case nil:
		return 0, true
	default:
		return 0, false
	}
}

func (rt *Router) handleDeleteDocument(w http.ResponseWriter, r *http.Request) {
	dbId := r.PathValue("dbId")
	collId := r.PathValue("collId")
	docId := r.PathValue("docId")

	partitionKey := parsePartitionKey(r.Header.Get("x-ms-documentdb-partitionkey"))

	if err := rt.store.DeleteDocument(dbId, collId, docId, partitionKey); err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (rt *Router) handleQueryDocuments(w http.ResponseWriter, r *http.Request) {
	dbId := r.PathValue("dbId")
	collId := r.PathValue("collId")

	var body struct {
		Query      string             `json:"query"`
		Parameters []store.QueryParam `json:"parameters"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequest", "invalid JSON body")
		return
	}

	docs, err := rt.store.QueryDocuments(dbId, collId, body.Query, body.Parameters)
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if docs == nil {
		docs = []store.Document{}
	}

	collRid := fmt.Sprintf("%s.%s", dbId, collId)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"_rid":      collRid,
		"_count":    len(docs),
		"Documents": docs,
	})
}

// parsePartitionKey extracts the first element from a JSON array header
// like `["value"]` and returns it as a string.
func parsePartitionKey(header string) string {
	if header == "" {
		return ""
	}
	var arr []interface{}
	if err := json.Unmarshal([]byte(header), &arr); err != nil || len(arr) == 0 {
		return ""
	}
	switch v := arr[0].(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
