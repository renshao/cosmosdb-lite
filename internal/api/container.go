package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/shaoren/cosmosdb-lite/internal/store"
)

type createContainerRequest struct {
	ID           string            `json:"id"`
	PartitionKey *store.PartitionKey `json:"partitionKey,omitempty"`
}

func (rt *Router) handleCreateContainer(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("dbId")

	var req createContainerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequest", "invalid JSON body")
		return
	}

	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "BadRequest", "missing container id")
		return
	}

	pk := store.PartitionKey{Paths: []string{"/id"}, Kind: "Hash"}
	if req.PartitionKey != nil {
		pk = *req.PartitionKey
	}

	coll, err := rt.store.CreateContainer(dbID, req.ID, pk)
	if err != nil {
		var nf *store.ErrNotFound
		var cf *store.ErrConflict
		switch {
		case errors.As(err, &nf):
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
		case errors.As(err, &cf):
			writeError(w, http.StatusConflict, "Conflict", cf.Error())
		default:
			writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		}
		return
	}

	w.Header().Set("Location", "dbs/"+dbID+"/colls/"+req.ID)
	if db, dbErr := rt.store.GetDatabase(dbID); dbErr == nil {
		w.Header().Set("x-ms-alt-content-path", "dbs/"+db.ID)
	}
	w.Header().Set("x-ms-quorum-acked-lsn", "1")
	w.Header().Set("x-ms-current-write-quorum", "3")
	w.Header().Set("x-ms-current-replica-set-size", "4")
	w.Header().Set("x-ms-documentdb-partitionkeyrangeid", "0")
	w.Header().Set("x-ms-cosmos-llsn", "1")
	w.Header().Set("x-ms-cosmos-quorum-acked-llsn", "1")
	w.Header().Set("x-ms-cosmos-item-llsn", "1")
	w.Header().Set("x-ms-cosmos-physical-partition-id", "0")
	w.Header().Set("x-ms-cosmos-max-content-length", "2097152")
	w.Header().Set("x-ms-cosmos-internal-partition-id", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")
	writeJSON(w, http.StatusCreated, coll)
}

func (rt *Router) handleListContainers(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("dbId")

	db, err := rt.store.GetDatabase(dbID)
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	containers, err := rt.store.ListContainers(dbID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"_rid":                db.RID,
		"_count":              len(containers),
		"DocumentCollections": containers,
	})
}

func (rt *Router) handleGetContainer(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("dbId")
	collID := r.PathValue("collId")

	coll, err := rt.store.GetContainer(dbID, collID)
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// The SDK uses these headers to construct RID-based paths for subsequent
	// requests (e.g., pkranges). x-ms-content-path is the database RID,
	// x-ms-alt-content-path is the name-based database path.
	if db, dbErr := rt.store.GetDatabase(dbID); dbErr == nil {
		w.Header().Set("x-ms-content-path", db.RID)
		w.Header().Set("x-ms-alt-content-path", "dbs/"+db.ID)
	}

	writeJSON(w, http.StatusOK, coll)
}

func (rt *Router) handleDeleteContainer(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("dbId")
	collID := r.PathValue("collId")

	if err := rt.store.DeleteContainer(dbID, collID); err != nil {
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
