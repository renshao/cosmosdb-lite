package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/shaoren/cosmosdb-lite/internal/store"
)

func (rt *Router) handleCreateDatabase(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequest", "invalid JSON")
		return
	}

	db, err := rt.store.CreateDatabase(body.ID)
	if err != nil {
		var conflict *store.ErrConflict
		if errors.As(err, &conflict) {
			writeError(w, http.StatusConflict, "Conflict", conflict.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("Location", db.Self)
	writeJSON(w, http.StatusCreated, db)
}

func (rt *Router) handleListDatabases(w http.ResponseWriter, r *http.Request) {
	dbs, err := rt.store.ListDatabases()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"_rid":      "",
		"_count":    len(dbs),
		"Databases": dbs,
	})
}

func (rt *Router) handleGetDatabase(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("dbId")

	db, err := rt.store.GetDatabase(dbID)
	if err != nil {
		var notFound *store.ErrNotFound
		if errors.As(err, &notFound) {
			writeError(w, http.StatusNotFound, "NotFound", notFound.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, db)
}

func (rt *Router) handleDeleteDatabase(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("dbId")

	if err := rt.store.DeleteDatabase(dbID); err != nil {
		var notFound *store.ErrNotFound
		if errors.As(err, &notFound) {
			writeError(w, http.StatusNotFound, "NotFound", notFound.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
