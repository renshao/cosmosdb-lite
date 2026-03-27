package api

import (
	"errors"
	"net/http"

	"github.com/shaoren/cosmosdb-lite/internal/store"
)

// handleGetPKRanges returns partition key ranges for a container.
// For this lightweight emulator, we always return a single range covering the full hash space.
func (rt *Router) handleGetPKRanges(w http.ResponseWriter, r *http.Request) {
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

	resp := map[string]interface{}{
		"_rid":   coll.RID,
		"_count": 1,
		"PartitionKeyRanges": []map[string]interface{}{
			{
				"id":           "0",
				"_rid":         coll.RID,
				"_self":        coll.Self + "pkranges/0/",
				"_etag":        coll.ETag,
				"minInclusive": "",
				"maxExclusive": "FF",
				"ridPrefix":    0,
				"_ts":          store.NowUnix(),
				"status":       "online",
			},
		},
	}

	writeJSON(w, http.StatusOK, resp)
}
