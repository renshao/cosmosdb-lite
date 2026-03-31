package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/shaoren/cosmosdb-lite/internal/store"
)

// handleGetPKRanges returns partition key ranges for a container.
// For this lightweight emulator, we always return a single range covering the full hash space.
//
// The .NET SDK fetches pkranges using a change-feed loop:
//  1. First request (no If-None-Match) → expects 200 with data and an ETag header
//  2. Second request (If-None-Match: <etag>) → expects 304 NotModified to exit the loop
//
// Without 304 support, the SDK loops forever. We handle this by returning 304
// when the client sends an If-None-Match header that matches.
//
// The SDK sometimes sends requests like /dbs/AQAAAA==/colls/AQAAAA==/pkranges where the
// collId is the database's _rid rather than a container's _rid. In that case, we return
// pkranges for all containers in the database.
func (rt *Router) handleGetPKRanges(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("dbId")
	collID := r.PathValue("collId")

	coll, err := rt.store.GetContainer(dbID, collID)
	if err != nil {
		var nf *store.ErrNotFound
		if errors.As(err, &nf) {
			// The collID may be the database's _rid — the SDK uses this pattern
			// to request pkranges at the database level.
			db, dbErr := rt.store.GetDatabase(dbID)
			if dbErr == nil && db.RID == collID {
				rt.writeDBPKRanges(w, r, dbID)
				return
			}
			writeError(w, http.StatusNotFound, "NotFound", nf.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	etag := coll.ETag
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" && ifNoneMatch == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("ETag", etag)
	writeJSON(w, http.StatusOK, buildPKRangesResponse(coll))
}

// writeDBPKRanges returns partition key ranges for all containers in a database.
// Each container contributes one pkrange entry. The top-level _rid uses the first
// container's RID so the SDK's PartitionKeyRangeCache can match it.
func (rt *Router) writeDBPKRanges(w http.ResponseWriter, r *http.Request, dbID string) {
	containers, err := rt.store.ListContainers(dbID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if len(containers) == 0 {
		writeError(w, http.StatusNotFound, "NotFound", "no containers in database")
		return
	}

	etag := containers[0].ETag
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" && ifNoneMatch == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	ranges := make([]map[string]interface{}, 0, len(containers))
	for i, coll := range containers {
		ranges = append(ranges, buildPKRangeEntry(coll, i))
	}

	resp := map[string]interface{}{
		"_rid":               containers[0].RID,
		"_count":             len(ranges),
		"PartitionKeyRanges": ranges,
	}

	w.Header().Set("ETag", etag)
	writeJSON(w, http.StatusOK, resp)
}

// buildPKRangesResponse builds a full pkranges response for a single container.
func buildPKRangesResponse(coll *store.Container) map[string]interface{} {
	return map[string]interface{}{
		"_rid":               coll.RID,
		"_count":             1,
		"PartitionKeyRanges": []map[string]interface{}{buildPKRangeEntry(coll, 0)},
	}
}

// buildPKRangeEntry builds a single partition key range entry matching the
// real CosmosDB response format, including fields the SDK validates.
func buildPKRangeEntry(coll *store.Container, index int) map[string]interface{} {
	pkRangeRID := store.GeneratePKRangeRID(coll.RID, index)
	return map[string]interface{}{
		"id":                 fmt.Sprintf("%d", index),
		"_rid":               pkRangeRID,
		"_self":              coll.Self + fmt.Sprintf("pkranges/%s/", pkRangeRID),
		"_etag":              coll.ETag,
		"minInclusive":       "",
		"maxExclusive":       "FF",
		"ridPrefix":          index,
		"_ts":                store.NowUnix(),
		"throughputFraction": 1,
		"status":             "online",
		"parents":            []string{},
		"lsn":                1,
	}
}
