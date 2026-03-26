package api

import (
	"fmt"
	"net/http"

	"github.com/shaoren/cosmosdb-light/internal/store"
)

// handleGetAccount returns the database account metadata.
// The .NET SDK (and others) call GET / on startup to discover account properties.
func (rt *Router) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if host == "" {
		host = "localhost:8081"
	}
	endpoint := fmt.Sprintf("https://%s/", host)

	resp := map[string]interface{}{
		"id":                "localhost",
		"_rid":              "",
		"media":             "//",
		"addresses":         "//",
		"_self":             "",
		"_dbs":              "//dbs/",
		"systemReplicatorStatus": "N/A",
		"queryEngineConfiguration": `{"maxSqlQueryInputLength":262144,"maxJoinsPerSqlQuery":5,"maxLogicalAndPerSqlQuery":500,"maxLogicalOrPerSqlQuery":500,"maxUdfRefPerSqlQuery":10,"maxInExpressionItemsCount":16000,"queryMaxInMemorySortDocumentCount":500,"maxQueryRequestTimeoutFraction":0.9,"sqlAllowNonFiniteNumbers":false,"sqlAllowAggregateFunctions":true,"sqlAllowSubQuery":true,"sqlAllowScalarSubQuery":true,"allowNewKeywords":true,"sqlAllowLike":true,"sqlAllowGroupByClause":true,"maxSpatialQueryCells":12,"spatialMaxGeometryPointCount":256,"sqlDisableOptimizationFlags":0,"sqlAllowTop":true,"enableSpatialIndexing":true}`,
		"writableLocations": []map[string]interface{}{
			{
				"name":                    "South Central US",
				"databaseAccountEndpoint": endpoint,
			},
		},
		"readableLocations": []map[string]interface{}{
			{
				"name":                    "South Central US",
				"databaseAccountEndpoint": endpoint,
			},
		},
		"enableMultipleWriteLocations": false,
		"userConsistencyPolicy": map[string]interface{}{
			"defaultConsistencyLevel": "Session",
			"maxStalenessPrefix":      100,
			"maxIntervalInSeconds":    5,
		},
		"userReplicationPolicy": map[string]interface{}{
			"asyncReplication":  false,
			"minReplicaSetSize": 1,
			"maxReplicaSetSize": 4,
		},
		"_ts": store.NowUnix(),
	}

	writeJSON(w, http.StatusOK, resp)
}
