package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shaoren/cosmosdb-lite/internal/auth"
	"github.com/shaoren/cosmosdb-lite/internal/store"
)

// authRequest creates an authenticated HTTP request.
// resourceType and resourceLink must match what parseAuthResource derives from the path.
func authRequest(t *testing.T, method, url, resourceType, resourceLink string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	date := time.Now().UTC().Format(http.TimeFormat)
	token := auth.GenerateAuth(method, resourceType, resourceLink, date)
	req.Header.Set("Authorization", token)
	req.Header.Set("x-ms-date", date)
	req.Header.Set("x-ms-version", "2018-12-31")
	req.Header.Set("Content-Type", "application/json")
	return req
}

func setupServer(t *testing.T) *httptest.Server {
	t.Helper()
	ms, err := store.NewMemoryStore("")
	if err != nil {
		t.Fatalf("creating memory store: %v", err)
	}
	router := NewRouter(ms, true)
	return httptest.NewServer(router)
}

func readBody(t *testing.T, resp *http.Response) map[string]interface{} {
	t.Helper()
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decoding response body: %v", err)
	}
	return result
}

func TestIntegration(t *testing.T) {
	srv := setupServer(t)
	defer srv.Close()
	client := srv.Client()

	// 1. Create database
	t.Run("CreateDatabase", func(t *testing.T) {
		req := authRequest(t, "POST", srv.URL+"/dbs", "dbs", "", strings.NewReader(`{"id":"testdb"}`))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("expected 201, got %d: %s", resp.StatusCode, body)
		}
		result := readBody(t, resp)
		for _, field := range []string{"id", "_rid", "_self", "_etag", "_ts"} {
			if _, ok := result[field]; !ok {
				t.Errorf("missing field %q in response", field)
			}
		}
		if result["id"] != "testdb" {
			t.Errorf("expected id=testdb, got %v", result["id"])
		}
	})

	// 2. Create database conflict
	t.Run("CreateDatabaseConflict", func(t *testing.T) {
		req := authRequest(t, "POST", srv.URL+"/dbs", "dbs", "", strings.NewReader(`{"id":"testdb"}`))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("expected 409, got %d", resp.StatusCode)
		}
	})

	// 3. List databases
	t.Run("ListDatabases", func(t *testing.T) {
		req := authRequest(t, "GET", srv.URL+"/dbs", "dbs", "", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		result := readBody(t, resp)
		dbs, ok := result["Databases"].([]interface{})
		if !ok {
			t.Fatal("Databases is not an array")
		}
		if len(dbs) == 0 {
			t.Fatal("expected at least 1 database")
		}
	})

	// 4. Get database
	t.Run("GetDatabase", func(t *testing.T) {
		req := authRequest(t, "GET", srv.URL+"/dbs/testdb", "dbs", "dbs/testdb", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		result := readBody(t, resp)
		if result["id"] != "testdb" {
			t.Errorf("expected id=testdb, got %v", result["id"])
		}
	})

	// 5. Create container
	t.Run("CreateContainer", func(t *testing.T) {
		body := `{"id":"testcoll","partitionKey":{"paths":["/category"],"kind":"Hash"}}`
		req := authRequest(t, "POST", srv.URL+"/dbs/testdb/colls", "colls", "dbs/testdb", strings.NewReader(body))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode != http.StatusCreated {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("expected 201, got %d: %s", resp.StatusCode, b)
		}
		result := readBody(t, resp)
		if result["id"] != "testcoll" {
			t.Errorf("expected id=testcoll, got %v", result["id"])
		}
	})

	// 6. Create document
	t.Run("CreateDocument", func(t *testing.T) {
		body := `{"id":"doc1","category":"books","title":"Go Programming"}`
		req := authRequest(t, "POST", srv.URL+"/dbs/testdb/colls/testcoll/docs", "docs", "dbs/testdb/colls/testcoll", strings.NewReader(body))
		req.Header.Set("x-ms-documentdb-partitionkey", `["books"]`)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode != http.StatusCreated {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("expected 201, got %d: %s", resp.StatusCode, b)
		}
		result := readBody(t, resp)
		if result["id"] != "doc1" {
			t.Errorf("expected id=doc1, got %v", result["id"])
		}
		if result["title"] != "Go Programming" {
			t.Errorf("expected title=Go Programming, got %v", result["title"])
		}
	})

	// 7. Get document
	t.Run("GetDocument", func(t *testing.T) {
		req := authRequest(t, "GET", srv.URL+"/dbs/testdb/colls/testcoll/docs/doc1", "docs", "dbs/testdb/colls/testcoll/docs/doc1", nil)
		req.Header.Set("x-ms-documentdb-partitionkey", `["books"]`)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("expected 200, got %d: %s", resp.StatusCode, b)
		}
		result := readBody(t, resp)
		if result["title"] != "Go Programming" {
			t.Errorf("expected title=Go Programming, got %v", result["title"])
		}
	})

	// 8. List documents
	t.Run("ListDocuments", func(t *testing.T) {
		req := authRequest(t, "GET", srv.URL+"/dbs/testdb/colls/testcoll/docs", "docs", "dbs/testdb/colls/testcoll", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		result := readBody(t, resp)
		docs, ok := result["Documents"].([]interface{})
		if !ok {
			t.Fatal("Documents is not an array")
		}
		if len(docs) == 0 {
			t.Fatal("expected at least 1 document")
		}
	})

	// 9. Replace document
	t.Run("ReplaceDocument", func(t *testing.T) {
		body := `{"id":"doc1","category":"books","title":"Advanced Go Programming"}`
		req := authRequest(t, "PUT", srv.URL+"/dbs/testdb/colls/testcoll/docs/doc1", "docs", "dbs/testdb/colls/testcoll/docs/doc1", strings.NewReader(body))
		req.Header.Set("x-ms-documentdb-partitionkey", `["books"]`)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("expected 200, got %d: %s", resp.StatusCode, b)
		}
		result := readBody(t, resp)
		if result["title"] != "Advanced Go Programming" {
			t.Errorf("expected title=Advanced Go Programming, got %v", result["title"])
		}
	})

	// 10. Query documents
	t.Run("QueryDocuments", func(t *testing.T) {
		qBody := map[string]interface{}{
			"query":      "SELECT * FROM c WHERE c.category = 'books'",
			"parameters": []interface{}{},
		}
		b, _ := json.Marshal(qBody)
		req := authRequest(t, "POST", srv.URL+"/dbs/testdb/colls/testcoll/docs", "docs", "dbs/testdb/colls/testcoll", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/query+json")
		req.Header.Set("x-ms-documentdb-isquery", "True")
		req.Header.Set("x-ms-documentdb-partitionkey", `["books"]`)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			rb, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("expected 200, got %d: %s", resp.StatusCode, rb)
		}
		result := readBody(t, resp)
		docs, ok := result["Documents"].([]interface{})
		if !ok {
			t.Fatal("Documents is not an array")
		}
		if len(docs) == 0 {
			t.Fatal("expected at least 1 document in query results")
		}
		first := docs[0].(map[string]interface{})
		if first["title"] != "Advanced Go Programming" {
			t.Errorf("expected title=Advanced Go Programming, got %v", first["title"])
		}
	})

	// 11. Delete document
	t.Run("DeleteDocument", func(t *testing.T) {
		req := authRequest(t, "DELETE", srv.URL+"/dbs/testdb/colls/testcoll/docs/doc1", "docs", "dbs/testdb/colls/testcoll/docs/doc1", nil)
		req.Header.Set("x-ms-documentdb-partitionkey", `["books"]`)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("expected 204, got %d", resp.StatusCode)
		}
	})

	// 12. Delete container
	t.Run("DeleteContainer", func(t *testing.T) {
		req := authRequest(t, "DELETE", srv.URL+"/dbs/testdb/colls/testcoll", "colls", "dbs/testdb/colls/testcoll", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("expected 204, got %d", resp.StatusCode)
		}
	})

	// 13. Delete database
	t.Run("DeleteDatabase", func(t *testing.T) {
		req := authRequest(t, "DELETE", srv.URL+"/dbs/testdb", "dbs", "dbs/testdb", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("expected 204, got %d", resp.StatusCode)
		}
	})

	// 14. PKRanges with db-level RID (SDK sends db RID as collId)
	// 15. Auth failure
	t.Run("AuthFailure", func(t *testing.T) {
		req, err := http.NewRequest("GET", srv.URL+"/dbs", nil)
		if err != nil {
			t.Fatalf("creating request: %v", err)
		}
		// No Authorization header set
		req.Header.Set("x-ms-date", time.Now().UTC().Format(http.TimeFormat))
		req.Header.Set("x-ms-version", "2018-12-31")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}
	})
}
