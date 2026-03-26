package store

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// dbEntry holds a database and its containers.
type dbEntry struct {
	DB         *Database             `json:"db"`
	Containers map[string]*collEntry `json:"containers"`
}

// collEntry holds a container and its documents keyed by partitionKey → docID → doc.
type collEntry struct {
	Container *Container                      `json:"container"`
	Documents map[string]map[string]Document  `json:"documents"`
}

// MemoryStore is an in-memory implementation of Store with optional JSON persistence.
type MemoryStore struct {
	mu        sync.RWMutex
	databases map[string]*dbEntry
	dataDir   string
}

// persistState is the JSON-serializable snapshot of all data.
type persistState struct {
	Databases map[string]*dbEntry `json:"databases"`
}

// NewMemoryStore creates a new MemoryStore. If dataDir is non-empty, it loads
// existing data from disk and persists mutations to {dataDir}/data.json.
func NewMemoryStore(dataDir string) (*MemoryStore, error) {
	m := &MemoryStore{
		databases: make(map[string]*dbEntry),
		dataDir:   dataDir,
	}
	if dataDir != "" {
		if err := m.loadFromDisk(); err != nil {
			return nil, fmt.Errorf("loading data: %w", err)
		}
	}
	return m, nil
}

// ---- Database operations ----

func (m *MemoryStore) CreateDatabase(id string) (*Database, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.databases[id]; ok {
		return nil, &ErrConflict{Resource: "database"}
	}
	rid := encodeRID(generateRIDBytes(4))
	db := &Database{
		ID:    id,
		RID:   rid,
		Self:  "dbs/" + rid + "/",
		ETag:  generateETag(),
		Colls: "colls/",
		Users: "users/",
		TS:    NowUnix(),
	}
	m.databases[id] = &dbEntry{
		DB:         db,
		Containers: make(map[string]*collEntry),
	}
	m.persistLocked()
	return db, nil
}

func (m *MemoryStore) GetDatabase(id string) (*Database, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.databases[id]
	if !ok {
		return nil, &ErrNotFound{Resource: "database"}
	}
	return entry.DB, nil
}

func (m *MemoryStore) ListDatabases() ([]*Database, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*Database, 0, len(m.databases))
	for _, entry := range m.databases {
		result = append(result, entry.DB)
	}
	return result, nil
}

func (m *MemoryStore) DeleteDatabase(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.databases[id]; !ok {
		return &ErrNotFound{Resource: "database"}
	}
	delete(m.databases, id)
	m.persistLocked()
	return nil
}

// ---- Container operations ----

func (m *MemoryStore) CreateContainer(dbID string, id string, pk PartitionKey) (*Container, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	dbE, ok := m.databases[dbID]
	if !ok {
		return nil, &ErrNotFound{Resource: "database"}
	}
	if _, ok := dbE.Containers[id]; ok {
		return nil, &ErrConflict{Resource: "container"}
	}

	dbRidBytes := decodeRID(dbE.DB.RID)
	collRidBytes := make([]byte, len(dbRidBytes)+4)
	copy(collRidBytes, dbRidBytes)
	copy(collRidBytes[len(dbRidBytes):], generateRIDBytes(4))
	rid := encodeRID(collRidBytes)
	coll := &Container{
		ID:        id,
		RID:       rid,
		Self:      "dbs/" + dbE.DB.RID + "/colls/" + rid + "/",
		ETag:      generateETag(),
		Docs:      "docs/",
		Sprocs:    "sprocs/",
		Triggers:  "triggers/",
		Udfs:      "udfs/",
		Conflicts: "conflicts/",
		TS:        NowUnix(),
		PartitionKey: pk,
		IndexingPolicy: IndexPolicy{
			IndexingMode: "consistent",
			Automatic:    true,
		},
	}
	dbE.Containers[id] = &collEntry{
		Container: coll,
		Documents: make(map[string]map[string]Document),
	}
	m.persistLocked()
	return coll, nil
}

func (m *MemoryStore) GetContainer(dbID, id string) (*Container, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dbE, ok := m.databases[dbID]
	if !ok {
		return nil, &ErrNotFound{Resource: "database"}
	}
	ce, ok := dbE.Containers[id]
	if !ok {
		return nil, &ErrNotFound{Resource: "container"}
	}
	return ce.Container, nil
}

func (m *MemoryStore) ListContainers(dbID string) ([]*Container, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dbE, ok := m.databases[dbID]
	if !ok {
		return nil, &ErrNotFound{Resource: "database"}
	}
	result := make([]*Container, 0, len(dbE.Containers))
	for _, ce := range dbE.Containers {
		result = append(result, ce.Container)
	}
	return result, nil
}

func (m *MemoryStore) DeleteContainer(dbID, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	dbE, ok := m.databases[dbID]
	if !ok {
		return &ErrNotFound{Resource: "database"}
	}
	if _, ok := dbE.Containers[id]; !ok {
		return &ErrNotFound{Resource: "container"}
	}
	delete(dbE.Containers, id)
	m.persistLocked()
	return nil
}

// ---- Document operations ----

func (m *MemoryStore) CreateDocument(dbID, containerID string, doc Document) (Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ce, err := m.getCollEntryLocked(dbID, containerID)
	if err != nil {
		return nil, err
	}

	docID, ok := doc["id"].(string)
	if !ok || docID == "" {
		return nil, fmt.Errorf("document must have a string 'id' field")
	}

	pkValue := extractPartitionKey(doc, ce.Container.PartitionKey)

	if partition, exists := ce.Documents[pkValue]; exists {
		if _, exists := partition[docID]; exists {
			return nil, &ErrConflict{Resource: "document"}
		}
	}

	dbE := m.databases[dbID]
	collRidBytes := decodeRID(ce.Container.RID)
	docRidBytes := make([]byte, len(collRidBytes)+8)
	copy(docRidBytes, collRidBytes)
	copy(docRidBytes[len(collRidBytes):], generateRIDBytes(8))
	rid := encodeRID(docRidBytes)
	doc["_rid"] = rid
	doc["_self"] = "dbs/" + dbE.DB.RID + "/colls/" + ce.Container.RID + "/docs/" + rid + "/"
	doc["_etag"] = generateETag()
	doc["_ts"] = float64(NowUnix())

	if ce.Documents[pkValue] == nil {
		ce.Documents[pkValue] = make(map[string]Document)
	}
	ce.Documents[pkValue][docID] = doc
	m.persistLocked()
	return doc, nil
}

func (m *MemoryStore) GetDocument(dbID, containerID, docID, partitionKey string) (Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ce, err := m.getCollEntryLocked(dbID, containerID)
	if err != nil {
		return nil, err
	}

	if partitionKey == "" {
		// Search across all partitions.
		for _, partition := range ce.Documents {
			if doc, ok := partition[docID]; ok {
				return doc, nil
			}
		}
		return nil, &ErrNotFound{Resource: "document"}
	}

	partition, ok := ce.Documents[partitionKey]
	if !ok {
		return nil, &ErrNotFound{Resource: "document"}
	}
	doc, ok := partition[docID]
	if !ok {
		return nil, &ErrNotFound{Resource: "document"}
	}
	return doc, nil
}

func (m *MemoryStore) ListDocuments(dbID, containerID string) ([]Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ce, err := m.getCollEntryLocked(dbID, containerID)
	if err != nil {
		return nil, err
	}

	var result []Document
	for _, partition := range ce.Documents {
		for _, doc := range partition {
			result = append(result, doc)
		}
	}
	return result, nil
}

func (m *MemoryStore) ReplaceDocument(dbID, containerID, docID string, doc Document) (Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ce, err := m.getCollEntryLocked(dbID, containerID)
	if err != nil {
		return nil, err
	}

	doc["id"] = docID

	pkValue := extractPartitionKey(doc, ce.Container.PartitionKey)

	// Find and remove the old document from any partition.
	found := false
	for pk, partition := range ce.Documents {
		if _, ok := partition[docID]; ok {
			delete(partition, docID)
			if len(partition) == 0 {
				delete(ce.Documents, pk)
			}
			found = true
			break
		}
	}
	if !found {
		return nil, &ErrNotFound{Resource: "document"}
	}

	dbE := m.databases[dbID]
	collRidBytes := decodeRID(ce.Container.RID)
	docRidBytes := make([]byte, len(collRidBytes)+8)
	copy(docRidBytes, collRidBytes)
	copy(docRidBytes[len(collRidBytes):], generateRIDBytes(8))
	rid := encodeRID(docRidBytes)
	doc["_rid"] = rid
	doc["_self"] = "dbs/" + dbE.DB.RID + "/colls/" + ce.Container.RID + "/docs/" + rid + "/"
	doc["_etag"] = generateETag()
	doc["_ts"] = float64(NowUnix())

	if ce.Documents[pkValue] == nil {
		ce.Documents[pkValue] = make(map[string]Document)
	}
	ce.Documents[pkValue][docID] = doc
	m.persistLocked()
	return doc, nil
}

func (m *MemoryStore) DeleteDocument(dbID, containerID, docID, partitionKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ce, err := m.getCollEntryLocked(dbID, containerID)
	if err != nil {
		return err
	}

	if partitionKey == "" {
		for pk, partition := range ce.Documents {
			if _, ok := partition[docID]; ok {
				delete(partition, docID)
				if len(partition) == 0 {
					delete(ce.Documents, pk)
				}
				m.persistLocked()
				return nil
			}
		}
		return &ErrNotFound{Resource: "document"}
	}

	partition, ok := ce.Documents[partitionKey]
	if !ok {
		return &ErrNotFound{Resource: "document"}
	}
	if _, ok := partition[docID]; !ok {
		return &ErrNotFound{Resource: "document"}
	}
	delete(partition, docID)
	if len(partition) == 0 {
		delete(ce.Documents, partitionKey)
	}
	m.persistLocked()
	return nil
}

func (m *MemoryStore) QueryDocuments(dbID, containerID, query string, params []QueryParam) ([]Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ce, err := m.getCollEntryLocked(dbID, containerID)
	if err != nil {
		return nil, err
	}

	// Collect all documents.
	var allDocs []Document
	for _, partition := range ce.Documents {
		for _, doc := range partition {
			allDocs = append(allDocs, doc)
		}
	}

	conditions, err := parseQuery(query, params)
	if err != nil {
		return nil, err
	}

	// No WHERE clause — return all.
	if len(conditions) == 0 {
		return allDocs, nil
	}

	var result []Document
	for _, doc := range allDocs {
		if matchesConditions(doc, conditions) {
			result = append(result, doc)
		}
	}
	return result, nil
}

// ---- Internal helpers ----

// ResolveRIDPath takes a _rid-based path like "dbs/abc==/colls/def==/pkranges"
// and resolves it to a name-based path like "dbs/mydb/colls/mycoll/pkranges".
// Returns the resolved path and true if resolution succeeded, or ("", false).
func (m *MemoryStore) ResolveRIDPath(path string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	n := len(parts)
	if n < 2 {
		return "", false
	}

	// Resolve database by _rid
	if parts[0] != "dbs" || n < 2 {
		return "", false
	}
	dbRID := parts[1]
	var dbName string
	var dbE *dbEntry
	for name, entry := range m.databases {
		if entry.DB.RID == dbRID {
			dbName = name
			dbE = entry
			break
		}
	}
	if dbName == "" {
		return "", false
	}

	resolved := []string{"dbs", dbName}

	if n < 4 {
		// Just dbs/{rid} or dbs/{rid}/something
		if n == 3 {
			resolved = append(resolved, parts[2])
		}
		return strings.Join(resolved, "/"), true
	}

	// Resolve container by _rid
	if parts[2] != "colls" {
		return "", false
	}
	collRID := parts[3]
	var collName string
	for name, ce := range dbE.Containers {
		if ce.Container.RID == collRID {
			collName = name
			break
		}
	}
	if collName == "" {
		return "", false
	}

	resolved = append(resolved, "colls", collName)

	// Append remaining segments (e.g., "pkranges", "docs", "docs/{rid}")
	for i := 4; i < n; i++ {
		resolved = append(resolved, parts[i])
	}

	return strings.Join(resolved, "/"), true
}

// ResolveDBID resolves a database identifier (name or _rid) to its name.
func (m *MemoryStore) ResolveDBID(idOrRID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Try direct name lookup first
	if _, ok := m.databases[idOrRID]; ok {
		return idOrRID, true
	}
	// Try _rid lookup
	for name, entry := range m.databases {
		if entry.DB.RID == idOrRID {
			return name, true
		}
	}
	return "", false
}

// ResolveContainerID resolves a container identifier (name or _rid) within a database to its name.
// dbName must already be resolved to a name (not a _rid).
func (m *MemoryStore) ResolveContainerID(dbName, idOrRID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dbE, ok := m.databases[dbName]
	if !ok {
		return "", false
	}
	// Try direct name lookup first
	if _, ok := dbE.Containers[idOrRID]; ok {
		return idOrRID, true
	}
	// Try _rid lookup
	for name, ce := range dbE.Containers {
		if ce.Container.RID == idOrRID {
			return name, true
		}
	}
	return "", false
}

func (m *MemoryStore) getCollEntryLocked(dbID, containerID string) (*collEntry, error) {
	dbE, ok := m.databases[dbID]
	if !ok {
		return nil, &ErrNotFound{Resource: "database"}
	}
	ce, ok := dbE.Containers[containerID]
	if !ok {
		return nil, &ErrNotFound{Resource: "container"}
	}
	return ce, nil
}

// extractPartitionKey gets the partition key value from a document based on
// the container's partition key configuration.
func extractPartitionKey(doc Document, pk PartitionKey) string {
	if len(pk.Paths) == 0 {
		return ""
	}
	path := pk.Paths[0]
	// Strip leading '/' — e.g. "/category" → "category"
	field := strings.TrimPrefix(path, "/")

	val, ok := doc[field]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v", val)
}

// ridCounter is an atomic counter for generating sequential _rid values.
var ridCounter uint32

// generateRIDBytes generates n bytes for use as a resource ID component.
// Uses an atomic counter to produce sequential, deterministic values
// that encode cleanly in standard base64 without producing '/' characters.
func generateRIDBytes(n int) []byte {
	ridCounter++
	b := make([]byte, n)
	val := ridCounter
	// Write counter value in little-endian (matching CosmosDB's internal format),
	// which keeps base64 output free of '/' for reasonable counter values.
	for i := 0; i < n && val > 0; i++ {
		b[i] = byte(val & 0x3F) // Keep values in 0-63 range to avoid '/' in base64
		val >>= 6
	}
	return b
}

// encodeRID encodes raw bytes as standard base64 (matching real CosmosDB _rid format).
func encodeRID(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// decodeRID decodes a base64-encoded _rid back to raw bytes.
func decodeRID(rid string) []byte {
	b, _ := base64.StdEncoding.DecodeString(rid)
	return b
}

func generateETag() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("\"%08x-%04x-%04x-%04x-%012x\"",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ---- Persistence ----

func (m *MemoryStore) persistLocked() {
	if m.dataDir == "" {
		return
	}
	state := persistState{Databases: m.databases}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return
	}
	_ = os.MkdirAll(m.dataDir, 0o755)
	_ = os.WriteFile(filepath.Join(m.dataDir, "data.json"), data, 0o644)
}

func (m *MemoryStore) loadFromDisk() error {
	path := filepath.Join(m.dataDir, "data.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var state persistState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Databases != nil {
		m.databases = state.Databases
	}
	// Ensure nested maps are non-nil after deserialization.
	for _, dbE := range m.databases {
		if dbE.Containers == nil {
			dbE.Containers = make(map[string]*collEntry)
		}
		for _, ce := range dbE.Containers {
			if ce.Documents == nil {
				ce.Documents = make(map[string]map[string]Document)
			}
		}
	}
	return nil
}

// ---- Basic SQL query parser ----

type queryCondition struct {
	field string      // e.g. "category"
	value interface{} // string or float64
}

// parseQuery extracts WHERE conditions from a simple SQL query.
// Supported forms:
//
//	SELECT * FROM c
//	SELECT * FROM c WHERE c.field = 'value'
//	SELECT * FROM c WHERE c.field = 'value' AND c.field2 = 123
//	SELECT * FROM c WHERE c.field = @param
func parseQuery(query string, params []QueryParam) ([]queryCondition, error) {
	normalized := strings.TrimSpace(query)

	// Check basic structure.
	upper := strings.ToUpper(normalized)
	if !strings.HasPrefix(upper, "SELECT") {
		return nil, fmt.Errorf("unsupported query: must start with SELECT")
	}

	whereIdx := findWhereIndex(normalized)
	if whereIdx == -1 {
		return nil, nil // no WHERE clause
	}

	whereClause := strings.TrimSpace(normalized[whereIdx+5:]) // skip "WHERE"

	paramMap := make(map[string]interface{}, len(params))
	for _, p := range params {
		paramMap[p.Name] = p.Value
	}

	// Split on AND (case-insensitive).
	parts := splitAND(whereClause)

	var conditions []queryCondition
	for _, part := range parts {
		cond, err := parseCondition(strings.TrimSpace(part), paramMap)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}
	return conditions, nil
}

// findWhereIndex returns the index of the WHERE keyword (case-insensitive, word boundary).
func findWhereIndex(q string) int {
	re := regexp.MustCompile(`(?i)\bWHERE\b`)
	loc := re.FindStringIndex(q)
	if loc == nil {
		return -1
	}
	return loc[0]
}

// splitAND splits a WHERE clause on AND keywords (case-insensitive, word boundary).
func splitAND(clause string) []string {
	re := regexp.MustCompile(`(?i)\bAND\b`)
	return re.Split(clause, -1)
}

var conditionRe = regexp.MustCompile(
	`^([a-zA-Z_][a-zA-Z0-9_.]*)\s*=\s*(.+)$`,
)

func parseCondition(expr string, paramMap map[string]interface{}) (queryCondition, error) {
	m := conditionRe.FindStringSubmatch(expr)
	if m == nil {
		return queryCondition{}, fmt.Errorf("unsupported condition: %s", expr)
	}

	fieldPath := m[1]
	rawValue := strings.TrimSpace(m[2])

	// Strip alias prefix (e.g. "c.category" → "category").
	if dotIdx := strings.IndexByte(fieldPath, '.'); dotIdx != -1 {
		fieldPath = fieldPath[dotIdx+1:]
	}

	value, err := resolveValue(rawValue, paramMap)
	if err != nil {
		return queryCondition{}, err
	}

	return queryCondition{field: fieldPath, value: value}, nil
}

func resolveValue(raw string, paramMap map[string]interface{}) (interface{}, error) {
	// Parameter reference: @paramName
	if strings.HasPrefix(raw, "@") {
		v, ok := paramMap[raw]
		if !ok {
			return nil, fmt.Errorf("unknown parameter: %s", raw)
		}
		return v, nil
	}

	// String literal: 'value'
	if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
		return raw[1 : len(raw)-1], nil
	}

	// Number literal
	if n, err := strconv.ParseFloat(raw, 64); err == nil {
		return n, nil
	}

	// Boolean
	switch strings.ToLower(raw) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}

	return nil, fmt.Errorf("unsupported value: %s", raw)
}

func matchesConditions(doc Document, conditions []queryCondition) bool {
	for _, cond := range conditions {
		docVal, ok := doc[cond.field]
		if !ok {
			return false
		}
		if !valuesEqual(docVal, cond.value) {
			return false
		}
	}
	return true
}

// valuesEqual compares a document value to a condition value, handling
// JSON number (float64) vs int comparisons.
func valuesEqual(docVal, condVal interface{}) bool {
	// Direct equality.
	if fmt.Sprintf("%v", docVal) == fmt.Sprintf("%v", condVal) {
		return true
	}
	// Numeric comparison: JSON unmarshals numbers as float64.
	dF, dOk := toFloat64(docVal)
	cF, cOk := toFloat64(condVal)
	if dOk && cOk {
		return dF == cF
	}
	return false
}

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
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}
