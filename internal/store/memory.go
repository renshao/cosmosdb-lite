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

// generateRIDBytes generates n random bytes for use as a resource ID component.
// Uses crypto/rand for realistic values matching real CosmosDB RID byte ranges.
// Retries if the base64 encoding contains '/' or '+' (which break URL paths).
func generateRIDBytes(n int) []byte {
	for {
		b := make([]byte, n)
		_, _ = rand.Read(b)
		encoded := base64.StdEncoding.EncodeToString(b)
		if !strings.ContainsAny(encoded, "/+") {
			return b
		}
	}
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

// GeneratePKRangeRID builds a 16-byte pkrange-specific _rid from a container
// RID (8 bytes) plus an 8-byte suffix encoding the range index.
// Real CosmosDB pkrange RIDs follow this hierarchical pattern.
func GeneratePKRangeRID(containerRID string, index int) string {
	collBytes := decodeRID(containerRID)
	b := make([]byte, len(collBytes)+8)
	copy(b, collBytes)
	// Encode the index in the first byte of the suffix (little-endian),
	// matching the pattern seen in real CosmosDB responses.
	b[len(collBytes)] = byte(index + 2) // +2: real CosmosDB starts pkrange suffix at 0x02
	return encodeRID(b)
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

// queryCondition represents a single WHERE condition.
type queryCondition struct {
	field    string        // e.g. "category"
	op       string        // "=" or "IN"
	value    interface{}   // single value for "="
	values   []interface{} // multiple values for "IN"
}

// parseQuery extracts WHERE conditions from a simple SQL query.
// Supported forms:
//
//	SELECT * FROM c
//	SELECT * FROM c WHERE c.field = 'value'
//	SELECT * FROM c WHERE c.field = 'value' AND c.field2 = 123
//	SELECT * FROM c WHERE c.field = @param
//	SELECT * FROM c WHERE c.field IN (@p1, @p2)
//	SELECT * FROM c WHERE c.field = 'a' OR c.field = 'b'
//	SELECT * FROM c WHERE (c.field = 'a' OR c.field = 'b')
//
// For unsupported query syntax, returns nil conditions (matches all documents)
// rather than an error, so the SDK doesn't receive 500 for queries we can't parse.
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

	// Split on top-level AND (case-insensitive), respecting parentheses.
	parts := splitTopLevelAND(whereClause)

	var conditions []queryCondition
	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Strip wrapping parentheses: "(expr)" → "expr"
		part = stripParens(part)

		conds, err := parseConditionGroup(part, paramMap)
		if err != nil {
			// For unsupported syntax, return nil to match all documents
			// rather than returning 500 to the SDK.
			return nil, nil
		}
		conditions = append(conditions, conds...)
	}
	return conditions, nil
}

// stripParens removes a single layer of wrapping parentheses if present.
// findWhereIndex returns the index of the WHERE keyword (case-insensitive, word boundary).
func findWhereIndex(q string) int {
	re := regexp.MustCompile(`(?i)\bWHERE\b`)
	loc := re.FindStringIndex(q)
	if loc == nil {
		return -1
	}
	return loc[0]
}

func stripParens(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' {
		// Verify the parens are balanced (the closing paren matches the opening one).
		depth := 0
		for i, ch := range s {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			if depth == 0 && i < len(s)-1 {
				return s // closing paren doesn't match the opening one
			}
		}
		return strings.TrimSpace(s[1 : len(s)-1])
	}
	return s
}

// parseConditionGroup parses a condition that may contain OR.
// "c.x = 1 OR c.x = 2" → single IN-style condition on field x.
func parseConditionGroup(expr string, paramMap map[string]interface{}) ([]queryCondition, error) {
	orParts := splitTopLevelOR(expr)
	if len(orParts) == 1 {
		cond, err := parseSingleCondition(strings.TrimSpace(orParts[0]), paramMap)
		if err != nil {
			return nil, err
		}
		return []queryCondition{cond}, nil
	}

	// Multiple OR branches — merge into an IN condition if all on same field.
	var field string
	var values []interface{}
	for _, orPart := range orParts {
		orPart = stripParens(strings.TrimSpace(orPart))
		cond, err := parseSingleCondition(orPart, paramMap)
		if err != nil {
			return nil, err
		}
		if cond.op == "IN" {
			if field == "" {
				field = cond.field
			} else if field != cond.field {
				return nil, fmt.Errorf("unsupported OR across different fields")
			}
			values = append(values, cond.values...)
		} else {
			if field == "" {
				field = cond.field
			} else if field != cond.field {
				return nil, fmt.Errorf("unsupported OR across different fields")
			}
			values = append(values, cond.value)
		}
	}
	return []queryCondition{{field: field, op: "IN", values: values}}, nil
}

// splitTopLevelAND splits on AND keywords not inside parentheses.
func splitTopLevelAND(clause string) []string {
	return splitTopLevel(clause, `(?i)\bAND\b`)
}

// splitTopLevelOR splits on OR keywords not inside parentheses.
func splitTopLevelOR(clause string) []string {
	return splitTopLevel(clause, `(?i)\bOR\b`)
}

// splitTopLevel splits a clause on a keyword pattern, respecting parentheses.
func splitTopLevel(clause, pattern string) []string {
	re := regexp.MustCompile(pattern)
	locs := re.FindAllStringIndex(clause, -1)
	if len(locs) == 0 {
		return []string{clause}
	}

	var parts []string
	prev := 0
	for _, loc := range locs {
		// Only split if the keyword is at parenthesis depth 0.
		if parenDepth(clause[:loc[0]]) == 0 {
			parts = append(parts, clause[prev:loc[0]])
			prev = loc[1]
		}
	}
	parts = append(parts, clause[prev:])
	return parts
}

// parenDepth counts the net parenthesis depth of a string.
func parenDepth(s string) int {
	depth := 0
	for _, ch := range s {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}
	}
	return depth
}

var conditionInRe = regexp.MustCompile(
	`(?i)^([a-zA-Z_][a-zA-Z0-9_.]*)\s+IN\s*\((.+)\)$`,
)

var conditionEqRe = regexp.MustCompile(
	`^([a-zA-Z_][a-zA-Z0-9_.]*)\s*=\s*(.+)$`,
)

// parseSingleCondition parses "field = value" or "field IN (v1, v2, ...)"
func parseSingleCondition(expr string, paramMap map[string]interface{}) (queryCondition, error) {
	expr = stripParens(expr)

	// Try IN first
	if m := conditionInRe.FindStringSubmatch(expr); m != nil {
		fieldPath := stripAlias(m[1])
		valuesStr := m[2]
		valParts := strings.Split(valuesStr, ",")
		var values []interface{}
		for _, vp := range valParts {
			v, err := resolveValue(strings.TrimSpace(vp), paramMap)
			if err != nil {
				return queryCondition{}, err
			}
			values = append(values, v)
		}
		return queryCondition{field: fieldPath, op: "IN", values: values}, nil
	}

	// Try equality
	if m := conditionEqRe.FindStringSubmatch(expr); m != nil {
		fieldPath := stripAlias(m[1])
		rawValue := strings.TrimSpace(m[2])
		value, err := resolveValue(rawValue, paramMap)
		if err != nil {
			return queryCondition{}, err
		}
		return queryCondition{field: fieldPath, op: "=", value: value}, nil
	}

	return queryCondition{}, fmt.Errorf("unsupported condition: %s", expr)
}

// stripAlias removes a table alias prefix (e.g. "c.category" → "category").
func stripAlias(field string) string {
	if dotIdx := strings.IndexByte(field, '.'); dotIdx != -1 {
		return field[dotIdx+1:]
	}
	return field
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
		switch cond.op {
		case "IN":
			matched := false
			for _, v := range cond.values {
				if valuesEqual(docVal, v) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		default:
			if !valuesEqual(docVal, cond.value) {
				return false
			}
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
