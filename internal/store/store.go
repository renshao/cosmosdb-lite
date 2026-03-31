package store

import "time"

// Database represents a CosmosDB database.
type Database struct {
	ID    string `json:"id"`
	RID   string `json:"_rid"`
	Self  string `json:"_self"`
	ETag  string `json:"_etag"`
	Colls string `json:"_colls"`
	Users string `json:"_users"`
	TS    int64  `json:"_ts"`
}

// Container represents a CosmosDB container (collection).
type Container struct {
	ID             string       `json:"id"`
	RID            string       `json:"_rid"`
	Self           string       `json:"_self"`
	ETag           string       `json:"_etag"`
	Docs           string       `json:"_docs"`
	Sprocs         string       `json:"_sprocs"`
	Triggers       string       `json:"_triggers"`
	Udfs           string       `json:"_udfs"`
	Conflicts      string       `json:"_conflicts"`
	TS             int64        `json:"_ts"`
	PartitionKey   PartitionKey `json:"partitionKey"`
	IndexingPolicy IndexPolicy  `json:"indexingPolicy"`
}

// PartitionKey defines the partition key configuration.
type PartitionKey struct {
	Paths   []string `json:"paths"`
	Kind    string   `json:"kind"`
	Version int      `json:"version,omitempty"`
}

// IndexPolicy is a simplified indexing policy.
type IndexPolicy struct {
	IndexingMode string `json:"indexingMode"`
	Automatic    bool   `json:"automatic"`
}

// Document represents a CosmosDB document as a generic JSON map.
type Document = map[string]interface{}

// Store defines the storage interface for the emulator.
type Store interface {
	// Database operations
	CreateDatabase(id string) (*Database, error)
	GetDatabase(id string) (*Database, error)
	ListDatabases() ([]*Database, error)
	DeleteDatabase(id string) error

	// Container operations
	CreateContainer(dbID string, id string, pk PartitionKey) (*Container, error)
	GetContainer(dbID, id string) (*Container, error)
	ListContainers(dbID string) ([]*Container, error)
	DeleteContainer(dbID, id string) error

	// Document operations
	CreateDocument(dbID, containerID string, doc Document) (Document, error)
	GetDocument(dbID, containerID, docID, partitionKey string) (Document, error)
	ListDocuments(dbID, containerID string) ([]Document, error)
	ReplaceDocument(dbID, containerID, docID string, doc Document) (Document, error)
	DeleteDocument(dbID, containerID, docID, partitionKey string) error
	QueryDocuments(dbID, containerID, query string, params []QueryParam) ([]Document, error)

	// RID resolution — maps _rid values back to human-readable names
	ResolveRIDPath(path string) (resolvedPath string, ok bool)

	// ResolveDBID resolves a database identifier (name or _rid) to its name.
	ResolveDBID(idOrRID string) (name string, ok bool)

	// ResolveContainerID resolves a container identifier (name or _rid) within a database to its name.
	ResolveContainerID(dbName, idOrRID string) (name string, ok bool)
}

// QueryParam represents a parameterized query parameter.
type QueryParam struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

// Common errors
type ErrNotFound struct {
	Resource string
}

func (e *ErrNotFound) Error() string {
	return e.Resource + " not found"
}

type ErrConflict struct {
	Resource string
}

func (e *ErrConflict) Error() string {
	return e.Resource + " already exists"
}

// NowUnix returns the current Unix timestamp.
func NowUnix() int64 {
	return time.Now().Unix()
}
