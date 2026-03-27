# cosmosdb-lite

Lightweight Azure CosmosDB (NoSQL) emulator for local development and CI.
Single binary, no external dependencies — existing SDKs connect with zero code changes.

## Overview

**cosmosdb-lite** implements a subset of the Azure CosmosDB NoSQL REST API so you
can develop and test locally without an Azure subscription or the full
[Azure Cosmos DB Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator).
It speaks the same HTTPS protocol and accepts the same well-known emulator master
key, making it a drop-in replacement for most development workflows.

## Features

- **CosmosDB REST API compatible** — databases, containers, and documents
- **HMAC-SHA256 authentication** using the well-known emulator master key
- **Self-signed TLS certificate** auto-generated on first run
- **Basic SQL query support** — `SELECT * FROM c WHERE ...`
- **Built-in web explorer** at `/_explorer/`
- **In-memory storage** with optional JSON file persistence (`--data-dir`)
- **Single binary, no external dependencies**

## Quick Start

```bash
go build -o cosmosdb-lite .
./cosmosdb-lite
```

Server starts at **https://localhost:8081**.

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `8081` | HTTPS port |
| `--data-dir` | *(empty)* | Directory for persistent JSON storage (default: in-memory only) |
| `--cert-dir` | `~/.cosmosdb-lite/` | Directory for the auto-generated TLS cert and key |
| `--no-auth` | `false` | Disable HMAC auth validation |
| `--log-level` | `info` | Log verbosity: `debug`, `info`, `warn`, `error` |

## Connection String

Use the standard emulator connection string — it works with every Azure SDK:

```
AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;
```

## SDK Examples

Because the emulator uses a self-signed certificate, each example disables TLS
certificate validation. For production you should
[import the certificate](#importing-the-tls-certificate) instead.

### .NET

```csharp
using Microsoft.Azure.Cosmos;

var client = new CosmosClient(
    "https://localhost:8081",
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
    new CosmosClientOptions
    {
        HttpClientFactory = () =>
        {
            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback =
                    HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
            };
            return new HttpClient(handler);
        },
        ConnectionMode = ConnectionMode.Gateway
    });
```

### Python

```python
from azure.cosmos import CosmosClient

client = CosmosClient(
    url="https://localhost:8081",
    credential="C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
    connection_verify=False,
)
```

### JavaScript / TypeScript

```typescript
import { CosmosClient } from "@azure/cosmos";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"; // disable TLS verification

const client = new CosmosClient({
  endpoint: "https://localhost:8081",
  key: "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
});
```

### Go

```go
import (
    "crypto/tls"
    "net/http"

    "github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

cred, _ := azcosmos.NewKeyCredential(
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
)

transport := &http.Client{
    Transport: &http.Transport{
        TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
    },
}

client, _ := azcosmos.NewClientWithKey(
    "https://localhost:8081",
    cred,
    &azcosmos.ClientOptions{
        ClientOptions: policy.ClientOptions{Transport: transport},
    },
)
```

### Java

```java
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClient;

CosmosClient client = new CosmosClientBuilder()
    .endpoint("https://localhost:8081")
    .key("C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    .gatewayMode()
    .buildClient();
```

> **Tip (Java):** Add the emulator certificate to a custom trust store or set
> `-Djavax.net.ssl.trustStore` to avoid `SSLHandshakeException`.

## Importing the TLS Certificate

Instead of disabling certificate validation, you can trust the auto-generated
certificate at the OS level.

### Windows

```powershell
Import-Certificate -FilePath "$env:USERPROFILE\.cosmosdb-lite\cosmosdb-lite.crt" `
    -CertStoreLocation Cert:\CurrentUser\Root
```

### macOS

```bash
sudo security add-trusted-cert -d -r trustRoot \
    -k /Library/Keychains/System.keychain \
    ~/.cosmosdb-lite/cosmosdb-lite.crt
```

### Linux (Ubuntu / Debian)

```bash
sudo cp ~/.cosmosdb-lite/cosmosdb-lite.crt /usr/local/share/ca-certificates/cosmosdb-lite.crt
sudo update-ca-certificates
```

### Linux (RHEL / CentOS / Fedora)

```bash
sudo cp ~/.cosmosdb-lite/cosmosdb-lite.crt /etc/pki/ca-trust/source/anchors/cosmosdb-lite.crt
sudo update-ca-trust
```

## Web Explorer

Navigate to **https://localhost:8081/_explorer/** to open the built-in web UI.
From there you can browse databases, containers, and documents, create or delete
resources, and run SQL queries — all without leaving the browser.

## API Compatibility

The following CosmosDB REST API endpoints are implemented:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/dbs` | Create database |
| `GET` | `/dbs` | List databases |
| `GET` | `/dbs/{dbId}` | Get database |
| `DELETE` | `/dbs/{dbId}` | Delete database |
| `POST` | `/dbs/{dbId}/colls` | Create container |
| `GET` | `/dbs/{dbId}/colls` | List containers |
| `GET` | `/dbs/{dbId}/colls/{collId}` | Get container |
| `DELETE` | `/dbs/{dbId}/colls/{collId}` | Delete container |
| `POST` | `/dbs/{dbId}/colls/{collId}/docs` | Create document *or* query (see below) |
| `GET` | `/dbs/{dbId}/colls/{collId}/docs` | List documents |
| `GET` | `/dbs/{dbId}/colls/{collId}/docs/{docId}` | Get document |
| `PUT` | `/dbs/{dbId}/colls/{collId}/docs/{docId}` | Replace document |
| `DELETE` | `/dbs/{dbId}/colls/{collId}/docs/{docId}` | Delete document |

**Querying:** Send a `POST` to the documents endpoint with header
`x-ms-documentdb-isquery: true` or `Content-Type: application/query+json` and a
JSON body `{ "query": "SELECT * FROM c WHERE ..." }`.

All paths also respond to `OPTIONS` (CORS preflight).

## Limitations / Out of Scope

The following CosmosDB features are **not** implemented:

- Stored procedures, triggers, and UDFs
- Change feed
- Throughput / RU simulation
- Multi-region / geo-replication
- TTL policies
- Indexing policies (all documents are indexed by default)
- Continuation token pagination
- Resource tokens (only master key authentication is supported)

## License

[MIT](LICENSE)