# Bug Report: PartitionKeyRangeCache uses database RID instead of container RID in pkranges requests

## Summary

The .NET CosmosDB SDK's `PartitionKeyRangeCache` intermittently sends partition key range requests using the **database RID** as both `dbId` and `collId` in the URL, while the authorization signature is computed using the **real container RID**.

## Symptom

The SDK sends requests like:

```
GET /dbs/{dbRid}/colls/{dbRid}/pkranges
```

Where the same 4-byte database RID (e.g., `vCHWYA==`) appears in both the `dbId` and `collId` positions. However, the `Authorization` header is signed using the actual 8-byte container RID (e.g., `vCHWYNa97mg=`, lowercased in the string-to-sign).

This causes authentication failures on servers that derive the resource link from the URL path, because the URL and the signature disagree on the resource identity.

## Repro

1. Create a new database
2. Create a container in that database
3. Immediately create and read documents in the container
4. Observe that the pkranges request sometimes uses the database RID instead of the container RID in the URL

The issue is intermittent — it appears to be a race condition related to cache warming after container creation.

## Affected Version

- `Microsoft.Azure.Cosmos` SDK v3.41.0 (.NET)
- Observed on `cosmos-netstandard-sdk/3.41.0`

## Root Cause Analysis

### Call chain

1. **`GatewayStoreModel.TryResolvePartitionKeyRangeAsync`** (`GatewayStoreModel.cs:429-507`)
   - Calls `clientCollectionCache.ResolveCollectionAsync(...)` to get container info
   - Passes `collection.ResourceId` to `PartitionKeyRangeCache.TryLookupAsync()`

2. **`CollectionCache.ResolveCollectionAsync`** (`CollectionCache.cs:128-195`)
   - For RID-based paths, parses the request's `ResourceId` via `ResourceId.Parse()`
   - Extracts `resourceIdParsed.DocumentCollectionId.ToString()` as the collection RID
   - For name-based paths, sets `request.ResourceId = collectionInfo.ResourceId`

3. **`PartitionKeyRangeCache.ExecutePartitionKeyRangeReadChangeFeedAsync`** (`PartitionKeyRangeCache.cs:269-326`)
   - Creates the HTTP request using the received `collectionRid` for **both** path segments:
     ```csharp
     DocumentServiceRequest.Create(
         OperationType.ReadFeed,
         collectionRid,          // ← used as BOTH dbId and collId in URL
         ResourceType.PartitionKeyRange,
         AuthorizationTokenType.PrimaryMasterKey,
         headers)
     ```
   - The auth signature is computed from `DocumentServiceRequest.ResourceAddress`, which is the same `collectionRid` value

### Suspicious code path

**`CollectionCache.RefreshAsync`** (`CollectionCache.cs:309-345`) uses `request.RequestContext.ResolvedCollectionRid` as a placeholder value. If this RID is stale or was never properly set (e.g., the container was just created and the cache hasn't been populated yet), the wrong RID can propagate through the system.

The `PartitionKeyRangeCache` blindly trusts whatever `collectionRid` it receives — it uses it for both the URL construction and the auth signature. If `CollectionCache` returns the database RID as `collection.ResourceId` before the cache is fully warmed, the pkranges request will use the database RID in the URL.

### Evidence

- Database RID: `vCHWYA==` (4 bytes: `[188, 33, 214, 96]`)
- Container RID: `vCHWYNa97mg=` (8 bytes: `[188, 33, 214, 96, 214, 189, 238, 104]`)
- The container RID correctly starts with the database RID bytes (hierarchical structure)
- The pkranges URL used `vCHWYA==` (database RID) in both positions
- The auth signature was computed using the container RID (matched via brute-force verification against all containers in the database)

### Key files

| File | Lines | Description |
|------|-------|-------------|
| `src/Routing/PartitionKeyRangeCache.cs` | 269-326 | URL construction using `collectionRid` for both path segments |
| `src/Routing/PartitionKeyRangeCache.cs` | 121-137 | `TryLookupAsync` passes `collectionRid` to routing map |
| `src/Routing/CollectionCache.cs` | 128-195 | `ResolveCollectionAsync` resolves container RID |
| `src/Routing/CollectionCache.cs` | 309-345 | `RefreshAsync` — can reuse stale `ResolvedCollectionRid` |
| `src/GatewayStoreModel.cs` | 429-507 | `TryResolvePartitionKeyRangeAsync` bridges cache → pkranges |
| `src/Authorization/AuthorizationTokenProviderMasterKey.cs` | 60-112 | Signs using `request.ResourceAddress` directly |

## Workaround

On the server/emulator side, when pkranges auth fails, iterate all container RIDs in the database and try each (lowercased) as the resource link in the signature verification. This handles the case where the URL contains the database RID but the signature uses the real container RID.
