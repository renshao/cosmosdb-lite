using Microsoft.Azure.Cosmos;
using System.Net;
using System.Text.Json.Serialization;

// Cosmos DB Emulator defaults
const string EmulatorEndpoint = "https://localhost:8081";
const string EmulatorKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
const string DatabaseName = "RaceConditionTestDb";

// Allow self-signed cert from emulator
CosmosClientOptions options = new()
{
    HttpClientFactory = () =>
    {
        HttpMessageHandler handler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback =
                HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
        };
        return new HttpClient(handler);
    },
    ConnectionMode = ConnectionMode.Gateway,
    UseSystemTextJsonSerializerWithOptions = new System.Text.Json.JsonSerializerOptions()
};

using CosmosClient client = new(EmulatorEndpoint, EmulatorKey, options);

Console.WriteLine("=== CosmosDB SDK PartitionKeyRangeCache Race Condition Repro ===");
Console.WriteLine();
Console.WriteLine("This program repeatedly creates a container and immediately reads a document");
Console.WriteLine("to trigger the SDK's PartitionKeyRangeCache race condition, where the SDK");
Console.WriteLine("sends a pkranges request with the database RID instead of the container RID.");
Console.WriteLine();

// Clean up any previous run
try
{
    await client.GetDatabase(DatabaseName).DeleteAsync();
    Console.WriteLine($"Deleted pre-existing database '{DatabaseName}'.");
}
catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound) { }

// Create database once
Console.WriteLine($"Creating database '{DatabaseName}'...");
DatabaseResponse dbResponse = await client.CreateDatabaseAsync(DatabaseName);
Database database = dbResponse.Database;
Console.WriteLine($"  Database created. StatusCode: {dbResponse.StatusCode}");
Console.WriteLine();

int iteration = 0;
int raceConditionCount = 0;

while (true)
{
    iteration++;
    string containerName = $"TestContainer-{iteration}";

    try
    {
        // Create a new container
        ContainerResponse containerResponse = await database.CreateContainerAsync(
            new ContainerProperties(containerName, "/partitionKey"));
        Container container = containerResponse.Container;

        // Immediately attempt to read a document — exercises RID resolution right
        // after creation, which is where the race condition occurs.
        string testDocId = $"doc-{Guid.NewGuid():N}"[..16];

        try
        {
            await container.ReadItemAsync<TestDoc>(testDocId, new PartitionKey("test-pk"));
            Console.WriteLine($"  [{iteration}] Unexpected success reading document.");
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Expected: document doesn't exist, but pkranges resolution succeeded
            Console.WriteLine($"  [{iteration}] OK - container '{containerName}' created, read returned 404 (expected).");
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Unauthorized)
        {
            raceConditionCount++;
            Console.WriteLine();
            Console.WriteLine($"  [{iteration}] *** RACE CONDITION HIT ({raceConditionCount} total) ***");
            Console.WriteLine($"  Container: {containerName}");
            Console.WriteLine($"  StatusCode: {ex.StatusCode}");
            Console.WriteLine($"  Message: {ex.Message}");
            Console.WriteLine($"  Check the cosmosdb-lite server logs for RACE-DIAG output.");
            Console.WriteLine();

            if (raceConditionCount >= 3)
            {
                Console.WriteLine("Race condition detected 3 times. Stopping.");
                await database.DeleteAsync();
                return;
            }
        }

        // Delete the container to keep the database clean for next iteration
        await container.DeleteContainerAsync();
    }
    catch (CosmosException ex)
    {
        Console.WriteLine($"  [{iteration}] Error: {ex.StatusCode} - {ex.Message}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  [{iteration}] Exception: {ex.GetType().Name} - {ex.Message}");
    }
}

public class TestDoc
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = "";

    [JsonPropertyName("partitionKey")]
    public string PartitionKey { get; set; } = "";
}
