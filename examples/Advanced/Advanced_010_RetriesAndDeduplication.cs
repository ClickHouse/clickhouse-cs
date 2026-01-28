using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using Polly;
using Polly.Retry;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates retry patterns with Polly and deduplication using ReplacingMergeTree.
///
/// When retrying inserts, you risk inserting duplicate data if the first attempt
/// succeeded but the response was lost (e.g., network timeout). ReplacingMergeTree
/// solves this by automatically deduplicating rows with the same sorting key,
/// keeping only the latest version.
///
/// This combination achieves "exactly-once" semantics for inserts:
/// - Polly ensures transient failures are retried
/// - ReplacingMergeTree ensures duplicates are eventually merged away
/// </summary>
public static class RetriesAndDeduplication
{
    private const string TableName = "example_retries_dedup";

    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        // Create a ReplacingMergeTree table for deduplication
        await SetupReplacingMergeTreeTable(connection);

        // Demonstrate retry with simulated random failures
        await InsertWithRetryAndSimulatedFailures(connection);

        // Show how duplicates are handled
        await DemonstrateDuplicateHandling(connection);

        await Cleanup(connection);
    }

    /// <summary>
    /// Creates a ReplacingMergeTree table that automatically deduplicates rows.
    /// The 'version' column determines which row to keep (highest version wins).
    /// </summary>
    private static async Task SetupReplacingMergeTreeTable(ClickHouseConnection connection)
    {
        Console.WriteLine("1. Creating ReplacingMergeTree table:");

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {TableName} (
                id UInt64,
                data String,
                version UInt64,
                created_at DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)
        ");

        Console.WriteLine($"   Table '{TableName}' created with ReplacingMergeTree engine\n");
    }

    /// <summary>
    /// Demonstrates using Polly to retry inserts with simulated random failures.
    /// Uses ClickHouse's throwIf() function to randomly fail ~33% of inserts.
    /// </summary>
    private static async Task InsertWithRetryAndSimulatedFailures(ClickHouseConnection connection)
    {
        Console.WriteLine("2. Insert with Polly retry policy (simulating ~33% failure rate):");

        // Define a retry policy for transient failures
        var retryPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 10,
                Delay = TimeSpan.FromMilliseconds(100),
                BackoffType = DelayBackoffType.Constant,
                ShouldHandle = new PredicateBuilder()
                    .Handle<HttpRequestException>()
                    .Handle<TaskCanceledException>()
                    .Handle<ClickHouseServerException>(), // Retry all ClickHouse errors for this demo. You can filter specific error codes here. In production, you should only filter retryable errors (eg not syntax errors).
                OnRetry = args =>
                {
                    Console.WriteLine($"   Retry attempt {args.AttemptNumber} (simulated transient failure)");
                    return ValueTask.CompletedTask;
                }
            })
            .Build();

        var records = new[]
        {
            (Id: 1UL, Data: "Record A", Version: 1UL),
            (Id: 2UL, Data: "Record B", Version: 1UL),
            (Id: 3UL, Data: "Record C", Version: 1UL),
            (Id: 4UL, Data: "Record D", Version: 1UL),
            (Id: 5UL, Data: "Record E", Version: 1UL),
        };

        foreach (var record in records)
        {
            Console.WriteLine($"   Inserting record {record.Id}...");
            await retryPipeline.ExecuteAsync(async ct =>
            {
                // Use throwIf() to simulate random failures (~33% chance)
                // This throws FUNCTION_THROW_IF_VALUE_IS_NON_ZERO (error code 395)
                await connection.ExecuteStatementAsync($@"
                    SELECT throwIf(rand() % 3 = 0, 'Simulated transient failure for record {record.Id}!')");

                // If we get here, the "pre-check" passed - do the actual insert
                using var command = connection.CreateCommand();
                command.CommandText = $"INSERT INTO {TableName} (id, data, version) VALUES ({{id:UInt64}}, {{data:String}}, {{version:UInt64}})";
                command.AddParameter("id", record.Id);
                command.AddParameter("data", record.Data);
                command.AddParameter("version", record.Version);
                await command.ExecuteNonQueryAsync(ct);
            });
        }

        Console.WriteLine($"   All {records.Length} records inserted successfully!\n");
    }

    /// <summary>
    /// Shows how ReplacingMergeTree handles duplicate inserts.
    /// </summary>
    private static async Task DemonstrateDuplicateHandling(ClickHouseConnection connection)
    {
        Console.WriteLine("3. Demonstrating duplicate handling:");

        // Insert a "duplicate" with the same id but higher version (simulating a retry)
        Console.WriteLine("   Inserting duplicate of id=1 with version=2 (simulating retry)...");
        await connection.ExecuteStatementAsync(
            $"INSERT INTO {TableName} (id, data, version) VALUES (1, 'Record A - Updated', 2)");

        // Both versions exist
        var countBefore = await connection.ExecuteScalarAsync($"SELECT count() FROM {TableName} WHERE id = 1");
        Console.WriteLine($"   Rows with id=1: {countBefore}");

        // Show the final state
        Console.WriteLine("\n   Final table contents (FINAL modifier ensures deduplicated view):");
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, data, version FROM {TableName} FINAL ORDER BY id");

        Console.WriteLine("   ID  Data                    Version");
        Console.WriteLine("   --  ----                    -------");
        while (reader.Read())
        {
            var id = reader.GetFieldValue<ulong>(0);
            var data = reader.GetString(1);
            var version = reader.GetFieldValue<ulong>(2);
            Console.WriteLine($"   {id,-3} {data,-23} {version}");
        }
    }

    private static async Task Cleanup(ClickHouseConnection connection)
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {TableName}");
        Console.WriteLine($"Table '{TableName}' dropped");
    }
}
