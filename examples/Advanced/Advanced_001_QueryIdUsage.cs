using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to use Query IDs to track and monitor query execution.
/// Query IDs are useful for:
/// - Debugging and troubleshooting specific queries
/// - Tracking query execution in system.query_log
/// - Correlating client-side operations with server-side logs
/// - Monitoring query progress and performance
/// </summary>
public static class QueryIdUsage
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        Console.WriteLine("Query ID Usage Examples\n");

        // Example 1: Automatic Query ID
        Console.WriteLine("1. Automatic Query ID assignment:");
        await Example1_AutomaticQueryId(client);

        // Example 2: Custom Query ID
        Console.WriteLine("\n2. Setting a custom Query ID:");
        await Example2_CustomQueryId(client);

        // Example 3: Tracking query execution
        Console.WriteLine("\n3. Tracking query execution in system.query_log:");
        await Example3_TrackingQueryExecution(client);

        // Example 4: Cancelling a query by Query ID
        Console.WriteLine("\n4. Query cancellation using Query ID:");
        await Example4_QueryCancellation(client);

        Console.WriteLine("\nAll Query ID examples completed!");
    }

    private static async Task Example1_AutomaticQueryId(ClickHouseClient client)
    {
        // When you don't set a QueryId, the client automatically generates a GUID
        var result = await client.ExecuteScalarAsync("SELECT 'Hello from ClickHouse' AS message");
        Console.WriteLine($"   Result: {result}");
        Console.WriteLine("   (QueryId was auto-generated for this query)");
    }

    private static async Task Example2_CustomQueryId(ClickHouseClient client)
    {
        // You can set your own Query ID before executing a query
        // This is useful for correlation with your application logs
        var customQueryId = $"example-{Guid.NewGuid()}";
        var options = new QueryOptions { QueryId = customQueryId };

        Console.WriteLine($"   Custom QueryId: {customQueryId}");

        var version = await client.ExecuteScalarAsync("SELECT version()", options: options);
        Console.WriteLine($"   ClickHouse version: {version}");
    }

    private static async Task Example3_TrackingQueryExecution(ClickHouseClient client)
    {
        // Execute a query with a custom Query ID
        var trackableQueryId = $"trackable-{Guid.NewGuid()}";
        var options = new QueryOptions { QueryId = trackableQueryId };

        await client.ExecuteNonQueryAsync("SELECT 1", options: options);
        Console.WriteLine($"   Executed query with ID: {trackableQueryId}");

        // Wait a moment for the query to be logged
        await Task.Delay(2000);

        // Query system.query_log to get information about our query
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("queryId", trackableQueryId);
        try
        {
            using var reader = await client.ExecuteReaderAsync(@"
                SELECT
                    query_id,
                    type,
                    query_duration_ms,
                    read_rows,
                    written_rows,
                    memory_usage
                FROM system.query_log
                WHERE query_id = {queryId:String}
                  AND type = 'QueryFinish'
                ORDER BY event_time DESC
                LIMIT 1
            ", parameters);

            if (reader.Read())
            {
                Console.WriteLine("   Query execution details from system.query_log:");
                Console.WriteLine($"     Query ID: {reader.GetString(0)}");
                Console.WriteLine($"     Type: {reader.GetString(1)}");
                Console.WriteLine($"     Duration: {reader.GetFieldValue<ulong>(2)} ms");
                Console.WriteLine($"     Rows read: {reader.GetFieldValue<ulong>(3)}");
                Console.WriteLine($"     Rows written: {reader.GetFieldValue<ulong>(4)}");
                Console.WriteLine($"     Memory usage: {reader.GetFieldValue<ulong>(5)} bytes");
            }
            else
            {
                Console.WriteLine("   (Query not yet in system.query_log - this table may have a delay or be disabled)");
            }
        }
        catch (ClickHouseServerException ex) when (ex.ErrorCode == 60)
        {
            Console.WriteLine("   (system.query_log table not available on this server)");
        }
    }

    private static async Task Example4_QueryCancellation(ClickHouseClient client)
    {
        // Demonstrate cancelling a long-running query using Query ID
        var cancellableQueryId = $"cancellable-{Guid.NewGuid()}";
        var options = new QueryOptions { QueryId = cancellableQueryId };

        Console.WriteLine($"   Query ID: {cancellableQueryId}");
        Console.WriteLine("   Starting a long-running query (SELECT sleep(3))...");

        // Start the long-running query in a background task
        var queryTask = Task.Run(async () =>
        {
            try
            {
                await client.ExecuteScalarAsync("SELECT sleep(3)", options: options);
                Console.WriteLine("     Query completed (should have been cancelled)");
            }
            catch (ClickHouseServerException ex)
            {
                // Query was killed on the server
                Console.WriteLine($"   Server error: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"   Query failed: {ex.Message}");
            }
        }, CancellationToken.None);

        // Wait a bit for the query to start
        await Task.Delay(1000);

        // Cancel using KILL QUERY
        Console.WriteLine($"   Cancelling query using KILL QUERY...");
        try
        {
            var killParams = new ClickHouseParameterCollection();
            killParams.AddParameter("queryId", cancellableQueryId);
            await client.ExecuteNonQueryAsync("KILL QUERY WHERE query_id = {queryId:String}", killParams);
            Console.WriteLine("     KILL QUERY command sent");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   Note: KILL QUERY failed (may require permissions): {ex.Message}");
        }

        // Wait for the query task to complete
        await queryTask;
    }
}
