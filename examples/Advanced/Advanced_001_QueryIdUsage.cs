using ClickHouse.Driver.ADO;
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
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("Query ID Usage Examples\n");

        // Example 1: Automatic Query ID
        Console.WriteLine("1. Automatic Query ID assignment:");
        await Example1_AutomaticQueryId(connection);

        // Example 2: Custom Query ID
        Console.WriteLine("\n2. Setting a custom Query ID:");
        await Example2_CustomQueryId(connection);

        // Example 3: Tracking query execution
        Console.WriteLine("\n3. Tracking query execution in system.query_log:");
        await Example3_TrackingQueryExecution(connection);

        // Example 4: Cancelling a query by Query ID
        Console.WriteLine("\n4. Query cancellation using Query ID:");
        await Example4_QueryCancellation(connection);

        Console.WriteLine("\nAll Query ID examples completed!");
    }

    private static async Task Example1_AutomaticQueryId(ClickHouseConnection connection)
    {
        // When you don't set a QueryId, the client automatically generates a GUID
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'Hello from ClickHouse' AS message";

        Console.WriteLine($"   QueryId before execution: {command.QueryId ?? "(null)"}");

        var result = await command.ExecuteScalarAsync();

        // After execution, the QueryId contains the auto-generated GUID
        Console.WriteLine($"   QueryId after execution: {command.QueryId}");
        Console.WriteLine($"   Result: {result}");
    }

    private static async Task Example2_CustomQueryId(ClickHouseConnection connection)
    {
        // You can set your own Query ID before executing a query
        // This is useful for correlation with your application logs
        var customQueryId = $"example-{Guid.NewGuid()}";

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT version()";
        command.QueryId = customQueryId;

        Console.WriteLine($"   Custom QueryId: {customQueryId}");

        var version = await command.ExecuteScalarAsync();
        Console.WriteLine($"   ClickHouse version: {version}");
        Console.WriteLine($"   QueryId remained: {command.QueryId}");
    }

    private static async Task Example3_TrackingQueryExecution(ClickHouseConnection connection)
    {
        // Execute a query with a custom Query ID
        var trackableQueryId = $"trackable-{Guid.NewGuid()}";
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $"SELECT 1";
            command.QueryId = trackableQueryId;
            await command.ExecuteNonQueryAsync();
        }

        Console.WriteLine($"   Executed query with ID: {trackableQueryId}");

        // Wait a moment for the query to be logged
        await Task.Delay(1000);

        // Query system.query_log to get information about our query
        // Note: system.query_log may need to be enabled in your ClickHouse configuration
        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
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
            ";
            command.AddParameter("queryId", trackableQueryId);

            try
            {
                using var reader = await command.ExecuteReaderAsync();
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
    }

    private static async Task Example4_QueryCancellation(ClickHouseConnection connection)
    {
        // Demonstrate cancelling a long-running query using Query ID
        var cancellableQueryId = $"cancellable-{Guid.NewGuid()}";

        Console.WriteLine($"   Query ID: {cancellableQueryId}");
        Console.WriteLine("   Starting a long-running query (SELECT sleep(5))...");

        // Start the long-running query in a background task
        var queryTask = Task.Run(async () =>
        {
            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT sleep(3)";
                command.QueryId = cancellableQueryId;

                await command.ExecuteScalarAsync();
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

        // Wait a bit for the query to start and be present in the log
        await Task.Delay(1000);

        // Cancel using KILL QUERY from another connection. Note that closing a connection will NOT kill any running queries opened by that connection.
        Console.WriteLine($"   Cancelling query using KILL QUERY...");
        try
        {
            // Create a separate connection for cancellation
            using var cancelConnection = new ClickHouseConnection("Host=localhost");
            await cancelConnection.OpenAsync();

            using var cancelCommand = cancelConnection.CreateCommand();
            cancelCommand.CommandText = $"KILL QUERY WHERE query_id = '{cancellableQueryId}'";
            await cancelCommand.ExecuteNonQueryAsync();

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
