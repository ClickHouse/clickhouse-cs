using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates strategies for handling long-running queries that might be terminated
/// by load balancers or proxies due to idle connections. Typical idle timeouts are ~30s.
///
/// These are typically INSERT .. FROM SELECT type queries that move large amounts of data. 
///
/// Two main approaches are shown:
/// 1. Progress Headers: Keep the connection alive by having ClickHouse send periodic progress updates
/// 2. Fire-and-Forget with Query ID: Start the query, disconnect, and poll for completion
/// </summary>
public static class LongRunningQueries
{
    public static async Task Run()
    {
        Console.WriteLine("Long-Running Query Management Examples\n");

        // Example 1: Using progress headers to keep connection alive
        Console.WriteLine("1. Using progress headers to prevent connection timeout:");
        await Example1_ProgressHeaders();

        // Example 2: Fire-and-forget pattern with Query ID tracking
        Console.WriteLine("\n2. Fire-and-forget pattern with Query ID tracking:");
        await Example2_FireAndForget();

        Console.WriteLine("\nAll long-running query examples completed!");
    }

    private static async Task Example1_ProgressHeaders()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("   Configuring query with progress headers...");
        Console.WriteLine("   This approach keeps the HTTP connection alive by sending periodic progress updates.");
        Console.WriteLine();
        
        // Execute a query with progress headers enabled
        using (var command = connection.CreateCommand())
        {
            // This simulates a long-running query using the sleep() function
            command.CommandText = @"
                SELECT
                    *,
                    sleep(0.01) as delay
                FROM system.numbers
                LIMIT 100
            ";

            // Enable progress headers to keep the connection alive
            // These settings tell ClickHouse to send HTTP headers with progress information
            // It is also possible to set these settings at the Connection level.
            // If your queries are extremely long, they may fill up the default 64kb header buffer.
            // In that you could modify HttpClientHandler's MaxResponseHeadersLength property,
            // but the preferred solution is to break up the data into more manageable chunks.
            command.CustomSettings.Add("send_progress_in_http_headers", 1);
            command.CustomSettings.Add("http_headers_progress_interval_ms", "1000");

            Console.WriteLine("   Custom Settings configured:");
            Console.WriteLine("     send_progress_in_http_headers = 1");
            Console.WriteLine("     http_headers_progress_interval_ms = 1000");
            Console.WriteLine();
            Console.WriteLine("   Executing query (this may take a moment)...");

            var startTime = DateTime.UtcNow;
            var rowCount = 0;

            using var reader = await command.ExecuteReaderAsync();
            while (reader.Read())
            {
                rowCount++;
            }

            var duration = DateTime.UtcNow - startTime;
            Console.WriteLine($"   Query completed: {rowCount} rows in {duration.TotalMilliseconds:F0}ms");
        }
    }

    private static async Task Example2_FireAndForget()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("   Fire-and-forget pattern for very long queries...");
        Console.WriteLine();

        // Create a test table
        var tableName = "example_fire_and_forget";
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                data String
            )
            ENGINE = MergeTree()
            ORDER BY id
        ");

        // Generate a unique query ID
        var queryId = $"long-query-{Guid.NewGuid()}";
        Console.WriteLine($"   Query ID: {queryId}");

        // Start a long-running query (in practice, this would be much longer)
        // In a real scenario, you would:
        // 1. Start the query with a specific Query ID
        // 2. Immediately cancel the HTTP request (or let it timeout)
        // 3. The query continues running on the server
        // 4. Periodically poll system.query_log to check status

        Console.WriteLine("\n   Starting the query...");
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                INSERT INTO {tableName} (id, data)
                SELECT number, toString(number)
                FROM numbers(10000)
            ";
            command.QueryId = queryId;

            await command.ExecuteNonQueryAsync();
            Console.WriteLine("   Query submitted successfully");
        }

        // Wait a moment for the query to be logged
        await Task.Delay(500);

        // Poll system.query_log to check query status
        Console.WriteLine("\n   Checking query status in system.query_log...");
        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
                SELECT
                    query_id,
                    type,
                    query_duration_ms,
                    read_rows,
                    written_rows,
                    result_rows
                FROM system.query_log
                WHERE query_id = {queryId:String}
                ORDER BY event_time DESC
                LIMIT 2
            ";
            command.AddParameter("queryId", queryId);

            try
            {
                using var reader = await command.ExecuteReaderAsync();
                if (reader.Read())
                {
                    Console.WriteLine("   Query status from system.query_log:");
                    Console.WriteLine($"     Query ID: {reader.GetString(0)}");
                    Console.WriteLine($"     Status: {reader.GetString(1)}");
                    Console.WriteLine($"     Duration: {reader.GetFieldValue<ulong>(2)} ms");
                    Console.WriteLine($"     Rows read: {reader.GetFieldValue<ulong>(3)}");
                    Console.WriteLine($"     Rows written: {reader.GetFieldValue<ulong>(4)}");
                    Console.WriteLine($"     Result rows: {reader.GetFieldValue<ulong>(5)}");
                }
                else
                {
                    Console.WriteLine("   (Query not yet in system.query_log)");
                }
            }
            catch (ClickHouseServerException ex) when (ex.ErrorCode == 60)
            {
                Console.WriteLine("   (system.query_log not available)");
            }
        }

        Console.WriteLine();

        // Clean up
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }
}
