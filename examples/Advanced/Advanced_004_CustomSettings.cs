using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to use custom ClickHouse settings to control query behavior.
/// Settings can be applied at:
/// - Client level (applies to all queries)
/// - Query level via QueryOptions (applies to a specific query)
///
/// Common use cases:
/// - Resource limits (max_execution_time, max_memory_usage)
/// - Query optimization (max_threads, max_block_size)
/// - Output formats (FORMAT JSON, FORMAT CSV)
/// - Query profiling and statistics
/// </summary>
public static class CustomSettings
{
    public static async Task Run()
    {
        Console.WriteLine("Custom Settings Examples\n");

        // Example 1: Client-level settings
        Console.WriteLine("1. Client-level custom settings:");
        await Example1_ClientLevelSettings();

        // Example 2: Query-level settings
        Console.WriteLine("\n2. Query-level custom settings:");
        await Example2_QueryLevelSettings();

        // Example 3: Execution time limits
        Console.WriteLine("\n3. Setting execution time limits:");
        await Example3_ExecutionTimeLimits();

        Console.WriteLine("\nAll custom settings examples completed!");
    }

    private static async Task Example1_ClientLevelSettings()
    {
        // Settings applied at the client level affect all queries
        var settings = new ClickHouseClientSettings("Host=localhost");

        // Add custom ClickHouse settings
        settings.CustomSettings.Add("max_threads", 4);
        settings.CustomSettings.Add("max_block_size", 65536);

        using var client = new ClickHouseClient(settings);

        Console.WriteLine("   Client-level settings applied:");
        Console.WriteLine("     max_threads = 4");
        Console.WriteLine("     max_block_size = 65536");

        // Verify the settings are actually configured by querying system.settings
        Console.WriteLine("\n   Verifying settings from system.settings:");
        using (var reader = await client.ExecuteReaderAsync(@"
            SELECT name, value
            FROM system.settings
            WHERE name IN ('max_threads', 'max_block_size')
            ORDER BY name
        "))
        {
            while (reader.Read())
            {
                var name = reader.GetString(0);
                var value = reader.GetString(1);
                Console.WriteLine($"     {name} = {value}");
            }
        }
    }

    private static async Task Example2_QueryLevelSettings()
    {
        using var client = new ClickHouseClient("Host=localhost");

        Console.WriteLine("   Applying settings to a specific query:");

        // Query-level settings override client-level settings
        var options = new QueryOptions
        {
            CustomSettings = new Dictionary<string, object>
            {
                ["max_execution_time"] = 5,
                ["result_overflow_mode"] = "break",
            },
        };

        // Verify the settings are actually configured by querying system.settings
        Console.WriteLine("\n   Verifying settings from system.settings:");
        string sql = @"
            SELECT name, value
            FROM system.settings
            WHERE name IN ('max_execution_time', 'result_overflow_mode')
            ORDER BY name
        ";
        using (var reader = await client.ExecuteReaderAsync(sql,  options: options))
        {
            while (reader.Read())
            {
                var name = reader.GetString(0);
                var value = reader.GetString(1);
                Console.WriteLine($"     {name} = {value}");
            }
        }
    }

    private static async Task Example3_ExecutionTimeLimits()
    {
        using var client = new ClickHouseClient("Host=localhost");

        Console.WriteLine("   Setting max_execution_time to limit query duration:");

        var options = new QueryOptions
        {
            CustomSettings = new Dictionary<string, object>
            {
                ["max_execution_time"] = 1,
            },
        };

        try
        {
            Console.WriteLine("   Executing query with max_execution_time = 1 second...");
            var startTime = DateTime.UtcNow;
            var rowCount = 0;

            using var reader = await client.ExecuteReaderAsync(@"
                SELECT sleep(0.1), number
                FROM numbers(100)
            ", options: options);

            while (reader.Read())
            {
                rowCount++;
            }

            var duration = DateTime.UtcNow - startTime;
            Console.WriteLine($"   Query completed: {rowCount} rows in {duration.TotalMilliseconds:F0}ms");
        }
        catch (ClickHouseServerException ex)
        {
            Console.WriteLine($"   Query failed as expected: {ex.Message}");
            Console.WriteLine("   (max_execution_time limit was enforced)");
        }
    }
}
