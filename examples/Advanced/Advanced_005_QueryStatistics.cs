using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to access and use query statistics returned by ClickHouse.
/// Query statistics provide valuable information about query execution:
/// - Rows read/written
/// - Bytes processed
/// - Execution time
/// - Result set size
///
/// This information is useful for:
/// - Query performance monitoring
/// - Resource usage tracking
/// - Optimization decisions
/// - Debugging slow queries
/// </summary>
public static class QueryStatistics
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("Query Statistics Examples\n");

        Console.WriteLine("   Executing a simple query and reading statistics...\n");

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT number FROM numbers(100000) WHERE number % 2 = 0";

        // Execute the query
        using var reader = await command.ExecuteReaderAsync();
        var rowCount = 0;
        while (reader.Read())
        {
            rowCount++;
        }

        Console.WriteLine($"   Query returned {rowCount} rows\n");

        // Access query statistics from the command
        // QueryStats is populated after the query executes
        var stats = command.QueryStats;
        if (stats != null)
        {
            Console.WriteLine("   Query Statistics:");
            Console.WriteLine($"     Rows Read: {stats.ReadRows:N0}");
            Console.WriteLine($"     Bytes Read: {stats.ReadBytes:N0}");
            Console.WriteLine($"     Rows Written: {stats.WrittenRows:N0}");
            Console.WriteLine($"     Bytes Written: {stats.WrittenBytes:N0}");
            Console.WriteLine($"     Total Rows to Read: {stats.TotalRowsToRead:N0}");
            Console.WriteLine($"     Result Rows: {stats.ResultRows:N0}");
            Console.WriteLine($"     Result Bytes: {stats.ResultBytes:N0}");
            Console.WriteLine($"     Elapsed Time: {stats.ElapsedNs / 1_000_000_000.0:F3} seconds");
        }
        else
        {
            Console.WriteLine("   (Query statistics not available in response)");
        }

        Console.WriteLine("\nAll query statistics examples completed!");
    }

}
