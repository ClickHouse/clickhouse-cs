using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to create tables on a single-node ClickHouse deployment.
/// Shows various table engines and column types.
/// </summary>
public static class CreateTableSingleNode
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("Creating tables on a single ClickHouse node\n");

        // Example 1: Simple MergeTree table
        Console.WriteLine("1. Creating a simple MergeTree table:");
        var tableName1 = "example_simple_mergetree";
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName1}
            (
                id UInt64,
                name String,
                created_at DateTime
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");
        Console.WriteLine($"   Table '{tableName1}' created\n");

        // Example 2: MergeTree with partition by
        Console.WriteLine("2. Creating a MergeTree table with partitioning:");
        var tableName2 = "example_partitioned_mergetree";
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName2}
            (
                event_date Date,
                user_id UInt64,
                event_type String,
                value Float64
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (event_date, user_id)
        ");
        Console.WriteLine($"   Table '{tableName2}' created with monthly partitions\n");
        
        // Example 3: Table with default values
        Console.WriteLine("3. Creating a table with default values:");
        var tableName3 = "example_with_defaults";
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName3}
            (
                id UInt64,
                name String,
                created_at DateTime DEFAULT now(),
                status String DEFAULT 'active'
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");
        Console.WriteLine($"   Table '{tableName3}' created with default values\n");

        // Example 4: Memory engine table (for temporary data)
        Console.WriteLine("4. Creating a Memory engine table:");
        var tableName4 = "example_memory";
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName4}
            (
                id UInt64,
                value String
            )
            ENGINE = Memory
        ");
        Console.WriteLine($"   Table '{tableName4}' created with Memory engine\n");

        // Verify tables were created
        Console.WriteLine("Verifying created tables:");
        using (var reader = await connection.ExecuteReaderAsync(
            "SELECT name, engine FROM system.tables WHERE name LIKE 'example_%' AND database = currentDatabase() ORDER BY name"))
        {
            Console.WriteLine("Table Name\t\t\t\t\tEngine");
            Console.WriteLine("----------\t\t\t\t------");
            while (reader.Read())
            {
                var name = reader.GetString(0);
                var engine = reader.GetString(1);
                Console.WriteLine($"{name,-40}\t{engine}");
            }
        }

        // Clean up - drop all created tables
        Console.WriteLine("\nCleaning up example tables...");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName1}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName2}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName3}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName4}");
        Console.WriteLine("All example tables dropped");
    }
}
