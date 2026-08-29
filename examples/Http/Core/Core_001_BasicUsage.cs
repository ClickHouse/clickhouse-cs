using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// A simple example demonstrating the basic usage of the ClickHouse C# driver.
/// This example shows how to:
/// - Create a table
/// - Insert data
/// - Query data
///
/// <para>
/// <strong>Two APIs are available:</strong>
/// <list type="bullet">
/// <item><see cref="ClickHouseClient"/> - Recommended for new code. Thread-safe, singleton-friendly.</item>
/// <item><see cref="ClickHouseConnection"/> - For ADO.NET compatibility (Dapper, EF Core, etc.).</item>
/// </list>
/// </para>
/// </summary>
public static class BasicUsage
{
    public static async Task Run()
    {
        Console.WriteLine("=== Using ClickHouseClient (recommended) ===\n");
        await UsingClickHouseClient();

        Console.WriteLine("\n=== Using ClickHouseConnection (ADO.NET) ===\n");
        await UsingClickHouseConnection();
    }

    private static async Task UsingClickHouseClient()
    {
        // ClickHouseClient is thread-safe and designed for singleton usage
        var settings = new ClickHouseClientSettings("Host=localhost");
        using var client = new ClickHouseClient(settings);

        var version = await client.ExecuteScalarAsync("SELECT version()");
        Console.WriteLine($"Connected to ClickHouse version: {version}");

        // Create a table
        var tableName = "example_basic_client";
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                name String,
                timestamp DateTime
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");
        Console.WriteLine($"Table '{tableName}' created");

        // Insert data using bulk insert
        var rows = new List<object[]>
        {
            new object[] { 1UL, "Alice", DateTime.UtcNow },
            new object[] { 2UL, "Bob", DateTime.UtcNow },
        };
        await client.InsertBinaryAsync(tableName, new[] { "id", "name", "timestamp" }, rows);
        Console.WriteLine("Data inserted");

        // Query data
        using (var reader = await client.ExecuteReaderAsync($"SELECT * FROM {tableName} ORDER BY id"))
        {
            Console.WriteLine("\nID\tName\tTimestamp");
            Console.WriteLine("--\t----\t---------");
            while (reader.Read())
            {
                Console.WriteLine($"{reader.GetFieldValue<ulong>(0)}\t{reader.GetString(1)}\t{reader.GetDateTime(2):yyyy-MM-dd HH:mm:ss}");
            }
        }

        // Clean up
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"Table '{tableName}' dropped");
    }

    private static async Task UsingClickHouseConnection()
    {
        // ClickHouseConnection provides ADO.NET compatibility for Dapper, EF Core, etc.
        var settings = new ClickHouseClientSettings("Host=localhost");
        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        Console.WriteLine($"Connection state: {connection.State}");

        // Create a table
        var tableName = "example_basic_ado";
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                name String,
                timestamp DateTime
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");
        Console.WriteLine($"Table '{tableName}' created");

        // Insert data using parameterized query
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $"INSERT INTO {tableName} (id, name, timestamp) VALUES ({{id:UInt64}}, {{name:String}}, {{timestamp:DateTime}})";
            command.AddParameter("id", 1);
            command.AddParameter("name", "Alice");
            command.AddParameter("timestamp", DateTime.UtcNow);
            await command.ExecuteNonQueryAsync();

            command.Parameters.Clear();
            command.AddParameter("id", 2);
            command.AddParameter("name", "Bob");
            command.AddParameter("timestamp", DateTime.UtcNow);
            await command.ExecuteNonQueryAsync();
        }
        Console.WriteLine("Data inserted");

        // Query data
        using (var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} ORDER BY id"))
        {
            Console.WriteLine("\nID\tName\tTimestamp");
            Console.WriteLine("--\t----\t---------");
            while (reader.Read())
            {
                Console.WriteLine($"{reader.GetFieldValue<ulong>(0)}\t{reader.GetString(1)}\t{reader.GetDateTime(2):yyyy-MM-dd HH:mm:ss}");
            }
        }

        // Clean up
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"Table '{tableName}' dropped");
    }
}
