using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// A simple example demonstrating the basic usage of the ClickHouse C# driver.
/// This example shows how to:
/// - Create a connection to ClickHouse
/// - Create a table
/// - Insert data
/// - Query data
/// </summary>
public static class BasicUsage
{
    public static async Task Run()
    {
        // Create a connection to ClickHouse using ClickHouseClientSettings
        // By default, connects to localhost:8123 with user 'default' and no password
        var settings = new ClickHouseClientSettings("Host=localhost;Port=8123;Protocol=http;Username=default;Password=;Database=default");
        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        Console.WriteLine($"Connection state: {connection.State}");

        // Create a table
        var tableName = "example_basic_usage";
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

        // Insert data using a parameterized query
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
            Console.WriteLine("\nQuerying data from table:");
            Console.WriteLine("ID\tName\tTimestamp");
            Console.WriteLine("--\t----\t---------");

            while (reader.Read())
            {
                var id = reader.GetFieldValue<UInt64>(0);
                var name = reader.GetString(1);
                var timestamp = reader.GetDateTime(2);

                Console.WriteLine($"{id}\t{name}\t{timestamp:yyyy-MM-dd HH:mm:ss}");
            }
        }

        // Clean up
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nTable '{tableName}' dropped");
    }
}
