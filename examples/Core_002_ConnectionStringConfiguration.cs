using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Examples of different ways to configure a connection to ClickHouse.
/// Shows various connection string formats and options.
/// </summary>
public static class ConnectionStringConfiguration
{
    public static async Task Run()
    {
        // 1: Connection string with named parameters
        Console.WriteLine("1. Connection string with named parameters:");
        using (var connection = new ClickHouseConnection(
            "Host=localhost;Port=8123;Protocol=http;Username=default;Password=;Database=default"))
        {
            await connection.OpenAsync();
            var version = await connection.ExecuteScalarAsync("SELECT version()");
            Console.WriteLine($"   Connected to ClickHouse version: {version}");
        }

        // 2: Using ClickHouseConnectionStringBuilder
        Console.WriteLine("\n2. Using ClickHouseConnectionStringBuilder:");
        var builder = new ClickHouseConnectionStringBuilder
        {
            Host = "localhost",
            Port = 8123,
            Username = "default",
            Password = "",
            Database = "default",
            Protocol = "http"
        };
        using (var connection = new ClickHouseConnection(builder.ToString()))
        {
            await connection.OpenAsync();
            var version = await connection.ExecuteScalarAsync("SELECT version()");
            Console.WriteLine($"   Connected to ClickHouse version: {version}");
        }
        
        // 3: HTTPS connection (for ClickHouse Cloud or secure deployments)
        Console.WriteLine("\n3. HTTPS connection configuration:");
        var secureBuilder = new ClickHouseConnectionStringBuilder
        {
            Host = "your-clickhouse-instance.cloud",
            Port = 8443,
            Protocol = "https",
            Username = "default",
            Password = "your_password",
            Database = "default"
        };
        Console.WriteLine($"   Connection string: {secureBuilder}");
        
        // 4: Connection with custom settings
        Console.WriteLine("\n4. Connection with custom ClickHouse settings:");
        using (var connection = new ClickHouseConnection("Host=localhost"))
        {
            // Add custom settings for this connection
            connection.CustomSettings.Add("max_execution_time", 10);
            connection.CustomSettings.Add("max_memory_usage", 10000000000);
            
            await connection.OpenAsync();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            await command.ExecuteScalarAsync();
            Console.WriteLine("   Executed query with custom ClickHouse settings");
        }

        Console.WriteLine("\nAll connection examples completed successfully!");
    }
}
