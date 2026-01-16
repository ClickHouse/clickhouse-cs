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

        // 2: Using ClickHouseClientSettings
        Console.WriteLine("\n2. Using ClickHouseClientSettings:");
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Port = 8123,
            Username = "default",
            Password = "",
            Database = "default",
            Protocol = "http",
        };
        using (var connection = new ClickHouseConnection(settings))
        {
            await connection.OpenAsync();
            var version = await connection.ExecuteScalarAsync("SELECT version()");
            Console.WriteLine($"   Connected to ClickHouse version: {version}");
        }

        // 3: HTTPS connection (for ClickHouse Cloud or secure deployments)
        Console.WriteLine("\n3. HTTPS connection configuration:");
        var secureSettings = new ClickHouseClientSettings
        {
            Host = "your-clickhouse-instance.cloud",
            Port = 8443,
            Protocol = "https",
            Username = "default",
            Password = "your_password",
            Database = "default",
        };
        Console.WriteLine($"   Settings: Host={secureSettings.Host}, Port={secureSettings.Port}, Protocol={secureSettings.Protocol}");
        

        // 4: Connection with custom settings
        Console.WriteLine("\n4. Connection with custom ClickHouse settings:");
        var settingsWithCustom = new ClickHouseClientSettings("Host=localhost");
        settingsWithCustom.CustomSettings.Add("max_execution_time", 10);
        settingsWithCustom.CustomSettings.Add("max_memory_usage", 10000000000);
        
        using (var connection = new ClickHouseConnection(settingsWithCustom))
        {
            await connection.OpenAsync();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            await command.ExecuteScalarAsync();
            Console.WriteLine("   Executed query with custom ClickHouse settings");
        }

        Console.WriteLine("\nAll connection examples completed successfully!");
    }
}
