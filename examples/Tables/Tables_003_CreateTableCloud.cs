using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to create tables on ClickHouse Cloud.
/// ClickHouse Cloud automatically handles replication, so ENGINE and ON CLUSTER
/// clauses can be omitted - the service uses ReplicatedMergeTree by default.
/// </summary>
public static class CreateTableCloud
{
    public static async Task Run()
    {
        Console.WriteLine("Creating tables on ClickHouse Cloud\n");

        // In a real scenario, get these from environment variables or config
        var cloudHost = Environment.GetEnvironmentVariable("CLICKHOUSE_CLOUD_HOST");
        var cloudPassword = Environment.GetEnvironmentVariable("CLICKHOUSE_CLOUD_PASSWORD");

        // Connect to ClickHouse Cloud
        var connectionString = $"Host={cloudHost};Port=8443;Protocol=https;Username=default;Password={cloudPassword}";
        using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        Console.WriteLine($"Connected to ClickHouse Cloud: {cloudHost}\n");

        Console.WriteLine("Creating a simple table (Cloud handles replication):");
        var tableName1 = "example_cloud_simple";

        using (var command = connection.CreateCommand())
        {
            // Note: No ENGINE clause needed - Cloud defaults to ReplicatedMergeTree
            // No ON CLUSTER needed - Cloud handles distribution automatically
            command.CommandText = $@"
                CREATE TABLE IF NOT EXISTS {tableName1}
                (
                    id UInt64,
                    name String,
                    created_at DateTime DEFAULT now()
                )
                ORDER BY (id)
            ";

            command.CustomSettings.Add("wait_end_of_query", "1");

            await command.ExecuteNonQueryAsync();
        }

        Console.WriteLine($"   Table '{tableName1}' created\n");
    }
}
