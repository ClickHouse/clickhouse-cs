using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Connects to ClickHouse, creates a table, inserts rows, and reads them back.</summary>
public static class TcpBasicUsage
{
    private const string TableName = "example_tcp_basic_usage";

    public static async Task Run()
    {
        // Reuse one client in your application. It is thread-safe and owns a connection pool.
        await using var client = ExampleConfig.CreateTcpClient();

        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();
        Console.WriteLine($"Connected to {server} through {ExampleConfig.TcpEndpoint}");

        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");

        try
        {
            await client.ExecuteAsync($"""
                CREATE TABLE {TableName}
                (
                    id UInt64,
                    name String,
                    score Float64
                )
                ENGINE = MergeTree
                ORDER BY id
                """);

            var rows = new List<object[]>
            {
                new object[] { 1UL, "Ada", 99.5 },
                new object[] { 2UL, "Grace", 97.25 },
                new object[] { 3UL, "Alan", 91.0 },
            };

            // End the statement at VALUES. The rows are encoded as native columnar blocks.
            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id, name, score) VALUES",
                rows);

            await foreach (object[] row in client.QueryAsync(
                $"SELECT id, name, score FROM {TableName} ORDER BY id"))
            {
                Console.WriteLine($"{row[0]}: {row[1]} ({row[2]})");
            }

            object count = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
            Console.WriteLine($"Row count: {count}");
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }
}
