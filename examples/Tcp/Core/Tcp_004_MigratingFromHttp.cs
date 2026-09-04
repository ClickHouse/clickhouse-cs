using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Shows the native-protocol equivalents of common HTTP client operations.</summary>
public static class TcpMigratingFromHttp
{
    private const string TableName = "example_tcp_migrating_from_http";

    public static async Task Run()
    {
        using var http = ExampleConfig.CreateHttpClient();
        await using var tcp = ExampleConfig.CreateTcpClient();

        await http.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {TableName}");

        try
        {
            await http.ExecuteNonQueryAsync($"""
                CREATE TABLE {TableName} (id UInt64, name String, source String)
                ENGINE = MergeTree
                ORDER BY id
                """);

            await http.InsertBinaryAsync(
                TableName,
                new[] { "id", "name", "source" },
                new[] { new object[] { 1UL, "Ada", "HTTP" } });

            await tcp.InsertRowsAsync(
                $"INSERT INTO {TableName} (id, name, source) VALUES",
                new[] { new object[] { 2UL, "Grace", "TCP" } });

            Console.WriteLine("HTTP: ExecuteReaderAsync returns a DbDataReader");
            using (var reader = await http.ExecuteReaderAsync(
                       $"SELECT id, name, source FROM {TableName} ORDER BY id"))
            {
                while (reader.Read())
                {
                    Console.WriteLine($"  {reader.GetFieldValue<ulong>(0)}: " +
                                      $"{reader.GetString(1)} ({reader.GetString(2)})");
                }
            }

            Console.WriteLine("TCP: QueryAsync streams object[] rows");
            await foreach (object[] row in tcp.QueryAsync(
                               $"SELECT id, name, source FROM {TableName} ORDER BY id"))
            {
                Console.WriteLine($"  {row[0]}: {row[1]} ({row[2]})");
            }

            object httpCount = await http.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
            object tcpCount = await tcp.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
            Console.WriteLine($"HTTP count: {httpCount}; TCP count: {tcpCount}");
        }
        finally
        {
            await tcp.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }

        Console.WriteLine("Use the HTTP client for ADO.NET, ORMs, and non-Native formats.");
        Console.WriteLine("Use the TCP client for blocks, sessions, and live progress callbacks.");
    }
}
