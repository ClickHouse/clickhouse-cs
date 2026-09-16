using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Handles server, transport, and protocol errors and retries a read that failed to connect.</summary>
public static class TcpErrorsAndRetries
{
    private const string TableName = "example_tcp_retry_deduplication";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();
        string missingTable = $"example_tcp_missing_{Guid.NewGuid():N}";

        try
        {
            await client.ExecuteScalarAsync($"SELECT * FROM {missingTable}");
        }
        catch (ClickHouseTcpServerException ex)
        {
            Console.WriteLine($"Server error: {ex.Code} ({ex.RawCode})");
        }

        await using var unreachable = new ClickHouseTcpClient(
            ExampleConfig.TcpBuilder().ToOptions() with
            {
                Port = 1,
                DialTimeout = TimeSpan.FromSeconds(1),
            });

        // A read is safe to retry. A failed write may already have reached the server.
        int attempts = 0;
        object result = await RetryRead(async () =>
        {
            attempts++;
            return attempts == 1
                ? await unreachable.ExecuteScalarAsync("SELECT 1")
                : await client.ExecuteScalarAsync("SELECT 1");
        });
        Console.WriteLine($"Read succeeded after {attempts} attempts: {result}");

        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        try
        {
            await client.ExecuteAsync($"""
                CREATE TABLE {TableName} (id UInt64)
                ENGINE = MergeTree
                ORDER BY id
                SETTINGS non_replicated_deduplication_window = 100
                """);

            object[][] batch = { new object[] { 1UL }, new object[] { 2UL } };
            // Reuse one token for retries of the same logical batch. The table must enable deduplication.
            var insertOptions = new ClickHouseTcpInsertOptions
            {
                DeduplicationToken = "example-logical-batch-1",
            };

            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id) VALUES",
                batch,
                insertOptions);
            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id) VALUES",
                batch,
                insertOptions);

            object count = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
            Console.WriteLine($"Rows after retrying the same deduplicated insert: {count}");
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }

    private static async Task<object> RetryRead(Func<Task<object>> operation)
    {
        const int MaxAttempts = 3;

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await operation();
            }
            // The connection failed, so the read never ran. Which failures are worth a retry is the
            // caller's policy: a server rejection of the query itself would repeat, so it is not caught.
            catch (ClickHouseTcpTransportException ex) when (attempt < MaxAttempts)
            {
                Console.WriteLine($"{ex.GetType().Name}; retrying.");
                await Task.Delay(TimeSpan.FromMilliseconds(100 * attempt));
            }
        }
    }
}
