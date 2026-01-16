using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to use ClickHouse async inserts via CustomSettings.
///
/// Async inserts shift batching responsibility from the client to the server. Instead of
/// requiring client-side batching, the server buffers incoming data and
/// flushes it to storage based on configurable thresholds. This reduces part creation
/// overhead and keeps ingestion efficient under high concurrency.
///
/// When to use async inserts:
///   - Observability workloads with many agents sending small payloads (logs, metrics, traces)
///   - High-concurrency scenarios where client-side batching isn't feasible
///   - When you want to avoid "too many parts" errors from many small synchronous inserts
///
/// Two modes (controlled by wait_for_async_insert):
///   1. wait_for_async_insert=1 (RECOMMENDED): Insert returns only after data is flushed
///   to disk. Provides strong durability guarantees and straightforward error handling.
///   Errors during flush are returned to the client.
///   2. wait_for_async_insert=0 (fire-and-forget): Insert returns as soon as data is
///   buffered. Ultra-low-latency but NO guarantee data will be persisted. Errors only surface
///   during flush and are difficult to trace. Use only if your workload can tolerate data loss.
///
/// Important caveats:
///   - Data cannot be queried until a flush occurs
///   - Insert validation and schema parsing happen only during buffer flush, so type errors
///     surface at that point
///   - Automatic deduplication is DISABLED for async inserts by default (unlike sync inserts)
///   - With wait_for_async_insert=0, the client won't know about errors and can overload the
///     server since there's no backpressure
///
/// Key settings:
///   - async_insert: Enable async insert mode (1=on, 0=off)
///   - wait_for_async_insert: Wait for flush acknowledgment (1) or return immediately (0)
///   - async_insert_max_data_size: Flush when buffer reaches this size in bytes
///   - async_insert_busy_timeout_ms: Flush after this timeout in milliseconds
///   - async_insert_max_query_number: Flush after this many insert queries accumulate
///
/// See: https://clickhouse.com/docs/optimize/asynchronous-inserts
/// </summary>
public static class AsyncInsert
{
    public static async Task Run()
    {
        Console.WriteLine("Async Insert Examples");
        Console.WriteLine("=====================\n");

        // Example 1: Async insert WITH waiting for acknowledgment (RECOMMENDED)
        Console.WriteLine("1. Async insert with wait_for_async_insert=1 (RECOMMENDED):");
        Console.WriteLine("   Insert returns only after data is flushed to disk.\n");
        await Example1_AsyncInsertWithWait();

        Console.WriteLine();

        // Example 2: Async insert WITHOUT waiting - fire and forget with monitoring
        Console.WriteLine("2. Async insert with wait_for_async_insert=0 (fire-and-forget):");
        Console.WriteLine("   Insert returns immediately - NO guarantee data will be persisted!");
        Console.WriteLine("   Use only if your workload can tolerate data loss.\n");
        await Example2_AsyncInsertWithoutWait();

        Console.WriteLine("\nAsync insert examples completed!");
    }

    /// <summary>
    /// Demonstrates async inserts with wait_for_async_insert=1 (RECOMMENDED).
    /// </summary>
    private static async Task Example1_AsyncInsertWithWait()
    {
        // Configure async inserts at connection level.
        // You can also set these directly in the connection string using the "set_" prefix:
        //
        //   "Host=localhost;set_async_insert=1;set_wait_for_async_insert=1;set_async_insert_busy_timeout_ms=1000"
        //
        var settings = new ClickHouseClientSettings("Host=localhost");
        settings.CustomSettings["async_insert"] = 1;
        settings.CustomSettings["wait_for_async_insert"] = 1;
        settings.CustomSettings["async_insert_max_data_size"] = 1_000_000;
        settings.CustomSettings["async_insert_busy_timeout_ms"] = 1000;

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var tableName = "example_async_insert_wait";

        // Create table
        await connection.ExecuteStatementAsync($@"
            CREATE OR REPLACE TABLE {tableName}
            (id Int32, data String)
            ENGINE MergeTree
            ORDER BY id
        ");

        Console.WriteLine($"   Created table '{tableName}'");

        // Simulate multiple concurrent insert requests
        // Each insert is small, but server batches them together
        const int concurrentInserts = 10;
        const int rowsPerInsert = 10;
        var random = new Random(42);

        var tasks = Enumerable.Range(0, concurrentInserts).Select(async batchIndex =>
        {
            using var command = connection.CreateCommand();

            // Build VALUES clause for batch insert
            var values = string.Join(",\n",
                Enumerable.Range(0, rowsPerInsert).Select(_ =>
                {
                    var id = random.Next(1, 100_000);
                    var data = Guid.NewGuid().ToString("N")[..8];
                    return $"({id}, '{data}')";
                }));

            command.CommandText = $"INSERT INTO {tableName} (id, data) VALUES {values}";

            try
            {
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"   Insert batch {batchIndex} failed: {ex.Message}");
            }
        });

        await Task.WhenAll(tasks);
        
        Console.WriteLine($"   {concurrentInserts} concurrent inserts ({rowsPerInsert} rows each) completed");

        // Verify data
        var count = await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"   Total rows in table: {count} (expected: {concurrentInserts * rowsPerInsert})");

        // Cleanup
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Demonstrates async inserts with wait_for_async_insert=0 (fire-and-forget).
    ///
    /// Inserts return immediately after server accepts the request into its buffer.
    /// A background loop monitors how many rows are actually written to storage.
    ///
    /// WARNING: This mode has significant risks:
    /// - NO guarantee data will be persisted (data loss possible)
    /// - Errors only surface during flush and are difficult to trace
    /// - No backpressure - client can overload the server
    /// - Type/schema errors only detected at flush time
    ///
    /// Use only for high-velocity, low-criticality data where you can tolerate loss.
    /// </summary>
    private static async Task Example2_AsyncInsertWithoutWait()
    {
        // Configure async inserts WITHOUT waiting
        var settings = new ClickHouseClientSettings("Host=localhost");
        settings.CustomSettings["async_insert"] = 1;
        settings.CustomSettings["wait_for_async_insert"] = 0; // Fire and forget
        settings.CustomSettings["async_insert_max_data_size"] = 1_000_000;
        settings.CustomSettings["async_insert_busy_timeout_ms"] = 1000;

        using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var tableName = "example_async_insert_nowait";

        // Create table
        await connection.ExecuteStatementAsync($@"
            CREATE OR REPLACE TABLE {tableName}
            (id Int32, name String)
            ENGINE MergeTree
            ORDER BY id
        ");

        Console.WriteLine($"   Created table '{tableName}'");

        // Track rows sent vs written
        var rowsSent = 0;
        var insertCount = 0;
        var random = new Random(42);

        // Cancellation for the monitoring loop
        using var cts = new CancellationTokenSource();

        // Background task: periodically send random batches
        var insertTask = Task.Run(async () =>
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            while (stopwatch.ElapsedMilliseconds < 5000 && !cts.Token.IsCancellationRequested)
            {
                var batchSize = random.Next(10, 100);
                var startId = rowsSent;

                using var command = connection.CreateCommand();
                var values = string.Join(",\n",
                    Enumerable.Range(0, batchSize).Select(i =>
                        $"({startId + i}, 'Name {startId + i}')"));

                command.CommandText = $"INSERT INTO {tableName} (id, name) VALUES {values}";

                try
                {
                    await command.ExecuteNonQueryAsync();

                    Interlocked.Add(ref rowsSent, batchSize);
                    Interlocked.Increment(ref insertCount);
                    
                    Console.WriteLine($"   Insert #{insertCount}: {batchSize} rows sent");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"   Insert failed: {ex.Message}");
                }

                // Delay between inserts
                await Task.Delay(100, cts.Token);
            }
        }, cts.Token);

        // Background task: periodically check how many rows are actually written
        var monitorTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                await Task.Delay(1000, cts.Token);

                try
                {
                    var written = await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
                    Console.WriteLine($"   >> Status: {rowsSent} rows sent, {written} rows written to table");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"   Monitor query failed: {ex.Message}");
                }
            }
        }, cts.Token);

        // Wait for insert task to complete
        await insertTask;

        // Give async inserts time to flush (wait for busy_timeout)
        Console.WriteLine("   Waiting for async insert flush...");
        await Task.Delay(1500);

        // Stop monitoring
        await cts.CancelAsync();
        try { await monitorTask; } catch (OperationCanceledException) { }

        // Final count
        var finalCount = await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"\n   Final: {rowsSent} rows sent, {finalCount} rows written");

        // Cleanup
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }
}
