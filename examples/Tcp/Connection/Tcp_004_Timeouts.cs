using System.Diagnostics;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Configures connection, pool, read, and operation timeouts.</summary>
public static class TcpTimeouts
{
    public static async Task Run()
    {
        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions() with
        {
            DialTimeout = TimeSpan.FromSeconds(5),
            PoolTimeout = TimeSpan.FromSeconds(2),
            ReadTimeout = TimeSpan.FromMilliseconds(150),
        };

        Console.WriteLine($"Dial timeout: {options.DialTimeout}");
        Console.WriteLine($"Pool timeout: {options.PoolTimeout}");
        Console.WriteLine($"Read timeout: {options.ReadTimeout}");

        await using var client = new ClickHouseTcpClient(options);

        // ReadTimeout limits server silence, not total query duration.
        var stopwatch = Stopwatch.StartNew();
        int rows = 0;
        await foreach (object[] _ in client.QueryAsync(
            "SELECT number, sleepEachRow(0.05) FROM numbers(6) SETTINGS max_block_size = 1"))
        {
            rows++;
        }

        Console.WriteLine($"Read {rows} active rows in {stopwatch.ElapsedMilliseconds} ms.");

        try
        {
            await client.ExecuteScalarAsync("SELECT sleep(0.4)");
        }
        catch (TimeoutException ex)
        {
            Console.WriteLine($"Read timeout during server silence: {ex.Message}");
        }

        // A cancellation token is the deadline for the complete operation.
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(150));
        try
        {
            await client.ExecuteScalarAsync("SELECT sleep(1)", cancellationToken: cancellation.Token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("The operation-wide cancellation deadline expired.");
        }
    }
}
