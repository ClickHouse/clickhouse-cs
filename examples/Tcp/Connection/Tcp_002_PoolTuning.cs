using System.Diagnostics;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Configures pool size, checkout timeout, lifetime, and reuse policy.</summary>
public static class TcpPoolTuning
{
    public static async Task Run()
    {
        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions() with
        {
            MinPoolSize = 0,
            MaxPoolSize = 2,
            PoolTimeout = TimeSpan.FromMilliseconds(250),
            IdleTimeout = TimeSpan.FromMinutes(2),
            MaxConnectionLifetime = TimeSpan.FromMinutes(30),
            PoolReusePolicy = ClickHouseTcpPoolReusePolicy.Lifo,
        };

        await using var client = new ClickHouseTcpClient(options);

        Console.WriteLine(
            $"Pool: min={options.MinPoolSize}, max={options.MaxPoolSize}, " +
            $"checkout timeout={options.PoolTimeout}");

        var stopwatch = Stopwatch.StartNew();
        Task[] queries = Enumerable.Range(0, 4)
            .Select(_ => client.ExecuteScalarAsync("SELECT sleep(0.15)").AsTask())
            .ToArray();
        await Task.WhenAll(queries);
        Console.WriteLine($"Four queries through a two-connection pool: {stopwatch.ElapsedMilliseconds} ms");

        await using IClickHouseTcpSession first = await client.OpenSessionAsync();
        await using IClickHouseTcpSession second = await client.OpenSessionAsync();

        // Both pool slots are pinned by sessions, so the next checkout reaches PoolTimeout.
        stopwatch.Restart();
        try
        {
            await client.PingAsync();
        }
        catch (TimeoutException ex)
        {
            Console.WriteLine(
                $"Pool checkout timed out after {stopwatch.ElapsedMilliseconds} ms: {ex.Message}");
        }
    }
}
