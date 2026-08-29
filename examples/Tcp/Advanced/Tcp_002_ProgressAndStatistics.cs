using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Receives progress and profile callbacks while a query runs.</summary>
public static class TcpProgressAndStatistics
{
    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        ClickHouseTcpProgress total = default;
        ClickHouseTcpProfileInfo profile = default;
        int progressUpdates = 0;
        int profileEventBlocks = 0;

        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string>
            {
                ["interactive_delay"] = "30000",
                ["max_block_size"] = "1",
            },
            Callbacks = new ClickHouseTcpQueryCallbacks
            {
                OnProgress = progress =>
                {
                    // Progress values are increments, so add them to get a query-wide total.
                    total += progress;
                    progressUpdates++;
                    Console.WriteLine($"Progress: +{progress.Rows} rows");
                },
                OnProfileInfo = info => profile = info,
                OnProfileEvents = _ => profileEventBlocks++,
            },
        };

        int rows = 0;
        await foreach (object[] _ in client.QueryAsync(
            "SELECT number, sleepEachRow(0.04) FROM numbers(8)",
            options))
        {
            rows++;
        }

        Console.WriteLine($"Rows read: {rows}");
        Console.WriteLine($"Progress updates: {progressUpdates}; reported rows: {total.Rows}");
        Console.WriteLine($"Profile rows: {profile.Rows}; blocks: {profile.Blocks}");
        Console.WriteLine($"Profile event blocks: {profileEventBlocks}");

        // Callbacks run synchronously while the response is read. Keep them fast and do not throw.
    }
}
