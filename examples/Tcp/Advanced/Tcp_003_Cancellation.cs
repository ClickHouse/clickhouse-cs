using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Cancels native-protocol operations with a CancellationToken.</summary>
public static class TcpCancellation
{
    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();
        using var cancellation = new CancellationTokenSource();

        int rows = 0;
        try
        {
            await foreach (object[] _ in client.QueryAsync(
                "SELECT number, sleepEachRow(0.05) FROM numbers(40) SETTINGS max_block_size = 1",
                cancellationToken: cancellation.Token))
            {
                rows++;
                if (rows == 3)
                {
                    cancellation.Cancel();
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"Cancelled after {rows} rows.");
        }

        // Cancellation abandons the response, so its connection is not returned to the pool.
        // The client remains usable and opens or reuses another connection.
        object value = await client.ExecuteScalarAsync("SELECT 'still usable'");
        Console.WriteLine(value);

        using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(150));
        try
        {
            await client.ExecuteAsync(
                "SELECT sleep(1)",
                cancellationToken: deadline.Token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("ExecuteAsync reached its operation deadline.");
        }
    }
}
