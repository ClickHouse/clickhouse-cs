using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates using CancellationToken to cancel long-running queries.
/// This is useful for implementing timeouts, user-initiated cancellation, or graceful shutdown.
/// </summary>
public static class QueryCancellation
{
    public static async Task Run()
    {
        Console.WriteLine("Query Cancellation Examples\n");

        await CancelWithTimeout();
        await CancelManually();

        Console.WriteLine("\nAll cancellation examples completed!");
    }

    /// <summary>
    /// Demonstrates cancelling a query after a timeout using CancellationTokenSource.
    /// </summary>
    private static async Task CancelWithTimeout()
    {
        Console.WriteLine("1. Cancel query after timeout:");

        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        using var command = connection.CreateCommand();

        // This query would take 3 seconds to complete, but we'll cancel it after 1 second
        command.CommandText = "SELECT sleep(3)";

        // Create a cancellation token that will cancel after 1 second
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        Console.WriteLine("   Starting a 3-second query with 1-second timeout...");
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            await command.ExecuteNonQueryAsync(cts.Token);
            Console.WriteLine("   Query completed (unexpected)");
        }
        catch (OperationCanceledException)
        {
            stopwatch.Stop();
            Console.WriteLine($"   Query cancelled after {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine("   OperationCanceledException caught as expected");
        }
        catch (HttpRequestException ex) when (ex.InnerException is TaskCanceledException)
        {
            stopwatch.Stop();
            Console.WriteLine($"   Query cancelled after {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine("   HttpRequestException (TaskCanceledException) caught as expected");
        }
    }

    /// <summary>
    /// Demonstrates manually cancelling a query from another task.
    /// </summary>
    private static async Task CancelManually()
    {
        Console.WriteLine("\n2. Cancel query manually from another task:");

        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        using var command = connection.CreateCommand();

        // This query would take 3 seconds to complete
        command.CommandText = "SELECT sleep(3)";

        using var cts = new CancellationTokenSource();

        Console.WriteLine("   Starting a 3-second query...");
        Console.WriteLine("   A separate task will cancel it after 500ms");

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Start a task that will cancel the query after 500ms
        var cancelTask = Task.Run(async () =>
        {
            await Task.Delay(500);
            Console.WriteLine("   Requesting cancellation...");
            cts.Cancel();
        });

        try
        {
            await command.ExecuteNonQueryAsync(cts.Token);
            Console.WriteLine("   Query completed (unexpected)");
        }
        catch (OperationCanceledException)
        {
            stopwatch.Stop();
            Console.WriteLine($"   Query cancelled after {stopwatch.ElapsedMilliseconds}ms");
        }
        catch (HttpRequestException ex) when (ex.InnerException is TaskCanceledException)
        {
            stopwatch.Stop();
            Console.WriteLine($"   Query cancelled after {stopwatch.ElapsedMilliseconds}ms");
        }

        await cancelTask;
    }
}
