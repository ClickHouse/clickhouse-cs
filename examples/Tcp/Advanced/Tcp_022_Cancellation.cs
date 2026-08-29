using System.Diagnostics;
using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Cancelling a native-protocol operation: what the caller sees, what the server is told, and what it costs the
/// connection pool.
///
/// <para>
/// Every method takes a <see cref="CancellationToken"/>, and it is the only bound on a whole operation — the three
/// deadlines in <c>Tcp_019_Timeouts</c> each cover one phase. Cancelling is not free, though: the client tells the
/// server the result is abandoned and then closes the connection, because a socket part-way through a response
/// nobody will read is of no use to the next caller. The client itself stays usable; its pool opens another.
/// </para>
/// </summary>
public static class TcpCancellation
{
    public static async Task Run()
    {
        await CancellingMidResult();
        await WhatTheServerWasTold();
        int abandonedAfter = await ThePoolDiscardsIt();
        WhatThePoolLinesSay(abandonedAfter);
        await ExecuteAndStream();
        TheOtherWaysAnOperationEnds();
    }

    private static async Task CancellingMidResult()
    {
        Console.WriteLine("1. Cancelling part-way through a result\n");

        await using var client = ExampleConfig.CreateTcpClient();
        using var cancellation = new CancellationTokenSource();

        int rows = 0;
        var clock = Stopwatch.StartNew();
        try
        {
            // 40 rows at 50 ms each, one row per block, so the loop body really does run between rows.
            await foreach (object[] row in client.QueryAsync(
                "SELECT number, sleepEachRow(0.05) FROM numbers(40) SETTINGS max_block_size = 1",
                cancellationToken: cancellation.Token))
            {
                rows++;
                if (rows == 3)
                {
                    cancellation.Cancel();
                }
            }

            Console.WriteLine("   The loop finished, which is not what this example expected");
        }
        catch (OperationCanceledException ex)
        {
            Console.WriteLine($"   Cancelled after {rows} of 40 rows, {clock.ElapsedMilliseconds} ms in.");
            Console.WriteLine($"   Caught {ex.GetType().Name}: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("   The runtime raises TaskCanceledException here, which derives from");
            Console.WriteLine("   OperationCanceledException — catch the base one. It is not a");
            Console.WriteLine("   ClickHouseTcpException: nothing went wrong between the client and the server,");
            Console.WriteLine("   the caller asked to stop. Tcp_023 covers the exceptions that are.");
        }

        // The same client, straight afterwards. Cancelling costs a connection, not the client.
        object still = await client.ExecuteScalarAsync("SELECT 'the client is still usable'");
        Console.WriteLine($"\n   Next operation on the same client: {still}");
    }

    private static async Task WhatTheServerWasTold()
    {
        Console.WriteLine("\n2. What the server was told\n");

        await using var client = ExampleConfig.CreateTcpClient();
        string queryId = $"example-tcp-022-{Guid.NewGuid():N}";
        using var cancellation = new CancellationTokenSource();

        try
        {
            await foreach (object[] row in client.QueryAsync(
                "SELECT number, sleepEachRow(0.05) FROM numbers(40) SETTINGS max_block_size = 1",
                new ClickHouseTcpQueryOptions { QueryId = queryId },
                cancellation.Token))
            {
                cancellation.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
        }

        // The QueryFinish/ExceptionWhileProcessing record is queued independently of the response reaching the
        // client, so the flush and the read are retried rather than delayed.
        string logged = await ReadLog(
            client,
            "SELECT type::String || '  exception_code=' || toString(exception_code) || '  ' || splitByChar('(', exception)[1] " +
            "FROM system.query_log WHERE query_id = {id:String} AND type != 'QueryStart'",
            queryId);

        Console.WriteLine($"   system.query_log for that query_id:\n     {logged}");
        Console.WriteLine();
        Console.WriteLine("   735 is QUERY_WAS_CANCELLED_BY_CLIENT. The client sent a Cancel packet before closing");
        Console.WriteLine("   the connection, so the server stopped the query rather than finishing it into a socket");
        Console.WriteLine("   nobody was reading. That is the difference between cancelling and hanging up: the");
        Console.WriteLine("   work stops, and the reason is in the log.");
    }

    /// <summary>
    /// Six operations on a one-connection pool, with the pool's own log lines: two ordinary ones, a cancelled one,
    /// an abandoned one, and an ordinary one after each. Its own method so that the logger factory is disposed —
    /// and its lines flushed to the console — before the interpretation prints.
    /// </summary>
    /// <returns>How many rows the abandoned enumeration read before breaking out.</returns>
    private static async Task<int> ThePoolDiscardsIt()
    {
        Console.WriteLine("\n3. The connection is closed, not pooled\n");
        Console.WriteLine("   MaxPoolSize = 1, and the pool's own log lines. Had a connection gone back into the");
        Console.WriteLine("   pool, the operation after it would be reusing it — the pool holds only one.\n");

        using ILoggerFactory poolLog = LoggerFactory.Create(builder => builder
            .AddFilter((category, _) => category == "ClickHouse.Driver.Tcp.Pool")
            .AddSimpleConsole(console => console.SingleLine = true)
            .SetMinimumLevel(LogLevel.Trace));

        await using var client = new ClickHouseTcpClient(ExampleConfig.TcpBuilder().ToOptions() with
        {
            MaxPoolSize = 1,
            LoggerFactory = poolLog,
        });

        // Two ordinary operations first, so that a reuse line is in the output to compare against.
        _ = await client.ExecuteScalarAsync("SELECT 1");
        _ = await client.ExecuteScalarAsync("SELECT 2");

        using var cancellation = new CancellationTokenSource();
        try
        {
            await foreach (object[] row in client.QueryAsync(
                "SELECT number, sleepEachRow(0.05) FROM numbers(40) SETTINGS max_block_size = 1",
                cancellationToken: cancellation.Token))
            {
                cancellation.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
        }

        _ = await client.ExecuteScalarAsync("SELECT 3");

        // No token this time: the loop simply stops reading, which is abandonment rather than cancellation.
        int rows = 0;
        await foreach (object[] row in client.QueryAsync(
            "SELECT number FROM numbers(10000000) SETTINGS max_block_size = 100"))
        {
            if (++rows == 5)
            {
                break;
            }
        }

        _ = await client.ExecuteScalarAsync("SELECT 4");
        return rows;
    }

    private static void WhatThePoolLinesSay(int abandonedAfter)
    {
        Console.WriteLine("\n   Read that as four pairs. SELECT 1 opened a connection and SELECT 2 reused it — 'its 2");
        Console.WriteLine("   operation'. The cancelled query got 'its 3 operation' and then ended it: 'Closing a");
        Console.WriteLine("   returned connection rather than pooling it', so SELECT 3 had to open another.");
        Console.WriteLine();
        Console.WriteLine($"   Then the same thing with no token at all: a loop that read {abandonedAfter} rows of ten million");
        Console.WriteLine("   and broke out. The same two lines follow it, so abandoning a result is treated exactly");
        Console.WriteLine("   as cancelling one — the connection is closed, and SELECT 4 opened a fresh one.");
        Console.WriteLine();
        Console.WriteLine("   So a cancellation costs a dial, and a loop that cancels every query keeps the pool");
        Console.WriteLine("   empty. It does not cost the client: every operation after one of these succeeded.");
        Console.WriteLine();
        Console.WriteLine("   `break` inside an `await foreach` disposes the enumerator, which is what sends the");
        Console.WriteLine("   Cancel packet and returns the connection. So does `return`, and so does an exception");
        Console.WriteLine("   thrown from the loop body.");
        Console.WriteLine();
        Console.WriteLine("   The one shape that does not is a hand-rolled enumerator that is never disposed:");
        Console.WriteLine("     var e = client.QueryAsync(sql).GetAsyncEnumerator();   // no await using");
        Console.WriteLine("   Its connection is neither returned nor closed, and nothing reclaims it — there is no");
        Console.WriteLine("   finalizer to free the pool slot, so it is gone for as long as the client lives. Use");
        Console.WriteLine("   `await foreach`, or `await using` on the enumerator.");
    }

    private static async Task ExecuteAndStream()
    {
        Console.WriteLine("\n4. The same token on ExecuteAsync and StreamAsync\n");

        await using var client = ExampleConfig.CreateTcpClient();

        // ExecuteAsync drains the whole response before returning, so there is no loop to break out of and the
        // token is the only way to stop waiting.
        using (var deadline = new CancellationTokenSource(150))
        {
            var clock = Stopwatch.StartNew();
            try
            {
                await client.ExecuteAsync("SELECT sleepEachRow(0.2) FROM numbers(5)", cancellationToken: deadline.Token);
                Console.WriteLine("   ExecuteAsync returned, which is not what this example expected");
            }
            catch (OperationCanceledException ex)
            {
                Console.WriteLine($"   ExecuteAsync, token cancelled after 150 ms: {ex.GetType().Name} at {clock.ElapsedMilliseconds} ms");
            }
        }

        // StreamAsync is the same contract one level down: the block being iterated is released, the enumerator
        // is disposed by the loop, and the connection is closed.
        using (var cancellation = new CancellationTokenSource())
        {
            int blocks = 0;
            try
            {
                await foreach (Block block in client.StreamAsync(
                    "SELECT number, sleepEachRow(0.05) FROM numbers(40) SETTINGS max_block_size = 4",
                    cancellationToken: cancellation.Token))
                {
                    blocks++;
                    cancellation.Cancel();
                }
            }
            catch (OperationCanceledException ex)
            {
                Console.WriteLine($"   StreamAsync, cancelled after {blocks} block: {ex.GetType().Name}");
            }
        }

        Console.WriteLine($"   And afterwards: {await client.ExecuteScalarAsync("SELECT 'still usable'")}");
        Console.WriteLine();
        Console.WriteLine("   A token already cancelled when the call is made throws before the pool is touched, so");
        Console.WriteLine("   nothing is dialled and nothing is closed — the pool's log stays silent, and the next");
        Console.WriteLine("   operation reuses whatever was idle:");

        using (var alreadyDone = new CancellationTokenSource())
        {
            await alreadyDone.CancelAsync();
            try
            {
                _ = await client.ExecuteScalarAsync("SELECT 1", cancellationToken: alreadyDone.Token);
                Console.WriteLine("     it ran anyway, which is not what this example expected");
            }
            catch (OperationCanceledException ex)
            {
                Console.WriteLine($"     {ex.GetType().Name} straight away");
            }
        }
    }

    private static void TheOtherWaysAnOperationEnds()
    {
        Console.WriteLine("\n5. Three ways to stop a query, and which one to reach for\n");
        Console.WriteLine("   CancellationToken       The caller changed its mind: a request was abandoned, a");
        Console.WriteLine("                           timeout of your own elapsed, the process is shutting down.");
        Console.WriteLine("                           Throws OperationCanceledException, costs the connection,");
        Console.WriteLine("                           and the server is told (section 2).");
        Console.WriteLine();
        Console.WriteLine("   ReadTimeout             The server went quiet. An idle deadline, not a time limit —");
        Console.WriteLine("                           Tcp_019 measures it. Throws TimeoutException and also costs");
        Console.WriteLine("                           the connection, because a socket that stopped answering");
        Console.WriteLine("                           mid-response cannot be reused either.");
        Console.WriteLine();
        Console.WriteLine("   max_execution_time      The server gives up, as a per-query setting (Tcp_020). The");
        Console.WriteLine("                           query stops server-side, the client gets an ordinary");
        Console.WriteLine("                           ClickHouseTcpServerException with code 159, and the");
        Console.WriteLine("                           connection survives, because the response completed — with");
        Console.WriteLine("                           an error rather than rows. Tcp_023 covers it.");
        Console.WriteLine();
        Console.WriteLine("   So the cheapest of the three is the server-side one: it is the only one that does not");
        Console.WriteLine("   end a connection. Prefer max_execution_time for 'this query must not run longer than N");
        Console.WriteLine("   seconds', and keep the token for 'this caller no longer wants the answer'.");
    }

    /// <summary>
    /// Reads one row out of <c>system.query_log</c>, retrying the flush and the read. A query's record is queued
    /// independently of its response, so one flush straight after the query can miss it.
    /// </summary>
    private static async Task<string> ReadLog(ClickHouseTcpClient client, string sql, string queryId)
    {
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "id", queryId } },
        };

        for (int attempt = 1; attempt <= 5; attempt++)
        {
            await client.ExecuteAsync("SYSTEM FLUSH LOGS");
            await foreach (object[] row in client.QueryAsync(sql, options))
            {
                return (string)row[0];
            }

            await Task.Delay(50);
        }

        return "no row appeared in system.query_log after 5 attempts";
    }
}
