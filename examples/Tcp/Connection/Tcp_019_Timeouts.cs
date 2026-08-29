using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The native client's deadlines and limits: <c>DialTimeout</c>, <c>ReadTimeout</c>, <c>PoolTimeout</c>,
/// <c>StatementMaxLength</c> and <c>MaxSendBufferBytes</c>.
///
/// <para>
/// The three deadlines cover three different phases and never overlap: <c>PoolTimeout</c> bounds the wait for a
/// pool slot, <c>DialTimeout</c> bounds the connect and handshake that may follow it, and <c>ReadTimeout</c> bounds
/// how long the <b>server may stay silent</b> while a response is being read. That last one is the one to
/// understand — it measures silence, not duration, so a query that streams for an hour never trips it.
/// </para>
///
/// <para>
/// None of them bounds a whole operation. That is what a <c>CancellationToken</c> is for, and every method takes
/// one; <c>Tcp_022_Cancellation</c> is about what cancelling does to the connection.
/// </para>
/// </summary>
public static class TcpTimeouts
{
    public static async Task Run()
    {
        WhichDeadlineCoversWhat();
        await DialingTheWrongThing();
        await ReadTimeoutMeasuresSilence();
        PoolTimeoutInOneLine();
        await StatementMaxLengthCapsWhatIsLogged();
        MaxSendBufferBytesAndTheValues();
    }

    private static void WhichDeadlineCoversWhat()
    {
        ClickHouseTcpClientOptions defaults = new();

        Console.WriteLine("1. Four bounds, four different phases\n");
        Console.WriteLine($"   PoolTimeout   {defaults.PoolTimeout.TotalSeconds,5}s   waiting for one of MaxPoolSize connections");
        Console.WriteLine($"   DialTimeout   {defaults.DialTimeout.TotalSeconds,5}s   socket connect plus the protocol handshake");
        Console.WriteLine($"   ReadTimeout   {defaults.ReadTimeout.TotalSeconds,5}s   the longest silence allowed while reading a response");
        Console.WriteLine("   CancellationToken        the whole operation, and the only one that bounds it");
        Console.WriteLine();
        Console.WriteLine("   A checkout that has to open a connection can therefore take up to PoolTimeout plus");
        Console.WriteLine("   DialTimeout: the two apply to different phases, so they add rather than overlap.");
        Console.WriteLine();
        Console.WriteLine("   ReadTimeout = TimeSpan.Zero removes the deadline and leaves the caller's token as the");
        Console.WriteLine("   only bound. PoolTimeout and DialTimeout must be positive — there is no opting out of");
        Console.WriteLine("   those, because a wait with no bound at all is how a request hangs forever.");
    }

    private static async Task DialingTheWrongThing()
    {
        Console.WriteLine("\n2. DialTimeout, and the two dial failures that are not timeouts\n");

        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions();

        // Refused: the port answers at once with a reset, so the deadline is never involved.
        var clock = Stopwatch.StartNew();
        Exception? refused = await Failing(options with { Port = 1, DialTimeout = TimeSpan.FromSeconds(2) });
        Console.WriteLine($"   Nothing listening on the port, after {clock.ElapsedMilliseconds} ms:");
        Console.WriteLine($"     {Describe(refused)}");
        Console.WriteLine($"     inner: {refused?.InnerException?.GetType().Name} — a refusal is instant, so DialTimeout never came up");

        // The HTTP port. Both interfaces are ClickHouse, but they speak different protocols, and the native client
        // reads the HTTP server's reply as a protocol packet.
        clock.Restart();
        Exception? wrongPort = await Failing(options with { Port = ExampleConfig.HttpPort, DialTimeout = TimeSpan.FromSeconds(2) });
        Console.WriteLine($"\n   The HTTP port ({ExampleConfig.HttpPort}) instead of the native one, after {clock.ElapsedMilliseconds} ms:");
        Console.WriteLine($"     {Describe(wrongPort)}");
        Console.WriteLine("     Packet type 72 is 'H', the first byte of the HTTP response. Not a timeout either.");

        // What DialTimeout is actually for: a peer that accepts the connection and then says nothing. A firewall,
        // or a load balancer with no healthy backend behind it, looks exactly like this local listener.
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int silentPort = ((IPEndPoint)listener.LocalEndpoint).Port;
        Task<TcpClient?> accepted = AcceptOneAndSayNothing(listener);

        try
        {
            clock.Restart();
            Exception? silent = await Failing(options with
            {
                Host = "127.0.0.1",
                Port = silentPort,
                DialTimeout = TimeSpan.FromMilliseconds(300),
            });

            Console.WriteLine("\n   A socket that accepts and never answers, with DialTimeout = 300 ms:");
            Console.WriteLine($"     {Describe(silent)}");
            Console.WriteLine($"     ... after {clock.ElapsedMilliseconds} ms. The connect succeeded; it is the handshake that never");
            Console.WriteLine("     finished. DialTimeout covers both, which is why an endpoint that answers the socket");
            Console.WriteLine("     and nothing else is bounded at all.");
        }
        finally
        {
            listener.Stop();
            (await accepted)?.Dispose();
        }
    }

    /// <summary>Pings a server that is expected to be unreachable, and reports why it was.</summary>
    private static async Task<Exception?> Failing(ClickHouseTcpClientOptions options)
    {
        try
        {
            await using var client = new ClickHouseTcpClient(options);
            await client.PingAsync();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private static string Describe(Exception? failure)
        => failure is null ? "it answered, which is not what this example expected" : $"{failure.GetType().Name}: {failure.Message}";

    private static async Task<TcpClient?> AcceptOneAndSayNothing(TcpListener listener)
    {
        try
        {
            return await listener.AcceptTcpClientAsync();
        }
        catch (Exception ex) when (ex is SocketException or ObjectDisposedException)
        {
            // The listener was stopped first, which is the normal ending here.
            return null;
        }
    }

    private static async Task ReadTimeoutMeasuresSilence()
    {
        Console.WriteLine("\n3. ReadTimeout is an idle deadline, not a time limit on the query\n");

        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions();

        // sleepEachRow with max_block_size = 1 makes the server send one row at a time with a gap between them,
        // which is what a slow-but-alive server looks like from here.
        await using (var chatty = new ClickHouseTcpClient(options with { ReadTimeout = TimeSpan.FromMilliseconds(250) }))
        {
            var clock = Stopwatch.StartNew();
            int rows = 0;
            await foreach (object[] row in chatty.QueryAsync(
                "SELECT number, sleepEachRow(0.05) FROM numbers(8) SETTINGS max_block_size = 1"))
            {
                rows++;
            }

            Console.WriteLine("   ReadTimeout = 250 ms, 8 rows arriving 50 ms apart:");
            Console.WriteLine($"     read {rows} rows in {clock.ElapsedMilliseconds} ms — longer than the deadline, and it never fired.");
            Console.WriteLine("     Every byte that arrives resets the clock, so total duration is not what it bounds.");
        }

        await OneGapTooWide(options);

        // The other half of "silence": a slow consumer is not a silent server.
        await using (var pausing = new ClickHouseTcpClient(options with { ReadTimeout = TimeSpan.FromMilliseconds(150) }))
        {
            var clock = Stopwatch.StartNew();
            int rows = 0;
            await foreach (object[] row in pausing.QueryAsync("SELECT number FROM numbers(4) SETTINGS max_block_size = 1"))
            {
                rows++;

                // Holding each row for longer than the deadline before asking for the next one.
                await Task.Delay(200);
            }

            Console.WriteLine("\n   ReadTimeout = 150 ms, and a consumer that sits on each row for 200 ms:");
            Console.WriteLine($"     read {rows} rows in {clock.ElapsedMilliseconds} ms, no timeout. The clock runs only while");
            Console.WriteLine("     the client is waiting on the transport, so your own processing time is never on it.");
        }

        // The opt-out, for a stream that is legitimately silent for a long time.
        await using (var unbounded = new ClickHouseTcpClient(options with { ReadTimeout = TimeSpan.Zero }))
        {
            var clock = Stopwatch.StartNew();
            _ = await unbounded.ExecuteScalarAsync("SELECT sleepEachRow(0.4) FROM numbers(1)");
            Console.WriteLine($"\n   ReadTimeout = TimeSpan.Zero, a 400 ms silence: completed in {clock.ElapsedMilliseconds} ms.");
            Console.WriteLine("     With no deadline the caller's CancellationToken is the only bound left. Prefer a");
            Console.WriteLine("     generous ReadTimeout to none: what it catches is a connection dropped without a");
            Console.WriteLine("     FIN, which nothing else notices and TCP alone takes about fifteen minutes to give up on.");
        }
    }

    /// <summary>
    /// One silence wider than the deadline, and what the pool then does with the connection. Its own method so
    /// that the logger factory below is disposed — and its lines flushed to the console — before the next
    /// section prints.
    /// </summary>
    private static async Task OneGapTooWide(ClickHouseTcpClientOptions options)
    {
        // The pool's own lines, at Trace, because the reuse line a healthy connection produces is a Trace line
        // (Tcp_017 shows what one looks like). Its absence below is the evidence.
        using ILoggerFactory poolLog = LoggerFactory.Create(builder => builder
            .AddFilter((category, _) => category == "ClickHouse.Driver.Tcp.Pool")
            .AddSimpleConsole(console => console.SingleLine = true)
            .SetMinimumLevel(LogLevel.Trace));

        await using var strict = new ClickHouseTcpClient(options with
        {
            ReadTimeout = TimeSpan.FromMilliseconds(150),
            MaxPoolSize = 1,
            LoggerFactory = poolLog,
        });

        Console.WriteLine("\n   ReadTimeout = 150 ms, the same query with 500 ms between rows, and the pool's own lines:");

        var clock = Stopwatch.StartNew();
        try
        {
            await foreach (object[] row in strict.QueryAsync(
                "SELECT number, sleepEachRow(0.5) FROM numbers(3) SETTINGS max_block_size = 1"))
            {
            }
        }
        catch (TimeoutException ex)
        {
            Console.WriteLine($"     TimeoutException after {clock.ElapsedMilliseconds} ms: {ex.Message}");
        }

        // A second query on the same client, whose pool holds exactly one connection. Had the timed-out
        // connection gone back into the pool, this is the one that would have got it.
        _ = await strict.ExecuteScalarAsync("SELECT 1");

        Console.WriteLine("     The pool closed that connection instead of pooling it — 'no longer reusable' — and");
        Console.WriteLine("     the query after it opened another rather than reusing one. A socket that stopped");
        Console.WriteLine("     answering mid-response is of no use to the next caller.");
    }

    private static void PoolTimeoutInOneLine()
    {
        Console.WriteLine("\n4. PoolTimeout\n");
        Console.WriteLine("   The third deadline belongs to the pool, so Tcp_017_PoolTuning demonstrates it: with");
        Console.WriteLine("   MaxPoolSize connections in use, the next operation waits PoolTimeout for a free one and");
        Console.WriteLine("   then throws TimeoutException. Two things hold a connection longer than a caller expects —");
        Console.WriteLine("   a session, for its whole lifetime, and a streamed result nobody finished reading.");
    }

    private static async Task StatementMaxLengthCapsWhatIsLogged()
    {
        Console.WriteLine("\n5. StatementMaxLength, which caps the query text that leaves the client\n");
        Console.WriteLine("   It bounds two channels: the Debug log line below, and the db.query.text span attribute");
        Console.WriteLine("   that IncludeSqlInActivityTags turns on. The default is 5 — a stub, not a statement — so");
        Console.WriteLine("   recording query text is something you ask for.\n");

        const string sql = "SELECT 'a statement long enough to show the cut'";

        foreach (int max in new[] { 5, 60 })
        {
            // Only the client category, so the pool and connection lines stay out of the way. A real application
            // configures this through the container; see Tcp_003.
            using ILoggerFactory factory = LoggerFactory.Create(builder => builder
                .AddFilter((category, level) => category == "ClickHouse.Driver.Tcp.Client" && level >= LogLevel.Debug)
                .AddSimpleConsole(console => console.SingleLine = true)
                .SetMinimumLevel(LogLevel.Debug));

            Console.WriteLine($"   StatementMaxLength = {max}, and the client's own log lines that follow:");

            await using (var client = new ClickHouseTcpClient(ExampleConfig.TcpBuilder().ToOptions() with
            {
                LoggerFactory = factory,
                StatementMaxLength = max,
            }))
            {
                _ = await client.ExecuteScalarAsync(sql);
            }

            // The factory is disposed at the end of this iteration, which drains the console logger, so its lines
            // land before the next heading prints.
        }

        Console.WriteLine();
        Console.WriteLine($"   The statement was {sql.Length} characters, so at 5 the log line carries a stub of it and");
        Console.WriteLine("   at 60 the whole thing. Zero or less keeps the text out even where the span attribute is on.");
    }

    private static void MaxSendBufferBytesAndTheValues()
    {
        ClickHouseTcpClientOptions defaults = new();

        Console.WriteLine("\n6. MaxSendBufferBytes, and what the constructor refuses\n");
        Console.WriteLine($"   MaxSendBufferBytes defaults to {defaults.MaxSendBufferBytes / (1024 * 1024)} MiB. It is a soft cap on the client's send");
        Console.WriteLine("   buffer during an insert: while a wire block is written, buffered bytes are flushed to the");
        Console.WriteLine("   socket whenever they exceed it. Soft, because a single column larger than the cap still");
        Console.WriteLine("   buffers in full.");
        Console.WriteLine();
        Console.WriteLine("   It is independent of MaxRowsPerBlock (Tcp_009), which decides how large a block is; this");
        Console.WriteLine("   decides how much of one is held in memory on the way out. Peak send-buffer memory is");
        Console.WriteLine($"   about MaxSendBufferBytes × MaxPoolSize — {defaults.MaxSendBufferBytes / (1024 * 1024)} MiB × {defaults.MaxPoolSize} at the defaults — when every");
        Console.WriteLine("   connection is inserting at once. Nothing reports how much is buffered, so this one is a");
        Console.WriteLine("   sizing decision rather than something to watch.");
        Console.WriteLine();
        Console.WriteLine("   Every value here is checked when the client is constructed, not on first use:\n");

        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions();

        Refused("MaxSendBufferBytes = 0", options with { MaxSendBufferBytes = 0 });
        Refused("ReadTimeout = -1s", options with { ReadTimeout = TimeSpan.FromSeconds(-1) });
        Refused("PoolTimeout = TimeSpan.Zero", options with { PoolTimeout = TimeSpan.Zero });
        Refused("DialTimeout = 30 days", options with { DialTimeout = TimeSpan.FromDays(30) });
    }

    private static void Refused(string what, ClickHouseTcpClientOptions options)
    {
        try
        {
            // Never reaches a socket, so a synchronous Dispose is all this needs.
            using var client = new ClickHouseTcpClient(options);
            Console.WriteLine($"   {what} -> accepted, which is not what this example expected");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"   {what,-28} -> {ex.GetType().Name}: {ex.Message.Split(" (Parameter")[0]}");
        }
    }
}
