using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// <see cref="ClickHouseTcpClientOptions.LoggerFactory"/> and the three categories the native client logs under —
/// <see cref="ClickHouseTcpDiagnostics.ClientLogCategory"/>, <see cref="ClickHouseTcpDiagnostics.ConnectionLogCategory"/>
/// and <see cref="ClickHouseTcpDiagnostics.PoolLogCategory"/>: what each reports, at which level, and how to keep
/// one and drop the rest.
///
/// <para>
/// The client logs its <b>own</b> lifecycle. It never logs what the server reports; the server's log lines arrive
/// as a callback (Tcp_028). Nothing here reports pool state either — the pool's lines are the only window into it,
/// which is what Tcp_017 reads.
/// </para>
///
/// <para>
/// The levels are the thing to plan around: the client writes almost everything at <c>Debug</c> or <c>Trace</c>,
/// and the statement text rides on a <c>Debug</c> line. So the two filter sets below are genuinely different
/// configurations, not one dialled up — and a stock <see cref="ILoggerFactory"/>, whose minimum level is
/// <c>Information</c>, shows nearly none of it.
/// </para>
/// </summary>
public static class TcpLogging
{
    public static async Task Run()
    {
        await OneWorkloadThreeCategories();
        await WhichLevelsAreUsed();
        await AStockFactoryShowsAlmostNothing();
        await TwoFilterSets();
        await StatementTextRidesOnADebugLine();
        WhatIsNotHere();
    }

    private static async Task OneWorkloadThreeCategories()
    {
        Console.WriteLine("1. Three categories, one workload\n");

        var recorder = new Recorder();
        using ILoggerFactory factory = LoggerFactory.Create(builder => builder
            .AddProvider(recorder)
            .SetMinimumLevel(LogLevel.Trace));

        await Workload(factory);

        Console.WriteLine("   Two queries, then one that names a table that does not exist:\n");
        foreach (Line line in recorder.Lines)
        {
            Console.WriteLine($"     {line.ShortCategory,-10} {line.Level,-11} {line.Message}");
        }

        Console.WriteLine();
        Console.WriteLine("     Client      what ran, how long it took, how it ended — and the statement text");
        Console.WriteLine("     Connection  the dial, the TLS negotiation, and the handshake result");
        Console.WriteLine("     Pool        checkouts, retirement, exhaustion, and the background work nobody awaits");
        Console.WriteLine();
        Console.WriteLine("   Each is a full logger category, so the usual per-category configuration applies:");
        Console.WriteLine("   appsettings' Logging:LogLevel section, AddFilter, or a filter predicate as below.");
    }

    private static async Task WhichLevelsAreUsed()
    {
        Console.WriteLine("\n2. Which levels each category actually uses\n");

        var recorder = new Recorder();
        using ILoggerFactory factory = LoggerFactory.Create(builder => builder
            .AddProvider(recorder)
            .SetMinimumLevel(LogLevel.Trace));

        // The same workload, plus the two things that log above Debug: a dial that fails, and a pool with nothing
        // left to hand out.
        await Workload(factory);
        await FailedDial(factory);
        await ExhaustedPool(factory);

        foreach (var group in recorder.Lines
            .GroupBy(l => (l.ShortCategory, l.Level))
            .OrderBy(g => g.Key.ShortCategory, StringComparer.Ordinal)
            .ThenByDescending(g => g.Key.Level))
        {
            Console.WriteLine($"     {group.Key.ShortCategory,-10} {group.Key.Level,-11} {group.Count(),2} line(s)   e.g. {Trim(group.First().Message)}");
        }

        Console.WriteLine();
        Console.WriteLine("   Nothing is logged at Information or Critical. Warning is the top of the range and it is");
        Console.WriteLine("   reserved for four messages: a dial that failed, PoolTimeout, and the two background jobs");
        Console.WriteLine("   nobody awaits (a failed top-up towards MinPoolSize, a failed sweep) — which are reported");
        Console.WriteLine("   nowhere else at all. Error is a single message, an operation that threw — twice here,");
        Console.WriteLine("   once for the unknown table and once for the query that never got a connection.");
    }

    private static async Task AStockFactoryShowsAlmostNothing()
    {
        Console.WriteLine("\n3. A factory with no minimum level set shows almost none of it\n");

        var recorder = new Recorder();
        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddProvider(recorder));

        await Workload(factory);

        Console.WriteLine($"   LoggerFactory.Create(b => b.AddProvider(...)) with no SetMinimumLevel: {recorder.Lines.Count} line(s) kept");
        foreach (Line line in recorder.Lines)
        {
            Console.WriteLine($"     {line.ShortCategory,-10} {line.Level,-11} {line.Message}");
        }

        Console.WriteLine();
        Console.WriteLine("   Microsoft.Extensions.Logging defaults its minimum to Information, and the client logs");
        Console.WriteLine("   nothing there, so a factory that was wired up correctly still looks broken. Set the level");
        Console.WriteLine("   for the categories you want, not globally: Trace across the whole application is a lot of");
        Console.WriteLine("   log.");
    }

    private static async Task TwoFilterSets()
    {
        Console.WriteLine("\n4. Two filter sets: one for production, one for a connection problem\n");

        // Production: only the lines that mean something is wrong. No statement text, because the line that
        // carries it is a Debug line.
        var production = new Recorder();
        using (ILoggerFactory factory = LoggerFactory.Create(builder => builder
            .AddProvider(production)
            .AddFilter((category, level) => category?.StartsWith("ClickHouse.Driver.Tcp.", StringComparison.Ordinal) == true && level >= LogLevel.Warning)
            .SetMinimumLevel(LogLevel.Warning)))
        {
            await Workload(factory);
            await FailedDial(factory);
            await ExhaustedPool(factory);
        }

        Console.WriteLine($"   Production — every category, Warning and worse: {production.Lines.Count} line(s)\n");
        foreach (Line line in production.Lines)
        {
            Console.WriteLine($"     {line.ShortCategory,-10} {line.Level,-11} {Trim(line.Message)}");
        }

        // Debugging a connection problem: the two categories that know about sockets, at Trace, and nothing else.
        var connections = new Recorder();
        using (ILoggerFactory factory = LoggerFactory.Create(builder => builder
            .AddProvider(connections)
            .AddFilter((category, level) => category switch
            {
                ClickHouseTcpDiagnostics.ConnectionLogCategory => level >= LogLevel.Trace,
                ClickHouseTcpDiagnostics.PoolLogCategory => level >= LogLevel.Trace,
                _ => false,
            })
            .SetMinimumLevel(LogLevel.Trace)))
        {
            await Workload(factory);
            await FailedDial(factory);
            await ExhaustedPool(factory);
        }

        Console.WriteLine($"\n   Debugging a connection problem — Connection and Pool at Trace, nothing else: {connections.Lines.Count} line(s)\n");
        foreach (Line line in connections.Lines)
        {
            Console.WriteLine($"     {line.ShortCategory,-10} {line.Level,-11} {Trim(line.Message)}");
        }

        Console.WriteLine();
        Console.WriteLine("   The second set answers the questions the first cannot: how many connections were opened,");
        Console.WriteLine("   whether a query reused one or dialled, how old the reused one was, and whether a returned");
        Console.WriteLine("   connection went back into the pool. Note that no Client line appears in it — the query");
        Console.WriteLine("   text is deliberately out, which is what makes the set safe to turn on against a live");
        Console.WriteLine("   system.");
        Console.WriteLine();
        Console.WriteLine("   Both are predicates over the category string, so they can key on");
        Console.WriteLine("   ClickHouseTcpDiagnostics.ClientLogCategory and its two siblings rather than a literal.");
    }

    private static async Task StatementTextRidesOnADebugLine()
    {
        Console.WriteLine("\n5. The statement text, and the one line that carries it\n");

        const string sql = "SELECT 'the whole statement, or as much of it as StatementMaxLength allows'";

        foreach (int max in new[] { 0, 30, 200 })
        {
            var recorder = new Recorder();
            using ILoggerFactory factory = LoggerFactory.Create(builder => builder
                .AddProvider(recorder)
                .AddFilter((category, level) => category == ClickHouseTcpDiagnostics.ClientLogCategory && level >= LogLevel.Debug)
                .SetMinimumLevel(LogLevel.Debug));

            await using (var client = new ClickHouseTcpClient(Options() with
            {
                LoggerFactory = factory,
                StatementMaxLength = max,
            }))
            {
                _ = await client.ExecuteScalarAsync(sql);
            }

            string running = recorder.Lines.First(l => l.Message.StartsWith("Running", StringComparison.Ordinal)).Message;
            Console.WriteLine($"     StatementMaxLength = {max,3}   {running}");
        }

        Console.WriteLine();
        Console.WriteLine($"   The statement was {sql.Length} characters. Zero keeps the text out of the log line while");
        Console.WriteLine("   leaving the line itself — which is the production recipe if you want a record of what ran");
        Console.WriteLine("   and how long it took without putting query text in your logs. The same knob caps the");
        Console.WriteLine("   db.query.text span attribute (Tcp_027), and Tcp_019 covers it as a limit.");
    }

    private static void WhatIsNotHere()
    {
        Console.WriteLine("\n6. What these categories do not carry\n");
        Console.WriteLine("   The server's own log lines. Those come from the query, not the client, and reach you");
        Console.WriteLine("   through ClickHouseTcpQueryCallbacks.OnLog with send_logs_level set — Tcp_028. Bridging");
        Console.WriteLine("   them into an ILogger is a few lines, and yours to write.");
        Console.WriteLine();
        Console.WriteLine("   Pool counters. There is no open/idle/in-use to read, so the Pool category's lines are the");
        Console.WriteLine("   only window into the pool; Tcp_017 measures it that way.");
        Console.WriteLine();
        Console.WriteLine("   A connection identity. The reuse line carries a use count and an age, but nothing names");
        Console.WriteLine("   the connection, so two lines about the same socket cannot be tied together.");
        Console.WriteLine();
        Console.WriteLine("   In an application you would not build the factory by hand at all: register logging in the");
        Console.WriteLine("   container and AddClickHouseTcpDataSource fills LoggerFactory in from it (Tcp_003).");
    }

    private static ClickHouseTcpClientOptions Options() => ExampleConfig.TcpBuilder().ToOptions();

    /// <summary>Two queries that succeed and one that does not, on a client of this example's own.</summary>
    private static async Task Workload(ILoggerFactory factory)
    {
        await using var client = new ClickHouseTcpClient(Options() with
        {
            LoggerFactory = factory,
            StatementMaxLength = 60,
        });

        _ = await client.ExecuteScalarAsync("SELECT 'the first query has to open a connection'");
        _ = await client.ExecuteScalarAsync("SELECT 'the second reuses it'");

        // An unknown table is reported after the server accepted the query, so the connection survives it and goes
        // back into the pool. The client logs one Error line and rethrows.
        try
        {
            _ = await client.ExecuteScalarAsync("SELECT * FROM example_tcp_logging_no_such_table");
        }
        catch (ClickHouseTcpServerException)
        {
        }
    }

    /// <summary>A dial that fails at once, for the Connection category's one Warning.</summary>
    private static async Task FailedDial(ILoggerFactory factory)
    {
        await using var client = new ClickHouseTcpClient(Options() with
        {
            LoggerFactory = factory,
            Port = 1,
            DialTimeout = TimeSpan.FromSeconds(2),
        });

        try
        {
            await client.PingAsync();
        }
        catch (ClickHouseTcpTransportException)
        {
        }
    }

    /// <summary>A pool with its only connection pinned by a session, for the Pool category's PoolTimeout Warning.</summary>
    private static async Task ExhaustedPool(ILoggerFactory factory)
    {
        await using var client = new ClickHouseTcpClient(Options() with
        {
            LoggerFactory = factory,
            MaxPoolSize = 1,
            PoolTimeout = TimeSpan.FromMilliseconds(200),
        });

        await using IClickHouseTcpSession session = await client.OpenSessionAsync();

        try
        {
            _ = await client.ExecuteScalarAsync("SELECT 1");
        }
        catch (TimeoutException)
        {
        }
    }

    private static string Trim(string message)
        => message.Length <= 96 ? message : message[..96] + "...";

    private readonly record struct Line(string Category, LogLevel Level, string Message)
    {
        /// <summary>The part after the last dot — Client, Connection or Pool.</summary>
        public string ShortCategory => Category[(Category.LastIndexOf('.') + 1)..];
    }

    /// <summary>
    /// An <see cref="ILoggerProvider"/> that keeps the lines rather than printing them, so a section can report
    /// what its filter kept. Registered with <c>AddProvider</c>, so the builder's filters really do apply — a
    /// factory that only wraps <see cref="ILogger"/> would bypass them and prove nothing.
    /// </summary>
    private sealed class Recorder : ILoggerProvider
    {
        private readonly List<Line> lines = [];

        public IReadOnlyList<Line> Lines
        {
            get
            {
                lock (lines)
                {
                    return lines.ToArray();
                }
            }
        }

        public ILogger CreateLogger(string categoryName) => new Sink(categoryName, lines);

        public void Dispose()
        {
        }

        private sealed class Sink(string category, List<Line> lines) : ILogger
        {
            public bool IsEnabled(LogLevel logLevel) => true;

            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                lock (lines)
                {
                    lines.Add(new Line(category, logLevel, formatter(state, exception)));
                }
            }
        }
    }
}
