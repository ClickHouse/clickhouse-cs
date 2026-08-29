using System.Diagnostics;
using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Sizing the native client's connection pool: <c>MinPoolSize</c>, <c>MaxPoolSize</c>, <c>PoolTimeout</c>,
/// <c>IdleTimeout</c>, <c>MaxConnectionLifetime</c>, <c>SweepInterval</c> and <c>PoolReusePolicy</c> — what each
/// one does, and what it looks like when it acts.
///
/// <para>
/// One connection carries one query, so <c>MaxPoolSize</c> is the client's concurrency limit, for the whole
/// process rather than per caller. Everything below is measured: the counts come from the pool's own log and from
/// <c>system.processes</c> on the server, because nothing on the client reports how many connections are open,
/// idle or in use.
/// </para>
///
/// <para>
/// The lifetime limits default to minutes, which an example cannot wait out, so the sections that show them set
/// them to milliseconds. The code path is the same one a 30-minute limit takes.
/// </para>
/// </summary>
public static class TcpPoolTuning
{
    public static async Task Run()
    {
        TheKnobs();
        await ReuseAndTheOnlyWindowIntoThePool();
        await MaxPoolSizeCapsConcurrency();
        await PoolTimeoutExpires();
        await RetirementAndTheSweep();
        await LifoAgainstFifo();
        await WhatADataSourceShares();
    }

    private static void TheKnobs()
    {
        ClickHouseTcpClientOptions defaults = new();

        Console.WriteLine("1. The pool keys and their defaults\n");
        Console.WriteLine($"   MinPoolSize            {defaults.MinPoolSize,-8} connections kept open when the pool can");
        Console.WriteLine($"   MaxPoolSize            {defaults.MaxPoolSize,-8} hard cap, and so the concurrency limit");
        Console.WriteLine($"   PoolTimeout            {defaults.PoolTimeout.TotalSeconds + "s",-8} wait for a slot before TimeoutException");
        Console.WriteLine($"   IdleTimeout            {defaults.IdleTimeout.TotalMinutes + "m",-8} unused for this long, and it is retired");
        Console.WriteLine($"   MaxConnectionLifetime  {defaults.MaxConnectionLifetime.TotalMinutes + "m",-8} open for this long, and it is retired");
        Console.WriteLine($"   SweepInterval          {"derived",-8} how often the pool looks for work to do");
        Console.WriteLine($"   PoolReusePolicy        {defaults.PoolReusePolicy,-8} which idle connection is handed out next");
        Console.WriteLine();
        Console.WriteLine("   TimeSpan.Zero opts out of IdleTimeout and MaxConnectionLifetime; PoolTimeout has to be");
        Console.WriteLine("   positive. A null SweepInterval derives the period as a quarter of the shorter of the two");
        Console.WriteLine("   limits, held between 1 and 30 seconds — 30 seconds at these defaults. The derived value");
        Console.WriteLine("   is not exposed, so the rule is the only way to know it.");

        // The same keys exist on the connection string, so a deployment can size the pool without a rebuild.
        var builder = ExampleConfig.TcpBuilder();
        builder.MinPoolSize = 2;
        builder.MaxPoolSize = 8;
        builder.PoolTimeout = TimeSpan.FromSeconds(5);
        builder.IdleTimeout = TimeSpan.FromSeconds(45);
        builder.MaxConnectionLifetime = TimeSpan.FromMinutes(10);
        builder.PoolReusePolicy = ClickHouseTcpPoolReusePolicy.Fifo;

        ClickHouseTcpClientOptions tuned = builder.ToOptions();
        Console.WriteLine("\n   The same thing through the connection string (MinPoolSize=2;MaxPoolSize=8;...):");
        Console.WriteLine($"     Min {tuned.MinPoolSize}, Max {tuned.MaxPoolSize}, PoolTimeout {tuned.PoolTimeout}, IdleTimeout {tuned.IdleTimeout}, Lifetime {tuned.MaxConnectionLifetime}, {tuned.PoolReusePolicy}");
    }

    private static async Task ReuseAndTheOnlyWindowIntoThePool()
    {
        Console.WriteLine("\n2. What the pool will tell you\n");
        Console.WriteLine("   There are no counters to read, so the pool's log is the window into it. These lines come");
        Console.WriteLine("   from the ClickHouse.Driver.Tcp.Pool and .Connection categories, at Debug and Trace.\n");

        var capture = new LogCapture();
        await using (var client = new ClickHouseTcpClient(Options() with { LoggerFactory = capture }))
        {
            _ = await client.ExecuteScalarAsync("SELECT 1");
            _ = await client.ExecuteScalarAsync("SELECT 2");
        }

        Print(capture.Lines.Where(l => !l.StartsWith("Client", StringComparison.Ordinal)));
        Console.WriteLine("\n   Two queries, one connection: the first opened it, the second reused it, and the drain");
        Console.WriteLine("   at disposal closed it. 'its 2 operation' is that connection's use count.");
    }

    private static async Task MaxPoolSizeCapsConcurrency()
    {
        Console.WriteLine("\n3. MaxPoolSize caps how many operations run at once\n");
        Console.WriteLine("   Four queries, each sleeping 150 ms, started together. A second client watches");
        Console.WriteLine("   system.processes to see how many of them the server is really running.\n");

        await Measure(maxPoolSize: 2);
        await Measure(maxPoolSize: 4);

        Console.WriteLine("\n   The queries are not lost when the pool is full, only queued: each waits for a slot for");
        Console.WriteLine("   up to PoolTimeout. So MaxPoolSize is a throughput knob, and PoolTimeout is the deadline");
        Console.WriteLine("   on getting one of its slots.");
    }

    private static async Task Measure(int maxPoolSize)
    {
        string marker = $"example_tcp_pool_cap_{maxPoolSize}";
        var capture = new LogCapture();

        await using var observer = ExampleConfig.CreateTcpClient();
        await using var client = new ClickHouseTcpClient(Options() with { MaxPoolSize = maxPoolSize, LoggerFactory = capture });

        var clock = Stopwatch.StartNew();
        Task work = Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
            await client.ExecuteScalarAsync($"SELECT sleep(0.15) /* {marker} */"))));

        // The marker is a comment, so it appears in the query text the server reports. The observer's own query
        // carries it too, hence the second condition.
        string count = $"SELECT count() FROM system.processes WHERE query LIKE '%{marker}%' AND query NOT LIKE '%system.processes%'";

        int mostSeen = 0;
        while (!work.IsCompleted && clock.ElapsedMilliseconds < 5000)
        {
            mostSeen = Math.Max(mostSeen, Convert.ToInt32(await observer.ExecuteScalarAsync(count)));
            await Task.Delay(20);
        }

        await work;
        long elapsed = clock.ElapsedMilliseconds;

        Console.WriteLine($"   MaxPoolSize = {maxPoolSize}");
        Console.WriteLine($"     connections opened, from the pool log : {capture.Count("opening one")}");
        Console.WriteLine($"     most running at once, from the server : {mostSeen}");
        Console.WriteLine($"     wall clock for all four              : {elapsed} ms");
    }

    private static async Task PoolTimeoutExpires()
    {
        Console.WriteLine("\n4. PoolTimeout, when there is nothing left to hand out\n");

        var capture = new LogCapture();
        await using var client = new ClickHouseTcpClient(Options() with
        {
            MaxPoolSize = 1,
            PoolTimeout = TimeSpan.FromMilliseconds(250),
            LoggerFactory = capture,
        });

        // A session pins its connection for its whole lifetime, so one session against a pool of one is an
        // exhausted pool — no sleeping query needed.
        await using IClickHouseTcpSession session = await client.OpenSessionAsync();
        Console.WriteLine("   MaxPoolSize = 1, PoolTimeout = 250 ms, and a session holds the only connection.");

        var clock = Stopwatch.StartNew();
        try
        {
            _ = await client.ExecuteScalarAsync("SELECT 1");
        }
        catch (TimeoutException ex)
        {
            Console.WriteLine($"\n   A query on the client threw TimeoutException after {clock.ElapsedMilliseconds} ms:");
            Console.WriteLine($"     {ex.Message}");
        }

        Console.WriteLine("\n   The pool logged it too:");
        Print(capture.Lines.Where(l => l.Contains("PoolTimeout", StringComparison.Ordinal)));
        Console.WriteLine();
        Console.WriteLine("   Raising PoolTimeout only makes the caller wait longer for a pool that is too small.");
        Console.WriteLine("   The message also names the other cause: a streamed result nobody finished still holds");
        Console.WriteLine("   its connection.");
    }

    private static async Task RetirementAndTheSweep()
    {
        Console.WriteLine("\n5. Retiring connections: MaxConnectionLifetime, IdleTimeout, SweepInterval, MinPoolSize\n");

        // Age is read at checkout and at return, so a 1 ms limit means no connection is ever reused.
        var byAge = new LogCapture();
        await using (var client = new ClickHouseTcpClient(Options() with
        {
            MaxConnectionLifetime = TimeSpan.FromMilliseconds(1),
            LoggerFactory = byAge,
        }))
        {
            _ = await client.ExecuteScalarAsync("SELECT 1");
            _ = await client.ExecuteScalarAsync("SELECT 2");
        }

        Console.WriteLine("   MaxConnectionLifetime = 1 ms, two queries:");
        Print(byAge.Lines.Where(l => l.StartsWith("Pool", StringComparison.Ordinal)));
        Console.WriteLine("   No reuse at all: the connection is over age by the time it comes back, so it is closed");
        Console.WriteLine("   on return and the next query opens another. That check is between operations, never");
        Console.WriteLine("   inside one, so no query is ever cut short by it.\n");

        // Idle retirement is the sweep's work, so it happens without any operation to trigger it.
        var byIdle = new LogCapture();
        await using (var client = new ClickHouseTcpClient(Options() with
        {
            IdleTimeout = TimeSpan.FromMilliseconds(150),
            SweepInterval = TimeSpan.FromMilliseconds(100),
            LoggerFactory = byIdle,
        }))
        {
            _ = await client.ExecuteScalarAsync("SELECT 1");
            long waited = await WaitFor(byIdle, "Retired");
            Console.WriteLine("   IdleTimeout = 150 ms, SweepInterval = 100 ms, one query then nothing:");
            Print(byIdle.Lines.Where(l => l.Contains("Retired", StringComparison.Ordinal)));
            Console.WriteLine($"   The sweep retired it {waited} ms after the query, with no operation involved.");
        }

        Console.WriteLine();

        // The same sweep restores the floor, which is why MinPoolSize needs no traffic to take effect.
        var byFloor = new LogCapture();
        await using (var client = new ClickHouseTcpClient(Options() with
        {
            MinPoolSize = 3,
            MaxPoolSize = 5,
            SweepInterval = TimeSpan.FromMilliseconds(100),
            LoggerFactory = byFloor,
        }))
        {
            long waited = await WaitFor(byFloor, "Connected to ClickHouse", occurrences: 3);
            Console.WriteLine("   MinPoolSize = 3, SweepInterval = 100 ms, and not one query run:");
            Console.WriteLine($"     connections opened by the sweep: {byFloor.Count("Connected to ClickHouse")} after {waited} ms");
        }

        Console.WriteLine();
        Console.WriteLine("   The floor and IdleTimeout multiply: neither limit respects MinPoolSize, so a quiet pool");
        Console.WriteLine("   retires its connections and the sweep opens replacements. A floor of 10 against a");
        Console.WriteLine("   5-second idle limit is 10 handshakes every 5 seconds from an idle application. Size the");
        Console.WriteLine("   two together. Set IdleTimeout below the shortest idle timeout on the path to the server:");
        Console.WriteLine("   a proxy that drops an idle connection without a FIN leaves one that only looks alive.");
    }

    private static async Task LifoAgainstFifo()
    {
        Console.WriteLine("\n6. PoolReusePolicy: which idle connection comes back out\n");
        Console.WriteLine("   Three queries at once fill a pool of three, then three run one after another. The use");
        Console.WriteLine("   count in the reuse line says whether they landed on one connection or on all three.\n");

        foreach (ClickHouseTcpPoolReusePolicy policy in new[] { ClickHouseTcpPoolReusePolicy.Lifo, ClickHouseTcpPoolReusePolicy.Fifo })
        {
            var capture = new LogCapture();
            await using var client = new ClickHouseTcpClient(Options() with
            {
                MaxPoolSize = 3,
                PoolReusePolicy = policy,
                LoggerFactory = capture,
            });

            await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => Task.Run(async () =>
                await client.ExecuteScalarAsync("SELECT sleep(0.15)"))));

            for (int i = 0; i < 3; i++)
            {
                _ = await client.ExecuteScalarAsync("SELECT 1");
            }

            // "Reusing a pooled connection, its N operation, ..." — N is that connection's use count, which is
            // what tells one policy from the other.
            IEnumerable<string> counts = capture.Lines
                .Where(l => l.Contains("Reusing", StringComparison.Ordinal))
                .Select(l => l[(l.IndexOf("its ", StringComparison.Ordinal) + 4)..].Split(' ')[0]);

            string shape = policy == ClickHouseTcpPoolReusePolicy.Lifo
                ? "one connection, used again and again"
                : "each of the three in turn";

            Console.WriteLine($"   {policy,-4}  use count of the connection each sequential query got: {string.Join(", ", counts)}  ({shape})");
        }

        Console.WriteLine();
        Console.WriteLine("   Lifo keeps returning to the connection that came back last, so traffic concentrates on a");
        Console.WriteLine("   hot few and the rest go idle and close — a pool sized for peak load costs little");
        Console.WriteLine("   off-peak. Fifo spreads the work, so under steady load every connection is used again");
        Console.WriteLine("   inside its idle window and the whole pool stays warm. Both are equally correct: age,");
        Console.WriteLine("   idleness and liveness are checked whichever end the connection comes from.");
    }

    private static async Task WhatADataSourceShares()
    {
        Console.WriteLine("\n7. What a ClickHouseTcpDataSource shares\n");

        await using var dataSource = new ClickHouseTcpDataSource(Options() with { MaxPoolSize = 8 });

        Console.WriteLine($"   One data source owns one client, and that client owns one pool: {dataSource.Options.MaxPoolSize} connections");
        Console.WriteLine("   for every consumer that is injected with it (Tcp_003 registers one). So MaxPoolSize is");
        Console.WriteLine("   the whole application's concurrency budget, not each service's.");
        Console.WriteLine();
        Console.WriteLine("   Two data sources, or two clients built with 'new', are two pools that share nothing but");
        Console.WriteLine("   the server. That is what a keyed registration per endpoint buys, and it is also the");
        Console.WriteLine("   accident behind a client built per request: every one pays a handshake and none of them");
        Console.WriteLine("   reuses anything.");
        Console.WriteLine();
        Console.WriteLine("   Sizing, in short: MaxPoolSize at or a little above the number of operations you want in");
        Console.WriteLine("   flight, remembering each inserting connection can buffer MaxSendBufferBytes (Tcp_019);");
        Console.WriteLine("   MinPoolSize only where a cold first query matters; and one slot per session you hold.");
    }

    private static ClickHouseTcpClientOptions Options() => ExampleConfig.TcpBuilder().ToOptions();

    private static void Print(IEnumerable<string> lines)
    {
        foreach (string line in lines)
        {
            Console.WriteLine($"     {line}");
        }
    }

    /// <summary>Waits for the pool to log something, so the example never sleeps longer than it must.</summary>
    private static async Task<long> WaitFor(LogCapture capture, string contains, int occurrences = 1)
    {
        var clock = Stopwatch.StartNew();
        while (clock.ElapsedMilliseconds < 3000 && capture.Count(contains) < occurrences)
        {
            await Task.Delay(25);
        }

        return clock.ElapsedMilliseconds;
    }

    /// <summary>
    /// An <see cref="ILoggerFactory"/> that keeps the lines instead of printing them, so the example can show only
    /// the ones under discussion. A real application passes the container's factory; see Tcp_003.
    /// </summary>
    private sealed class LogCapture : ILoggerFactory
    {
        private readonly List<string> lines = [];

        public IReadOnlyList<string> Lines
        {
            get
            {
                lock (lines)
                {
                    return lines.ToArray();
                }
            }
        }

        public int Count(string contains)
            => Lines.Count(l => l.Contains(contains, StringComparison.Ordinal));

        public ILogger CreateLogger(string categoryName) => new Sink(categoryName, lines);

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public void Dispose()
        {
        }

        private sealed class Sink(string category, List<string> lines) : ILogger
        {
            // The client asks before formatting, so answering true is what makes Trace-level lines appear.
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
                    lines.Add($"{category[(category.LastIndexOf('.') + 1)..]}: {formatter(state, exception)}");
                }
            }
        }
    }
}
