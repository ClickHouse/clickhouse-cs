using System.Diagnostics;
using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// <c>PingAsync</c> as a health check, wired into <c>Microsoft.Extensions.Diagnostics.HealthChecks</c> over an
/// <c>AddClickHouseTcpDataSource</c> registration.
///
/// <para>
/// A Ping is a protocol packet pair, not a statement: the server answers Pong and nothing is parsed, planned,
/// executed or written to <c>system.query_log</c>. That makes it the cheapest liveness probe this transport has —
/// the numbers are below — and it is the reason a native health check does not look like the HTTP one, which has
/// to send <c>SELECT 1</c>.
/// </para>
///
/// <para>
/// Cheap also means narrow. A Pong says the socket reaches a ClickHouse that is still talking; it does not say a
/// query will succeed. Section 4 is the list of what it does and does not prove, which matters because a probe
/// that answers the wrong question is worse than none.
/// </para>
/// </summary>
public static class TcpHealthChecks
{
    public static async Task Run()
    {
        await WhatAPingCosts();
        await ThreeEndpointsOneReport();
        WhatAPongProves();
    }

    private static async Task WhatAPingCosts()
    {
        Console.WriteLine("1. What a Ping costs against what SELECT 1 costs\n");

        await using var client = ExampleConfig.CreateTcpClient();

        // Both warmed up first, so neither measurement pays for the handshake.
        await client.PingAsync();
        _ = await client.ExecuteScalarAsync("SELECT 1");

        const int rounds = 50;

        var clock = Stopwatch.StartNew();
        for (int i = 0; i < rounds; i++)
        {
            await client.PingAsync();
        }

        double pings = clock.Elapsed.TotalMilliseconds;

        clock.Restart();
        for (int i = 0; i < rounds; i++)
        {
            _ = await client.ExecuteScalarAsync("SELECT 1");
        }

        double selects = clock.Elapsed.TotalMilliseconds;

        Console.WriteLine($"     {$"{rounds} × PingAsync()",-38} {pings,7:0.0} ms   {pings / rounds,5:0.00} ms each");
        Console.WriteLine($"     {$"{rounds} × ExecuteScalarAsync(\"SELECT 1\")",-38} {selects,7:0.0} ms   {selects / rounds,5:0.00} ms each");
        Console.WriteLine();
        Console.WriteLine("   Against a loopback server the difference is almost all server-side work, because the");
        Console.WriteLine("   round trip costs the same either way: SELECT 1 is parsed, planned, executed, and recorded");
        Console.WriteLine("   in system.query_log, and a Ping is one packet answered by one packet. Across a real");
        Console.WriteLine("   network the round trip dominates both and the gap narrows — the reason to prefer the Ping");
        Console.WriteLine("   is then that a probe every few seconds from every instance leaves no trace in the query");
        Console.WriteLine("   log to read past.");
        Console.WriteLine();
        Console.WriteLine("   A Ping still needs a pool connection, so the first one after a cold start pays for a dial");
        Console.WriteLine("   and a handshake like any other operation.");
    }

    private static async Task ThreeEndpointsOneReport()
    {
        Console.WriteLine("\n2. Registered as a health check, over three endpoints\n");

        var services = new ServiceCollection();
        services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.None));

        // The endpoint that works, registered without a key, exactly as Tcp_003 does.
        services.AddClickHouseTcpDataSource(ExampleConfig.TcpConnectionString);

        // One that nothing is listening on, so its dial is refused at once.
        services.AddClickHouseTcpDataSource(
            ExampleConfig.TcpBuilder().ToOptions() with { Port = 1, DialTimeout = TimeSpan.FromSeconds(2) },
            serviceKey: "unreachable");

        // One whose pool holds a single connection, which a session below will pin — a healthy server that this
        // process cannot reach a connection to.
        services.AddClickHouseTcpDataSource(
            ExampleConfig.TcpBuilder().ToOptions() with
            {
                MaxPoolSize = 1,
                PoolTimeout = TimeSpan.FromMilliseconds(200),
            },
            serviceKey: "saturated");

        services.AddHealthChecks()
            .AddClickHouseTcpPing("clickhouse")
            .AddClickHouseTcpPing("clickhouse-unreachable", serviceKey: "unreachable")
            .AddClickHouseTcpPing("clickhouse-saturated", serviceKey: "saturated");

        await using ServiceProvider provider = services.BuildServiceProvider();

        // Hold the saturated pool's only connection for the duration of the report.
        var saturated = provider.GetRequiredKeyedService<IClickHouseTcpClient>("saturated");
        await using IClickHouseTcpSession pinned = await saturated.OpenSessionAsync();

        var health = provider.GetRequiredService<HealthCheckService>();
        HealthReport report = await health.CheckHealthAsync();

        Console.WriteLine($"   Overall: {report.Status}, in {report.TotalDuration.TotalMilliseconds:0} ms\n");
        foreach ((string name, HealthReportEntry entry) in report.Entries.OrderBy(e => e.Key, StringComparer.Ordinal))
        {
            Console.WriteLine($"     {name,-24} {entry.Status,-9} {entry.Duration.TotalMilliseconds,6:0} ms");
            Console.WriteLine($"       {Trim(entry.Description)}");
            foreach (KeyValuePair<string, object> item in entry.Data)
            {
                Console.WriteLine($"       {item.Key,-10} {item.Value}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   Three states, and the middle one is the point: a pool with nothing free throws");
        Console.WriteLine("   TimeoutException, which says nothing about the server, so reporting it as Unhealthy would");
        Console.WriteLine("   take an instance out of rotation for being busy. Degraded is the honest answer. The");
        Console.WriteLine("   distinction has to be made on the exception type, because there is no status on the");
        Console.WriteLine("   client to ask.");
        Console.WriteLine();
        Console.WriteLine("   Every registration resolves the client the data source owns, so the check runs on the");
        Console.WriteLine("   application's own pool and measures the path a request would take. That is also why the");
        Console.WriteLine("   check must never dispose it (Tcp_003): the pool is shared, and the container owns it.");
        Console.WriteLine();
        Console.WriteLine("   In ASP.NET Core the rest is MapHealthChecks(\"/health\") plus, if you want the endpoints");
        Console.WriteLine("   split, a predicate over the tags each registration carries.");
    }

    private static void WhatAPongProves()
    {
        Console.WriteLine("\n3. What a Pong does and does not prove\n");
        Console.WriteLine("   It proves:");
        Console.WriteLine("     - a connection to the endpoint exists, or could be opened inside DialTimeout;");
        Console.WriteLine("     - the process answering it speaks the native protocol;");
        Console.WriteLine("     - if the pool had to dial, that the credentials were accepted — the handshake");
        Console.WriteLine("       authenticates, so a wrong password fails there rather than at the Ping;");
        Console.WriteLine("     - the server is not so stalled that it cannot answer a packet.");
        Console.WriteLine();
        Console.WriteLine("   It does not prove:");
        Console.WriteLine("     - that a query will succeed. No table is read, no permission is checked, no memory or");
        Console.WriteLine("       concurrency limit is tested, and a server refusing queries still Pongs;");
        Console.WriteLine("     - that the database the client is configured for exists;");
        Console.WriteLine("     - that replication is caught up, or that any part of a cluster beyond this one node is");
        Console.WriteLine("       reachable;");
        Console.WriteLine("     - anything at all about credentials on a warm pool, where the Ping travels over a");
        Console.WriteLine("       connection whose handshake happened minutes ago.");
        Console.WriteLine();
        Console.WriteLine("   So a Ping is a liveness probe. For readiness — should this instance take traffic — pick a");
        Console.WriteLine("   statement that touches what the service actually needs (SELECT 1, or a count against one");
        Console.WriteLine("   table), accept that it costs a query, and run it far less often.");
    }

    private static string Trim(string? description)
        => description is null ? "(no description)"
        : description.Length <= 100 ? description
        : description[..100] + "...";

    /// <summary>
    /// Registers a health check that Pings whichever registration <paramref name="serviceKey"/> names. Kept in
    /// this example rather than shipped by the driver, so that the mapping from exception to
    /// <see cref="HealthStatus"/> stays the application's decision.
    /// </summary>
    /// <param name="builder">The health check builder.</param>
    /// <param name="name">The name the entry appears under in the report.</param>
    /// <param name="serviceKey">The keyed registration to check, or null for the unkeyed one.</param>
    /// <returns>The builder, for chaining.</returns>
    private static IHealthChecksBuilder AddClickHouseTcpPing(
        this IHealthChecksBuilder builder,
        string name,
        string? serviceKey = null)
        => builder.Add(new HealthCheckRegistration(
            name,
            provider => new TcpPingHealthCheck(serviceKey is null
                ? provider.GetRequiredService<IClickHouseTcpClient>()
                : provider.GetRequiredKeyedService<IClickHouseTcpClient>(serviceKey)),
            failureStatus: HealthStatus.Unhealthy,
            tags: ["clickhouse", "native"]));

    /// <summary>
    /// Takes the interface rather than the concrete client, and never disposes it: the container owns the pool.
    /// </summary>
    private sealed class TcpPingHealthCheck(IClickHouseTcpClient client) : IHealthCheck
    {
        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            var clock = Stopwatch.StartNew();

            // ToString() on the options is the only public rendering that resolves the port, and it is written to
            // be safe to log — it leaves the password out.
            var data = new Dictionary<string, object> { ["endpoint"] = client.Options.ToString() };

            try
            {
                await client.PingAsync(cancellationToken);
                data["pong_ms"] = Math.Round(clock.Elapsed.TotalMilliseconds, 1);
                return HealthCheckResult.Healthy("Pong", data);
            }
            catch (TimeoutException e)
            {
                // No connection came free within PoolTimeout. The server has not been reached, so this is a
                // statement about this process, not about ClickHouse.
                return HealthCheckResult.Degraded("No pooled connection was free", e, data);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                return new HealthCheckResult(context.Registration.FailureStatus, e.Message, e, data);
            }
        }
    }
}
