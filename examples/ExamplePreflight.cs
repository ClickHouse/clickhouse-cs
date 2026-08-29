using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Reaches the server once before any example runs, so that an unreachable or misconfigured endpoint
/// is reported as itself rather than as a failure inside whichever example happened to run first.
/// </summary>
/// <remarks>
/// A failure exits non-zero instead of skipping. CI runs the whole suite with no filter, and a skip
/// would leave the run green while nothing had been exercised.
/// </remarks>
public static class ExamplePreflight
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Checks the endpoints the given examples need, and reports what to fix if one is unreachable.
    /// </summary>
    /// <param name="examples">The examples about to run. Only their transports are checked.</param>
    /// <returns>True when every needed endpoint answered.</returns>
    public static Task<bool> CheckAsync(IEnumerable<ExampleRunner.ExampleInfo> examples)
        => CheckAsync(examples.SelectMany(e => e.RequiredTransports).Distinct().ToArray());

    /// <summary>
    /// Checks the named endpoints, and reports what to fix if one is unreachable.
    /// </summary>
    /// <param name="transports">The transports to check. Duplicates are checked once.</param>
    /// <returns>True when every named endpoint answered.</returns>
    public static async Task<bool> CheckAsync(params ExampleTransport[] transports)
    {
        var ok = true;

        foreach (var transport in transports.Distinct().OrderBy(t => t))
        {
            string? failure = transport == ExampleTransport.Http
                ? await CheckHttpAsync()
                : await CheckTcpAsync();

            if (failure is not null)
            {
                Report(transport, failure);
                ok = false;
            }
        }

        return ok;
    }

    private static async Task<string?> CheckHttpAsync()
    {
        try
        {
            using var cancellation = new CancellationTokenSource(Timeout);
            using var client = ExampleConfig.CreateHttpClient();
            await client.ExecuteScalarAsync("SELECT 1", cancellationToken: cancellation.Token);
            return null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    private static async Task<string?> CheckTcpAsync()
    {
        try
        {
            using var cancellation = new CancellationTokenSource(Timeout);
            await using var client = ExampleConfig.CreateTcpClient();
            await client.PingAsync(cancellation.Token);
            return null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    private static void Report(ExampleTransport transport, string failure)
    {
        // Reported from the effective endpoint, so that a whole-string override is described as itself
        // rather than as whatever the component variables say.
        var http = ExampleConfig.HttpEndpoint;
        var tcp = ExampleConfig.TcpEndpoint;

        var (name, endpoint, user, port, source) = transport == ExampleTransport.Http
            ? ("HTTP interface", $"{http.Host}:{http.Port}", ExampleConfig.HttpBuilder().Username, "CLICKHOUSE_HTTP_PORT", "CLICKHOUSE_HTTP_CONNECTION_STRING")
            : ("native protocol", $"{tcp.Host}:{tcp.Port}", ExampleConfig.TcpBuilder().Username, "CLICKHOUSE_TCP_PORT", "CLICKHOUSE_TCP_CONNECTION_STRING");

        Console.WriteLine();
        Console.WriteLine($"Cannot reach ClickHouse on the {name} at {endpoint} as user '{user}'.");
        Console.WriteLine($"  {failure}");
        Console.WriteLine();

        if (transport == ExampleTransport.Tcp)
        {
            Console.WriteLine($"  The native protocol listens on port 9000 by default, not on the HTTP port ({http.Port}).");
            Console.WriteLine();
        }

        Console.WriteLine("  Start a server with both ports published:");
        Console.WriteLine("    docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server");
        Console.WriteLine();
        Console.WriteLine("  Or point the examples somewhere else:");
        Console.WriteLine($"    CLICKHOUSE_HOST, {port}, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE");
        Console.WriteLine($"    {source} replaces the whole connection string.");
        Console.WriteLine();
    }
}
