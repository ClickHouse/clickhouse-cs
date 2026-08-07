using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Benchmark.Blog.Infrastructure;

/// <summary>
/// Everything the harnesses need from the outside world, in one place: where the server is, which
/// database the staged datasets live in, and the confounder set that has to be published alongside
/// any number this suite produces.
/// </summary>
/// <remarks>
/// <para>
/// Every knob is an environment variable so a run is reproducible from a shell history line, and so
/// the same binary can be pointed at the local box and at a cloud endpoint without a rebuild.
/// </para>
/// <para>
/// <b>Environment variables</b>
/// </para>
/// <list type="table">
///   <item><term>CLICKHOUSE_CONNECTION</term><description>DSN. Default <c>Host=localhost;Port=8124</c>,
///     the dedicated benchmark container (see <c>env/docker-compose.yml</c>) rather than port 8123,
///     which on a dev box is usually some other project's shared server.</description></item>
///   <item><term>BENCH_DATASET_DB</term><description>Database holding <c>hits</c> and
///     <c>types_wide</c>. Default <c>bench</c>.</description></item>
///   <item><term>BENCH_SINK_DB</term><description>Database for insert targets. Default
///     <c>bench</c>.</description></item>
///   <item><term>BENCH_ENV_LABEL</term><description>Free-text environment tag recorded in results,
///     e.g. <c>L-ec2-4c</c> or <c>C-aws-us-east-1</c>. Default <c>unlabelled</c>.</description></item>
/// </list>
/// </remarks>
internal static class BenchEnv
{
    /// <summary>Dedicated benchmark server, deliberately not 8123. See the class remarks.</summary>
    public const string DefaultConnection = "Host=localhost;Port=8124";

    public static string ConnectionString =>
        Get("CLICKHOUSE_CONNECTION", DefaultConnection);

    /// <summary>Database holding the staged read datasets (<c>hits</c>, <c>types_wide</c>).</summary>
    public static string DatasetDb => Get("BENCH_DATASET_DB", "bench");

    /// <summary>Database for insert sinks. Separate knob so a read-only dataset can be shared.</summary>
    public static string SinkDb => Get("BENCH_SINK_DB", "bench");

    /// <summary>Free-text environment tag (§5 of the plan: L = local, C = cloud same-region).</summary>
    public static string EnvLabel => Get("BENCH_ENV_LABEL", "unlabelled");

    public static string Hits => $"{DatasetDb}.hits";

    public static string TypesWide => $"{DatasetDb}.types_wide";

    /// <summary>
    /// A client on the configured endpoint. Callers own the lifetime; a benchmark should build one in
    /// <c>[GlobalSetup]</c> and dispose it in <c>[GlobalCleanup]</c> so connection-pool state is not
    /// rebuilt per iteration (which would turn every measurement into a cold-start measurement).
    /// </summary>
    public static ClickHouseClient CreateClient() => new(ConnectionString);

    public static ClickHouseClient CreateClient(ClickHouseClientSettings settings) => new(settings);

    /// <summary>
    /// The confounder set from §8 of the benchmark plan, resolved at runtime in the process that is
    /// actually doing the measuring.
    /// </summary>
    /// <remarks>
    /// This must run in the benchmark's own process, not the BenchmarkDotNet host: GC mode is a
    /// per-job property applied to the child, so a host-side reading of
    /// <see cref="GCSettings.IsServerGC"/> would report the host's mode for every job and quietly
    /// mislabel half the runs.
    /// </remarks>
    public static async Task<IReadOnlyList<KeyValuePair<string, string>>> DescribeAsync()
    {
        var rows = new List<KeyValuePair<string, string>>
        {
            new("env_label", EnvLabel),
            new("connection", Redact(ConnectionString)),
            new("dataset_db", DatasetDb),
            new("sink_db", SinkDb),
            new("dotnet", RuntimeInformation.FrameworkDescription),
            new("os", RuntimeInformation.OSDescription),
            new("arch", RuntimeInformation.ProcessArchitecture.ToString()),
            new("logical_cores", Environment.ProcessorCount.ToString(CultureInfo.InvariantCulture)),
            // Server GC scales its heap count with core count, so both belong in the published header.
            new("gc_mode", GCSettings.IsServerGC ? "Server" : "Workstation"),
            new("gc_latency_mode", GCSettings.LatencyMode.ToString()),
            new("gc_heap_count", GCHeapCount()),
        };

        try
        {
            using var client = CreateClient();
            rows.Add(new("server_version", (await client.ExecuteScalarAsync("SELECT version()"))?.ToString() ?? "?"));
            rows.Add(new("server_cores", (await client.ExecuteScalarAsync(
                "SELECT value FROM system.settings WHERE name = 'max_threads'"))?.ToString() ?? "?"));
            rows.Add(new("http_zlib_compression_level", (await client.ExecuteScalarAsync(
                "SELECT value FROM system.settings WHERE name = 'http_zlib_compression_level'"))?.ToString() ?? "?"));

            foreach (var table in new[] { "hits", "types_wide" })
            {
                var count = await client.ExecuteScalarAsync(
                    $"SELECT sum(rows) FROM system.parts WHERE active AND database = '{DatasetDb}' AND table = '{table}'");
                rows.Add(new($"{DatasetDb}.{table}.rows", count is null or DBNull ? "absent" : count.ToString()));
            }
        }
        catch (Exception ex)
        {
            // Reporting the environment must never be the thing that fails a run; an unreachable
            // server is diagnosed by the benchmark itself with a much better message.
            rows.Add(new("server_probe_error", ex.GetType().Name + ": " + ex.Message));
        }

        return rows;
    }

    /// <summary>
    /// Server GC's heap count, which is what actually differs between the two GC arms. Exposed as a
    /// config knob (<c>GCHeapCount</c>) rather than an API, so read it back from the runtime config.
    /// </summary>
    private static string GCHeapCount()
    {
        if (!GCSettings.IsServerGC)
            return "1 (workstation)";

        var configured = AppContext.GetData("System.GC.HeapCount")?.ToString();
        return string.IsNullOrEmpty(configured)
            ? $"{Environment.ProcessorCount} (default = logical cores)"
            : configured;
    }

    /// <summary>Strips credentials so a DSN can be written into a results file that gets shared.</summary>
    public static string Redact(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
            return connectionString;

        var parts = connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries)
            .Select(part =>
            {
                var eq = part.IndexOf('=');
                if (eq < 0)
                    return part;

                var key = part[..eq].Trim();
                return key.Equals("Password", StringComparison.OrdinalIgnoreCase)
                    || key.Equals("BearerToken", StringComparison.OrdinalIgnoreCase)
                    ? $"{key}=***"
                    : part;
            });

        return string.Join(';', parts);
    }

    private static string Get(string name, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? fallback : value;
    }

    /// <summary>Reads a non-negative int from the environment, falling back when unset or invalid.</summary>
    public static int GetInt(string name, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) && value >= 0
            ? value
            : fallback;
    }

    /// <summary>Reads a boolean-ish flag: <c>1</c>/<c>true</c>/<c>yes</c> (case-insensitive) are true.</summary>
    public static bool GetFlag(string name, bool fallback = false)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(raw))
            return fallback;

        return raw.Trim() is "1" or "true" or "True" or "TRUE" or "yes" or "YES" or "on" or "ON";
    }
}
