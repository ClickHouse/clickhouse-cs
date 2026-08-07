using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime;
using System.Text;
using System.Threading;

namespace ClickHouse.Driver.Benchmark.Blog.Infrastructure;

/// <summary>
/// Side channel for the metrics BenchmarkDotNet's summary table cannot carry: server CPU, client CPU,
/// bytes on the wire, compression ratios, connection-pool state.
/// </summary>
/// <remarks>
/// <para>
/// BenchmarkDotNet reports wall clock and (via <c>MemoryDiagnoser</c>) allocated bytes and
/// per-generation collection counts. The codec matrix needs four numbers per cell and the
/// overhead-bound family needs pool health, so the rest is appended here as CSV and joined to
/// BenchmarkDotNet's own <c>*-report.csv</c> / <c>*-measurements.csv</c> during post-processing.
/// </para>
/// <para>
/// <b>Long format on purpose.</b> One row per metric sample rather than one row per iteration with a
/// fixed set of columns: harnesses keep adding metrics, and a wide schema would either churn or grow
/// a forest of empty columns. Pivot in the analysis step.
/// </para>
/// <para>
/// Rows are written by the benchmark's own child process, which is the only place that knows the
/// resolved GC mode and the referenced driver version — the BenchmarkDotNet host process would
/// mislabel both for every job but its own.
/// </para>
/// </remarks>
internal static class SideMetrics
{
    private const string Header = "run_id,ts,pid,env_label,profile,gc_mode,driver_version,benchmark,args,iteration,metric,value";

    /// <summary>
    /// This process's id. It is the join key between this CSV and <c>gc-summary.csv</c>: a
    /// <c>.nettrace</c> carries the pid but not the BenchmarkDotNet job, and GC mode cannot be
    /// recovered from the trace itself (TraceEvent reports <c>IsServerGCUsed = -1</c> for EventPipe
    /// traces), so pid is how a trace is attributed to a GC arm at all.
    /// </summary>
    private static readonly int ProcessId = Environment.ProcessId;

    private static readonly object Gate = new();
    private static readonly string RunId =
        Environment.GetEnvironmentVariable("BENCH_RUN_ID") is { Length: > 0 } id ? id : "adhoc";

    private static readonly Lazy<string> Path = new(ResolvePath);
    private static readonly Lazy<string> DriverVersion = new(ResolveDriverVersion);

    /// <summary>Per-benchmark-process iteration counter, so samples can be aligned with BDN measurements.</summary>
    private static int iteration;

    /// <summary>Where rows land. <c>BENCH_METRICS_CSV</c>, else <c>results/side-metrics.csv</c>.</summary>
    public static string File => Path.Value;

    /// <summary>
    /// Version of the <c>ClickHouse.Driver</c> assembly actually loaded, which is how a package arm is
    /// told apart from the working tree without trusting an environment variable to have been set.
    /// </summary>
    public static string ResolvedDriverVersion => DriverVersion.Value;

    /// <summary>Advances the iteration counter. Call from <c>[IterationSetup]</c>.</summary>
    public static int NextIteration() => Interlocked.Increment(ref iteration);

    /// <summary>Current iteration index, or 0 before the first one.</summary>
    public static int CurrentIteration => Volatile.Read(ref iteration);

    /// <summary>Records one sample.</summary>
    /// <param name="benchmark">Harness name, e.g. <c>SmallQueryOverhead</c>.</param>
    /// <param name="args">The parameter combination, e.g. <c>conc=32;codec=zstd</c>.</param>
    /// <param name="metric">Metric name, e.g. <c>server_cpu_us</c>.</param>
    /// <param name="value">Metric value.</param>
    public static void Record(string benchmark, string args, string metric, double value)
    {
        var line = string.Create(
            CultureInfo.InvariantCulture,
            $"{Csv(RunId)},{DateTime.UtcNow:O},{ProcessId},{Csv(BenchEnv.EnvLabel)},{Csv(BenchProfile.IsSmoke ? "smoke" : "full")},{(GCSettings.IsServerGC ? "Server" : "Workstation")},{Csv(ResolvedDriverVersion)},{Csv(benchmark)},{Csv(args)},{CurrentIteration},{Csv(metric)},{value:R}");

        Append(line);
    }

    /// <summary>Records every field of a <see cref="ServerCost"/> under a stable metric naming scheme.</summary>
    public static void Record(string benchmark, string args, in ServerCost cost)
    {
        Record(benchmark, args, "server_queries", cost.Queries);
        Record(benchmark, args, "server_cpu_us", cost.CpuMicroseconds);
        Record(benchmark, args, "server_cpu_wait_us", cost.CpuWaitMicroseconds);
        Record(benchmark, args, "server_cpu_wait_ratio", cost.CpuWaitRatio);
        Record(benchmark, args, "server_net_send_bytes", cost.NetworkSendBytes);
        Record(benchmark, args, "server_net_recv_bytes", cost.NetworkReceiveBytes);
        Record(benchmark, args, "server_read_rows", cost.ReadRows);
        Record(benchmark, args, "server_read_bytes", cost.ReadBytes);
        Record(benchmark, args, "server_result_bytes", cost.ResultBytes);
        Record(benchmark, args, "server_memory_usage", cost.MemoryUsage);
        Record(benchmark, args, "server_duration_ms", cost.DurationMilliseconds);
    }

    private static void Append(string line)
    {
        // Launches are sequential and BenchmarkDotNet runs one child at a time, but a harness may
        // record from several tasks inside one iteration, and a retry costs less than a lost sample.
        lock (Gate)
        {
            for (var attempt = 0; attempt < 5; attempt++)
            {
                try
                {
                    var path = Path.Value;
                    var directory = System.IO.Path.GetDirectoryName(path);
                    if (!string.IsNullOrEmpty(directory))
                        Directory.CreateDirectory(directory);

                    var needsHeader = !System.IO.File.Exists(path) || new FileInfo(path).Length == 0;
                    using var stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
                    using var writer = new StreamWriter(stream, new UTF8Encoding(false));
                    if (needsHeader)
                        writer.WriteLine(Header);

                    writer.WriteLine(line);
                    return;
                }
                catch (IOException)
                {
                    Thread.Sleep(20 * (attempt + 1));
                }
            }
        }
    }

    private static string ResolvePath()
    {
        var configured = Environment.GetEnvironmentVariable("BENCH_METRICS_CSV");
        return string.IsNullOrWhiteSpace(configured)
            ? System.IO.Path.Combine(Directory.GetCurrentDirectory(), "results", "side-metrics.csv")
            : configured;
    }

    private static string ResolveDriverVersion()
    {
        var assembly = typeof(ClickHouseClient).Assembly;
        var informational = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

        // Source builds carry a git hash after a '+'; keep it, it identifies the "new" arm exactly.
        return informational ?? assembly.GetName().Version?.ToString() ?? "unknown";
    }

    private static string Csv(string value)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;

        return value.Contains(',', StringComparison.Ordinal)
            || value.Contains('"', StringComparison.Ordinal)
            || value.Contains('\n', StringComparison.Ordinal)
            ? '"' + value.Replace("\"", "\"\"", StringComparison.Ordinal) + '"'
            : value;
    }
}

/// <summary>
/// Client-side CPU across a measured region, for the codec matrix's "client CPU" column.
/// </summary>
/// <remarks>
/// Process-wide, because that is the only CPU accounting the runtime offers cheaply and it is what
/// "what did this cost my application host" means. Inside a BenchmarkDotNet child during the timed
/// region the process is running the workload and nothing else, so the attribution is clean — but it
/// does include the runtime's own threads (GC, timer, thread pool), which is the honest number for a
/// codec comparison and is <i>not</i> the same thing as the benchmark method's exclusive CPU.
/// </remarks>
internal struct CpuProbe
{
    private TimeSpan startTotal;
    private TimeSpan startUser;
    private long startTimestamp;

    public static CpuProbe Start()
    {
        using var process = Process.GetCurrentProcess();
        return new CpuProbe
        {
            startTotal = process.TotalProcessorTime,
            startUser = process.UserProcessorTime,
            startTimestamp = Stopwatch.GetTimestamp(),
        };
    }

    /// <summary>Total (user + kernel) process CPU consumed since <see cref="Start"/>.</summary>
    public double TotalMicroseconds
    {
        get
        {
            using var process = Process.GetCurrentProcess();
            return (process.TotalProcessorTime - startTotal).TotalMicroseconds;
        }
    }

    /// <summary>User-mode process CPU consumed since <see cref="Start"/>.</summary>
    public double UserMicroseconds
    {
        get
        {
            using var process = Process.GetCurrentProcess();
            return (process.UserProcessorTime - startUser).TotalMicroseconds;
        }
    }

    /// <summary>Wall clock since <see cref="Start"/>, for a CPU-per-second ratio.</summary>
    public double ElapsedMicroseconds =>
        Stopwatch.GetElapsedTime(startTimestamp).TotalMicroseconds;
}
