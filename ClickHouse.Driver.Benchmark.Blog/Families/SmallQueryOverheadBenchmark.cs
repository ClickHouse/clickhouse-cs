using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Benchmark.Blog.Infrastructure;

namespace ClickHouse.Driver.Benchmark.Blog.Families;

/// <summary>
/// <b>Family O — overhead-bound.</b> Many small queries, with concurrency, sustained long enough that
/// per-query fixed costs compound into something visible.
/// </summary>
/// <remarks>
/// <para>
/// One BenchmarkDotNet iteration is one sustained burst of <see cref="BenchProfile.BurstSeconds"/>
/// seconds at a fixed concurrency, not one query. That is the whole design: the two effects this
/// family exists to show only appear over time.
/// </para>
/// <list type="number">
///   <item>
///     <b>Leaked response objects degrade the connection pool.</b> An undisposed
///     <c>HttpResponseMessage</c> holds its pooled connection checked out until finalization, which is
///     invisible at concurrency 1 for three seconds and obvious at concurrency 64 for a minute. The
///     evidence is <c>pool_peak_http11_connections_current_total</c>, not latency —
///     see <see cref="HttpPoolCounters"/>.
///   </item>
///   <item>
///     <b>A per-query large-object allocation compounds into fragmentation.</b> The 512 KiB
///     per-query response buffer was above the 85,000-byte LOH threshold, so every single query put
///     an object on a heap that is only collected with gen2 and is not compacted by default. A
///     sustained run is the only place that becomes Gen2 pauses instead of a footnote. Requires
///     <c>BENCH_GC_TRACE=1</c> and <c>gc-report</c> to see; requires <c>GcForce=false</c> to exist at
///     all, which <see cref="LongRunConfig"/> sets.
///   </item>
/// </list>
/// <para>
/// <b>Two workload shapes.</b> <see cref="SelectOne"/> is the pure fixed-cost floor: no storage, no
/// decode worth the name. <see cref="PointLookup"/> hits a real MergeTree so the fixed cost is
/// measured next to a realistic minimum of server work — a fixed-cost win that only shows up against
/// <c>SELECT 1</c> is not a win any user will notice.
/// </para>
/// <para>
/// <b>The mandatory honesty check.</b> <see cref="SelectOneUncompressed"/> runs the same tiny-response
/// workload with compression disabled. Compression is not free at small sizes, and zstd is now the
/// default both ways. If the new default makes small queries slower, this is the arm that says so, and
/// it gets published.
/// </para>
/// <para>
/// Reported wall clock is the burst duration, which is fixed by construction — so it is <i>not</i> the
/// metric. Read <c>queries_per_second</c> and the latency percentiles from the side-channel CSV.
/// </para>
/// </remarks>
[Config(typeof(LongRunConfig))]
public class SmallQueryOverheadBenchmark
{
    private const string Name = nameof(SmallQueryOverheadBenchmark);

    /// <summary>
    /// Concurrency levels from §1 of the plan. 1 isolates per-query cost; 64 on a 4-core box is
    /// deliberately past saturation, which is where pool and GC pathologies surface.
    /// </summary>
    [Params(1, 8, 32, 64)]
    public int Concurrency { get; set; }

    private ClickHouseClient client;
    private ClickHouseClient uncompressedClient;
    private HttpPoolCounters poolCounters;
    private LatencyHistogram[] histograms;
    private LatencyHistogram merged;
    private long pointLookupCounterId;

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchEnv.CreateClient();

        // UseCompression=false is the control arm for the compression-latency question. Everything
        // else about the two clients is identical.
        uncompressedClient = BenchEnv.CreateClient(new ClickHouseClientSettings(BenchEnv.ConnectionString)
        {
            UseCompression = false,
        });

        // Per-worker histograms, allocated once. Allocating these inside the burst would show up as
        // the allocation the burst is trying to measure.
        histograms = Enumerable.Range(0, Concurrency).Select(_ => new LatencyHistogram()).ToArray();
        merged = new LatencyHistogram();

        poolCounters = new HttpPoolCounters();

        // A CounterID that actually exists, so the point lookup reads a real granule rather than
        // matching nothing and measuring an empty result.
        var counterId = await client.ExecuteScalarAsync(
            $"SELECT CounterID FROM {BenchEnv.Hits} ORDER BY CounterID LIMIT 1");
        if (counterId is null or DBNull)
        {
            throw new InvalidOperationException(
                $"{BenchEnv.Hits} is empty or missing. Run scripts/stage-datasets.sh first — a point " +
                $"lookup against an absent table would measure error handling, not the driver.");
        }

        pointLookupCounterId = Convert.ToInt64(counterId, CultureInfo.InvariantCulture);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        poolCounters?.Dispose();
        uncompressedClient?.Dispose();
        client?.Dispose();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        SideMetrics.NextIteration();
        poolCounters.Reset();
        merged.Reset();
        foreach (var histogram in histograms)
            histogram.Reset();
    }

    /// <summary>The fixed-cost floor: smallest possible response, no storage touched.</summary>
    [Benchmark(Baseline = true)]
    public Task<long> SelectOne() => Burst(client, "SELECT 1", nameof(SelectOne));

    /// <summary>The same, with compression off — the honesty check on the new zstd default.</summary>
    [Benchmark]
    public Task<long> SelectOneUncompressed() =>
        Burst(uncompressedClient, "SELECT 1", nameof(SelectOneUncompressed));

    /// <summary>Fixed cost measured against a realistic minimum of real server work.</summary>
    [Benchmark]
    public Task<long> PointLookup() => Burst(
        client,
        $"SELECT WatchID, EventTime, URL FROM {BenchEnv.Hits} WHERE CounterID = {pointLookupCounterId} LIMIT 1",
        nameof(PointLookup));

    /// <summary>
    /// Runs <paramref name="sql"/> from <see cref="Concurrency"/> workers until the burst deadline,
    /// returning the total query count.
    /// </summary>
    private async Task<long> Burst(ClickHouseClient target, string sql, string arm)
    {
        var burst = TimeSpan.FromSeconds(BenchProfile.BurstSeconds);
        var started = Stopwatch.GetTimestamp();
        var deadline = started + (long)(burst.TotalSeconds * Stopwatch.Frequency);
        var cpu = CpuProbe.Start();

        var workers = new Task<long>[Concurrency];
        for (var i = 0; i < workers.Length; i++)
        {
            var histogram = histograms[i];
            workers[i] = Task.Run(() => WorkerLoop(target, sql, deadline, histogram));
        }

        var perWorker = await Task.WhenAll(workers);
        var total = perWorker.Sum();
        var elapsed = Stopwatch.GetElapsedTime(started);

        foreach (var histogram in histograms)
            merged.Add(histogram);

        var args = string.Create(CultureInfo.InvariantCulture, $"arm={arm};conc={Concurrency}");

        SideMetrics.Record(Name, args, "queries", total);
        SideMetrics.Record(Name, args, "burst_seconds", elapsed.TotalSeconds);
        SideMetrics.Record(Name, args, "queries_per_second", elapsed.TotalSeconds > 0 ? total / elapsed.TotalSeconds : 0);
        SideMetrics.Record(Name, args, "client_cpu_us", cpu.TotalMicroseconds);
        SideMetrics.Record(Name, args, "client_cpu_per_query_us", total > 0 ? cpu.TotalMicroseconds / total : 0);
        // CPU cores' worth of client work: > core count means the client itself is the bottleneck and
        // the server-side numbers from this arm describe a starved server.
        SideMetrics.Record(Name, args, "client_cpu_cores", elapsed.TotalMicroseconds > 0 ? cpu.TotalMicroseconds / elapsed.TotalMicroseconds : 0);

        // Slowest-worker vs fastest-worker query count. A wide spread at high concurrency means
        // workers were not sharing the pool evenly, which is itself the finding.
        SideMetrics.Record(Name, args, "worker_queries_min", perWorker.Min());
        SideMetrics.Record(Name, args, "worker_queries_max", perWorker.Max());

        merged.RecordTo(Name, args, "latency");
        poolCounters.RecordTo(Name, args);

        return total;
    }

    private static async Task<long> WorkerLoop(ClickHouseClient target, string sql, long deadline, LatencyHistogram histogram)
    {
        long count = 0;

        while (Stopwatch.GetTimestamp() < deadline)
        {
            var queryStarted = Stopwatch.GetTimestamp();

            // ExecuteScalarAsync is the smallest complete round trip the driver offers: request URI
            // construction, send, response read, decode, disposal. All of #457, #451, #492 are on it.
            await target.ExecuteScalarAsync(sql);

            histogram.Record((long)Stopwatch.GetElapsedTime(queryStarted).TotalMicroseconds);
            count++;
        }

        return count;
    }
}
