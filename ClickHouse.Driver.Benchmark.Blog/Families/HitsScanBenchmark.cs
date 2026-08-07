using System;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace ClickHouse.Driver.Benchmark.Blog.Families;

/// <summary>
/// <b>Family C — content-bound, read side.</b> One big scan of <c>bench.hits</c>: per-row and per-byte
/// decode cost.
/// </summary>
/// <remarks>
/// <para>
/// Two shapes, because the per-row cost and the per-column cost are different claims.
/// <see cref="WideScan"/> reads all 105 columns — the shape where per-value decode work dominates.
/// <see cref="NarrowScan"/> reads 10, which is what an application actually selects, and is where a
/// fixed per-row cost stops being amortised across a hundred columns.
/// </para>
/// <para>
/// <b>Read the allocation column, not the wall clock — on loopback.</b> On a local server the
/// transport is nearly free and the wall-clock difference between arms sits inside the noise. That is
/// not a reason to skip the local run: allocation and GC behaviour are exactly what the local
/// environment isolates. The wall-clock claim belongs to the cloud run.
/// </para>
/// <para>
/// Server cost is recorded per iteration, so a scan whose wall clock moved can be checked against the
/// server's own CPU accounting before anyone attributes the change to the driver.
/// </para>
/// </remarks>
[Config(typeof(LongRunConfig))]
public class HitsScanBenchmark
{
    private const string Name = nameof(HitsScanBenchmark);

    /// <summary>Ten columns an application would plausibly select, spanning four decode families.</summary>
    private const string NarrowColumns =
        "WatchID, UserID, EventTime, EventDate, CounterID, RegionID, URL, Title, ResolutionWidth, IsMobile";

    private readonly Consumer consumer = new();
    private readonly DeferredServerCost serverCost = new();
    private ClickHouseClient client;

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchEnv.CreateClient();

        var rows = await client.ExecuteScalarAsync($"SELECT count() FROM {BenchEnv.Hits}");
        var count = rows is null or DBNull ? 0 : Convert.ToInt64(rows, CultureInfo.InvariantCulture);
        if (count < BenchProfile.ContentRows)
        {
            throw new InvalidOperationException(
                $"{BenchEnv.Hits} has {count} rows but the profile asks for {BenchProfile.ContentRows}. " +
                $"Run scripts/stage-datasets.sh, or lower BENCH_CONTENT_ROWS. Silently scanning fewer rows " +
                $"would make this arm incomparable with every other one.");
        }
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [IterationSetup]
    public void IterationSetup() => SideMetrics.NextIteration();

    /// <summary>
    /// Server-cost lookups happen here, not in the benchmark methods: a SYSTEM FLUSH LOGS and its
    /// retry delay inside a timed region would be reported as part of the scan.
    /// </summary>
    [IterationCleanup]
    public void IterationCleanup() => serverCost.Drain(client);

    /// <summary>All 105 columns: per-value decode cost at its most exposed.</summary>
    [Benchmark(Baseline = true)]
    public Task<long> WideScan() => Scan("*", nameof(WideScan));

    /// <summary>Ten columns: the realistic shape, where per-row fixed cost matters most.</summary>
    [Benchmark]
    public Task<long> NarrowScan() => Scan(NarrowColumns, nameof(NarrowScan));

    /// <summary>
    /// Ten columns, every value actually materialized through the typed accessors. <see cref="NarrowScan"/>
    /// only advances the reader, so it measures decode; this measures decode plus access, which is
    /// what separates "the row got cheaper" from "reading a value got cheaper".
    /// </summary>
    [Benchmark]
    public async Task<long> NarrowScanTyped()
    {
        var queryId = ServerMetrics.NewQueryId(nameof(NarrowScanTyped));
        var cpu = CpuProbe.Start();
        long rows = 0;

        using (var reader = await client.ExecuteReaderAsync(
            $"SELECT {NarrowColumns} FROM {BenchEnv.Hits} LIMIT {BenchProfile.ContentRows}",
            options: new QueryOptions { QueryId = queryId }))
        {
            while (reader.Read())
            {
                consumer.Consume(reader.GetInt64(0));
                consumer.Consume(reader.GetInt64(1));
                consumer.Consume(reader.GetDateTime(2));
                consumer.Consume(reader.GetDateTime(3));
                consumer.Consume(reader.GetInt32(4));
                consumer.Consume(reader.GetInt32(5));
                consumer.Consume(reader.GetString(6));
                consumer.Consume(reader.GetString(7));
                consumer.Consume(reader.GetInt16(8));
                consumer.Consume(reader.GetInt16(9));
                rows++;
            }
        }

        Record(nameof(NarrowScanTyped), queryId, rows, cpu);
        return rows;
    }

    private async Task<long> Scan(string columns, string arm)
    {
        var queryId = ServerMetrics.NewQueryId(arm);
        var cpu = CpuProbe.Start();
        long rows = 0;

        using (var reader = await client.ExecuteReaderAsync(
            $"SELECT {columns} FROM {BenchEnv.Hits} LIMIT {BenchProfile.ContentRows}",
            options: new QueryOptions { QueryId = queryId }))
        {
            while (reader.Read())
                rows++;
        }

        Record(arm, queryId, rows, cpu);
        return rows;
    }

    /// <summary>
    /// Records the client-side metrics (all cheap, all in-process) and queues the server-side lookup
    /// for <see cref="IterationCleanup"/>. Nothing here does I/O.
    /// </summary>
    private void Record(string arm, string queryId, long rows, CpuProbe cpu)
    {
        var elapsed = cpu.ElapsedMicroseconds;
        var args = string.Create(CultureInfo.InvariantCulture, $"arm={arm};rows={BenchProfile.ContentRows}");

        SideMetrics.Record(Name, args, "rows", rows);
        SideMetrics.Record(Name, args, "client_cpu_us", cpu.TotalMicroseconds);
        SideMetrics.Record(Name, args, "elapsed_us", elapsed);
        SideMetrics.Record(Name, args, "rows_per_second", elapsed > 0 ? rows / (elapsed / 1_000_000d) : 0);

        serverCost.Enqueue(Name, args, queryId);
    }
}
