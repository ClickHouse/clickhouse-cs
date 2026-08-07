using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace ClickHouse.Driver.Benchmark.Blog.Families;

/// <summary>
/// <b>Family C — content-bound, write side.</b> One big <c>hits</c>-shaped insert: per-row and per-byte
/// serialization cost.
/// </summary>
/// <remarks>
/// <para>
/// Full 105-column width in both arms, streamed rather than pre-materialized: a million
/// pre-built <c>object[]</c> rows of 105 boxed values is several gigabytes, and holding it would make
/// the measurement about the setup. Streaming also matches how callers actually insert.
/// </para>
/// <para>
/// <b>Two batch sizes, and the reason matters.</b> Per-batch costs — allocating a writer, allocating a
/// compression buffer, building a request — scale with batch <i>count</i>, not row count. At the
/// default 100k batch a 1M-row insert is 10 batches and those costs are invisible; at 10k it is 100
/// batches and they are the story. Reporting only the default size would hide the change.
/// </para>
/// <para>
/// <b>Two sinks, for two different claims.</b> <c>ENGINE = Null</c> isolates client cost: the server
/// parses the block and drops it, so no sorting, no compression, no part writing. That is the arm for
/// any statement about serialization — and it must be labelled as such, because it is not what an
/// insert costs in production. The MergeTree arm is the credibility check, truncated between
/// iterations so part accumulation does not make later iterations look slower than earlier ones.
/// </para>
/// <para>
/// The POCO arm needs <c>RegisterBinaryInsertType&lt;T&gt;</c>, which shipped in v1.2.0 — so it
/// compiles out of a v1.0.0 comparison arm. That is the corridor working as intended, not a gap.
/// </para>
/// </remarks>
[Config(typeof(LongRunConfig))]
public class HitsInsertBenchmark
{
    private const string Name = nameof(HitsInsertBenchmark);

    private readonly DeferredServerCost serverCost = new();
    private ClickHouseClient client;
    private string nullSink;
    private string mergeTreeSink;

    /// <summary>
    /// Default (100k) and a small value. See the remarks: per-batch costs scale with batch count.
    /// </summary>
    [Params(100_000, 10_000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchEnv.CreateClient();
        nullSink = $"{BenchEnv.SinkDb}.hits_sink_null";
        mergeTreeSink = $"{BenchEnv.SinkDb}.hits_sink_mt";

        foreach (var table in new[] { nullSink, mergeTreeSink })
        {
            var exists = await client.ExecuteScalarAsync(
                $"SELECT count() FROM system.tables WHERE database = '{BenchEnv.SinkDb}' AND name = '{table[(table.IndexOf('.') + 1)..]}'");
            if (Convert.ToInt64(exists, CultureInfo.InvariantCulture) == 0)
            {
                throw new InvalidOperationException(
                    $"Sink table {table} is missing. Run scripts/stage-datasets.sh — it creates both sinks " +
                    $"from bench.hits with CREATE TABLE ... AS, so they cannot drift from the source schema.");
            }
        }

#if CH_API_1_2
        client.RegisterBinaryInsertType<HitsRow>();
#endif
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [IterationSetup]
    public void IterationSetup() => SideMetrics.NextIteration();

    /// <summary>
    /// Server-cost lookups happen here, not in the benchmark methods: a SYSTEM FLUSH LOGS and its
    /// retry delay inside a timed region would be reported as part of the insert.
    /// </summary>
    [IterationCleanup]
    public void IterationCleanup() => serverCost.Drain(client);

    /// <summary>Untyped insert into the Null sink: client cost only, boxing included.</summary>
    [Benchmark(Baseline = true)]
    public Task<long> ObjectArrayNull() =>
        InsertObjectArray(nullSink, nameof(ObjectArrayNull));

#if CH_API_1_2
    /// <summary>Typed insert into the Null sink: the same bytes, without the boxing.</summary>
    [Benchmark]
    public Task<long> PocoNull() => InsertPoco(nullSink, nameof(PocoNull));
#endif

    /// <summary>Untyped insert into a real MergeTree: the credibility arm.</summary>
    [Benchmark]
    public async Task<long> ObjectArrayMergeTree()
    {
        // Truncate before, not after: a failed iteration then leaves the table for inspection rather
        // than erasing the evidence.
        await client.ExecuteNonQueryAsync($"TRUNCATE TABLE {mergeTreeSink}");
        return await InsertObjectArray(mergeTreeSink, nameof(ObjectArrayMergeTree));
    }

    private Task<long> InsertObjectArray(string table, string arm) =>
        Insert(arm, table, queryId => client.InsertBinaryAsync(
            table,
            HitsRow.Columns,
            StreamObjectArrays(BenchProfile.ContentRows),
            new InsertOptions
            {
                BatchSize = BatchSize,
                // Pinned to 1 for every per-row allocation claim: parallel batches interleave
                // allocations from several threads and the per-row figure stops meaning anything.
                MaxDegreeOfParallelism = 1,
                QueryId = queryId,
            }));

#if CH_API_1_2
    private Task<long> InsertPoco(string table, string arm) =>
        Insert(arm, table, queryId => client.InsertBinaryAsync(
            table,
            StreamPocos(BenchProfile.ContentRows),
            new InsertOptions
            {
                BatchSize = BatchSize,
                MaxDegreeOfParallelism = 1,
                QueryId = queryId,
            }));
#endif

    private async Task<long> Insert(string arm, string table, Func<string, Task<long>> insert)
    {
        var queryId = ServerMetrics.NewQueryId(arm);
        var cpu = CpuProbe.Start();

        var written = await insert(queryId);

        var clientCpu = cpu.TotalMicroseconds;
        var elapsed = cpu.ElapsedMicroseconds;
        var args = string.Create(
            CultureInfo.InvariantCulture,
            $"arm={arm};batch={BatchSize};rows={BenchProfile.ContentRows}");

        SideMetrics.Record(Name, args, "rows", written);
        SideMetrics.Record(Name, args, "client_cpu_us", clientCpu);
        SideMetrics.Record(Name, args, "rows_per_second", elapsed > 0 ? written / (elapsed / 1_000_000d) : 0);

        // Batches get query ids of the form {base}-N, so the expected row count is the batch count —
        // an exact-match lookup would find nothing, and a prefix lookup without this count would
        // happily sum a partial flush.
        var expectedBatches = (long)Math.Ceiling(BenchProfile.ContentRows / (double)BatchSize);
        SideMetrics.Record(Name, args, "batches", expectedBatches);
        SideMetrics.Record(Name, args, "elapsed_us", elapsed);

        // Queued, not executed: the lookup runs in IterationCleanup, outside the timed region.
        serverCost.Enqueue(Name, args, queryId, expectedBatches);

        return written;
    }

    private static IEnumerable<object[]> StreamObjectArrays(int count)
    {
        for (var i = 0; i < count; i++)
            yield return HitsRow.ToObjectArray(HitsRow.Create(i));
    }

    private static IEnumerable<HitsRow> StreamPocos(int count)
    {
        for (var i = 0; i < count; i++)
            yield return HitsRow.Create(i);
    }
}
