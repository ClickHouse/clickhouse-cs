using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace ClickHouse.Driver.Benchmark.Blog.Families;

/// <summary>
/// The composite-type long tail: one arm per column type, from <c>bench.types_wide</c>.
/// </summary>
/// <remarks>
/// <para>
/// <c>hits</c> is 105 columns of Int16/Int32/Int64/String/DateTime/Date and nothing else, so it
/// exercises none of the per-value decode work on Nullable, Decimal, Array, Map, Tuple, UUID,
/// LowCardinality, Enum, Dynamic, Variant, JSON, Int128, IPv6 or FixedString. This is where those
/// live, and it feeds the normalized strip chart in one run rather than eleven.
/// </para>
/// <para>
/// <b>Each arm reads exactly one column</b>, plus an <see cref="IdOnly"/> control that reads the key
/// alone. Subtracting the control gives the per-value decode cost with the query, transport and
/// row-iteration overhead removed — none of which are the thing being compared. Without that control
/// every arm would carry the same fixed cost and the cheap types would all look identical.
/// </para>
/// <para>
/// Column-oriented on the wire but row-oriented on read: RowBinary interleaves values, so a
/// single-column query genuinely transfers and decodes only that column.
/// </para>
/// </remarks>
[Config(typeof(LongRunConfig))]
public class TypesWideBenchmark
{
    private const string Name = nameof(TypesWideBenchmark);

    /// <summary>
    /// The columns, each named after the optimisation it exercises. Order is the chart's order.
    /// </summary>
    public static IEnumerable<string> Columns() =>
    [
        // The control: fixed cost only. Every other arm should be read relative to this.
        "id",
        "n_int32",
        "n_string",
        "arr_int32",
        "arr_nullable",
        "arr_string",
        "map_str_int",
        "tup",
        "dec128",
        "dyn",
        "var",
        "i128",
        "uuid",
        "ipv6",
        "fs16",
        "lc_str",
        "enum8",
        "json",
        "dt64",
    ];

    private readonly Consumer consumer = new();
    private readonly DeferredServerCost serverCost = new();
    private ClickHouseClient client;

    [ParamsSource(nameof(Columns))]
    public string Column { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchEnv.CreateClient();

        var rows = await client.ExecuteScalarAsync($"SELECT count() FROM {BenchEnv.TypesWide}");
        var count = rows is null or DBNull ? 0 : Convert.ToInt64(rows, CultureInfo.InvariantCulture);
        if (count < BenchProfile.TypesWideRows)
        {
            throw new InvalidOperationException(
                $"{BenchEnv.TypesWide} has {count} rows, profile wants {BenchProfile.TypesWideRows}. " +
                $"Run scripts/stage-datasets.sh.");
        }
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [IterationSetup]
    public void IterationSetup() => SideMetrics.NextIteration();

    [IterationCleanup]
    public void IterationCleanup() => serverCost.Drain(client);

    /// <summary>The control arm and every type arm run through here; only <see cref="Column"/> differs.</summary>
    [Benchmark]
    public async Task<long> ReadColumn()
    {
        var queryId = ServerMetrics.NewQueryId("types-" + Column);
        var cpu = CpuProbe.Start();
        long rows = 0;

        using (var reader = await client.ExecuteReaderAsync(
            $"SELECT {Column} FROM {BenchEnv.TypesWide} LIMIT {BenchProfile.TypesWideRows}",
            options: new QueryOptions { QueryId = queryId }))
        {
            while (reader.Read())
            {
                // GetValue on purpose: it is the untyped path every composite type still goes through,
                // and the one whose per-value allocation this release attacked. A typed accessor would
                // measure the box-free slot path instead, which is HitsScanBenchmark's job.
                consumer.Consume(reader.GetValue(0));
                rows++;
            }
        }

        var elapsed = cpu.ElapsedMicroseconds;
        var args = string.Create(
            CultureInfo.InvariantCulture,
            $"column={Column};rows={BenchProfile.TypesWideRows}");

        SideMetrics.Record(Name, args, "rows", rows);
        SideMetrics.Record(Name, args, "client_cpu_us", cpu.TotalMicroseconds);
        SideMetrics.Record(Name, args, "elapsed_us", elapsed);
        SideMetrics.Record(Name, args, "client_cpu_per_row_us", rows > 0 ? cpu.TotalMicroseconds / rows : 0);

        serverCost.Enqueue(Name, args, queryId);
        return rows;
    }
}
