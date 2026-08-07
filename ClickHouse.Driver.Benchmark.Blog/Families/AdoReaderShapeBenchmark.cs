using System;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using ClickHouse.Driver.ADO.Readers;

namespace ClickHouse.Driver.Benchmark.Blog.Families;

/// <summary>
/// The four ways a caller drives the ADO reader, over a real <c>hits</c> shape — the deep-dive figure
/// where <see cref="UntypedAccessor"/> stays visibly flat and validates the chart.
/// </summary>
/// <remarks>
/// <para>
/// <c>ClickHouse.Driver.Benchmark.AdoReadPathBenchmark</c> already covers these access patterns over a
/// synthesized <c>system.numbers</c> projection, and it stays where it is: it is the per-PR regression
/// benchmark and it runs in CI. This one differs in three ways that matter for a published figure —
/// real <c>hits</c> data rather than generated integers, the long-run config with GC forcing off and
/// both GC modes, and the version corridor so the same shape can be measured back to v1.0.0.
/// </para>
/// <para>
/// <b>Why <see cref="UntypedAccessor"/> is the control.</b> <c>GetValue</c> boxes by contract, so it
/// cannot benefit from typed column slots. If a chart shows it improving, the chart is wrong — a
/// measurement error, a caching artefact, or a mislabelled arm. A deep-dive figure that contains its
/// own falsification test is worth more than one that does not.
/// </para>
/// <para>
/// <see cref="TypedAccessorsProjected"/> reads 2 of the 10 selected columns: the case eager per-row
/// boxing punished hardest, because it paid for eight columns nobody looked at.
/// </para>
/// </remarks>
[Config(typeof(LongRunConfig))]
public class AdoReaderShapeBenchmark
{
    private const string Name = nameof(AdoReaderShapeBenchmark);

    /// <summary>
    /// Ten real columns: four integers of three widths, two strings, two timestamps. Value types
    /// dominate, which is where boxing was paid.
    /// </summary>
    private const string Columns =
        "WatchID, UserID, RefererHash, URLHash, CounterID, RegionID, URL, Title, EventTime, ClientEventTime";

    private readonly Consumer consumer = new();
    private ClickHouseClient client;

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchEnv.CreateClient();

        var rows = await client.ExecuteScalarAsync($"SELECT count() FROM {BenchEnv.Hits}");
        if (Convert.ToInt64(rows, CultureInfo.InvariantCulture) < BenchProfile.ContentRows)
            throw new InvalidOperationException($"{BenchEnv.Hits} is too small. Run scripts/stage-datasets.sh.");
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [IterationSetup]
    public void IterationSetup() => SideMetrics.NextIteration();

    private string Sql => $"SELECT {Columns} FROM {BenchEnv.Hits} LIMIT {BenchProfile.ContentRows}";

    /// <summary>
    /// The Dapper path: emitted IL calls the <c>this[int]</c> indexer, i.e. <c>GetValue</c>. Boxes by
    /// contract, so this is the arm that must NOT improve.
    /// </summary>
    [Benchmark(Baseline = true)]
    public async Task<long> UntypedAccessor()
    {
        var cpu = CpuProbe.Start();
        long rows = 0;

        using (var reader = await client.ExecuteReaderAsync(Sql))
        {
            while (reader.Read())
            {
                for (var i = 0; i < 10; i++)
                    consumer.Consume(reader.GetValue(i));

                rows++;
            }
        }

        Record(nameof(UntypedAccessor), rows, cpu);
        return rows;
    }

    /// <summary>The floor: decode cost with no accessor calls at all.</summary>
    [Benchmark]
    public async Task<long> Scan()
    {
        var cpu = CpuProbe.Start();
        long rows = 0;

        using (var reader = await client.ExecuteReaderAsync(Sql))
        {
            while (reader.Read())
                rows++;
        }

        Record(nameof(Scan), rows, cpu);
        return rows;
    }

    /// <summary>
    /// The linq2db path: a compiled mapper inlines the typed accessors per column per row. This is
    /// where box elimination is worth the most, because it is what ORMs actually generate.
    /// </summary>
    [Benchmark]
    public async Task<long> TypedAccessors()
    {
        var cpu = CpuProbe.Start();
        long rows = 0;

        using (var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(Sql))
        {
            while (reader.Read())
            {
                consumer.Consume(reader.GetInt64(0));
                consumer.Consume(reader.GetInt64(1));
                consumer.Consume(reader.GetInt64(2));
                consumer.Consume(reader.GetInt64(3));
                consumer.Consume(reader.GetInt32(4));
                consumer.Consume(reader.GetInt32(5));
                consumer.Consume(reader.GetString(6));
                consumer.Consume(reader.GetString(7));
                consumer.Consume(reader.GetDateTime(8));
                consumer.Consume(reader.GetDateTime(9));
                rows++;
            }
        }

        Record(nameof(TypedAccessors), rows, cpu);
        return rows;
    }

    /// <summary>Hand-written <c>GetFieldValue&lt;T&gt;</c> code.</summary>
    [Benchmark]
    public async Task<long> GenericAccessor()
    {
        var cpu = CpuProbe.Start();
        long rows = 0;

        using (var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(Sql))
        {
            while (reader.Read())
            {
                consumer.Consume(reader.GetFieldValue<long>(0));
                consumer.Consume(reader.GetFieldValue<long>(1));
                consumer.Consume(reader.GetFieldValue<long>(2));
                consumer.Consume(reader.GetFieldValue<long>(3));
                consumer.Consume(reader.GetFieldValue<int>(4));
                consumer.Consume(reader.GetFieldValue<int>(5));
                consumer.Consume(reader.GetFieldValue<string>(6));
                consumer.Consume(reader.GetFieldValue<string>(7));
                consumer.Consume(reader.GetFieldValue<DateTime>(8));
                consumer.Consume(reader.GetFieldValue<DateTime>(9));
                rows++;
            }
        }

        Record(nameof(GenericAccessor), rows, cpu);
        return rows;
    }

    /// <summary>Reads 2 of 10 columns: the projection case eager boxing punished hardest.</summary>
    [Benchmark]
    public async Task<long> TypedAccessorsProjected()
    {
        var cpu = CpuProbe.Start();
        long rows = 0;

        using (var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(Sql))
        {
            while (reader.Read())
            {
                consumer.Consume(reader.GetInt64(0));
                consumer.Consume(reader.GetString(6));
                rows++;
            }
        }

        Record(nameof(TypedAccessorsProjected), rows, cpu);
        return rows;
    }

    private void Record(string arm, long rows, CpuProbe cpu)
    {
        var elapsed = cpu.ElapsedMicroseconds;
        var args = string.Create(CultureInfo.InvariantCulture, $"arm={arm};rows={BenchProfile.ContentRows}");

        SideMetrics.Record(Name, args, "rows", rows);
        SideMetrics.Record(Name, args, "client_cpu_us", cpu.TotalMicroseconds);
        SideMetrics.Record(Name, args, "elapsed_us", elapsed);
        SideMetrics.Record(Name, args, "rows_per_second", elapsed > 0 ? rows / (elapsed / 1_000_000d) : 0);
    }
}
