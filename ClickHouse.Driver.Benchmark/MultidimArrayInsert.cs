using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures the multidimensional-array binary write fast path (issue #367). A rectangular
/// <c>int[Side, Side]</c> matrix takes the blit path (one bulk write per contiguous inner row,
/// zero per-element boxing); the equivalent jagged <c>int[Side][]</c> takes the boxing IList path
/// and serves as the baseline. Both produce identical wire bytes, so the interesting columns are
/// Allocated and Mean. Inserts into a <c>Null</c>-engine table to isolate client serialization.
/// <c>Leaf</c> also covers the wire-transparent wrappers of that primitive leaf (issue #553): they
/// serialize identically to a bare one, so they have to reach the blit path too.
/// </summary>
[BenchmarkCategory(BenchmarkCategories.HttpInvestigation)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class MultidimArrayInsert
{
    private ClickHouseClient client;
    private List<object[]> multidimRows;
    private List<object[]> jaggedRows;
    private string targetTable;

    [Params(100)]
    public int Rows { get; set; }

    [Params(100)]
    public int Side { get; set; }

    [Params("Int32", "LowCardinality(Int32)", "SimpleAggregateFunction(any, Int32)")]
    public string Leaf { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";
        client = new ClickHouseClient(connectionString);

        var leafSuffix = new string(Leaf.Select(c => char.IsLetterOrDigit(c) ? char.ToLowerInvariant(c) : '_').ToArray());
        targetTable = $"test.benchmark_multidim_{leafSuffix}";
        client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test").GetAwaiter().GetResult();
        // LowCardinality over a fixed-width integer is refused by default.
        var createOptions = new QueryOptions
        {
            CustomSettings = new Dictionary<string, object> { ["allow_suspicious_low_cardinality_types"] = 1 },
        };
        client.ExecuteNonQueryAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (arr Array(Array({Leaf}))) ENGINE Null", options: createOptions).GetAwaiter().GetResult();

        multidimRows = new List<object[]>(Rows);
        jaggedRows = new List<object[]>(Rows);
        for (var n = 0; n < Rows; n++)
        {
            var multi = new int[Side, Side];
            var jagged = new int[Side][];
            for (var r = 0; r < Side; r++)
            {
                jagged[r] = new int[Side];
                for (var c = 0; c < Side; c++)
                {
                    var v = (r * Side) + c;
                    multi[r, c] = v;
                    jagged[r][c] = v;
                }
            }

            multidimRows.Add(new object[] { multi });
            jaggedRows.Add(new object[] { jagged });
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        client?.Dispose();
    }

    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<long> JaggedBoxing() =>
        await client.InsertBinaryAsync(targetTable, new[] { "arr" }, jaggedRows);

    [Benchmark]
    public async Task<long> MultidimBlit() =>
        await client.InsertBinaryAsync(targetTable, new[] { "arr" }, multidimRows);
}
