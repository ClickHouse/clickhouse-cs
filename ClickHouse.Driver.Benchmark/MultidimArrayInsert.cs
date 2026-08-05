using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures the multidimensional-array binary write fast path (issue #367). A rectangular
/// <c>int[Side, Side]</c> matrix takes the blit path (one bulk write per contiguous inner row,
/// zero per-element boxing); the equivalent jagged <c>int[Side][]</c> takes the boxing IList path
/// and serves as the baseline. Both produce identical wire bytes, so the interesting columns are
/// Allocated and Mean. Inserts into a <c>Null</c>-engine table to isolate client serialization.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class MultidimArrayInsert
{
    private const string TargetTable = "test.benchmark_multidim_int32";

    private ClickHouseClient client;
    private List<object[]> multidimRows;
    private List<object[]> jaggedRows;

    [Params(100)]
    public int Rows { get; set; }

    [Params(100)]
    public int Side { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";
        client = new ClickHouseClient(connectionString);

        client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test").GetAwaiter().GetResult();
        client.ExecuteNonQueryAsync($"CREATE TABLE IF NOT EXISTS {TargetTable} (arr Array(Array(Int32))) ENGINE Null").GetAwaiter().GetResult();

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

    [Benchmark(Baseline = true)]
    public async Task<long> JaggedBoxing() =>
        await client.InsertBinaryAsync(TargetTable, new[] { "arr" }, jaggedRows);

    [Benchmark]
    public async Task<long> MultidimBlit() =>
        await client.InsertBinaryAsync(TargetTable, new[] { "arr" }, multidimRows);
}
