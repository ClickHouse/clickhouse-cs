using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures the <see cref="BigInteger"/>-backed wide-integer binary write path
/// (<c>Int128</c>/<c>UInt128</c>/<c>Int256</c>/<c>UInt256</c>, issue #553). <c>Int64</c> is the
/// baseline: it writes straight from the value, so the gap is what the wide types pay per value.
/// Inserts into a <c>Null</c>-engine table to isolate client serialization; the interesting
/// columns are Allocated and Mean.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class WideIntegerInsert
{
    private ClickHouseClient client;
    private List<object[]> rows;
    private string targetTable;

    [Params(100000)]
    public int Rows { get; set; }

    [Params("Int64", "Int128", "UInt128", "Int256", "UInt256")]
    public string ColumnType { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";
        client = new ClickHouseClient(connectionString);

        targetTable = $"test.benchmark_wide_integer_{ColumnType.ToLowerInvariant()}";
        client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test").GetAwaiter().GetResult();
        client.ExecuteNonQueryAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value {ColumnType}) ENGINE Null").GetAwaiter().GetResult();

        // Int64 takes a long (its own framework type); the wide types take a BigInteger large enough
        // to need most of the column width, so the write is not measuring a one-byte value.
        object Value(int n) => ColumnType == "Int64"
            ? (object)(long)n
            : (object)((BigInteger.One << 100) + n);

        rows = Enumerable.Range(0, Rows).Select(n => new[] { Value(n) }).ToList();
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [Benchmark]
    public async Task<long> Insert() =>
        await client.InsertBinaryAsync(targetTable, new[] { "value" }, rows);
}
