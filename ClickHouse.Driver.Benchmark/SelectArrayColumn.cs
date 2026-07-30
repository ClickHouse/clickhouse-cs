using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class SelectArrayColumn
{
    private readonly ClickHouseConnection connection;

    [Params(100000)]
    public int Count { get; set; }

    public SelectArrayColumn()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        connection = new ClickHouseConnection(connectionString);
    }

    private async Task RunBenchmark(string expression)
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT {expression} FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    // ~20-element arrays: per-element Array.SetValue cost dominates.
    [Benchmark]
    public async Task ArrayInt32() => await RunBenchmark("arrayMap(x -> toInt32(x + number), range(20))");

    [Benchmark]
    public async Task ArrayNullableInt32() => await RunBenchmark("arrayMap(x -> if(x % 3 = 0, NULL, toInt32(x + number)), range(20))");

    [Benchmark]
    public async Task ArrayString() => await RunBenchmark("arrayMap(x -> concat('s', toString(x + number)), range(20))");

    [Benchmark]
    public async Task ArrayDateTime64() => await RunBenchmark("arrayMap(x -> toDateTime64(x + number, 3), range(20))");

    [Benchmark]
    public async Task ArrayArrayInt32() => await RunBenchmark("arrayMap(x -> arrayMap(y -> toInt32(y + x), range(5)), range(4))");

    // Short arrays: per-array Array.CreateInstance fixed cost dominates.
    [Benchmark]
    public async Task ArrayInt32Short() => await RunBenchmark("array(toInt32(number), toInt32(number + 1), toInt32(number + 2))");
}
