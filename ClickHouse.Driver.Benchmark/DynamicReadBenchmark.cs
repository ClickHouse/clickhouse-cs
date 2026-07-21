using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures allocations when reading a <c>Dynamic</c> column. Every value in a Dynamic column
/// carries its own binary type header, so <c>DynamicType.Read</c> calls
/// <c>BinaryTypeDecoder.FromByteCode</c> once per row. Before #501 that allocated a fresh
/// <c>ClickHouseType</c> per row; now stateless types return a shared singleton, so the residual
/// per-row allocation should be just the boxed value.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class DynamicReadBenchmark
{
    private readonly Consumer consumer = new Consumer();
    private ClickHouseConnection connection;

    [Params(500000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ?? "Host=localhost";
        connection = new ClickHouseConnection(new ClickHouseClientSettings(connectionString));
    }

    [GlobalCleanup]
    public void Cleanup() => connection?.Dispose();

    // Int64 Dynamic: per-row header decodes to a (now shared) Int64Type.
    [Benchmark(Baseline = true)]
    public async Task Int64_Dynamic_GetValue()
    {
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT number::Dynamic FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetValue(0));
    }

    // String Dynamic: per-row header decodes to a (now shared) StringType variant.
    [Benchmark]
    public async Task String_Dynamic_GetValue()
    {
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT concat('test', toString(number))::Dynamic FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetValue(0));
    }
}
