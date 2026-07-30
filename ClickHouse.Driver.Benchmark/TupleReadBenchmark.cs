using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures allocations when reading <c>Tuple(...)</c> columns. Reading each row builds one
/// <c>System.Tuple&lt;...&gt;</c> per tuple column; the small-tuple read path constructs it directly
/// (no intermediate <c>object[]</c>, no reflection constructor invoke). The matrix spans arity 2/3/7
/// (fast path), a matching nullable-element case, an 8-element <c>LargeTuple</c> control (unchanged
/// object[] path), and a Dynamic-wrapped tuple (fresh TupleType per row via BinaryTypeDecoder).
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TupleReadBenchmark
{
    private readonly Consumer consumer = new Consumer();
    private ClickHouseConnection connection;

    [Params(200000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ?? "Host=localhost";
        connection = new ClickHouseConnection(new ClickHouseClientSettings(connectionString));
    }

    [GlobalCleanup]
    public void Cleanup() => connection?.Dispose();

    // Arity 2: Tuple(UInt64, String) — smallest fast-path tuple.
    [Benchmark(Baseline = true)]
    public async Task Tuple2_UInt64String()
    {
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT tuple(number, toString(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetValue(0));
    }

    // Arity 3: Tuple(Int32, Int32, Int32) — all value-type elements (three boxes).
    [Benchmark]
    public async Task Tuple3_Int32()
    {
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT tuple(toInt32(number), toInt32(number + 1), toInt32(number + 2)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetValue(0));
    }

    // Arity 2 with a Nullable(Int32) element that is NULL every 5th row (ClearDBNull path).
    [Benchmark]
    public async Task Tuple2_NullableInt32String()
    {
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT tuple(CAST(if(number % 5 = 0, NULL, toInt32(number)) AS Nullable(Int32)), toString(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetValue(0));
    }

    // Arity 7: upper bound of the small-tuple fast path.
    [Benchmark]
    public async Task Tuple7_Int32()
    {
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT tuple(toInt32(number), toInt32(number), toInt32(number), toInt32(number), toInt32(number), toInt32(number), toInt32(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetValue(0));
    }

    // Arity 8: LargeTuple control — stays on the object[] path, should be unchanged.
    [Benchmark]
    public async Task Tuple8_Int32_LargeTupleControl()
    {
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT tuple(toInt32(number), toInt32(number), toInt32(number), toInt32(number), toInt32(number), toInt32(number), toInt32(number), toInt32(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetValue(0));
    }

    // Dynamic-wrapped tuple: a fresh TupleType is decoded per row, so the compiled factory must be
    // cached across instances (keyed by the tuple CLR type) rather than rebuilt per row.
    [Benchmark]
    public async Task Tuple2_Dynamic()
    {
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT tuple(number, toString(number))::Dynamic FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetValue(0));
    }
}
