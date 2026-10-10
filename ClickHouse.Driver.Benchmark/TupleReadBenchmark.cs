using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Reading a <c>Tuple(...)</c> column into a <see cref="ValueTuple"/> through <c>GetFieldValue&lt;T&gt;</c>, against
/// the hand-written alternative: <c>GetValue</c>, a cast to <see cref="ITuple"/> and a cast per element.
///
/// <list type="bullet">
/// <item><c>Wide*</c> — ten elements, read from an object[]-backed tuple through the <c>TRest</c> chain.</item>
/// <item><c>Small*</c> — two elements, where the converter copies the typed <c>System.Tuple</c> properties.</item>
/// <item><c>Array*</c> — an <c>Array</c> of ten-element tuples, ten per row.</item>
/// </list>
/// </summary>
[BenchmarkCategory(BenchmarkCategories.HttpInvestigation)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TupleReadBenchmark
{
    private const string WideTuple =
        "tuple(toInt32(number), toInt64(number), toInt64(2), toInt64(3), toInt64(4), toInt32(5), " +
        "toDate('2025-01-15'), toDate('2025-01-31'), toDate('2025-02-01'), toInt8(1))";

    private readonly Consumer consumer = new();
    private ClickHouseConnection connection;

    [Params(100000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ?? "Host=localhost";
        connection = new ClickHouseConnection(new ClickHouseClientSettings(connectionString));
    }

    [GlobalCleanup]
    public void Cleanup() => connection?.Dispose();

    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task Wide_ManualCast()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT {WideTuple} FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(FromTuple((ITuple)reader.GetValue(0)));
    }

    [Benchmark]
    public async Task Wide_GetFieldValue()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT {WideTuple} FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetFieldValue<(int, long, long, long, long, int, DateTime, DateTime, DateTime, sbyte)>(0));
    }

    [Benchmark]
    public async Task Small_ManualCast()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT tuple(toInt32(number), toInt64(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
        {
            var tuple = (ITuple)reader.GetValue(0);
            consumer.Consume(((int)tuple[0], (long)tuple[1]));
        }
    }

    [Benchmark]
    public async Task Small_GetFieldValue()
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT tuple(toInt32(number), toInt64(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetFieldValue<(int, long)>(0));
    }

    [Benchmark]
    public async Task Array_ManualCast()
    {
        using var reader = await connection.ExecuteReaderAsync(ArraySql);
        while (reader.Read())
        {
            var tuples = (ITuple[])reader.GetValue(0);
            var result = new (int, long, long, long, long, int, DateTime, DateTime, DateTime, sbyte)[tuples.Length];
            for (var i = 0; i < tuples.Length; i++)
                result[i] = FromTuple(tuples[i]);
            consumer.Consume(result);
        }
    }

    [Benchmark]
    public async Task Array_GetFieldValue()
    {
        using var reader = await connection.ExecuteReaderAsync(ArraySql);
        while (reader.Read())
            consumer.Consume(reader.GetFieldValue<(int, long, long, long, long, int, DateTime, DateTime, DateTime, sbyte)[]>(0));
    }

    private string ArraySql =>
        $"SELECT groupArray({WideTuple}) FROM (SELECT number FROM system.numbers LIMIT {Count}) GROUP BY intDiv(number, 10)";

    private static (int, long, long, long, long, int, DateTime, DateTime, DateTime, sbyte) FromTuple(ITuple tuple) =>
        ((int)tuple[0], (long)tuple[1], (long)tuple[2], (long)tuple[3], (long)tuple[4], (int)tuple[5],
            (DateTime)tuple[6], (DateTime)tuple[7], (DateTime)tuple[8], (sbyte)tuple[9]);
}
