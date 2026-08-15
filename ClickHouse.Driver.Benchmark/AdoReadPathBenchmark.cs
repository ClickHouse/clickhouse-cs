using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// The four ways a caller drives <see cref="ClickHouseDataReader"/>, over one realistic wide row.
///
/// <para><see cref="ReadValueBenchmark"/> measures single-column reads with and without a converter, which
/// isolates per-accessor cost but hides the thing typed column slots actually changed: how much a row costs
/// to decode <i>before</i> anyone looks at it, and how that scales with the fraction of columns read. The
/// old <c>object[]</c> row buffer boxed every cell during <c>Read()</c>, so all five variants below allocated
/// identically — even <see cref="Scan"/>, which reads nothing.</para>
///
/// <list type="bullet">
/// <item><see cref="Scan"/> — the floor: decode cost with no accessor calls at all.</item>
/// <item><see cref="TypedAccessors"/> — the linq2db path. Its compiled mapper inlines
///   <c>GetInt64</c>/<c>GetDouble</c>/<c>GetString</c>/<c>GetDateTime</c>/<c>GetGuid</c> per column per row.</item>
/// <item><see cref="GenericAccessor"/> — hand-written <c>GetFieldValue&lt;T&gt;</c> code.</item>
/// <item><see cref="UntypedAccessor"/> — the Dapper path. Its emitted IL calls the <c>this[int]</c> indexer,
///   i.e. <c>GetValue</c>, so it still boxes and pays one fixed slot allocation per returned column; this
///   variant is the "must not regress" control.</item>
/// <item><see cref="TypedAccessorsProjected"/> — reads 2 of 10 columns, the case the old eager boxing
///   punished hardest.</item>
/// </list>
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class AdoReadPathBenchmark
{
    private readonly Consumer consumer = new();
    private ClickHouseConnection connection;

    // The short cases expose the fixed per-reader slot cost that a 200k-row allocation total rounds away.
    [Params(1, 10, 200000)]
    public int Count { get; set; }

    // Ten columns, eight of them value types — the shape that used to box eight times per row.
    private string Sql => $@"
SELECT toInt64(number)                             AS c0,
       toInt64(number * 2)                         AS c1,
       toInt64(number * 3)                         AS c2,
       toInt64(number * 5)                         AS c3,
       toFloat64(number) * 0.5                     AS c4,
       toFloat64(number) * 1.5                     AS c5,
       concat('s', toString(number % 8))           AS c6,
       concat('t', toString(number % 4))           AS c7,
       toDateTime(1700000000 + (number % 65536), 'UTC') AS c8,
       toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number % 1000), 12, '0'))) AS c9
FROM system.numbers LIMIT {Count}";

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ?? "Host=localhost";
        connection = new ClickHouseConnection(new ClickHouseClientSettings(connectionString));
    }

    [GlobalCleanup]
    public void Cleanup() => connection?.Dispose();

    [Benchmark(Baseline = true)]
    public async Task UntypedAccessor()
    {
        using var reader = await connection.ExecuteReaderAsync(Sql);
        while (reader.Read())
        {
            for (var i = 0; i < 10; i++)
                consumer.Consume(reader.GetValue(i));
        }
    }

    [Benchmark]
    public async Task Scan()
    {
        using var reader = await connection.ExecuteReaderAsync(Sql);
        while (reader.Read())
        {
        }
    }

    [Benchmark]
    public async Task TypedAccessors()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(Sql);
        while (reader.Read())
        {
            consumer.Consume(reader.GetInt64(0));
            consumer.Consume(reader.GetInt64(1));
            consumer.Consume(reader.GetInt64(2));
            consumer.Consume(reader.GetInt64(3));
            consumer.Consume(reader.GetDouble(4));
            consumer.Consume(reader.GetDouble(5));
            consumer.Consume(reader.GetString(6));
            consumer.Consume(reader.GetString(7));
            consumer.Consume(reader.GetDateTime(8));
            consumer.Consume(reader.GetGuid(9));
        }
    }

    [Benchmark]
    public async Task GenericAccessor()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(Sql);
        while (reader.Read())
        {
            consumer.Consume(reader.GetFieldValue<long>(0));
            consumer.Consume(reader.GetFieldValue<long>(1));
            consumer.Consume(reader.GetFieldValue<long>(2));
            consumer.Consume(reader.GetFieldValue<long>(3));
            consumer.Consume(reader.GetFieldValue<double>(4));
            consumer.Consume(reader.GetFieldValue<double>(5));
            consumer.Consume(reader.GetFieldValue<string>(6));
            consumer.Consume(reader.GetFieldValue<string>(7));
            consumer.Consume(reader.GetFieldValue<DateTime>(8));
            consumer.Consume(reader.GetFieldValue<Guid>(9));
        }
    }

    [Benchmark]
    public async Task TypedAccessorsProjected()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(Sql);
        while (reader.Read())
        {
            consumer.Consume(reader.GetInt64(0));
            consumer.Consume(reader.GetString(6));
        }
    }
}
