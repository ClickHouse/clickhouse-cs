using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures a binary insert where the caller already has the rows materialized as an
/// <c>object[][]</c>. Targets issue #506: today <c>IntoBatches</c> always routes rows through
/// <c>EnumerableExtensions.BatchRented</c>, which rents an <c>object[][]</c> from the pool and
/// copies every row reference into it — even when the input is already an <c>object[][]</c> that
/// could be sliced in place.
///
/// Two regimes are measured:
///  - BatchSize 100_000 (the default): the rented row array is &lt;= the ArrayPool.Shared max bucket
///    (2^20 = 1_048_576) so it is pooled/reused. The copy is the only avoidable work here.
///  - BatchSize 1_500_000 (single batch): the rented row array exceeds the pool's max bucket, so it
///    is freshly allocated and GC'd every operation. Slicing eliminates that allocation entirely.
///
/// The table uses ENGINE Null so the server discards rows and the benchmark isolates client-side
/// batching/serialization cost rather than storage.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class BinaryInsertObjectArrayBenchmark
{
    private const string TableName = "test.benchmark_objarray_insert";
    private static readonly string[] Columns = { "Id", "Name", "Value", "X", "Y" };

    private ClickHouseClient client;
    private object[][] rows;

    [Params(1_500_000)]
    public int Count { get; set; }

    // 100_000 => pooled row-array rent (default); 1_500_000 => single batch above the pool max.
    [Params(100_000, 1_500_000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";

        client = new ClickHouseClient(connectionString);

        await client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test");
        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE IF NOT EXISTS {TableName} (Id Int64, Name String, Value Float64, X Int64, Y Int64) ENGINE Null");

        // Pre-materialize the rows once so the benchmark measures batching/serialization, not row creation.
        rows = new object[Count][];
        for (int i = 0; i < Count; i++)
            rows[i] = new object[] { (long)i, MakeName(i), i * 0.5, (long)(i ^ 0x5a5a), (long)(i * 3) };
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [Benchmark]
    public async Task<long> ObjectArray()
    {
        var options = new InsertOptions { BatchSize = BatchSize, MaxDegreeOfParallelism = 1 };
        return await client.InsertBinaryAsync(TableName, Columns, rows, options);
    }

    // Deterministic pseudo-random-ish string so the payload does not compress to almost nothing.
    private static string MakeName(int i)
    {
        uint h = unchecked((uint)i * 2654435761u);
        return $"row-{i}-{h & 0xffffff:x6}";
    }
}
