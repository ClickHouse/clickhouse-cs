using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Isolates the per-batch allocation of the query line that precedes each batch's binary rows
/// (e.g. <c>INSERT INTO ... FORMAT RowBinary</c>). Before the change this line was written through a
/// <c>StreamWriter</c>, which allocated the writer plus a 4 KiB char buffer and a matching byte
/// buffer on <em>every</em> batch; after the change it is encoded into a pooled scratch buffer and
/// written directly.
///
/// To surface that per-<em>batch</em> cost the benchmark inserts a fixed number of rows split into
/// many small batches (small <c>BatchSize</c> ⇒ many batches ⇒ the query-line allocation dominates).
/// It uses:
///  - the POCO <c>RowBinary</c> path, whose value writes are box-free (issue #434), so the row
///    serialization contributes little managed allocation of its own;
///  - <c>Compressor = null</c> (uncompressed), so the GZip stream's own per-batch allocations do not
///    swamp the delta. The change is identical on the compressed path.
///
/// The table uses ENGINE Null so the server discards rows and the benchmark isolates client-side
/// serialization cost rather than storage.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class BatchQueryLineBenchmark
{
    private const string TableName = "test.benchmark_queryline_insert";

    private ClickHouseClient client;
    private SensorRow[] rows;

    [Params(200_000)]
    public int Count { get; set; }

    // Small batch sizes maximize the number of batches (and thus the number of query lines written).
    [Params(500, 5_000)]
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

        client.RegisterBinaryInsertType<SensorRow>();

        // Pre-materialize rows so the benchmark measures batching/serialization, not row creation.
        rows = new SensorRow[Count];
        for (int i = 0; i < Count; i++)
            rows[i] = new SensorRow { Id = i, Name = MakeName(i), Value = i * 0.5, X = i ^ 0x5a5a, Y = i * 3 };
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [Benchmark]
    public async Task<long> Poco()
    {
        // Compressor = null isolates the query-line allocation from GZip's own per-batch allocations.
        var options = new InsertOptions { BatchSize = BatchSize, MaxDegreeOfParallelism = 1, Compressor = null };
        return await client.InsertBinaryAsync(TableName, rows, options);
    }

    // Deterministic pseudo-random-ish string so the payload does not compress to almost nothing.
    private static string MakeName(int i)
    {
        uint h = unchecked((uint)i * 2654435761u);
        return $"row-{i}-{h & 0xffffff:x6}";
    }

    public class SensorRow
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
        public long X { get; set; }
        public long Y { get; set; }
    }
}
