using BenchmarkDotNet.Attributes;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Compares BinaryImport insert performance against the object[]/POCO path.
/// Insert the same data into an ENGINE Null table
/// so the measurement is purely client-side serialization + network overhead.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class BinaryImportBenchmark
{
    private ClickHouseClient client;
    private const string TableName = "test.benchmark_binary_import";

    public class SensorReading
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
    }

    [Params(100_000, 500_000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";
        client = new ClickHouseClient(connectionString);

        await client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test");
        await client.ExecuteNonQueryAsync($"CREATE TABLE IF NOT EXISTS {TableName} (Id Int64, Name String, Value Float64) ENGINE Null");

        client.RegisterBinaryInsertType<SensorReading>();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        client?.Dispose();
    }

    [Benchmark(Baseline = true)]
    public async Task ObjectArray()
    {
        var columns = new[] { "Id", "Name", "Value" };
        var objectRows = GenerateObjectArrayRows(Count).ToArray();

        await client.InsertBinaryAsync(TableName, columns, objectRows);
    }

    [Benchmark]
    public async Task Poco()
    {
        var pocoRows = GeneratePocoRows(Count).ToArray();

        await client.InsertBinaryAsync(TableName, pocoRows);
    }

    [Benchmark]
    public async Task BinaryImport()
    {
        var import = await client.StartInsertAsync(TableName, ["Id", "Name", "Value"]);
        var batch = import.StartNewBatch();

        var itemsInBatch = 0;
        for (int i = 0; i < Count; i++)
        {
            itemsInBatch++;
            batch.WriteData(0, (long)i);
            batch.WriteData(1, $"sensor_{i % 10}");
            batch.WriteData(2, (double)i * 0.1);

            if (itemsInBatch == 100_000)
            {
                batch.CompleteWrite();
                await import.SendBatchAsync(batch);

                batch.Dispose();
                batch = import.StartNewBatch();
                itemsInBatch = 0;
            }
        }

        if (batch != null && itemsInBatch != 0)
        {
            batch.CompleteWrite();
            await import.SendBatchAsync(batch);
            batch.Dispose();
        }
    }

    private static IEnumerable<object[]> GenerateObjectArrayRows(int count)
    {
        for (int i = 0; i < count; i++)
            yield return new object[] { (long)i, $"sensor_{i % 10}", (double)i * 0.1 };
    }

    private static IEnumerable<SensorReading> GeneratePocoRows(int count)
    {
        for (int i = 0; i < count; i++)
            yield return new SensorReading { Id = i, Name = $"sensor_{i % 10}", Value = i * 0.1 };
    }
}
