using BenchmarkDotNet.Attributes;
using System;
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

    private SensorReading[] _pocoRows;
    private object[][] _objectRows;

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

        _pocoRows = GeneratePocoRows(Count).ToArray();
        _objectRows = GenerateObjectArrayRows(Count).ToArray();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        client?.Dispose();

        _pocoRows = null;
        _objectRows = null;
    }

    [Benchmark(Baseline = true)]
    public async Task ObjectArray()
    {
        var columns = new[] { "Id", "Name", "Value" };
        await client.InsertBinaryAsync(TableName, columns, _objectRows);
    }

    [Benchmark]
    public async Task Poco()
    {
        await client.InsertBinaryAsync(TableName, _pocoRows);
    }

    [Benchmark]
    public async Task BinaryImport()
    {
        var import = await client.StartInsertAsync(TableName, ["Id", "Name", "Value"]);
        using var batch = import.StartNewBatch();

        foreach (var row in _pocoRows)
        {
            batch.WriteData(0, row.Id);
            batch.WriteData(1, row.Name);
            batch.WriteData(2, row.Value);
        }

        batch.CompleteWrite();

        await import.SendBatchAsync(batch);
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
