using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Compares POCO insert performance against the object[] path.
/// Both benchmarks insert the same data into an ENGINE Null table
/// so the measurement is purely client-side serialization + network overhead.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class PocoInsertBenchmark
{
    private ClickHouseClient client;
    private const string TableName = "test.benchmark_poco_insert";

    public class SensorReading
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
    }

    [Params(100_000, 500_000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";
        client = new ClickHouseClient(connectionString);

        client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test").Wait();
        client.ExecuteNonQueryAsync(
            $"CREATE TABLE IF NOT EXISTS {TableName} (Id Int64, Name String, Value Float64) ENGINE Null").Wait();

        client.RegisterBinaryInsertType<SensorReading>();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        client?.Dispose();
    }

    [Benchmark(Baseline = true)]
    public async Task<long> ObjectArray()
    {
        var columns = new[] { "Id", "Name", "Value" };
        var rows = GenerateObjectArrayRows(Count);
        return await client.InsertBinaryAsync(TableName, columns, rows);
    }

    [Benchmark]
    public async Task<long> Poco()
    {
        var rows = GeneratePocoRows(Count);
        return await client.InsertBinaryAsync(TableName, rows);
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
