using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Compares POCO read performance against the manual <c>GetValue</c> path.
/// Both benchmarks read the same projection from <c>system.numbers</c> and materialize a
/// <see cref="SensorReading"/> instance per row, isolating the per-row materialization overhead
/// from any disk/network dependency on a user-created table.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class PocoReadBenchmark
{
    private ClickHouseClient client;

    public class SensorReading
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
    }

    [Params(100_000, 500_000)]
    public int Count { get; set; }

    private string Sql => $"SELECT toInt64(number) AS Id, concat('sensor_', toString(number % 10)) AS Name, toFloat64(number) * 0.1 AS Value FROM system.numbers LIMIT {Count}";

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";
        client = new ClickHouseClient(connectionString);
        client.RegisterPocoType<SensorReading>();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        client?.Dispose();
    }

    [Benchmark(Baseline = true)]
    public async Task<long> ManualGetValue()
    {
        long count = 0;
        long checksum = 0;
        using var reader = await client.ExecuteReaderAsync(Sql);
        while (reader.Read())
        {
            var row = new SensorReading
            {
                Id = (long)reader.GetValue(0),
                Name = (string)reader.GetValue(1),
                Value = (double)reader.GetValue(2),
            };
            checksum ^= row.Id;
            count++;
        }
        return count + checksum;
    }

    [Benchmark]
    public async Task<long> Poco()
    {
        long count = 0;
        long checksum = 0;
        await foreach (var row in client.QueryAsync<SensorReading>(Sql))
        {
            checksum ^= row.Id;
            count++;
        }
        return count + checksum;
    }
}
