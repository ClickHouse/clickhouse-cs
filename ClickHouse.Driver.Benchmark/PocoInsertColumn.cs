using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace ClickHouse.Driver.Benchmark;

[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class PocoInsertColumn
{
    private ClickHouseClient client;

    public class Row
    {
        public long Col1 { get; set; }
    }

    [Params(500000)]
    public int Count { get; set; }

    private IEnumerable<Row> Rows
    {
        get
        {
            int counter = 0;
            while (counter < int.MaxValue)
                yield return new Row { Col1 = counter++ };
        }
    }

    [GlobalSetup]
    public async Task Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";
        client = new ClickHouseClient(connectionString);

        await client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test");
        await client.ExecuteNonQueryAsync(
            "CREATE TABLE IF NOT EXISTS test.benchmark_poco_insert_int64 (Col1 Int64) ENGINE Null");

        client.RegisterBinaryInsertType<Row>();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        client?.Dispose();
    }

    [Benchmark]
    public async Task<long> PocoInsertInt64() =>
        await client.InsertBinaryAsync("test.benchmark_poco_insert_int64", Rows.Take(Count));
}
