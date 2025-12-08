using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class BulkInsertColumn
{
    private ClickHouseConnection connection;
    private ClickHouseBulkCopy bulkCopyInt64;
    private ClickHouseBulkCopy bulkCopyFixedString;

    [Params(500000)]
    public int Count { get; set; }

    private IEnumerable<object[]> Int64Rows
    {
        get
        {
            int counter = 0;
            while (counter < int.MaxValue)
                yield return new object[] { counter++ };
        }
    }

    private IEnumerable<object[]> FixedStringRows
    {
        get
        {
            int counter = 0;
            while (counter < int.MaxValue)
                yield return new object[] { $"val{counter++:D12}" }; // 16-char string for FixedString(16)
        }
    }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        connection = new ClickHouseConnection(connectionString);

        // Create database
        connection.ExecuteStatementAsync("CREATE DATABASE IF NOT EXISTS test").Wait();

        // Setup Int64 benchmark
        var int64Table = "test.benchmark_bulk_insert_int64";
        connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {int64Table} (col1 Int64) ENGINE Null").Wait();
        bulkCopyInt64 = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = int64Table,
            BatchSize = 10000,
            MaxDegreeOfParallelism = 1,
            ColumnNames = new[] { "col1" }
        };
        bulkCopyInt64.InitAsync().Wait();

        // Setup FixedString benchmark
        var fixedStringTable = "test.benchmark_bulk_insert_fixedstring";
        connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {fixedStringTable} (col1 FixedString(16)) ENGINE Null").Wait();
        bulkCopyFixedString = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = fixedStringTable,
            BatchSize = 10000,
            MaxDegreeOfParallelism = 1,
            ColumnNames = new[] { "col1" }
        };
        bulkCopyFixedString.InitAsync().Wait();
    }

    [Benchmark]
    public async Task BulkInsertInt64() => await bulkCopyInt64.WriteToServerAsync(Int64Rows.Take(Count));

    [Benchmark]
    public async Task BulkInsertFixedString() => await bulkCopyFixedString.WriteToServerAsync(FixedStringRows.Take(Count));
}
