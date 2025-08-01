﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

[MemoryDiagnoser(true)]
public class BulkInsertColumn
{
    private readonly ClickHouseConnection connection;
    private readonly ClickHouseBulkCopy bulkCopy;

    [Params(100000)]
    public int Count { get; set; }

    private IEnumerable<object[]> Rows
    {
        get
        {
            int counter = 0;
            while (counter < int.MaxValue)
                yield return new object[] { counter++ };
        }
    }

    public BulkInsertColumn()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        connection = new ClickHouseConnection(connectionString);

        var targetTable = $"test.benchmark_bulk_insert_int64";

        // Create database and table for benchmark
        connection.ExecuteStatementAsync($"CREATE DATABASE IF NOT EXISTS test").Wait();
        connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (col1 Int64) ENGINE Null").Wait();

        bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
            BatchSize = 10000,
            MaxDegreeOfParallelism = 1,
            ColumnNames = new[] { "col1" }
        };
        bulkCopy.InitAsync().Wait();
    }

    [Benchmark]
    public async Task BulkInsertInt32() => await bulkCopy.WriteToServerAsync(Rows.Take(Count));
}
