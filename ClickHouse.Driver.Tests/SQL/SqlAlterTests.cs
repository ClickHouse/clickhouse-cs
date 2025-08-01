﻿using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

public class SqlAlterTests
{
    private readonly DbConnection connection;

    public SqlAlterTests()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.UseSession = true;
        builder.Compression = true;
        connection = new ClickHouseConnection(builder.ToString());
    }

    [Test]
    public async Task ShouldExecuteAlterTable()
    {
        await connection.ExecuteScalarAsync($"CREATE TABLE IF NOT EXISTS test.table_delete_from (value Int32) ENGINE MergeTree ORDER BY value");
        await connection.ExecuteScalarAsync($"ALTER TABLE test.table_delete_from DELETE WHERE 1=1");
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        connection?.Dispose();
    }
}
