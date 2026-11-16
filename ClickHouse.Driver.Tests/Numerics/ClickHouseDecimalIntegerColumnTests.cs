using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Numerics;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Numerics;

/// <summary>
/// Binary inserts of <see cref="ClickHouseDecimal"/> values into integer columns. The integer types
/// convert the value through its <see cref="IConvertible"/> members.
/// </summary>
[TestFixture]
[Category("ClickHouseDecimal")]
public class ClickHouseDecimalIntegerColumnTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task InsertBinaryAsync_ClickHouseDecimalIntoInt32Column_StoresExactValue()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id UInt8, v Int32) ENGINE = Memory");

        await client.InsertBinaryAsync(table, new[] { "id", "v" }, new List<object[]>
        {
            new object[] { (byte)1, new ClickHouseDecimal(100000.00m) },
            new object[] { (byte)2, new ClickHouseDecimal(-40000m) },
            new object[] { (byte)3, new ClickHouseDecimal(int.MinValue) },
            new object[] { (byte)4, new ClickHouseDecimal(int.MaxValue) },
        });

        using var reader = await client.ExecuteReaderAsync($"SELECT v FROM {table} ORDER BY id");
        var stored = new List<int>();
        while (reader.Read())
            stored.Add(reader.GetInt32(0));

        Assert.That(stored, Is.EqualTo(new[] { 100000, -40000, int.MinValue, int.MaxValue }));
    }

    [Test]
    [TestCase("Int8", 128)]
    [TestCase("UInt8", -1)]
    [TestCase("Int16", 40000)]
    [TestCase("UInt16", 70000)]
    public async Task InsertBinaryAsync_ClickHouseDecimalOutOfColumnRange_ThrowsOverflowException(string columnType, int value)
    {
        var table = CreateTableName($"decimal_overflow_{columnType}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (v {columnType}) ENGINE = Memory");

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(table, new[] { "v" }, new[] { new object[] { new ClickHouseDecimal(value) } }));

        Assert.That(ex.InnerException, Is.TypeOf<OverflowException>());
    }
}
