using System;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using NUnit.Framework.Legacy;
#pragma warning disable CS0618 // Type or member is obsolete

namespace ClickHouse.Driver.Tests.Types;

public class VariantTests : AbstractConnectionTestFixture
{
    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task Read_NoneDiscriminator_ShouldReturnDbNull()
    {
        await connection.ExecuteStatementAsync(
            "CREATE OR REPLACE TABLE test.test_variant_null (id UInt32, val Variant(String, UInt64)) ENGINE = Memory");

        await connection.ExecuteStatementAsync(
            "INSERT INTO test.test_variant_null VALUES (1, 'hello'), (2, 42), (3, NULL)");

        using var reader = await connection.ExecuteReaderAsync(
            "SELECT id, val FROM test.test_variant_null ORDER BY id");

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(1u));
        Assert.That(reader.GetValue(1), Is.EqualTo("hello"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(2u));
        Assert.That(reader.GetValue(1), Is.EqualTo((ulong)42));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(3u));
        Assert.That(reader.GetValue(1), Is.EqualTo(DBNull.Value));

        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task Write_Null_ShouldRoundTrip()
    {
        var targetTable = "test.test_variant_write_null";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, val Variant(String, UInt64)) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([
            new object[] { 1u, "hello" },
            new object[] { 2u, null },
        ]);

        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, val FROM {targetTable} ORDER BY id");

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(1u));
        Assert.That(reader.GetValue(1), Is.EqualTo("hello"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(2u));
        Assert.That(reader.GetValue(1), Is.EqualTo(DBNull.Value));

        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task ParameterizedSelect_Null_ShouldReturnDbNull()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {var:Variant(String, UInt64)}";
        command.AddParameter("var", DBNull.Value);

        using var reader = await command.ExecuteReaderAsync();
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(DBNull.Value));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task ParameterizedSelect_ArrayWithNullElement_ShouldRoundTrip()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {var:Array(Variant(UInt64, String))}";
        command.AddParameter("var", new object[] { (ulong)1, null, "hello" });

        using var reader = await command.ExecuteReaderAsync();
        ClassicAssert.IsTrue(reader.Read());
        var result = (object[])reader.GetValue(0);
        Assert.That(result[0], Is.EqualTo((ulong)1));
        Assert.That(result[1], Is.Null);
        Assert.That(result[2], Is.EqualTo("hello"));
        ClassicAssert.IsFalse(reader.Read());
    }
}
