using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class TupleTypeTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldSelectTuple([Range(1, 24, 4)] int count)
    {
        var items = string.Join(",", Enumerable.Range(1, count));
        var result = await connection.ExecuteScalarAsync($"select tuple({items})");
        ClassicAssert.IsInstanceOf<ITuple>(result);
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(count));
            Assert.That(AsEnumerable(tuple), Is.EqualTo(Enumerable.Range(1, count)).AsCollection);
        });
    }

    private static IEnumerable<object> AsEnumerable(ITuple tuple) => Enumerable.Range(0, tuple.Length).Select(i => tuple[i]);

    [Test]
    [TestCase("Tuple(String, Int32)")]
    [TestCase("Tuple(name String, age Int32)")]
    public void ShouldParseNamedTupleFields(string typeString)
    {
        var type = TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);
        ClassicAssert.IsInstanceOf<TupleType>(type);
    }

    [Test]
    [TestCase("Tuple(name String, status Enum8('Active' = 0, 'Inactive' = 1))")]
    [TestCase("Tuple(id Int32, value Decimal(10, 2))")]
    [TestCase("Tuple(timestamp DateTime64(3, 'UTC'), value Float64)")]
    [TestCase("Tuple(code FixedString(5), count Int32)")]
    [TestCase("Tuple(name String, tags Array(String))")]
    [TestCase("Tuple(name String, optional Nullable(Int32))")]
    [TestCase("Tuple(key String, value LowCardinality(String))")]
    public void ShouldParseNamedTupleWithParameterizedTypes(string typeString)
    {
        // Named tuple fields with parameterized types should parse without throwing
        Assert.DoesNotThrow(() =>
        {
            var type = TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);
            ClassicAssert.IsInstanceOf<TupleType>(type);
            var tupleType = (TupleType)type;
            Assert.That(tupleType.UnderlyingTypes.Length, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task InsertBinaryAsync_ValueTuple_ShouldRoundTrip()
    {
        var targetTable = "test.valuetuple_roundtrip";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {targetTable} (
                id UInt32,
                data Tuple(Int32, String)
            ) ENGINE = MergeTree() ORDER BY id");

        await client.InsertBinaryAsync(targetTable, ["id", "data"], [
            new object[] { 1u, (42, "hello") },
            new object[] { 2u, (99, "world") },
        ]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT id, data FROM {targetTable} ORDER BY id");
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<uint>(0), Is.EqualTo(1u));
        var tuple1 = (ITuple)reader.GetValue(1);
        Assert.That(tuple1[0], Is.EqualTo(42));
        Assert.That(tuple1[1], Is.EqualTo("hello"));

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<uint>(0), Is.EqualTo(2u));
        var tuple2 = (ITuple)reader.GetValue(1);
        Assert.That(tuple2[0], Is.EqualTo(99));
        Assert.That(tuple2[1], Is.EqualTo("world"));
    }

    [Test]
    public async Task InsertBinaryAsync_ValueTupleWithNullableElement_ShouldRoundTrip()
    {
        var targetTable = "test.valuetuple_nullable";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {targetTable} (
                id UInt32,
                data Tuple(Int32, Nullable(String))
            ) ENGINE = MergeTree() ORDER BY id");

        await client.InsertBinaryAsync(targetTable, ["id", "data"], [
            new object[] { 1u, (42, (string)null) },
            new object[] { 2u, (99, (string)"present") },
        ]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT id, data FROM {targetTable} ORDER BY id");
        Assert.That(reader.Read(), Is.True);
        var tuple1 = (ITuple)reader.GetValue(1);
        Assert.That(tuple1[0], Is.EqualTo(42));
        Assert.That(tuple1[1], Is.Null);

        Assert.That(reader.Read(), Is.True);
        var tuple2 = (ITuple)reader.GetValue(1);
        Assert.That(tuple2[0], Is.EqualTo(99));
        Assert.That(tuple2[1], Is.EqualTo("present"));
    }

    [Test]
    public async Task InsertBinaryAsync_NestedValueTuple_ShouldRoundTrip()
    {
        var targetTable = "test.valuetuple_nested";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {targetTable} (
                id UInt32,
                data Tuple(Int32, Tuple(String, UInt8))
            ) ENGINE = MergeTree() ORDER BY id");

        await client.InsertBinaryAsync(targetTable, ["id", "data"], [
            new object[] { 1u, (10, ("inner", (byte)5)) },
        ]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT id, data FROM {targetTable} ORDER BY id");
        Assert.That(reader.Read(), Is.True);
        var outer = (ITuple)reader.GetValue(1);
        Assert.That(outer[0], Is.EqualTo(10));
        var inner = (ITuple)outer[1];
        Assert.That(inner[0], Is.EqualTo("inner"));
        Assert.That(inner[1], Is.EqualTo((byte)5));
    }

    [Test]
    public async Task InsertBinaryAsync_ValueTupleInArray_ShouldRoundTrip()
    {
        var targetTable = "test.valuetuple_in_array";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {targetTable} (
                id UInt32,
                data Array(Tuple(Int32, String))
            ) ENGINE = MergeTree() ORDER BY id");

        // Use ITuple[] so the array element type is known to be tuple-compatible
        var tuples = new ITuple[] { (1, "one"), (2, "two") };
        await client.InsertBinaryAsync(targetTable, ["id", "data"], [
            new object[] { 1u, tuples },
        ]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT id, data FROM {targetTable} ORDER BY id");
        Assert.That(reader.Read(), Is.True);
        var array = (Tuple<int, string>[])reader.GetValue(1);
        Assert.That(array.Length, Is.EqualTo(2));
        Assert.That(array[0].Item1, Is.EqualTo(1));
        Assert.That(array[0].Item2, Is.EqualTo("one"));
        Assert.That(array[1].Item1, Is.EqualTo(2));
        Assert.That(array[1].Item2, Is.EqualTo("two"));
    }

    [Test]
    public async Task InsertBinaryAsync_ValueTupleWithArrayElement_ShouldRoundTrip()
    {
        var targetTable = "test.valuetuple_with_array";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {targetTable} (
                id UInt32,
                data Tuple(String, Array(Int32))
            ) ENGINE = MergeTree() ORDER BY id");

        await client.InsertBinaryAsync(targetTable, ["id", "data"], [
            new object[] { 1u, ("tags", new[] { 10, 20, 30 }) },
        ]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT id, data FROM {targetTable} ORDER BY id");
        Assert.That(reader.Read(), Is.True);
        var tuple = (ITuple)reader.GetValue(1);
        Assert.That(tuple[0], Is.EqualTo("tags"));
        Assert.That(tuple[1], Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task InsertBinaryAsync_LargeValueTuple_ShouldRoundTrip()
    {
        var targetTable = "test.valuetuple_large";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {targetTable} (
                id UInt32,
                data Tuple(Int32, Int32, Int32, Int32, Int32, Int32, Int32, String)
            ) ENGINE = MergeTree() ORDER BY id");

        // C# (1,2,3,4,5,6,7,"eight") compiles to ValueTuple<int,int,int,int,int,int,int,ValueTuple<string>>
        // The ITuple interface flattens this, so tuple.Length == 8 and tuple[7] == "eight"
        await client.InsertBinaryAsync(targetTable, ["id", "data"], [
            new object[] { 1u, (1, 2, 3, 4, 5, 6, 7, "eight") },
        ]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT id, data FROM {targetTable} ORDER BY id");
        Assert.That(reader.Read(), Is.True);
        var tuple = (ITuple)reader.GetValue(1);
        Assert.That(tuple.Length, Is.EqualTo(8));
        for (int i = 0; i < 7; i++)
            Assert.That(tuple[i], Is.EqualTo(i + 1));
        Assert.That(tuple[7], Is.EqualTo("eight"));
    }
}
