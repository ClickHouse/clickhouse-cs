using System;
using System.Collections;
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
    public async Task ShouldReadSmallTuple_WithinFastPathArity_ReturnsSystemTupleWithValues([Range(1, 7)] int count)
    {
        var items = string.Join(",", Enumerable.Range(1, count).Select(i => $"toInt32({i})"));
        var result = await connection.ExecuteScalarAsync($"select tuple({items})");
        var tuple = result as ITuple;
        // Small tuples must materialize as exactly System.Tuple<int, ...> of the matching arity —
        // never a ValueTuple, a LargeTuple, or a Tuple`8 with TRest nesting.
        var expectedType = Type.GetType("System.Tuple`" + count)
            .MakeGenericType(Enumerable.Repeat(typeof(int), count).ToArray());
        Assert.Multiple(() =>
        {
            Assert.That(tuple, Is.Not.Null);
            Assert.That(result.GetType(), Is.EqualTo(expectedType));
            Assert.That(tuple.Length, Is.EqualTo(count));
            Assert.That(AsEnumerable(tuple), Is.EqualTo(Enumerable.Range(1, count)).AsCollection);
        });
    }

    [Test]
    public async Task ShouldReadLargeTuple_AboveFastPathArity_ReturnsLargeTupleWithValues()
    {
        var items = string.Join(",", Enumerable.Range(1, 8).Select(i => $"toInt32({i})"));
        var result = await connection.ExecuteScalarAsync($"select tuple({items})");
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            // 8+ element tuples have no System.Tuple<...> arity and stay on the LargeTuple path.
            Assert.That(result, Is.InstanceOf<LargeTuple>());
            Assert.That(tuple.Length, Is.EqualTo(8));
            Assert.That(AsEnumerable(tuple), Is.EqualTo(Enumerable.Range(1, 8)).AsCollection);
        });
    }

    [Test]
    public async Task ShouldReadTuple_WithNullableElement_PreservesNullsAsNull()
    {
        var result = await connection.ExecuteScalarAsync(
            "select tuple(CAST(NULL AS Nullable(Int32)), CAST(42 AS Nullable(Int32)), 'x')");
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            Assert.That(result.GetType().FullName, Does.StartWith("System.Tuple`"));
            Assert.That(tuple.Length, Is.EqualTo(3));
            Assert.That(tuple[0], Is.Null);
            Assert.That(tuple[1], Is.EqualTo(42));
            Assert.That(tuple[2], Is.EqualTo("x"));
        });
    }

    [Test]
    public async Task ShouldReadTuple_WithNestedCompositeElements_ReturnsElements()
    {
        var result = await connection.ExecuteScalarAsync(
            "select tuple([toInt32(1), toInt32(2), toInt32(3)], map('a', toUInt64(7)))");
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            Assert.That(result.GetType().FullName, Does.StartWith("System.Tuple`"));
            Assert.That(tuple.Length, Is.EqualTo(2));
            Assert.That(tuple[0], Is.EqualTo(new[] { 1, 2, 3 }));
            Assert.That(((IDictionary)tuple[1])["a"], Is.EqualTo(7UL));
        });
    }

    [Test]
    public async Task ShouldReadDynamicWrappedTuple_AcrossMultipleRows_ReturnsSystemTuplePerRow()
    {
        // Every value in a Dynamic column carries its own type header, so a fresh TupleType is
        // decoded per row. Reading several rows exercises the factory cache shared across those
        // instances rather than a single decode.
        var expectedStrings = new[] { "0", "1", "2" };
        var values = new List<ITuple>();
        using (var reader = await connection.ExecuteReaderAsync(
            "select tuple(toInt32(number), toString(number))::Dynamic from system.numbers limit 3"))
        {
            while (reader.Read())
                values.Add((ITuple)reader.GetValue(0));
        }

        Assert.Multiple(() =>
        {
            Assert.That(values, Has.Count.EqualTo(3));
            for (var i = 0; i < values.Count; i++)
            {
                Assert.That(values[i].GetType(), Is.EqualTo(typeof(Tuple<int, string>)));
                Assert.That(values[i][0], Is.EqualTo(i));
                Assert.That(values[i][1], Is.EqualTo(expectedStrings[i]));
            }
        });
    }

    [Test]
    public async Task ShouldReadTuple_WithNullableNothingElement_ReturnsNullElement()
    {
        // select tuple(NULL) yields Tuple(Nullable(Nothing)) — the one element whose framework type
        // is a reference type that only ever carries null.
        var result = await connection.ExecuteScalarAsync("select tuple(NULL)");
        var tuple = result as ITuple;
        Assert.Multiple(() =>
        {
            Assert.That(tuple, Is.Not.Null);
            Assert.That(result.GetType().FullName, Does.StartWith("System.Tuple`"));
            Assert.That(tuple.Length, Is.EqualTo(1));
            Assert.That(tuple[0], Is.Null);
        });
    }

    [Test]
    public async Task ShouldReadMap_WithTupleKey_ReturnsDictionaryKeyedByTuple()
    {
        // A tuple used as a Map key is materialized by the same read path and then used as a
        // dictionary key, so a wrong CLR type here would throw from Dictionary rather than compare.
        var result = await connection.ExecuteScalarAsync(
            "select map(tuple(toInt32(1), toInt32(2)), toInt32(3))");
        var dictionary = result as IDictionary;
        Assert.Multiple(() =>
        {
            Assert.That(dictionary, Is.Not.Null);
            Assert.That(dictionary.Count, Is.EqualTo(1));
            Assert.That(dictionary[Tuple.Create(1, 2)], Is.EqualTo(3));
        });
    }

    [Test]
    public void MakeTuple_WithSmallTupleType_ReturnsSystemTuplePassingValuesThrough()
    {
        // Read no longer routes small tuples through MakeTuple, so pin MakeTuple's retained
        // behaviour directly: values are passed to the constructor unchanged, and an arity mismatch
        // still throws.
        var tupleType = (TupleType)TypeConverter.ParseClickHouseType("Tuple(Int32, String)", TypeSettings.Default);
        var tuple = tupleType.MakeTuple(1, "a");
        Assert.Multiple(() =>
        {
            Assert.That(tuple, Is.TypeOf<Tuple<int, string>>());
            Assert.That(tuple[0], Is.EqualTo(1));
            Assert.That(tuple[1], Is.EqualTo("a"));
            Assert.Throws<ArgumentException>(() => tupleType.MakeTuple(1));
        });
    }

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

}
