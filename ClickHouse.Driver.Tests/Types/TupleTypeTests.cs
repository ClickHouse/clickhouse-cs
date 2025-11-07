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
#if NET48 || NET5_0_OR_GREATER
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
#endif

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
