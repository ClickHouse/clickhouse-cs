using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Poco;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Tests.Copy;

/// <summary>
/// Server-free tests for the key of the POCO insert writer cache. The compiled write delegates close
/// over the resolved <see cref="ClickHouseType"/>, so two column types that write differently must never
/// share a cache entry — while two identical column types still must, or every insert recompiles.
/// </summary>
[TestFixture]
public class PocoWriterCacheKeyTests
{
    private sealed class Row
    {
        public object First { get; set; }

        public object Second { get; set; }
    }

    private static readonly Func<Row, object>[] Getters = { row => row.First, row => row.Second };

    private static Action<Row, ExtendedBinaryWriter>[] GetWriters(PocoTypeRegistry registry, params string[] columnTypes)
    {
        var names = new[] { nameof(Row.First), nameof(Row.Second) };
        var properties = columnTypes
            .Select((_, i) => new PocoPropertyInfo
            {
                Property = typeof(Row).GetProperty(names[i]),
                ColumnName = names[i],
                PropertyName = names[i],
                PropertyType = typeof(object),
                CanAssignNull = true,
            })
            .ToArray();
        var getters = Getters.Take(columnTypes.Length).ToArray();
        var types = columnTypes
            .Select(columnType => TypeConverter.ParseClickHouseType(columnType, TypeSettings.Default))
            .ToArray();

        return registry.GetOrBuildWriters(properties, getters, types);
    }

    public static IEnumerable<TestCaseData> EquivalentColumnTypes()
    {
        yield return new TestCaseData("UInt64", "UInt64");
        yield return new TestCaseData("Decimal64(2)", "Decimal64(2)");
        yield return new TestCaseData("Array(Int64)", "Array(Int64)");
        yield return new TestCaseData("JSON(a Int64)", "JSON(a Int64)");
        yield return new TestCaseData("Array(JSON(a Int64))", "Array(JSON(a Int64))");

        // The same hints in a different declaration order write identically.
        yield return new TestCaseData("JSON(a Int64, b String)", "JSON(b String, a Int64)");

        // Composing types without a hidden-state child keep rendering as their own declaration.
        yield return new TestCaseData("Nullable(String)", "Nullable(String)");
        yield return new TestCaseData("LowCardinality(String)", "LowCardinality(String)");
        yield return new TestCaseData("Map(String, Int64)", "Map(String, Int64)");
        yield return new TestCaseData("Variant(Int64, String)", "Variant(Int64, String)");
        yield return new TestCaseData("SimpleAggregateFunction(anyLast, Int64)", "SimpleAggregateFunction(anyLast, Int64)");
        yield return new TestCaseData("QBit(BFloat16, 16)", "QBit(BFloat16, 16)");
        yield return new TestCaseData("Point", "Point");
        yield return new TestCaseData("Ring", "Ring");
    }

    public static IEnumerable<TestCaseData> DistinctColumnTypes()
    {
        // Types whose declaration already differs: cached separately before and after the JSON fix.
        yield return new TestCaseData("UInt64", "Int64");
        yield return new TestCaseData("Decimal64(2)", "Decimal64(4)");
        yield return new TestCaseData("Nullable(String)", "Nullable(Int64)");
        yield return new TestCaseData("LowCardinality(String)", "LowCardinality(FixedString(2))");
        yield return new TestCaseData("Variant(Int64, String)", "Variant(Int64, UInt64)");
        yield return new TestCaseData("SimpleAggregateFunction(anyLast, Int64)", "SimpleAggregateFunction(anyLast, String)");
        yield return new TestCaseData("QBit(BFloat16, 16)", "QBit(Float32, 16)");

        // JSON path hints steer the write but are absent from JsonType.ToString().
        yield return new TestCaseData("JSON(a Int64)", "JSON(a String)");
        yield return new TestCaseData("JSON(a Decimal64(2))", "JSON(a Decimal64(4))");
        yield return new TestCaseData("JSON(a Int64)", "JSON");
        yield return new TestCaseData("JSON(a Int64)", "JSON(b Int64)");
        yield return new TestCaseData("JSON(a Int64)", "JSON(a Int64, b String)");

        // The same hints nested inside a container type.
        yield return new TestCaseData("Array(JSON(a Int64))", "Array(JSON(a String))");
        yield return new TestCaseData("Map(String, JSON(a Int64))", "Map(String, JSON(a String))");
        // Canonical "Json" spelling: a named tuple element is only matched by the registered type name.
        yield return new TestCaseData("Tuple(x Json(a Int64))", "Tuple(x Json(a String))");
        yield return new TestCaseData("Array(Array(JSON(a Int64)))", "Array(Array(JSON(a String)))");
        yield return new TestCaseData("Nullable(JSON(a Int64))", "Nullable(JSON(a String))");
        yield return new TestCaseData("LowCardinality(JSON(a Int64))", "LowCardinality(JSON(a String))");
        yield return new TestCaseData("Variant(Int64, JSON(a Int64))", "Variant(Int64, JSON(a String))");
        yield return new TestCaseData("SimpleAggregateFunction(anyLast, JSON(a Int64))", "SimpleAggregateFunction(anyLast, JSON(a String))");
        yield return new TestCaseData("QBit(JSON(a Int64), 16)", "QBit(JSON(a String), 16)");

        // A geometric type writes differently from the tuple or array it is built on, and its name is
        // rendered by ToString() alone.
        yield return new TestCaseData("Point", "Tuple(Float64, Float64)");
        yield return new TestCaseData("Ring", "LineString");
        yield return new TestCaseData("Ring", "Array(Point)");
        yield return new TestCaseData("Polygon", "MultiLineString");
        yield return new TestCaseData("MultiPolygon", "Array(Polygon)");

        // A JSON path is an arbitrary identifier, so it can spell the separators between the hints.
        yield return new TestCaseData(@"JSON(`a Int64, b` String)", "JSON(a Int64, b String)");
    }

    [Test]
    [TestCaseSource(nameof(EquivalentColumnTypes))]
    public void GetOrBuildWriters_EquivalentColumnTypes_ReusesCachedDelegates(string first, string second)
    {
        var registry = new PocoTypeRegistry();

        Assert.That(GetWriters(registry, second), Is.SameAs(GetWriters(registry, first)));
    }

    [Test]
    [TestCaseSource(nameof(DistinctColumnTypes))]
    public void GetOrBuildWriters_ColumnTypesThatWriteDifferently_BuildsSeparateDelegates(string first, string second)
    {
        var registry = new PocoTypeRegistry();

        Assert.That(GetWriters(registry, second), Is.Not.SameAs(GetWriters(registry, first)));
    }

    [Test]
    public void GetOrBuildWriters_ColumnTypeContainingTheSeparator_BuildsSeparateDelegates()
    {
        // A JSON path is an arbitrary identifier, so it can hold any character a key separator might use —
        // the server escapes it, the client discloses it back. Two columns must not collide with one column
        // whose path happens to spell them both.
        var registry = new PocoTypeRegistry();
        var twoColumns = GetWriters(registry, "JSON(a Int64)", "JSON(b Int64)");
        var oneColumn = GetWriters(registry, @"JSON(`a Int64)\nJson(b` Int64)");

        Assert.That(oneColumn, Is.Not.SameAs(twoColumns));
        Assert.That(oneColumn, Has.Length.EqualTo(1));
    }
}
