using System;
using System.Collections.Generic;
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
        public object Value { get; set; }
    }

    private static Action<Row, ExtendedBinaryWriter>[] GetWriters(PocoTypeRegistry registry, string columnType)
    {
        var property = typeof(Row).GetProperty(nameof(Row.Value));
        var properties = new[]
        {
            new PocoPropertyInfo
            {
                Property = property,
                ColumnName = "Value",
                PropertyName = "Value",
                PropertyType = typeof(object),
                CanAssignNull = true,
            },
        };
        var getters = new Func<Row, object>[] { row => row.Value };
        var types = new[] { TypeConverter.ParseClickHouseType(columnType, TypeSettings.Default) };

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
    }

    public static IEnumerable<TestCaseData> DistinctColumnTypes()
    {
        // Types whose declaration already differs: cached separately before and after the JSON fix.
        yield return new TestCaseData("UInt64", "Int64");
        yield return new TestCaseData("Decimal64(2)", "Decimal64(4)");

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
}
