using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Types;

public class DynamicTests : AbstractConnectionTestFixture
{
    public static IEnumerable<TestCaseData> DirectDynamicCastQueries
    {
        get
        {
            foreach (var sample in TestUtilities.GetDataTypeSamples().Where(s => ShouldBeSupportedInDynamic(s.ClickHouseType)))
            {
                yield return new TestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue)
                    .SetName($"Direct_{sample.ClickHouseType}_{sample.ExampleValue}");
            }

            // Some additional test cases for dynamic specifically
            // JSON with complex type hints
            yield return new TestCaseData(
                "'{\"a\": 1}'",
                "Json(max_dynamic_paths=10, max_dynamic_types=3, a Int64, SKIP path.to.skip, SKIP REGEXP 'regex.path.*')",
                new JsonObject { ["a"] = 1L }
            ).SetName("Direct_Json_Complex");
            
            yield return new TestCaseData(
                "1::Int32",
                "Dynamic",
                1
            ).SetName("Nested_Dynamic");
        }
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    [TestCaseSource(typeof(DynamicTests), nameof(DirectDynamicCastQueries))]
    public async Task ShouldParseDirectDynamicCast(string valueSql, string clickHouseType, object expectedValue)
    {
        // Direct cast to Dynamic without going through JSON
        using var reader =
            (ClickHouseDataReader)await connection.ExecuteReaderAsync(
                $"SELECT ({valueSql}::{clickHouseType})::Dynamic");

        ClassicAssert.IsTrue(reader.Read());
        var result = reader.GetValue(0);
        TestUtilities.AssertEqual(expectedValue, result);
        ClassicAssert.IsFalse(reader.Read());
    }

    private static bool ShouldBeSupportedInDynamic(string clickHouseType)
    {
        // Geo types not supported
        if (clickHouseType is "Point" or "Ring" or "LineString" or "Polygon" or "MultiLineString" or "MultiPolygon" or "Geometry" or "Nothing")
        {
            return false;
        }

        return true;
    }

    public static IEnumerable<TestCaseData> SimpleSelectQueries => TestUtilities.GetDataTypeSamples()
        .Where(s => ShouldBeSupportedInJson(s.ClickHouseType))
        .Select(sample => GetTestCaseData(sample.ExampleExpression, sample.ClickHouseType, sample.ExampleValue))
        .Where(x => x != null);

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    [TestCaseSource(typeof(DynamicTests), nameof(SimpleSelectQueries))]
    public async Task ShouldMatchFrameworkTypeViaJson(string valueSql, Type frameworkType)
    {
        // This query returns the value as Dynamic type via JSON. The dynamicType may or may not match the actual type provided.
        // eg IPv4 will be a String.
        using var reader =
            (ClickHouseDataReader) await connection.ExecuteReaderAsync(
                $"select json.value from (select map('value', {valueSql})::JSON as json)");

        ClassicAssert.IsTrue(reader.Read());
        var result = reader.GetValue(0);
        Assert.That(result.GetType(), Is.EqualTo(frameworkType));
        ClassicAssert.IsFalse(reader.Read());
    }

    private static TestCaseData GetTestCaseData(string exampleExpression, string clickHouseType, object exampleValue)
    {
        if (clickHouseType.StartsWith("Date"))
        {
            return new TestCaseData(exampleExpression, typeof(DateTime));
        }

        if (clickHouseType.StartsWith("Time"))
        {
            return new TestCaseData(exampleExpression, typeof(string));
        }

        if (clickHouseType.StartsWith("Int") || clickHouseType.StartsWith("UInt"))
        {
            return new TestCaseData(exampleExpression, typeof(long));
        }

        if (clickHouseType.StartsWith("FixedString"))
        {
            return new TestCaseData(exampleExpression, typeof(string));
        }
        
        if (clickHouseType.StartsWith("Float"))
        {
            var floatRemainder =
                exampleValue switch
                {
                    double @double => @double % 10,
                    float @float => @float % 10,
                    _ => throw new ArgumentException($"{exampleValue.GetType().Name} not supported for Float")
                };
            return new TestCaseData(
                exampleExpression,
                floatRemainder is 0
                    ? typeof(long)
                    : typeof(double));
        }

        switch (clickHouseType)
        {
            case "Array(Int32)" or "Array(Nullable(Int32))":
                return new TestCaseData(exampleExpression, typeof(long?[]));
            case "Array(Float32)" or "Array(Nullable(Float32))":
                return new TestCaseData(exampleExpression, typeof(double?[]));
            case "Array(String)":
                return new TestCaseData(exampleExpression, typeof(string[]));
            case "Array(Bool)":
                return new TestCaseData(exampleExpression, typeof(bool?[]));
            case "String" or "UUID":
                return new TestCaseData(exampleExpression, typeof(string));
            case "Nothing":
                return new TestCaseData(exampleExpression, typeof(DBNull));
            case "Bool":
                return new TestCaseData(exampleExpression, typeof(bool));
            case "IPv4" or "IPv6":
                return new TestCaseData(exampleExpression, typeof(string));
        }

        if (clickHouseType.StartsWith("Array"))
        {
            // Array handling is already covered above, we don't need to re-do it for every element type
            return null;
        }
        
        throw new ArgumentException($"{clickHouseType} not supported");
    }

    private static bool ShouldBeSupportedInJson(string clickHouseType)
    {
        if (clickHouseType.Contains("Decimal") ||
            clickHouseType.Contains("Enum") ||
            clickHouseType.Contains("LowCardinality") ||
            clickHouseType.Contains("Map") ||
            clickHouseType.Contains("Nested") ||
            clickHouseType.Contains("Nullable") ||
            clickHouseType.Contains("Tuple") ||
            clickHouseType.Contains("Variant") ||
            clickHouseType.Contains("BFloat16"))
        {
            return false;
        }

        switch (clickHouseType)
        {
            case "Int128":
            case "Int256":
            case "Json":
            case "UInt128":
            case "UInt256":
            case "Point":
            case "Ring":
            case "Geometry":
            case "LineString":
            case "MultiLineString":
            case "Polygon":
            case "MultiPolygon":
                return false;
            default:
                return true;
        }
    }
}
