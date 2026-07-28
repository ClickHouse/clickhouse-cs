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
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NUnit.Framework.Legacy;
#pragma warning disable CS0618 // Type or member is obsolete

namespace ClickHouse.Driver.Tests.Types;

public class DynamicTests : AbstractConnectionTestFixture
{
    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task ShouldReadDynamicStringAsByteArray()
    {
        // Test that ReadStringsAsByteArrays setting works with Dynamic type containing String
        var cb = TestUtilities.GetConnectionStringBuilder();
        cb.ReadStringsAsByteArrays = true;
        using var conn = new ClickHouseConnection(cb.ToString());

        using var reader = await conn.ExecuteReaderAsync("SELECT 'hello'::Dynamic");
        ClassicAssert.IsTrue(reader.Read());
        var result = reader.GetValue(0);
        Assert.That(result, Is.TypeOf<byte[]>());
        Assert.That(result, Is.EqualTo(new byte[] { 0x68, 0x65, 0x6C, 0x6C, 0x6F })); // "hello" in UTF-8
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task ShouldReadDynamicFixedStringAsByteArray()
    {
        // Test that ReadStringsAsByteArrays setting works with Dynamic type containing FixedString
        var cb = TestUtilities.GetConnectionStringBuilder();
        cb.ReadStringsAsByteArrays = true;
        using var conn = new ClickHouseConnection(cb.ToString());

        using var reader = await conn.ExecuteReaderAsync("SELECT 'test'::FixedString(4)::Dynamic");
        ClassicAssert.IsTrue(reader.Read());
        var result = reader.GetValue(0);
        Assert.That(result, Is.TypeOf<byte[]>());
        Assert.That(result, Is.EqualTo(new byte[] { 0x74, 0x65, 0x73, 0x74 })); // "test" in UTF-8
    }

    public static IEnumerable<TestCaseData> DirectDynamicCastQueries
    {
        get
        {
            foreach (var sample in TestCases.GetDataTypeSamples().Where(s => ShouldBeSupportedInDynamic(s.ClickHouseType)))
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

    public static IEnumerable<TestCaseData> SimpleSelectQueries => TestCases.GetDataTypeSamples()
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
            clickHouseType.Contains("BFloat16") ||
            clickHouseType.Contains("QBit"))
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

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_Int32_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_int32";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, 42 }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(42));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_Int64_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_int64";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, 9223372036854775807L }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(9223372036854775807L));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_Double_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_double";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, 3.14159 }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That((double)reader.GetValue(0), Is.EqualTo(3.14159).Within(0.00001));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_String_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_string";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, "hello world" }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo("hello world"));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_Bool_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_bool";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, true }, new object[] { 2u, false }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable} ORDER BY id");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(true));
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(false));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_DateTime_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_datetime";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        var dateTime = new DateTime(2024, 6, 15, 10, 30, 45, DateTimeKind.Unspecified);

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, dateTime }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (DateTime)reader.GetValue(0);
        Assert.That(result.Year, Is.EqualTo(2024));
        Assert.That(result.Month, Is.EqualTo(6));
        Assert.That(result.Day, Is.EqualTo(15));
        Assert.That(result.Hour, Is.EqualTo(10));
        Assert.That(result.Minute, Is.EqualTo(30));
        Assert.That(result.Second, Is.EqualTo(45));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_Guid_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_guid";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        var guid = Guid.NewGuid();

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, guid }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(guid));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_Decimal_ShouldPreservePrecision()
    {
        var targetTable = "test.dynamic_write_decimal";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        var decimalValue = 123.456789m;

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, decimalValue }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (ClickHouseDecimal)reader.GetValue(0);
        Assert.That(result, Is.EqualTo(new ClickHouseDecimal(123.456789m)));
    }

    // Regression coverage for issue #466: a decimal written to a Dynamic column was inferred from
    // the .NET type via a per-Type cache that hardcoded Decimal128(38, 9), so any value with scale
    // greater than 9 was silently truncated on write. These cases exercise scales the fixed scale
    // could not represent, spanning the Decimal32/64/128/256 widths, plus a low-scale contrast case
    // that must keep round-tripping unchanged.
    public static IEnumerable<TestCaseData> DynamicDecimalRoundTripCases
    {
        get
        {
            yield return new TestCaseData(0.0123456789012345m, "scale16").SetName("Write_DynamicDecimal_Scale16");
            yield return new TestCaseData(0.0000000001m, "tiny_scale10").SetName("Write_DynamicDecimal_TinyScale10");
            yield return new TestCaseData(12345.6789012345m, "intpart_scale10").SetName("Write_DynamicDecimal_IntegerPartScale10");
            // 28 significant digits with scale 10: the width must be chosen by the digit count
            // (forces Decimal128), not the scale alone (which would only need Decimal64).
            yield return new TestCaseData(123456789012345678.0123456789m, "digits28_scale10").SetName("Write_DynamicDecimal_DigitsDominatedScale10");
            yield return new TestCaseData(-0.0123456789012345m, "negative_scale16").SetName("Write_DynamicDecimal_NegativeScale16");
            yield return new TestCaseData(0.1234567890123456789012345678m, "scale28").SetName("Write_DynamicDecimal_MaxDecimalScale28");
            // Scale 40 is beyond System.Decimal's 28-digit limit, so it can only arrive as a
            // ClickHouseDecimal; it must widen to Decimal256.
            yield return new TestCaseData(
                new ClickHouseDecimal(BigInteger.Parse("123456789012345678901234567890"), 40), "scale40")
                .SetName("Write_DynamicClickHouseDecimal_Scale40");
            // Contrast: scale 6 already fit inside the old fixed scale of 9 and round-tripped; it must
            // keep round-tripping (now via a narrower Decimal32) with the same value.
            yield return new TestCaseData(123.456789m, "contrast_scale6").SetName("Write_DynamicDecimal_Scale6_Contrast");
        }
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    [TestCaseSource(typeof(DynamicTests), nameof(DynamicDecimalRoundTripCases))]
    public async Task Write_DecimalWithScaleAbove9_ShouldRoundTripWithoutTruncation(object value, string caseId)
    {
        var targetTable = "test." + SanitizeTableName($"dynamic_write_decimal_{caseId}_{Guid.NewGuid():N}");
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        var expected = value is ClickHouseDecimal chd ? chd : new ClickHouseDecimal((decimal)value);

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, value }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (ClickHouseDecimal)reader.GetValue(0);
        Assert.That(result, Is.EqualTo(expected));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_IntArray_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_int_array";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        var array = new[] { 1, 2, 3, 4, 5 };

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, array }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (int[])reader.GetValue(0);
        Assert.That(result, Is.EqualTo(array));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_StringList_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_string_list";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        var list = new List<string> { "a", "b", "c" };

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, list }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (string[])reader.GetValue(0);
        Assert.That(result, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_Dictionary_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_dictionary";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        var dict = new Dictionary<string, int> { ["one"] = 1, ["two"] = 2 };

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, dict }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (Dictionary<string, int>)reader.GetValue(0);
        Assert.That(result["one"], Is.EqualTo(1));
        Assert.That(result["two"], Is.EqualTo(2));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_MixedTypesInSameColumn_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_mixed";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([
            new object[] { 1u, 42 },
            new object[] { 2u, "hello" },
            new object[] { 3u, 3.14 },
            new object[] { 4u, true }
        ]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT id, value FROM {targetTable} ORDER BY id");

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo(42));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo("hello"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That((double)reader.GetValue(1), Is.EqualTo(3.14));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo(true));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_Null_ShouldRoundTrip()
    {
        var targetTable = "test.dynamic_write_null";
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, null }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(DBNull.Value));
    }
}
