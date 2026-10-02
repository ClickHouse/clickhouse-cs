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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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

    // A decimal written to Dynamic is stored as the narrowest Decimal32/64/128/256 whose precision P
    // holds its integer digits and scale, with the scale widened to P - integerDigits.
    // Args: (value, expected dynamicType() reported by the server).
    private static IEnumerable<TestCaseData> DynamicDecimalCases()
    {
        // Values that differ only in fractional length share one type.
        yield return new TestCaseData(1.2m, "Decimal(9, 8)");
        yield return new TestCaseData(1.20m, "Decimal(9, 8)");
        yield return new TestCaseData(1.23m, "Decimal(9, 8)");
        yield return new TestCaseData(1.2345m, "Decimal(9, 8)");
        yield return new TestCaseData(12.34m, "Decimal(9, 7)");
        yield return new TestCaseData(-1.2345m, "Decimal(9, 8)");
        yield return new TestCaseData(0m, "Decimal(9, 8)");
        yield return new TestCaseData(0.00m, "Decimal(9, 9)");
        yield return new TestCaseData(-0.5m, "Decimal(9, 9)");

        // Width boundaries, reached through the integer digits or through the scale.
        yield return new TestCaseData(123456789m, "Decimal(9, 0)");
        yield return new TestCaseData(1234567890m, "Decimal(18, 8)");
        yield return new TestCaseData(0.123456789m, "Decimal(9, 9)");
        yield return new TestCaseData(0.0000000001m, "Decimal(18, 18)");
        yield return new TestCaseData(-12345678.9m, "Decimal(9, 1)");
        yield return new TestCaseData(123456789.1m, "Decimal(18, 9)");
        yield return new TestCaseData(123456789012345678m, "Decimal(18, 0)");
        yield return new TestCaseData(1234567890123456789m, "Decimal(38, 19)");
        yield return new TestCaseData(0.0123456789012345m, "Decimal(18, 18)");
        yield return new TestCaseData(0.000000000000000001m, "Decimal(18, 18)");
        yield return new TestCaseData(0.0000000000000000001m, "Decimal(38, 38)");
        yield return new TestCaseData(0.1234567890123456789012345678m, "Decimal(38, 38)");
        yield return new TestCaseData(decimal.MaxValue, "Decimal(38, 9)");
        yield return new TestCaseData(decimal.MinValue, "Decimal(38, 9)");

        // Beyond System.Decimal: only reachable through ClickHouseDecimal.
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.Parse(new string('9', 38)), 0), "Decimal(38, 0)");
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.Parse("1" + new string('0', 38)), 0), "Decimal(76, 37)");
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.One, 38), "Decimal(38, 38)");
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.One, 39), "Decimal(76, 76)");
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.One, 76), "Decimal(76, 76)");
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.Parse("-" + new string('9', 76)), 0), "Decimal(76, 0)");
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.Parse("123456789012345678901234567890"), 40), "Decimal(76, 76)");
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.Zero, 80), "Decimal(76, 76)");
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.Pow(10, 77), 77), "Decimal(76, 75)");
    }

    // Values that System.Decimal holds exactly, read back with UseCustomDecimals=false.
    // Args: (value written, expected System.Decimal, expected dynamicType()).
    private static IEnumerable<TestCaseData> DynamicSystemDecimalCases()
    {
        foreach (var c in DynamicDecimalCases().Where(c => c.Arguments[0] is decimal))
            yield return new TestCaseData(c.Arguments[0], c.Arguments[0], c.Arguments[1]);

        // Stored as Decimal256.
        yield return new TestCaseData(new ClickHouseDecimal(5 * BigInteger.Pow(10, 39), 40), 0.5m, "Decimal(76, 76)");
        yield return new TestCaseData(new ClickHouseDecimal(BigInteger.Zero, 80), 0m, "Decimal(76, 76)");
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    [TestCaseSource(nameof(DynamicDecimalCases))]
    public async Task Write_DecimalToDynamic_StoresSharedTypeAndPreservesValue(object value, string expectedType)
    {
        var targetTable = CreateTableName($"dynamic_decimal_{expectedType}");
        await connection.ExecuteStatementAsync(
            $"CREATE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, value }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value, dynamicType(value) FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetString(1), Is.EqualTo(expectedType));
        var expected = value is ClickHouseDecimal chd ? chd : new ClickHouseDecimal((decimal)value);
        Assert.That((ClickHouseDecimal)reader.GetValue(0), Is.EqualTo(expected));
        if (value is decimal dec)
            Assert.That(reader.GetDecimal(0), Is.EqualTo(dec));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    [TestCaseSource(nameof(DynamicSystemDecimalCases))]
    public async Task Read_DecimalFromDynamicWithoutCustomDecimals_ReturnsSystemDecimalWithoutOverflow(object value, decimal expected, string expectedType)
    {
        var targetTable = CreateTableName($"dynamic_decimal_{expectedType}");
        await connection.ExecuteStatementAsync(
            $"CREATE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, value }]);

        using var systemDecimalConnection = TestUtilities.GetTestClickHouseConnection(customDecimals: false);
        using var reader = await systemDecimalConnection.ExecuteReaderAsync($"SELECT value, dynamicType(value) FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetString(1), Is.EqualTo(expectedType));
        Assert.That(reader.GetValue(0), Is.TypeOf<decimal>().And.EqualTo(expected));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_DecimalsOfDifferentFractionalLengthToDynamic_StoresOneSharedType()
    {
        var targetTable = CreateTableName();
        await connection.ExecuteStatementAsync(
            $"CREATE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        var values = new[] { 1.2m, 1.23m, 1.2345m, 9.87654321m, -3.5m };
        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync(values.Select((v, i) => new object[] { (uint)i, v }).ToList());

        using var reader = await connection.ExecuteReaderAsync($"SELECT value, dynamicType(value) FROM {targetTable} ORDER BY id");
        foreach (var expected in values)
        {
            ClassicAssert.IsTrue(reader.Read());
            Assert.That(reader.GetString(1), Is.EqualTo("Decimal(9, 8)"));
            Assert.That((ClickHouseDecimal)reader.GetValue(0), Is.EqualTo(new ClickHouseDecimal(expected)));
        }

        ClassicAssert.IsFalse(reader.Read());
    }

    // All decimals at one position of a collection written to Dynamic share one type: the narrowest
    // Decimal32/64/128/256 whose precision P holds the largest integer-digit count and the largest scale
    // among them, with the scale widened to P - integerDigits.
    // Args: (value, expected dynamicType() reported by the server, expected value read back).
    private static IEnumerable<TestCaseData> DynamicDecimalCollectionCases()
    {
        static TestCaseData Case(object value, string expectedType, object expected = null) =>
            new TestCaseData(value, expectedType, expected ?? value);

        // A single element gets the same type as the scalar value.
        yield return Case(new[] { 1.2m }, "Array(Decimal(9, 8))");
        yield return Case(new[] { 1.23m }, "Array(Decimal(9, 8))");
        yield return Case(new[] { 1.2345m }, "Array(Decimal(9, 8))");
        yield return Case(new[] { 12.34m }, "Array(Decimal(9, 7))");

        // The largest scale and the most integer digits can come from different elements, in any order.
        yield return Case(new[] { 0.0123456789012345m, 0.0000000001m }, "Array(Decimal(18, 18))");
        yield return Case(new[] { 1.5m, -0.0123456789012345m }, "Array(Decimal(18, 17))");
        yield return Case(new[] { -0.0123456789012345m, 1.5m }, "Array(Decimal(18, 17))");
        yield return Case(new List<decimal> { -12.5m, 0.0000000001m }, "Array(Decimal(18, 16))");
        yield return Case(new List<decimal> { 0.0000000001m, -12.5m }, "Array(Decimal(18, 16))");

        // Width boundaries that only the combination of elements reaches or crosses.
        yield return Case(new[] { 12345678m, 0.1m }, "Array(Decimal(9, 1))");
        yield return Case(new[] { 123456789m, 0.1m }, "Array(Decimal(18, 9))");
        yield return Case(new[] { 12345678m, 0.0000000001m }, "Array(Decimal(18, 10))");
        yield return Case(new[] { 0m, 0.0000000001m }, "Array(Decimal(18, 17))");
        yield return Case(new[] { 12345678901234567890m, 0.000000000000000001m }, "Array(Decimal(38, 18))");
        yield return Case(new[] { 0.0000000000000000001m, 12345678901234567890m }, "Array(Decimal(76, 56))");
        yield return Case(new List<decimal> { decimal.MaxValue, 0.0000000001m, decimal.MinValue }, "Array(Decimal(76, 47))");

        // ClickHouseDecimal, including values beyond System.Decimal.
        yield return Case(new[] { new ClickHouseDecimal(0.0123456789012345m), new ClickHouseDecimal(-0.0000000001m) }, "Array(Decimal(18, 18))");
        yield return Case(new List<ClickHouseDecimal> { new(1.5m), new(BigInteger.One, 30) }, "Array(Decimal(38, 37))");
        yield return Case(new[] { new ClickHouseDecimal(BigInteger.Pow(10, 38), 0), new ClickHouseDecimal(BigInteger.MinusOne, 30) }, "Array(Decimal(76, 37))");

        yield return Case(new[] { new ClickHouseDecimal(BigInteger.Pow(10, 27), 0), new ClickHouseDecimal(BigInteger.One, 48) }, "Array(Decimal(76, 48))");

        // Trailing fractional zeros, and all the digits of a zero, are exact at a smaller scale, so they do not
        // count toward the 76-digit limit.
        yield return Case(new[] { new ClickHouseDecimal(BigInteger.Zero, 80), new ClickHouseDecimal(1.5m) }, "Array(Decimal(76, 75))");
        yield return Case(new[] { new ClickHouseDecimal(BigInteger.Pow(10, 28), 0), new ClickHouseDecimal(BigInteger.Pow(10, 48), 48) }, "Array(Decimal(76, 47))");

        // Null elements are skipped. With no non-null element, the type is the Decimal(38, 9) default.
        yield return Case(new decimal?[] { null, 0.0000000001m, 1.5m }, "Array(Nullable(Decimal(18, 17)))");
        yield return Case(new List<ClickHouseDecimal?> { new(1.5m), null, new(BigInteger.One, 10) }, "Array(Nullable(Decimal(18, 17)))");
        yield return Case(new decimal?[] { null, null }, "Array(Nullable(Decimal(38, 9)))");
        yield return Case(Array.Empty<decimal>(), "Array(Decimal(38, 9))");
        yield return Case(new[] { null, new[] { 0.0000000001m } }, "Array(Array(Decimal(18, 18)))", new[] { Array.Empty<decimal>(), new[] { 0.0000000001m } });
        yield return Case(new Dictionary<string, decimal?> { ["a"] = null, ["b"] = 1.5m }, "Map(String, Nullable(Decimal(9, 8)))");

        // Nested arrays, maps and tuples.
        yield return Case(new[] { new[] { 1.5m }, new[] { 0.0000000001m } }, "Array(Array(Decimal(18, 17)))");
        yield return Case(new[,] { { 1.5m, 0.0000000001m } }, "Array(Array(Decimal(18, 17)))", new[] { new[] { 1.5m, 0.0000000001m } });
        yield return Case(new Dictionary<string, decimal> { ["a"] = 1.5m, ["b"] = 0.0000000001m }, "Map(String, Decimal(18, 17))");
        yield return Case(
            new Dictionary<decimal, int> { [1.5m] = 1, [0.0000000001m] = 2 },
            "Map(Decimal(18, 17), Int32)",
            new[] { KeyValuePair.Create(1.5m, 1), KeyValuePair.Create(0.0000000001m, 2) });
        yield return Case(
            new List<KeyValuePair<string, decimal>> { new("a", 1.5m), new("b", 0.0000000001m) },
            "Map(String, Decimal(18, 17))");
        yield return Case(Tuple.Create(1.5m, 0.0000000001m, "x"), "Tuple(Decimal(9, 8), Decimal(18, 18), String)");
        yield return Case((1.5m, 0.0000000001m), "Tuple(Decimal(9, 8), Decimal(18, 18))", Tuple.Create(1.5m, 0.0000000001m));
        yield return Case(new[] { Tuple.Create(1.5m, 1), Tuple.Create(0.0000000001m, 2) }, "Array(Tuple(Decimal(18, 17), Int32))");
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    [TestCaseSource(nameof(DynamicDecimalCollectionCases))]
    public async Task InsertBinaryAsync_DecimalCollectionToDynamic_StoresCommonTypeAndPreservesValues(object value, string expectedType, object expected)
    {
        var targetTable = CreateTableName($"dynamic_decimals_{expectedType}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        await client.InsertBinaryAsync(targetTable, ["id", "value"], [new object[] { 1u, value }]);

        using var reader = await client.ExecuteReaderAsync($"SELECT value, dynamicType(value) FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetString(1), Is.EqualTo(expectedType));
        Assert.That(reader.GetValue(0), Is.EqualTo(expected).Using<ClickHouseDecimal, decimal>((actual, written) => actual == written));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task InsertBinaryAsync_DecimalArraysOfOneClrTypeToDynamic_InfersTypeForEachCell()
    {
        var targetTable = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        var cells = new (decimal[] Value, string ExpectedType)[]
        {
            (new[] { 1.2m }, "Array(Decimal(9, 8))"),
            (new[] { 0.0000000001m }, "Array(Decimal(18, 18))"),
            (new[] { 123456789012345678m, 0.5m }, "Array(Decimal(38, 20))"),
            (new[] { -1.2345m }, "Array(Decimal(9, 8))"),
        };
        await client.InsertBinaryAsync(targetTable, ["id", "value"], cells.Select((c, i) => new object[] { (uint)i, c.Value }).ToList());

        using var reader = await client.ExecuteReaderAsync($"SELECT value, dynamicType(value) FROM {targetTable} ORDER BY id");
        foreach (var (value, expectedType) in cells)
        {
            ClassicAssert.IsTrue(reader.Read());
            Assert.That(reader.GetString(1), Is.EqualTo(expectedType));
            Assert.That(reader.GetValue(0), Is.EqualTo(value).Using<ClickHouseDecimal, decimal>((actual, written) => actual == written));
        }

        ClassicAssert.IsFalse(reader.Read());
    }

    // Collections of values that System.Decimal holds exactly, stored with a scale above 28 or a mantissa
    // above 96 bits. Args: (value written, expected dynamicType()).
    private static IEnumerable<TestCaseData> DynamicSystemDecimalCollectionCases()
    {
        yield return new TestCaseData(new[] { 0.1234567890123456789012345678m, -1.5m }, "Array(Decimal(38, 37))");
        yield return new TestCaseData(new List<decimal> { decimal.MaxValue, 0.0000000001m }, "Array(Decimal(76, 47))");
        yield return new TestCaseData(new decimal?[] { null, decimal.MinValue, 0.0000000001m }, "Array(Nullable(Decimal(76, 47)))");
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    [TestCaseSource(nameof(DynamicSystemDecimalCollectionCases))]
    public async Task Read_DecimalCollectionFromDynamicWithoutCustomDecimals_ReturnsSystemDecimals(object value, string expectedType)
    {
        var targetTable = CreateTableName($"dynamic_decimals_{expectedType}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        await client.InsertBinaryAsync(targetTable, ["id", "value"], [new object[] { 1u, value }]);

        using var systemDecimalClient = TestUtilities.GetTestClickHouseClient(customDecimals: false);
        using var reader = await systemDecimalClient.ExecuteReaderAsync($"SELECT value, dynamicType(value) FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetString(1), Is.EqualTo(expectedType));
        Assert.That(reader.GetValue(0), Is.EqualTo(value));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task InsertBinaryAsync_DecimalCollectionWithNoCommonDecimal256TypeToDynamic_Throws()
    {
        var targetTable = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        // Each value fits Decimal256 alone, but a common scale needs 29 integer and 48 fractional digits.
        var value = new[] { new ClickHouseDecimal(BigInteger.Pow(10, 28), 0), new ClickHouseDecimal(BigInteger.One, 48) };

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(
            () => client.InsertBinaryAsync(targetTable, ["id", "value"], [new object[] { 1u, value }]));
        Assert.That(ex.InnerException, Is.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(await client.ExecuteScalarAsync($"SELECT count() FROM {targetTable}"), Is.EqualTo(0UL));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task Write_IntArray_ShouldRoundTrip()
    {
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
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
        var targetTable = CreateTableName();
        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, value Dynamic) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = targetTable };
        await bulkCopy.WriteToServerAsync([new object[] { 1u, null }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT value FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(DBNull.Value));
    }
}
