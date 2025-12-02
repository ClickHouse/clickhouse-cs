
using System;
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class JsonTypeTests : AbstractConnectionTestFixture
{
    public static IEnumerable<TestCaseData> JsonTypeTestCases()
    {
        // Int256 - BigInteger must be a string in JSON for ClickHouse to parse it
        var bigIntValue = BigInteger.Parse("100000000000000000000000000000000000000000000000000");
        yield return new TestCaseData(
            "bigNumber Int256",
            "{\"bigNumber\": \"100000000000000000000000000000000000000000000000000\"}",
            "bigNumber",
            bigIntValue
        ).SetName("Int256");

        // IPv4 - IPAddress should be serialized as string in JSON
        var ipAddress = IPAddress.Parse("192.168.1.100");
        yield return new TestCaseData(
            "ipAddress IPv4",
            "{\"ipAddress\": \"192.168.1.100\"}",
            "ipAddress",
            ipAddress
        ).SetName("IPv4");

        // LowCardinality(String) - should work like regular string
        yield return new TestCaseData(
            "category LowCardinality(String)",
            "{\"category\": \"electronics\"}",
            "category",
            "electronics"
        ).SetName("LowCardinality(String)");

        // Map(String, Int32) - Returns JsonObject since JSON objects are key-value maps
        yield return new TestCaseData(
            "tags Map(String, Int32)",
            "{\"tags\": {\"priority\": 1, \"status\": 2}}",
            "tags",
            new JsonObject { ["priority"] = 1, ["status"] = 2 }
        ).SetName("Map(String, Int32)");

        // IPv6 - IPAddress for IPv6 addresses
        var ipv6Address = IPAddress.Parse("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        yield return new TestCaseData(
            "ipv6Address IPv6",
            "{\"ipv6Address\": \"2001:0db8:85a3:0000:0000:8a2e:0370:7334\"}",
            "ipv6Address",
            ipv6Address
        ).SetName("IPv6");

        // UUID - Guid type
        var guidValue = Guid.Parse("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        yield return new TestCaseData(
            "uuid UUID",
            "{\"uuid\": \"61f0c404-5cb3-11e7-907b-a6006ad3dba0\"}",
            "uuid",
            guidValue
        ).SetName("UUID");

        // Array(Int32) - Simple array of integers
        yield return new TestCaseData(
            "numbers Array(Int32)",
            "{\"numbers\": [1, 2, 3, 4, 5]}",
            "numbers",
            new JsonArray { 1, 2, 3, 4, 5 }
        ).SetName("Array(Int32)");

        // Array(String) - Array of strings
        yield return new TestCaseData(
            "names Array(String)",
            "{\"names\": [\"Alice\", \"Bob\", \"Charlie\"]}",
            "names",
            new JsonArray { "Alice", "Bob", "Charlie" }
        ).SetName("Array(String)");

        // Nullable(Int64) - Nullable with non-null value
        yield return new TestCaseData(
            "nullableInt Nullable(Int64)",
            "{\"nullableInt\": 42}",
            "nullableInt",
            (long?)42
        ).SetName("Nullable(Int64)");

        // Decimal64(4) - Decimal type
        yield return new TestCaseData(
            "price Decimal64(4)",
            "{\"price\": 123.4567}",
            "price",
            new ClickHouseDecimal(123.4567m)
        ).SetName("Decimal64(4)");

        // Decimal128(8) - Larger precision decimal
        // There are limits to parsing large decimals from json fields unless enclosed in quotes
        yield return new TestCaseData(
            "bigDecimal Decimal128(7)",
            "{\"bigDecimal\": \"11212212312368.1234567\"}",
            "bigDecimal",
            new ClickHouseDecimal(11212212312368.1234567m)
        ).SetName("Decimal128(7)");

        // Decimal256(8) - Larger precision decimal
        yield return new TestCaseData(
            "bigDecimal Decimal256(8)",
            "{\"bigDecimal\": \"11221233412168.12345678\"}",
            "bigDecimal",
            new ClickHouseDecimal(11221233412168.12345678m)
        ).SetName("Decimal256(8)");

        // Date - Date type (as string in JSON)
        yield return new TestCaseData(
            "eventDate Date",
            "{\"eventDate\": \"2024-06-15\"}",
            "eventDate",
            new DateTime(2024, 6, 15, 0, 0, 0, DateTimeKind.Unspecified)
        ).SetName("Date");

        // DateTime - DateTime type (as string in JSON)
        yield return new TestCaseData(
            "eventTime DateTime",
            "{\"eventTime\": \"2024-06-15 10:30:45\"}",
            "eventTime",
            new DateTime(2024, 6, 15, 10, 30, 45, DateTimeKind.Unspecified)
        ).SetName("DateTime");

        // Float32 - Single precision float
        yield return new TestCaseData(
            "temperature Float32",
            "{\"temperature\": 98.6}",
            "temperature",
            98.6f
        ).SetName("Float32");

        // Float64 - Double precision float
        yield return new TestCaseData(
            "pi Float64",
            "{\"pi\": 3.14159265358979}",
            "pi",
            3.14159265358979
        ).SetName("Float64");

        // Map(String, Array(Int32)) - Map with array values, returns JsonObject
        yield return new TestCaseData(
            "arrayMap Map(String, Array(Int32))",
            "{\"arrayMap\": {\"evens\": [2, 4, 6], \"odds\": [1, 3, 5]}}",
            "arrayMap",
            new JsonObject
            {
                ["evens"] = new JsonArray(2, 4, 6),
                ["odds"] = new JsonArray(1, 3, 5)
            }
        ).SetName("Map(String, Array(Int32))");

        // LowCardinality(Nullable(String)) - LowCardinality nullable string
        yield return new TestCaseData(
            "lcNullable LowCardinality(Nullable(String))",
            "{\"lcNullable\": \"lowcard_value\"}",
            "lcNullable",
            "lowcard_value"
        ).SetName("LowCardinality(Nullable(String))");

        // Array(Nullable(Int32)) - Array with nullable elements
        yield return new TestCaseData(
            "nullableArray Array(Nullable(Int32))",
            "{\"nullableArray\": [1, null, 3]}",
            "nullableArray",
            new JsonArray { 1, null, 3}
        ).SetName("Array(Nullable(Int32))");

        // Int128 - 128-bit integer
        var int128Value = BigInteger.Parse("170141183460469231731687303715884105727");
        yield return new TestCaseData(
            "bigInt128 Int128",
            "{\"bigInt128\": \"170141183460469231731687303715884105727\"}",
            "bigInt128",
            int128Value
        ).SetName("Int128");

        // UInt128 - Unsigned 128-bit integer
        var uint128Value = BigInteger.Parse("340282366920938463463374607431768211455");
        yield return new TestCaseData(
            "bigUInt128 UInt128",
            "{\"bigUInt128\": \"340282366920938463463374607431768211455\"}",
            "bigUInt128",
            uint128Value
        ).SetName("UInt128");

        // FixedString(10) - Fixed-length string
        yield return new TestCaseData(
            "code FixedString(10)",
            "{\"code\": \"ABC1234567\"}",
            "code",
            Encoding.UTF8.GetBytes("ABC1234567")
        ).SetName("FixedString(10)");
    }
    
    [Test]
    [TestCase("")]
    [TestCase("level1_int Int64, nested.level2_string String")]
    [TestCase("level1_int Int64, nested.level2_string String, skip path.to.ignore")]
    [TestCase("level1_int Int64, nested.level2_string String, SKIP path.to.skip, SKIP REGEXP 'regex.path.*'")]
    [TestCase("max_dynamic_paths=10, level1_int Int64, nested.level2_string String")]
    [TestCase("max_dynamic_paths=10, level1_int Int64, nested.level2_string String, SKIP path.to.skip")]
    [TestCase("max_dynamic_types=3, level1_int Int64, nested.level2_string String")]
    [TestCase("level1_int Int64, skip_items Int32, nested.level2_string String, SKIP path.to.skip, skip path.to.ignore")]
    public async Task ShouldSelectDataWithComplexHintedJsonType(string jsonDefinition)
    {
        var targetTable = "test.select_data_complex_hinted_json";

        await connection.ExecuteStatementAsync(
            $@"
            CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON({jsonDefinition})
            ) ENGINE = Memory;");

        var json = "{\"level1_int\": 789, \"skip_items\": 30, \"nested\": {\"level2_string\": \"nested_value\"}, \"unhinted_float\": 99.9}";
        await connection.ExecuteStatementAsync($"INSERT INTO {targetTable} VALUES (1, '{json}')");

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());

        var result = (JsonObject)reader.GetValue(0);

        // Assert hinted properties
        ClassicAssert.AreEqual(789, (long)result["level1_int"]);
        ClassicAssert.AreEqual("nested_value", (string)result["nested"]["level2_string"]);

        // Assert non-hinted property
        ClassicAssert.IsInstanceOf<JsonValue>(result["unhinted_float"]);
        ClassicAssert.AreEqual(99.9, (double)result["unhinted_float"]);
    }

    [Test]
    [TestCaseSource(nameof(JsonTypeTestCases))]
    public async Task ShouldParseJsonWithTypedPath(string typeDefinition, string jsonData, string pathName, object expectedValue)
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT '{jsonData}'::Json({typeDefinition})");
        ClassicAssert.IsTrue(reader.Read());

        var result = (JsonObject)reader.GetValue(0);
        var actualNode = result[pathName];

        // BigInteger and other complex types are serialized as strings in JsonValue
        if (expectedValue is BigInteger expectedBigInt && actualNode is JsonValue jv)
        {
            var actualBigInt = BigInteger.Parse(jv.GetValue<string>());
            Assert.That(actualBigInt, Is.EqualTo(expectedBigInt));
        }
        else if (expectedValue is IPAddress expectedIp && actualNode is JsonValue jv2)
        {
            var actualIp = IPAddress.Parse(jv2.GetValue<string>());
            Assert.That(actualIp, Is.EqualTo(expectedIp));
        }
        else if (expectedValue is ClickHouseDecimal expectedDec && actualNode is JsonValue jv3)
        {
            var actualDecimal = ClickHouseDecimal.Parse(jv3.GetValue<string>());
            Assert.That(actualDecimal, Is.EqualTo(expectedDec));
        }
        else if (expectedValue is Guid expectedGuid && actualNode is JsonValue jv4)
        {
            var actualGuid = Guid.Parse(jv4.GetValue<string>());
            Assert.That(actualGuid, Is.EqualTo(expectedGuid));
        }
        else if (expectedValue is JsonObject expectedObj && actualNode is JsonObject actualObj)
        {
            Assert.That(JsonNode.DeepEquals(expectedObj, actualObj), Is.True,
                $"Expected: {expectedObj.ToJsonString()}, Actual: {actualObj.ToJsonString()}");
        }
        else if (expectedValue is JsonArray expectedArray && actualNode is JsonArray actualArray)
        {
            Assert.That(actualArray.Count, Is.EqualTo(expectedArray.Count), "Array length mismatch");
            Assert.That(JsonNode.DeepEquals(expectedArray, actualNode), Is.True,
                $"Expected: {expectedArray.ToJsonString()}, Actual: {actualNode.ToJsonString()}");
        }
        else
        {
            TestUtilities.AssertEqual(expectedValue, actualNode?.GetValue<object>());
        }
    }
}
