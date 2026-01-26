
using System;
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class JsonTypeTests : AbstractConnectionTestFixture
{
    [SetUp]
    public void RegisterPocoTypes()
    {
        // Register all POCO types used in JSON serialization tests
        connection.RegisterJsonSerializationType<UInt64Data>();
        connection.RegisterJsonSerializationType<Int64Data>();
        connection.RegisterJsonSerializationType<UuidData>();
        connection.RegisterJsonSerializationType<DecimalData>();
        connection.RegisterJsonSerializationType<NestedOuter>();
        connection.RegisterJsonSerializationType<MixedData>();
        connection.RegisterJsonSerializationType<ArrayData>();
        connection.RegisterJsonSerializationType<ListData>();
        connection.RegisterJsonSerializationType<TestPocoClass>();
        connection.RegisterJsonSerializationType<NoHintData>();
        connection.RegisterJsonSerializationType<UnhintedDecimalData>();
        connection.RegisterJsonSerializationType<TestPocoWithPathAttribute>();
        connection.RegisterJsonSerializationType<TestPocoWithIgnoreAttribute>();
        connection.RegisterJsonSerializationType<TestPocoCaseSensitive>();
        connection.RegisterJsonSerializationType<NullableHintedData>();
        connection.RegisterJsonSerializationType<NullableUnhintedData>();
        connection.RegisterJsonSerializationType<NonNullableHintedData>();
        connection.RegisterJsonSerializationType<DictionaryData>();
        connection.RegisterJsonSerializationType<ComprehensiveTypesData>();
        connection.RegisterJsonSerializationType<TimeSpanData>();
        connection.RegisterJsonSerializationType<CircularRefA>();
        connection.RegisterJsonSerializationType<SelfReferencing>();
        connection.RegisterJsonSerializationType<ProductData>();
        connection.RegisterJsonSerializationType<WrongTypeData>();
        connection.RegisterJsonSerializationType<MissingPropertyData>();
        connection.RegisterJsonSerializationType<EmptyPocoData>();
        connection.RegisterJsonSerializationType<AllNullsData>();
    }

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
    [TestCase("max_dynamic_paths=0")]
    [TestCase("max_dynamic_paths=0, level1_int Int64, nested.level2_string String")]
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
        Assert.That((long)result["level1_int"], Is.EqualTo(789));
        Assert.That((string)result["nested"]["level2_string"], Is.EqualTo("nested_value"));

        // Assert non-hinted property
        ClassicAssert.IsInstanceOf<JsonValue>(result["unhinted_float"]);
        Assert.That((double)result["unhinted_float"], Is.EqualTo(99.9));
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

    private class UInt64Data { public ulong BigNumber { get; set; } }

    [Test]
    public async Task Write_WithUInt64Hint_ShouldPreservePrecision()
    {
        var targetTable = "test.json_write_uint64_hint";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(BigNumber UInt64)
            ) ENGINE = Memory");

        var data = new UInt64Data { BigNumber = 18446744073709551615UL };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That((ulong)result["BigNumber"], Is.EqualTo(18446744073709551615UL));
    }

    private class ProductData
    {
        public string Name { get; set; }
        public decimal Price { get; set; }
        public int Quantity { get; set; }
    }

    [Test]
    public async Task Write_WithMultipleRowsAndTrailingColumns_ShouldRoundTrip()
    {
        var targetTable = "test.json_write_multiple_rows_trailing";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Name String, Price Decimal64(2), Quantity Int32),
                category String,
                active UInt8
            ) ENGINE = Memory");

        var rows = new List<object[]>
        {
            new object[] { 1u, new ProductData { Name = "Widget", Price = 19.99m, Quantity = 100 }, "Electronics", (byte)1 },
            new object[] { 2u, new ProductData { Name = "Gadget", Price = 29.99m, Quantity = 50 }, "Electronics", (byte)1 },
            new object[] { 3u, new ProductData { Name = "Gizmo", Price = 9.99m, Quantity = 200 }, "Toys", (byte)0 },
            new object[] { 4u, new ProductData { Name = "Thingamajig", Price = 49.99m, Quantity = 25 }, "Hardware", (byte)1 },
        };

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(rows);

        using var reader = await connection.ExecuteReaderAsync($"SELECT id, data, category, active FROM {targetTable} ORDER BY id");

        for (int i = 0; i < rows.Count; i++)
        {
            ClassicAssert.IsTrue(reader.Read());

            var expectedRow = rows[i];
            var expectedProduct = (ProductData)expectedRow[1];

            Assert.That(reader.GetValue(0), Is.EqualTo(expectedRow[0]));
            var jsonResult = (JsonObject)reader.GetValue(1);
            Assert.That(jsonResult["Name"].GetValue<string>(), Is.EqualTo(expectedProduct.Name));
            Assert.That(reader.GetString(2), Is.EqualTo(expectedRow[2]));
            Assert.That(reader.GetByte(3), Is.EqualTo(expectedRow[3]));
        }

        ClassicAssert.IsFalse(reader.Read());
    }

    private class Int64Data { public long Value { get; set; } }

    [Test]
    public async Task Write_WithInt64Hint_ShouldWriteCorrectType()
    {
        var targetTable = "test.json_write_int64_hint";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Value Int64)
            ) ENGINE = Memory");

        var data = new Int64Data { Value = 9223372036854775807L };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That((long)result["Value"], Is.EqualTo(9223372036854775807L));
    }

    private class UuidData { public Guid Uuid { get; set; } }

    [Test]
    public async Task Write_WithUuidHint_ShouldPreserveGuid()
    {
        var targetTable = "test.json_write_uuid_hint";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Uuid UUID)
            ) ENGINE = Memory");

        var guid = Guid.Parse("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        var data = new UuidData { Uuid = guid };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        var actualGuid = Guid.Parse(result["Uuid"].GetValue<string>());
        Assert.That(actualGuid, Is.EqualTo(guid));
    }

    private class DecimalData { public decimal Price { get; set; } }

    [Test]
    public async Task Write_WithDecimalHint_ShouldPreservePrecision()
    {
        var targetTable = "test.json_write_decimal_hint";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Price Decimal64(4))
            ) ENGINE = Memory");

        var data = new DecimalData { Price = 123.4567m };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        var actualDecimal = ClickHouseDecimal.Parse(result["Price"].GetValue<string>());
        Assert.That(actualDecimal, Is.EqualTo(new ClickHouseDecimal(123.4567m)));
    }

    private class NestedOuter { public NestedInner Outer { get; set; } }
    private class NestedInner { public ulong Inner { get; set; } }

    [Test]
    public async Task Write_WithNestedHints_ShouldWriteNestedFields()
    {
        var targetTable = "test.json_write_nested_hint";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(`Outer.Inner` UInt64)
            ) ENGINE = Memory");

        var data = new NestedOuter { Outer = new NestedInner { Inner = 9999999999999UL } };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That((ulong)result["Outer"]["Inner"], Is.EqualTo(9999999999999UL));
    }

    private class MixedData { public long Id { get; set; } public string Name { get; set; } public double Score { get; set; } }

    [Test]
    public async Task Write_WithMixedHintedAndUnhinted_ShouldHandleBoth()
    {
        var targetTable = "test.json_write_mixed_hint";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Id Int64)
            ) ENGINE = Memory");

        var data = new MixedData { Id = 123L, Name = "test", Score = 99.5 };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That((long)result["Id"], Is.EqualTo(123L));
        Assert.That(result["Name"].ToString(), Is.EqualTo("test"));
        Assert.That((double)result["Score"], Is.EqualTo(99.5));
    }

    private class ArrayData { public ulong[] Ids { get; set; } }
    private class ListData { public List<ulong> Ids { get; set; } }

    [Test]
    public async Task Write_WithArrayHint_ShouldWriteTypedArray()
    {
        var targetTable = "test.json_write_array_hint";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Ids Array(UInt64))
            ) ENGINE = Memory");

        var data = new ArrayData { Ids = [1UL, 2UL, 3UL] };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        var arr = (JsonArray)result["Ids"];
        Assert.That(arr.Count, Is.EqualTo(3));
        Assert.That((ulong)arr[0], Is.EqualTo(1UL));
        Assert.That((ulong)arr[1], Is.EqualTo(2UL));
        Assert.That((ulong)arr[2], Is.EqualTo(3UL));
    }

    [Test]
    public async Task Write_WithArrayHint_ShouldWriteTypedList()
    {
        var targetTable = "test.json_write_list_hint";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Ids Array(UInt64))
            ) ENGINE = Memory");

        var data = new ListData { Ids = [1UL, 2UL, 3UL, 4UL] };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        var arr = (JsonArray)result["Ids"];
        Assert.That(arr.Count, Is.EqualTo(4));
        Assert.That((ulong)arr[0], Is.EqualTo(1UL));
        Assert.That((ulong)arr[1], Is.EqualTo(2UL));
        Assert.That((ulong)arr[2], Is.EqualTo(3UL));
        Assert.That((ulong)arr[3], Is.EqualTo(4UL));
    }

    [Test]
    public async Task Write_WithPocoObject_ShouldSerializeCorrectly()
    {
        var targetTable = "test.json_write_poco";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Id Int64, Name String)
            ) ENGINE = Memory");

        var poco = new TestPocoClass { Id = 123L, Name = "Test" };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, poco }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That((long)result["Id"], Is.EqualTo(123L));
        Assert.That(result["Name"].ToString(), Is.EqualTo("Test"));
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithDateTimeHint_BinaryMode_ShouldPreserveDateTime()
    {
        // DateTime hints require Binary mode for proper type conversion
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);

        var targetTable = "test.json_write_datetime_hint";
        await binaryConnection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Timestamp DateTime)
            ) ENGINE = Memory");

        var dt = new DateTime(2024, 6, 15, 10, 30, 45, DateTimeKind.Utc);
        var data = new { Timestamp = dt };
        binaryConnection.RegisterJsonSerializationType(data.GetType());
        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await binaryConnection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        var actualDateTime = result["Timestamp"].GetValue<DateTime>();
        Assert.That(actualDateTime, Is.EqualTo(dt));
    }

    private class NoHintData { public int Number { get; set; } public string Text { get; set; } public bool Flag { get; set; } }

    private class UnhintedDecimalData { public decimal Price { get; set; } }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithUnhintedDecimal_ShouldPreserveFractionalPart()
    {
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);
        binaryConnection.RegisterJsonSerializationType<UnhintedDecimalData>();
        var targetTable = "test.json_write_unhinted_decimal";
        await binaryConnection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        var data = new UnhintedDecimalData { Price = 123.4567m };

        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await binaryConnection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);

        // Decimal should preserve fractional part even without a type hint
        var actualDecimal = ClickHouseDecimal.Parse(result["Price"].GetValue<string>());
        Assert.That(actualDecimal, Is.EqualTo(new ClickHouseDecimal(123.4567m)));
    }

    [Test]
    public async Task Write_WithNoHints_ShouldUseExistingBehavior()
    {
        var targetTable = "test.json_write_no_hints";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        var data = new NoHintData { Number = 42, Text = "hello", Flag = true };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That(result["Number"].GetValue<long>(), Is.EqualTo(42));
        Assert.That(result["Text"].GetValue<string>(), Is.EqualTo("hello"));
        Assert.That(result["Flag"].GetValue<bool>(), Is.True);
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithPocoAndJsonPathAttribute_BinaryMode_ShouldUseCustomPath()
    {
        // ClickHouseJsonPath attribute only works in Binary mode
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);
        binaryConnection.RegisterJsonSerializationType<TestPocoWithPathAttribute>();

        var targetTable = "test.json_write_poco_path_attr";
        await binaryConnection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(user.id Int64, user.name String)
            ) ENGINE = Memory");

        var poco = new TestPocoWithPathAttribute { UserId = 456L, UserName = "CustomPath" };
        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, poco }]);

        using var reader = await binaryConnection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That((long)result["user"]["id"], Is.EqualTo(456L));
        Assert.That(result["user"]["name"].ToString(), Is.EqualTo("CustomPath"));
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithPocoAndJsonIgnoreAttribute_BinaryMode_ShouldSkipIgnoredProperties()
    {
        // ClickHouseJsonIgnore attribute only works in Binary mode
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);
        binaryConnection.RegisterJsonSerializationType<TestPocoWithIgnoreAttribute>();

        var targetTable = "test.json_write_poco_ignore_attr";
        await binaryConnection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Name String)
            ) ENGINE = Memory");

        var poco = new TestPocoWithIgnoreAttribute { Name = "Visible", Secret = "Hidden" };
        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, poco }]);

        using var reader = await binaryConnection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That(result["Name"].ToString(), Is.EqualTo("Visible"));
        Assert.That(result.ContainsKey("Secret"), Is.False);
    }

    [Test]
    public async Task Write_WithCaseSensitiveHint_ShouldMatchExactCase()
    {
        var targetTable = "test.json_write_case_sensitive";
        // Column has exact case hint "UserId" matching the POCO property
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(UserId Int64)
            ) ENGINE = Memory");

        var poco = new TestPocoCaseSensitive { UserId = 789L };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, poco }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        // Property matches exact case
        Assert.That((long)result["UserId"], Is.EqualTo(789L));
    }

    [Test]
    public async Task Write_WithCaseMismatchedHint_ShouldNotMatch()
    {
        var targetTable = "test.json_write_case_mismatch";
        // Column has lowercase hint "userid", but POCO property is "UserId" - should NOT match
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(userid Int64)
            ) ENGINE = Memory");

        var poco = new TestPocoCaseSensitive { UserId = 789L };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, poco }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        // "userid" hint doesn't match "UserId" property (case-sensitive), so:
        // - "userid" exists in schema with default value 0 (hinted path with no data written to it)
        // - "UserId" contains the actual value (written as unhinted dynamic path)
        Assert.That((long)result["userid"], Is.EqualTo(0L));
        Assert.That((long)result["UserId"], Is.EqualTo(789L));
    }

    private class TestPocoClass
    {
        public long Id { get; set; }
        public string Name { get; set; }
    }

    private class TestPocoWithPathAttribute
    {
        [ClickHouseJsonPath("user.id")]
        public long UserId { get; set; }

        [ClickHouseJsonPath("user.name")]
        public string UserName { get; set; }
    }

    private class TestPocoWithIgnoreAttribute
    {
        public string Name { get; set; }

        [ClickHouseJsonIgnore]
        public string Secret { get; set; }
    }

    private class TestPocoCaseSensitive
    {
        public long UserId { get; set; }
    }

    private class NullableHintedData
    {
        public int? Value { get; set; }
        public string Name { get; set; }
    }

    private class NullableUnhintedData
    {
        public int? Value { get; set; }
        public string Name { get; set; }
    }

    [Test]
    public async Task Write_WithNullableHintedProperty_ShouldWriteNull()
    {
        var targetTable = "test.json_write_nullable_hinted";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Value Nullable(Int32), Name String)
            ) ENGINE = Memory");

        var data = new NullableHintedData { Value = null, Name = "test" };
        connection.RegisterJsonSerializationType(data.GetType());
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That(result["Value"], Is.Null);
        Assert.That(result["Name"].GetValue<string>(), Is.EqualTo("test"));
    }

    [Test]
    public async Task Write_WithNullableUnhintedProperty_ShouldSkipField()
    {
        // Nullable/LowCardinality(Nullable) types are not allowed inside Variant type
        var targetTable = "test.json_write_nullable_unhinted";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        var data = new NullableUnhintedData { Value = null, Name = "test" };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That(result["Name"].GetValue<string>(), Is.EqualTo("test"));
        // Value should be skipped since string is not Nullable<T>
        Assert.That(result.ContainsKey("Value"), Is.False);
    }

    [Test]
    public async Task Write_WithNonNullableNullProperty_ShouldSkipField()
    {
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);
        var targetTable = "test.json_write_nonnullable_null";
        await binaryConnection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        // string is a reference type that can be null, but it's not Nullable<T>
        var data = new NoHintData { Number = 42, Text = null, Flag = true };
        binaryConnection.RegisterJsonSerializationType<NoHintData>();
        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await binaryConnection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That(result["Number"].GetValue<int>(), Is.EqualTo(42));
        Assert.That(result["Flag"].GetValue<bool>(), Is.True);
        // Text should be skipped since string is not Nullable<T>
        Assert.That(result.ContainsKey("Text"), Is.False);
    }

    private class NonNullableHintedData
    {
        public int? Value { get; set; }
        public string Name { get; set; }
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithNonNullableHintAndNullValue_ShouldWriteNothing()
    {
        // Tests the BinaryTypeIndex.Nothing path in WriteHintedValue
        // When a non-nullable hint (Int64) receives a null value, we write Nothing type
        // ClickHouse converts Nothing to the default value for the hinted type (0 for Int64)
        var targetTable = "test.json_write_nonnullable_hint_null";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Value Int64, Name String)
            ) ENGINE = Memory");

        // Value is null but hint is non-nullable Int64
        var data = new NonNullableHintedData { Value = null, Name = "test" };

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        // ClickHouse handles null json values on non-nullable columns by setting to the default value, 0 in this case
        Assert.That(result["Value"].GetValue<long>(), Is.EqualTo(0L));
        Assert.That(result["Name"].GetValue<string>(), Is.EqualTo("test"));
    }

    private class DictionaryData
    {
        public string Name { get; set; }
        public Dictionary<string, int> Scores { get; set; }
    }

    [Test]
    public async Task Write_WithDictionaryHint_ShouldWriteMap()
    {
        var targetTable = "test.json_write_dictionary_hint";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Name String, Scores Map(String, Int32))
            ) ENGINE = Memory");

        var data = new DictionaryData
        {
            Name = "test",
            Scores = new Dictionary<string, int>
            {
                ["math"] = 95,
                ["science"] = 88,
                ["english"] = 92
            }
        };
        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);
        Assert.That(result["Name"].GetValue<string>(), Is.EqualTo("test"));
        var scores = (JsonObject)result["Scores"];
        Assert.That(scores["math"].GetValue<int>(), Is.EqualTo(95));
        Assert.That(scores["science"].GetValue<int>(), Is.EqualTo(88));
        Assert.That(scores["english"].GetValue<int>(), Is.EqualTo(92));
    }

    /// <summary>
    /// POCO with many types to exercise BinaryTypeEncoder coverage.
    /// </summary>
    private class ComprehensiveTypesData
    {
        // Unsigned integers
        public byte ByteVal { get; set; }
        public ushort UShortVal { get; set; }
        public uint UIntVal { get; set; }
        public ulong ULongVal { get; set; }

        // Signed integers
        public sbyte SByteVal { get; set; }
        public short ShortVal { get; set; }
        public int IntVal { get; set; }
        public long LongVal { get; set; }

        // Floating point
        public float FloatVal { get; set; }
        public double DoubleVal { get; set; }

        // Boolean
        public bool BoolVal { get; set; }

        // String
        public string StringVal { get; set; }

        // UUID
        public Guid GuidVal { get; set; }

        // Date/Time
        public DateTime DateTimeVal { get; set; }

        // IP address (IPv4 only - type inference maps IPAddress to IPv4)
        public IPAddress IPv4Val { get; set; }

        // Collections
        public int[] IntArray { get; set; }
        public List<string> StringList { get; set; }
        public Dictionary<string, double> StringDoubleMap { get; set; }

        // Nested array
        public int[][] NestedIntArray { get; set; }
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithManyUnhintedTypes_BinaryMode_ShouldInferAndWriteAllTypes()
    {
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);
        binaryConnection.RegisterJsonSerializationType<ComprehensiveTypesData>();

        var targetTable = "test.json_write_comprehensive_types";
        await binaryConnection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        var testGuid = Guid.Parse("550e8400-e29b-41d4-a716-446655440000");
        var testDateTime = new DateTime(2024, 6, 15, 12, 30, 45, DateTimeKind.Utc);

        var data = new ComprehensiveTypesData
        {
            // Unsigned integers
            ByteVal = 255,
            UShortVal = 65535,
            UIntVal = 4294967295,
            ULongVal = 18446744073709551615,

            // Signed integers
            SByteVal = -128,
            ShortVal = -32768,
            IntVal = -2147483648,
            LongVal = -9223372036854775808,

            // Floating point
            FloatVal = 3.14159f,
            DoubleVal = 2.718281828459045,

            // Boolean
            BoolVal = true,

            // String
            StringVal = "Hello, JSON!",

            // UUID
            GuidVal = testGuid,

            // Date/Time
            DateTimeVal = testDateTime,

            // IP address
            IPv4Val = IPAddress.Parse("192.168.1.1"),

            // Collections
            IntArray = [1, 2, 3, 4, 5],
            StringList = ["apple", "banana", "cherry"],
            StringDoubleMap = new Dictionary<string, double>
            {
                ["pi"] = 3.14159,
                ["e"] = 2.71828
            },

            // Nested array
            NestedIntArray = [[1, 2], [3, 4, 5], [6]]
        };

        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await binaryConnection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);

        // Verify unsigned integers
        Assert.That((byte)result["ByteVal"], Is.EqualTo(255));
        Assert.That((ushort)result["UShortVal"], Is.EqualTo(65535));
        Assert.That((uint)result["UIntVal"], Is.EqualTo(4294967295));
        Assert.That((ulong)result["ULongVal"], Is.EqualTo(18446744073709551615));

        // Verify signed integers
        Assert.That((sbyte)result["SByteVal"], Is.EqualTo(-128));
        Assert.That((short)result["ShortVal"], Is.EqualTo(-32768));
        Assert.That((int)result["IntVal"], Is.EqualTo(-2147483648));
        Assert.That((long)result["LongVal"], Is.EqualTo(-9223372036854775808));

        // Verify floating point
        Assert.That((float)result["FloatVal"], Is.EqualTo(3.14159f).Within(0.0001f));
        Assert.That((double)result["DoubleVal"], Is.EqualTo(2.718281828459045).Within(0.0000001));

        // Verify boolean
        Assert.That((bool)result["BoolVal"], Is.True);

        // Verify string
        Assert.That(result["StringVal"].GetValue<string>(), Is.EqualTo("Hello, JSON!"));

        // Verify UUID
        Assert.That(Guid.Parse(result["GuidVal"].GetValue<string>()), Is.EqualTo(testGuid));

        // Verify DateTime
        var resultDateTime = result["DateTimeVal"].GetValue<DateTime>();
        Assert.That(resultDateTime.Year, Is.EqualTo(2024));
        Assert.That(resultDateTime.Month, Is.EqualTo(6));
        Assert.That(resultDateTime.Day, Is.EqualTo(15));

        // Verify IP address
        Assert.That(result["IPv4Val"].GetValue<string>(), Is.EqualTo("192.168.1.1"));

        // Verify int array
        var intArray = (JsonArray)result["IntArray"];
        Assert.That(intArray.Count, Is.EqualTo(5));
        Assert.That((int)intArray[0], Is.EqualTo(1));
        Assert.That((int)intArray[4], Is.EqualTo(5));

        // Verify string list
        var stringList = (JsonArray)result["StringList"];
        Assert.That(stringList.Count, Is.EqualTo(3));
        Assert.That(stringList[0].GetValue<string>(), Is.EqualTo("apple"));
        Assert.That(stringList[2].GetValue<string>(), Is.EqualTo("cherry"));

        // Verify map
        var mapResult = (JsonObject)result["StringDoubleMap"];
        Assert.That((double)mapResult["pi"], Is.EqualTo(3.14159).Within(0.0001));
        Assert.That((double)mapResult["e"], Is.EqualTo(2.71828).Within(0.0001));

        // Verify nested array
        var nestedArray = (JsonArray)result["NestedIntArray"];
        Assert.That(nestedArray.Count, Is.EqualTo(3));
        Assert.That(((JsonArray)nestedArray[0]).Count, Is.EqualTo(2));
        Assert.That((int)((JsonArray)nestedArray[1])[2], Is.EqualTo(5));
    }

    private class TimeSpanData { public TimeSpan Duration { get; set; } }

    private class CircularRefA
    {
        public int Id { get; set; }
        public CircularRefB RefB { get; set; }
    }

    private class CircularRefB
    {
        public int Id { get; set; }
        public CircularRefA RefA { get; set; }
    }

    private class SelfReferencing
    {
        public int Id { get; set; }
        public SelfReferencing Self { get; set; }
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithCircularReference_ShouldThrowInvalidOperationException()
    {
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);
        binaryConnection.RegisterJsonSerializationType<CircularRefA>();
        binaryConnection.RegisterJsonSerializationType<CircularRefB>();
        var targetTable = "test.json_write_circular_ref";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        // Create circular reference: A -> B -> A
        var a = new CircularRefA { Id = 1 };
        var b = new CircularRefB { Id = 2 };
        a.RefB = b;
        b.RefA = a;

        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await bulkCopy.WriteToServerAsync([new object[] { 1u, a }]));

        Assert.That(ex.InnerException, Is.TypeOf<InvalidOperationException>());
        Assert.That(ex.InnerException.Message, Does.Contain("Circular reference detected"));
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithSelfReference_ShouldThrowInvalidOperationException()
    {
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);
        binaryConnection.RegisterJsonSerializationType<SelfReferencing>();
        var targetTable = "test.json_write_self_ref";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        // Create self-reference
        var obj = new SelfReferencing { Id = 1 };
        obj.Self = obj;

        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await bulkCopy.WriteToServerAsync([new object[] { 1u, obj }]));

        Assert.That(ex.InnerException, Is.TypeOf<InvalidOperationException>());
        Assert.That(ex.InnerException.Message, Does.Contain("Circular reference detected"));
    }

    [Test]
    [RequiredFeature(Feature.Json | Feature.Time)]
    public async Task Write_WithTimeSpan_ShouldWriteAsTime64()
    {
        var targetTable = "test.json_write_timespan";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        var data = new TimeSpanData { Duration = new TimeSpan(1, 2, 3, 4, 567) };

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);

        // TimeSpan stored as Time64, returned as string
        var resultTimeSpan = result["Duration"].GetValue<string>();
        Assert.That(TimeSpan.Parse(resultTimeSpan), Is.EqualTo(data.Duration));
    }

    private class UnregisteredPocoData
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithUnregisteredType_BinaryMode_ShouldThrowClickHouseJsonSerializationException()
    {
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);

        var targetTable = "test.json_write_unregistered_type";
        await binaryConnection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        // UnregisteredPocoData is intentionally NOT registered
        var data = new UnregisteredPocoData { Id = 1, Name = "test" };

        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]));

        Assert.That(ex.InnerException, Is.TypeOf<ClickHouseJsonSerializationException>());
        var jsonEx = (ClickHouseJsonSerializationException)ex.InnerException;
        Assert.That(jsonEx.TargetType, Is.EqualTo(typeof(UnregisteredPocoData)));
        Assert.That(jsonEx.Message, Does.Contain("UnregisteredPocoData"));
        Assert.That(jsonEx.Message, Does.Contain("RegisterJsonSerializationType"));
    }

    private class WrongTypeData
    {
        public string Value { get; set; }
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithWrongPropertyType_BinaryMode_ShouldThrowFormatException()
    {
        using var binaryConnection = TestUtilities.GetTestClickHouseConnection(jsonWriteMode: JsonWriteMode.Binary, jsonReadMode: JsonReadMode.Binary);
        binaryConnection.RegisterJsonSerializationType<WrongTypeData>();

        // POCO has string property, but schema expects Int64
        var targetTable = "test.json_write_wrong_type";
        await binaryConnection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Value Int64)
            ) ENGINE = Memory");

        var data = new WrongTypeData { Value = "not a number" };

        using var bulkCopy = new ClickHouseBulkCopy(binaryConnection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();

        // Server should reject this - we're sending a string where Int64 is expected
        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]));

        Assert.That(ex.InnerException, Is.TypeOf<FormatException>());
    }

    private class MissingPropertyData
    {
        public int Id { get; set; }
        // Missing "Name" property that schema expects
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithMissingHintedProperty_ShouldSucceedWithNull()
    {
        // POCO is missing a property that the schema hints for - should be default
        var targetTable = "test.json_write_missing_property";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Id Int32, Name String)
            ) ENGINE = Memory");

        var data = new MissingPropertyData { Id = 42 };

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);

        // Id should be present, Name should be default
        Assert.That(result["Id"].GetValue<int>(), Is.EqualTo(42));
        Assert.That(result["Name"].GetValue<string>(), Is.EqualTo(string.Empty));
    }

    private class EmptyPocoData
    {
        // No properties
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithEmptyPoco_ShouldWriteEmptyObject()
    {
        var targetTable = "test.json_write_empty_poco";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        var data = new EmptyPocoData();

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);

        Assert.That(result.Count, Is.EqualTo(0));
    }

    private class AllNullsData
    {
        public string Name { get; set; }
        public int? Count { get; set; }
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_WithAllNullProperties_ShouldWriteEmptyObject()
    {
        var targetTable = "test.json_write_all_nulls";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON
            ) ENGINE = Memory");

        var data = new AllNullsData { Name = null, Count = null };

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);

        // Null properties should not be written to JSON
        Assert.That(result.Count, Is.EqualTo(0));
    }

    private class PocoWithIndexer
    {
        public int Id { get; set; }
        public string Name { get; set; }

        // Indexer - should be ignored during serialization
        public string this[int index] => $"Item{index}";
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Write_PocoWithIndexer_ShouldIgnoreIndexer()
    {
        var targetTable = "test.json_write_indexer";
        await connection.ExecuteStatementAsync(
            $@"CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(Id Int32, Name String)
            ) ENGINE = Memory");

        connection.RegisterJsonSerializationType<PocoWithIndexer>();
        var data = new PocoWithIndexer { Id = 42, Name = "test" };

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = targetTable,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync([new object[] { 1u, data }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        var result = (JsonObject)reader.GetValue(0);

        // Should have exactly 2 properties (Id and Name), indexer should be ignored
        Assert.That(result.Count, Is.EqualTo(2), "Should only have Id and Name, indexer should be ignored");
        Assert.That(result["Id"].GetValue<int>(), Is.EqualTo(42));
        Assert.That(result["Name"].GetValue<string>(), Is.EqualTo("test"));
    }
}
