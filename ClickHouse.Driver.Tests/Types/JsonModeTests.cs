using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class JsonModeTests
{
    private ClickHouseConnection GetConnectionWithJsonMode(JsonReadMode readMode = JsonReadMode.Binary, JsonWriteMode writeMode = JsonWriteMode.Binary)
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.JsonReadMode = readMode;
        builder.JsonWriteMode = writeMode;

        var settings = new ClickHouseClientSettings(builder);

        // Add experimental JSON type support
        settings.CustomSettings["allow_experimental_json_type"] = 1;

        var connection = new ClickHouseConnection(settings);
        connection.Open();
        return connection;
    }

    [Test]
    public async Task ShouldReadJsonAsString()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.String);

        using var reader = await connection.ExecuteReaderAsync("SELECT '{\"name\": \"John\", \"age\": 30}'::Json");
        ClassicAssert.IsTrue(reader.Read());

        var result = reader.GetValue(0);

        // With JsonReadMode.String, the result should be a string
        ClassicAssert.IsInstanceOf<string>(result);
        var jsonString = (string)result;

        // Verify the JSON string can be parsed
        var parsed = JsonNode.Parse(jsonString);
        Assert.That((string)parsed["name"], Is.EqualTo("John"));
        Assert.That((int)parsed["age"], Is.EqualTo(30));
    }

    [Test]
    public async Task ShouldReadJsonAsJsonNode_Default()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.Binary);

        using var reader = await connection.ExecuteReaderAsync("SELECT '{\"name\": \"Jane\", \"age\": 25}'::Json");
        ClassicAssert.IsTrue(reader.Read());

        var result = reader.GetValue(0);

        // With JsonReadMode.JsonNode (default), the result should be a JsonObject
        ClassicAssert.IsInstanceOf<JsonObject>(result);
        var jsonObj = (JsonObject)result;
        Assert.That((string)jsonObj["name"], Is.EqualTo("Jane"));
        Assert.That((long)jsonObj["age"], Is.EqualTo(25));
    }

    [Test]
    public async Task ShouldReadNestedJsonAsString()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.String);

        var jsonData = "{\"user\": {\"name\": \"Alice\", \"address\": {\"city\": \"NYC\"}}}";
        using var reader = await connection.ExecuteReaderAsync($"SELECT '{jsonData}'::Json");
        ClassicAssert.IsTrue(reader.Read());

        var result = reader.GetValue(0);
        ClassicAssert.IsInstanceOf<string>(result);

        // Verify nested structure is preserved
        var parsed = JsonNode.Parse((string)result);
        Assert.That((string)parsed["user"]["name"], Is.EqualTo("Alice"));
        Assert.That((string)parsed["user"]["address"]["city"], Is.EqualTo("NYC"));
    }

    [Test]
    public async Task ShouldReadJsonArrayAsString()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.String);

        var jsonData = "{\"numbers\": [1, 2, 3, 4, 5]}";
        using var reader = await connection.ExecuteReaderAsync($"SELECT '{jsonData}'::Json(numbers Array(Int32))");
        ClassicAssert.IsTrue(reader.Read());

        var result = reader.GetValue(0);
        ClassicAssert.IsInstanceOf<string>(result);

        var parsed = JsonNode.Parse((string)result);
        var numbers = parsed["numbers"].AsArray();
        Assert.That(numbers.Count, Is.EqualTo(5));
    }

    [Test]
    public async Task ShouldWriteJsonWithStringMode_BulkCopy()
    {
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.String);

        var tableName = "test.json_write_string_mode";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data Json) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        var rows = new[]
        {
            new object[] { 1u, "{\"name\": \"Alice\", \"score\": 100}" },
            new object[] { 2u, "{\"name\": \"Bob\", \"score\": 200}" },
        };

        await bulkCopy.WriteToServerAsync(rows);

        // Verify data was written correctly
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT id, data FROM {tableName} ORDER BY id");

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetUInt32(0), Is.EqualTo(1));
        // Read mode is JsonNode by default, so we get JsonObject back
        var data1 = (JsonObject)reader.GetValue(1);
        Assert.That((string)data1["name"], Is.EqualTo("Alice"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetUInt32(0), Is.EqualTo(2));
        var data2 = (JsonObject)reader.GetValue(1);
        Assert.That((string)data2["name"], Is.EqualTo("Bob"));
    }

    [Test]
    public async Task ShouldWriteJsonObjectWithStringMode_ViaToJsonString_BulkCopy()
    {
        // JsonObject is serialized to string via ToJsonString() in string mode
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.String);

        var tableName = "test.json_write_jsonobject_string_mode";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data Json) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        var jsonObj = new JsonObject
        {
            ["name"] = "Charlie",
            ["score"] = 300
        };

        var rows = new[]
        {
            new object[] { 1u, jsonObj },
        };

        await bulkCopy.WriteToServerAsync(rows);

        // Verify data was written correctly
        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        ClassicAssert.IsTrue(reader.Read());
        var data = (JsonObject)reader.GetValue(0);
        Assert.That((string)data["name"], Is.EqualTo("Charlie"));
        Assert.That((long)data["score"], Is.EqualTo(300));
    }

    [Test]
    public async Task ShouldRoundTrip_WriteStringReadString()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.String, writeMode: JsonWriteMode.String);

        var tableName = "test.json_roundtrip_string";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (data Json) ENGINE = Memory");

        var originalJson = "{\"key\": \"value\", \"num\": 42}";

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(new[] { new object[] { originalJson } });

        // Read back as string
        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        ClassicAssert.IsTrue(reader.Read());
        var result = reader.GetValue(0);
        ClassicAssert.IsInstanceOf<string>(result);

        // Verify the JSON content
        var parsed = JsonNode.Parse((string)result);
        Assert.That((string)parsed["key"], Is.EqualTo("value"));
        Assert.That((int)parsed["num"], Is.EqualTo(42));
    }

    [Test]
    public async Task ShouldWriteJsonObjectWithStringMode_BulkCopy()
    {
        // Tests: JsonObject jo => jo.ToJsonString() in WriteAsString
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.String);

        var tableName = "test.json_write_jsonobject_string_mode";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data Json) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        var jsonObj = new JsonObject
        {
            ["name"] = "Diana",
            ["score"] = 400
        };

        var rows = new[]
        {
            new object[] { 1u, jsonObj },
        };

        await bulkCopy.WriteToServerAsync(rows);

        // Verify data was written correctly
        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        ClassicAssert.IsTrue(reader.Read());
        var data = (JsonObject)reader.GetValue(0);
        Assert.That((string)data["name"], Is.EqualTo("Diana"));
        Assert.That((long)data["score"], Is.EqualTo(400));
    }

    [Test]
    public async Task ShouldWriteJsonNodeWithStringMode_BulkCopy()
    {
        // Tests: JsonNode jn => jn.ToJsonString() in WriteAsString
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.String);

        var tableName = "test.json_write_jsonnode_string_mode";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data Json) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        // Create a JsonNode (not JsonObject) by parsing
        JsonNode jsonNode = JsonNode.Parse("{\"name\": \"Eve\", \"score\": 500}");

        var rows = new[]
        {
            new object[] { 1u, jsonNode },
        };

        await bulkCopy.WriteToServerAsync(rows);

        // Verify data was written correctly
        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        ClassicAssert.IsTrue(reader.Read());
        var data = (JsonObject)reader.GetValue(0);
        Assert.That((string)data["name"], Is.EqualTo("Eve"));
        Assert.That((long)data["score"], Is.EqualTo(500));
    }

    [Test]
    public async Task ShouldWriteSerializedPocoAsStringWithStringMode_BulkCopy()
    {
        // POCOs go through POCO serialization path, not WriteAsString
        // To write a POCO as a string, serialize it to JSON string first
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.String);

        var tableName = "test.json_write_poco_string_mode";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data Json) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        // Serialize POCO to JSON string manually
        var jsonString = System.Text.Json.JsonSerializer.Serialize(new { name = "Frank", score = 600 });

        var rows = new[]
        {
            new object[] { 1u, jsonString },
        };

        await bulkCopy.WriteToServerAsync(rows);

        // Verify data was written correctly
        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        ClassicAssert.IsTrue(reader.Read());
        var data = (JsonObject)reader.GetValue(0);
        Assert.That((string)data["name"], Is.EqualTo("Frank"));
        Assert.That((long)data["score"], Is.EqualTo(600));
    }

    [Test]
    public async Task BulkCopy_TypedJsonSchema_WithDateTimeAndBestEffortParsing_ShouldSucceed()
    {
        // Regression test for datetime parsing issue with typed JSON schemas
        // Uses date_time_input_format=best_effort to parse ISO 8601 datetime strings
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.String);

        var tableName = "test.json_typed_datetime_schema";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {tableName}
            (
                Document JSON(
                    `Entity.DisplayName` String,
                    `Entity.ScopeIds` Array(String),
                    ExactSearchTermValues Array(String),
                    PartialSearchTermsUdmValue String,
                    SystemCreationTime DateTime,
                    SystemDeleted Bool,
                    SystemDeletionTime DateTime,
                    SystemUpdateTime DateTime,
                    TypeName String,
                    UdmTypeNames Array(String),
                    UiEntity Bool
                ),
                Id String
            )
            ENGINE = Memory
        ");

        // Enable best_effort datetime parsing for ISO 8601 format
        connection.CustomSettings["date_time_input_format"] = "best_effort";

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
            ColumnNames = ["Document", "Id"],
        };
        await bulkCopy.InitAsync();

        // JSON with ISO 8601 datetime format (e.g., "2024-07-09T14:06:05.083Z")
        var jsonDocument = """{"$type":"AwsIamUserModel","Id":"AIDA5BUDWVFSA4MIBMX3J","DisplayName":"lambda_UpdateAlias","SystemCreationTime":"2024-07-09T14:06:05.083Z"}""";

        await bulkCopy.WriteToServerAsync(new[]
        {
            new object[] { jsonDocument, "AIDA5BUDWVFSA4MIBMX3J" }
        });

        // Verify data was written correctly
        using var reader = await connection.ExecuteReaderAsync($"SELECT Id, Document FROM {tableName}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetString(0), Is.EqualTo("AIDA5BUDWVFSA4MIBMX3J"));

        // Read mode is JsonNode, so Document comes back as JsonObject
        var doc = (JsonObject)reader.GetValue(1);
        Assert.That((string)doc["DisplayName"], Is.EqualTo("lambda_UpdateAlias"));
    }

    [Test]
    public async Task ShouldThrowWhenWritingStringWithBinaryMode_BulkCopy()
    {
        // String inputs require JsonWriteMode.String
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.Binary);

        var tableName = "test.json_string_binary_mode_error";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (data Json) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        var jsonString = """{"name": "test"}""";

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await bulkCopy.WriteToServerAsync(new[] { new object[] { jsonString } }));

        Assert.That(ex.InnerException, Is.TypeOf<ArgumentException>());
        Assert.That(ex.InnerException.Message, Does.Contain("String and JsonNode inputs require JsonWriteMode.String"));
    }

    [Test]
    public async Task ShouldThrowWhenWritingJsonNodeWithBinaryMode_BulkCopy()
    {
        // JsonNode inputs require JsonWriteMode.String
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.Binary);

        var tableName = "test.json_node_binary_mode_error";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (data Json) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        var jsonObj = new JsonObject { ["name"] = "test" };

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await bulkCopy.WriteToServerAsync(new[] { new object[] { jsonObj } }));

        Assert.That(ex.InnerException, Is.TypeOf<ArgumentException>());
        Assert.That(ex.InnerException.Message, Does.Contain("String and JsonNode inputs require JsonWriteMode.String"));
    }

    [Test]
    public async Task ShouldWritePocoWithStringMode_BulkCopy()
    {
        // POCOs can be written with String mode via JsonSerializer
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.String);

        var tableName = "test.json_poco_string_mode";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (data Json) ENGINE = Memory");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        var poco = new { name = "test", value = 42 };
        await bulkCopy.WriteToServerAsync(new[] { new object[] { poco } });

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        ClassicAssert.IsTrue(reader.Read());
        var data = (JsonObject)reader.GetValue(0);
        Assert.That((string)data["name"], Is.EqualTo("test"));
        Assert.That((long)data["value"], Is.EqualTo(42));
    }

    [Test]
    public void ConnectionStringBuilder_ShouldParseJsonModes()
    {
        var builder = new ClickHouseConnectionStringBuilder("Host=localhost;JsonReadMode=String;JsonWriteMode=String");

        Assert.That(builder.JsonReadMode, Is.EqualTo(JsonReadMode.String));
        Assert.That(builder.JsonWriteMode, Is.EqualTo(JsonWriteMode.String));
    }

    [Test]
    public void ConnectionStringBuilder_ShouldHaveCorrectDefaults()
    {
        var builder = new ClickHouseConnectionStringBuilder("Host=localhost");

        Assert.That(builder.JsonReadMode, Is.EqualTo(JsonReadMode.Binary));
        Assert.That(builder.JsonWriteMode, Is.EqualTo(JsonWriteMode.String));
    }

    [Test]
    public void ClickHouseClientSettings_ShouldCopyJsonModes()
    {
        var original = new ClickHouseClientSettings
        {
            JsonReadMode = JsonReadMode.String,
            JsonWriteMode = JsonWriteMode.String,
        };

        var copy = new ClickHouseClientSettings(original);

        Assert.That(copy.JsonReadMode, Is.EqualTo(JsonReadMode.String));
        Assert.That(copy.JsonWriteMode, Is.EqualTo(JsonWriteMode.String));
    }

    [Test]
    public void ClickHouseClientSettings_ShouldHaveCorrectDefaults()
    {
        var settings = new ClickHouseClientSettings();

        Assert.That(settings.JsonReadMode, Is.EqualTo(JsonReadMode.Binary));
        Assert.That(settings.JsonWriteMode, Is.EqualTo(JsonWriteMode.String));
    }

    #region JSON Roundtrip Tests

    /// <summary>
    /// Tests roundtrip with default mode (Write: String, Read: Binary).
    /// Write JsonObject, read back as JsonObject.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Roundtrip_JsonObject_StringWriteBinaryRead_ShouldPreserveData()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.Binary, writeMode: JsonWriteMode.String);

        var tableName = "test.json_roundtrip_default";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data JSON) ENGINE = Memory");

        var original = new JsonObject
        {
            ["name"] = "test",
            ["count"] = 42,
            ["active"] = true,
            ["tags"] = new JsonArray("a", "b", "c")
        };

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = tableName };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(new[] { new object[] { 1u, original } });

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        Assert.That(reader.Read(), Is.True);

        var result = (JsonObject)reader.GetValue(0);
        Assert.That(result["name"]!.GetValue<string>(), Is.EqualTo("test"));
        Assert.That(result["count"]!.GetValue<long>(), Is.EqualTo(42));
        Assert.That(result["active"]!.GetValue<bool>(), Is.True);
        Assert.That(((JsonArray)result["tags"]!).Count, Is.EqualTo(3));

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Tests roundtrip with String/String mode.
    /// Write string, read back as string.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Roundtrip_String_StringWriteStringRead_ShouldPreserveData()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.String, writeMode: JsonWriteMode.String);

        var tableName = "test.json_roundtrip_string";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data JSON) ENGINE = Memory");

        var original = """{"name":"test","value":123}""";

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = tableName };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(new[] { new object[] { 1u, original } });

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        Assert.That(reader.Read(), Is.True);

        var result = (string)reader.GetValue(0);
        // Parse both to compare structure (server may reformat)
        var originalParsed = JsonNode.Parse(original);
        var resultParsed = JsonNode.Parse(result);
        Assert.That(resultParsed!["name"]!.GetValue<string>(), Is.EqualTo(originalParsed!["name"]!.GetValue<string>()));
        Assert.That(resultParsed["value"]!.GetValue<long>(), Is.EqualTo(originalParsed["value"]!.GetValue<long>()));

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Tests roundtrip with POCO using default String write mode.
    /// Write POCO, read back as JsonObject.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Roundtrip_Poco_StringWriteBinaryRead_ShouldPreserveData()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.Binary, writeMode: JsonWriteMode.String);

        var tableName = "test.json_roundtrip_poco";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data JSON) ENGINE = Memory");

        var original = new { name = "poco_test", score = 99, enabled = true };

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = tableName };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(new[] { new object[] { 1u, original } });

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        Assert.That(reader.Read(), Is.True);

        var result = (JsonObject)reader.GetValue(0);
        Assert.That(result["name"]!.GetValue<string>(), Is.EqualTo("poco_test"));
        Assert.That(result["score"]!.GetValue<long>(), Is.EqualTo(99));
        Assert.That(result["enabled"]!.GetValue<bool>(), Is.True);

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Tests roundtrip with nested JSON objects.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Roundtrip_NestedJson_ShouldPreserveStructure()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.Binary, writeMode: JsonWriteMode.String);

        var tableName = "test.json_roundtrip_nested";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data JSON) ENGINE = Memory");

        var original = new JsonObject
        {
            ["user"] = new JsonObject
            {
                ["profile"] = new JsonObject
                {
                    ["name"] = "deep",
                    ["level"] = 3
                }
            }
        };

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = tableName };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(new[] { new object[] { 1u, original } });

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {tableName}");
        Assert.That(reader.Read(), Is.True);

        var result = (JsonObject)reader.GetValue(0);
        Assert.That(result["user"]!["profile"]!["name"]!.GetValue<string>(), Is.EqualTo("deep"));
        Assert.That(result["user"]!["profile"]!["level"]!.GetValue<long>(), Is.EqualTo(3));

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Tests roundtrip with multiple rows.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task Roundtrip_MultipleRows_ShouldPreserveAllData()
    {
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.Binary, writeMode: JsonWriteMode.String);

        var tableName = "test.json_roundtrip_multi";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt32, data JSON) ENGINE = Memory");

        var rows = new List<object[]>
        {
            new object[] { 1u, new JsonObject { ["value"] = 100 } },
            new object[] { 2u, new JsonObject { ["value"] = 200 } },
            new object[] { 3u, new JsonObject { ["value"] = 300 } },
        };

        using var bulkCopy = new ClickHouseBulkCopy(connection) { DestinationTableName = tableName };
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(rows);

        using var reader = await connection.ExecuteReaderAsync($"SELECT id, data FROM {tableName} ORDER BY id");
        var results = new List<(uint id, long value)>();
        while (reader.Read())
        {
            var id = reader.GetFieldValue<uint>(0);
            var data = (JsonObject)reader.GetValue(1);
            results.Add((id, data["value"]!.GetValue<long>()));
        }

        Assert.That(results.Count, Is.EqualTo(3));
        Assert.That(results[0], Is.EqualTo((1u, 100L)));
        Assert.That(results[1], Is.EqualTo((2u, 200L)));
        Assert.That(results[2], Is.EqualTo((3u, 300L)));

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    #endregion
}
