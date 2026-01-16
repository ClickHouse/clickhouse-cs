using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class JsonModeTests
{
    private ClickHouseConnection GetConnectionWithJsonMode(JsonReadMode readMode = JsonReadMode.JsonNode, JsonWriteMode writeMode = JsonWriteMode.JsonNode)
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

    #region Read Tests

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
        using var connection = GetConnectionWithJsonMode(readMode: JsonReadMode.JsonNode);

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

    #endregion

    #region Write Tests (BulkCopy)

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
    public async Task ShouldWriteJsonWithJsonNodeMode_BulkCopy()
    {
        using var connection = GetConnectionWithJsonMode(writeMode: JsonWriteMode.JsonNode);

        var tableName = "test.json_write_jsonnode_mode";
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
        Assert.That((int)data["score"], Is.EqualTo(300));
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

    #endregion

    #region Configuration Tests

    [Test]
    public void ConnectionStringBuilder_ShouldParseJsonModes()
    {
        var builder = new ClickHouseConnectionStringBuilder("Host=localhost;JsonReadMode=String;JsonWriteMode=String");

        Assert.That(builder.JsonReadMode, Is.EqualTo(JsonReadMode.String));
        Assert.That(builder.JsonWriteMode, Is.EqualTo(JsonWriteMode.String));
    }

    [Test]
    public void ConnectionStringBuilder_ShouldDefaultToJsonNode()
    {
        var builder = new ClickHouseConnectionStringBuilder("Host=localhost");

        Assert.That(builder.JsonReadMode, Is.EqualTo(JsonReadMode.JsonNode));
        Assert.That(builder.JsonWriteMode, Is.EqualTo(JsonWriteMode.JsonNode));
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
    public void ClickHouseClientSettings_ShouldDefaultToJsonNode()
    {
        var settings = new ClickHouseClientSettings();

        Assert.That(settings.JsonReadMode, Is.EqualTo(JsonReadMode.JsonNode));
        Assert.That(settings.JsonWriteMode, Is.EqualTo(JsonWriteMode.JsonNode));
    }

    #endregion
}
