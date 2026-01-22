using System.Text.Json;
using System.Text.Json.Nodes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates working with ClickHouse JSON type.
/// Covers:
/// - Reading JSON as JsonObject (default)
/// - Reading JSON as string (JsonReadMode.String)
/// - Writing JSON from JsonObject, JsonNode, strings, and POCOs
/// - Configuring JsonReadMode and JsonWriteMode via connection string
/// - Round-trip scenarios with different mode combinations
/// - POCO serialization with type registration
/// </summary>
public static class JsonType
{
    public static async Task Run()
    {
        Console.WriteLine("JSON Type Examples\n");
        Console.WriteLine("=".PadRight(60, '='));

        // Example 1: Reading JSON as JsonObject (default)
        Console.WriteLine("\n1. Reading JSON as JsonObject (default):");
        await Example1_ReadAsJsonObject();

        // Example 2: Reading JSON as string
        Console.WriteLine("\n2. Reading JSON as String:");
        await Example2_ReadAsString();

        // Example 3: Writing JSON from different sources
        Console.WriteLine("\n3. Writing JSON from Different Sources:");
        await Example3_WritingJson();

        // Example 4: String mode for both read and write
        Console.WriteLine("\n4. String Mode for Both Read and Write:");
        await Example4_StringModeRoundTrip();

        // Example 5: POCO serialization with type registration
        Console.WriteLine("\n5. POCO Serialization with Type Registration:");
        await Example5_PocoSerialization();

        Console.WriteLine("\n" + "=".PadRight(60, '='));
        Console.WriteLine("All JSON type examples completed!");
    }

    /// <summary>
    /// By default, JSON columns are returned as System.Text.Json.Nodes.JsonObject.
    /// This allows programmatic access to JSON properties.
    /// </summary>
    private static async Task Example1_ReadAsJsonObject()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        // Enable experimental JSON type
        connection.CustomSettings["allow_experimental_json_type"] = 1;

        // Query JSON data
        using var reader = await connection.ExecuteReaderAsync(
            "SELECT '{\"name\": \"Alice\", \"age\": 30, \"active\": true}'::Json");

        if (reader.Read())
        {
            var json = (JsonObject)reader.GetValue(0);

            Console.WriteLine($"   Type: {json.GetType().Name}");
            Console.WriteLine($"   Name: {json["name"]}");
            Console.WriteLine($"   Age: {json["age"]}");
            Console.WriteLine($"   Active: {json["active"]}");

            // Access nested properties
            var nested = JsonNode.Parse("""{"user": {"profile": {"email": "alice@example.com"}}}""")!.AsObject();
            Console.WriteLine($"\n   Nested access example:");
            Console.WriteLine($"   Email: {nested["user"]!["profile"]!["email"]}");
        }
    }

    /// <summary>
    /// When JsonReadMode is set to String, JSON columns are returned as raw strings.
    /// This is useful when:
    /// - You want to avoid parsing overhead
    /// - You need the exact JSON representation from the server
    /// - You're passing JSON through without modification
    /// </summary>
    private static async Task Example2_ReadAsString()
    {
        // Configure JsonReadMode via connection string
        using var connection = new ClickHouseConnection("Host=localhost;JsonReadMode=String");
        await connection.OpenAsync();

        connection.CustomSettings["allow_experimental_json_type"] = 1;

        using var reader = await connection.ExecuteReaderAsync(
            "SELECT '{\"name\": \"Bob\", \"scores\": [95, 87, 92]}'::Json");

        if (reader.Read())
        {
            var jsonString = (string)reader.GetValue(0);

            Console.WriteLine($"   Type: {jsonString.GetType().Name}");
            Console.WriteLine($"   Raw JSON: {jsonString}");

            // You can still parse it if needed
            var parsed = JsonNode.Parse(jsonString);
            Console.WriteLine($"   Parsed name: {parsed!["name"]}");
        }
    }

    /// <summary>
    /// JSON can be written from various source types using JsonWriteMode.String:
    /// - JsonObject: Serialized via ToJsonString()
    /// - JsonNode: Serialized via ToJsonString()
    /// - String: Passed through directly
    /// - POCOs: Serialized via System.Text.Json.JsonSerializer
    ///
    /// All inputs are serialized to JSON strings and sent to the server.
    /// The server parses the strings (input_format_binary_read_json_as_string=1).
    /// </summary>
    private static async Task Example3_WritingJson()
    {
        // Use JsonWriteMode.String to write JSON from various source types
        using var connection = new ClickHouseConnection("Host=localhost;JsonWriteMode=String");
        await connection.OpenAsync();

        connection.CustomSettings["allow_experimental_json_type"] = 1;

        var tableName = "example_json_write";

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {tableName}
            (
                id UInt32,
                data Json
            )
            ENGINE = Memory
        ");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        // Write from different source types - all serialized to JSON strings
        var rows = new List<object[]>
        {
            // From JsonObject → ToJsonString()
            new object[] { 1u, new JsonObject { ["source"] = "JsonObject", ["value"] = 100 } },

            // From JsonNode (parsed) → ToJsonString()
            new object[] { 2u, JsonNode.Parse("""{"source": "JsonNode", "value": 200}""")! },

            // From String → passed through directly
            new object[] { 3u, """{"source": "String", "value": 300}""" },

            // From anonymous object (POCO) → JsonSerializer.Serialize()
            new object[] { 4u, new { source = "POCO", value = 400 } },
        };

        await bulkCopy.WriteToServerAsync(rows);
        Console.WriteLine($"   Inserted {bulkCopy.RowsWritten} rows");

        // Read back
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, data FROM {tableName} ORDER BY id");

        Console.WriteLine("\n   Results:");
        while (reader.Read())
        {
            var id = reader.GetFieldValue<uint>(0);
            var data = (JsonObject)reader.GetValue(1);
            Console.WriteLine($"     ID {id}: source={data["source"]}, value={data["value"]}");
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// String mode can be enabled/disabled separately for reading and writing.
    /// String mode for both reading and writing is useful when:
    /// - You have JSON strings and want to pass them through unchanged, letting the server parse them
    /// - You want to minimize serialization overhead
    /// - You need precise control over JSON formatting
    /// </summary>
    private static async Task Example4_StringModeRoundTrip()
    {
        // Configure both read and write modes via connection string
        using var connection = new ClickHouseConnection(
            "Host=localhost;JsonReadMode=String;JsonWriteMode=String");
        await connection.OpenAsync();

        connection.CustomSettings["allow_experimental_json_type"] = 1;

        var tableName = "example_json_string_mode";

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {tableName}
            (
                id UInt32,
                data Json
            )
            ENGINE = Memory
        ");

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        // Write raw JSON strings
        var rows = new List<object[]>
        {
            new object[] { 1u, """{"event": "click", "x": 100, "y": 200}""" },
            new object[] { 2u, """{"event": "scroll", "delta": -50}""" },

            // Can also write JsonObject/JsonNode - they get serialized to string
            new object[] { 3u, new JsonObject { ["event"] = "keypress", ["key"] = "Enter" } },
        };

        await bulkCopy.WriteToServerAsync(rows);
        Console.WriteLine($"   Inserted {bulkCopy.RowsWritten} rows with JsonWriteMode.String");

        // Read back as strings
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, data FROM {tableName} ORDER BY id");

        Console.WriteLine("\n   Results (read as strings):");
        while (reader.Read())
        {
            var id = reader.GetFieldValue<uint>(0);
            var data = (string)reader.GetValue(1);
            Console.WriteLine($"     ID {id}: {data}");
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// POCOs can be written to JSON columns in two ways:
    ///
    /// 1. String mode (simple): Uses System.Text.Json.JsonSerializer
    ///    - No type registration required
    ///    - Just set JsonWriteMode=String in connection string
    ///
    /// 2. Binary mode (advanced): Uses custom binary serialization
    ///    - Requires type registration via RegisterJsonSerializationType
    ///    - Supports custom path mappings via attributes
    ///    - Better for typed schemas with hints
    /// </summary>
    private static async Task Example5_PocoSerialization()
    {
        // APPROACH 1: String mode - simple, no registration needed
        Console.WriteLine("   Approach 1: String mode (simple)");

        using var stringModeConnection = new ClickHouseConnection("Host=localhost;JsonWriteMode=String");
        await stringModeConnection.OpenAsync();
        stringModeConnection.CustomSettings["allow_experimental_json_type"] = 1;

        var tableName = "example_json_poco_string";
        await stringModeConnection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await stringModeConnection.ExecuteStatementAsync($@"
            CREATE TABLE {tableName} (id UInt32, data Json) ENGINE = Memory
        ");

        using var bulkCopy = new ClickHouseBulkCopy(stringModeConnection)
        {
            DestinationTableName = tableName,
        };
        await bulkCopy.InitAsync();

        // POCOs serialized via System.Text.Json - no registration needed!
        var rows = new List<object[]>
        {
            new object[] { 1u, new UserEvent { UserId = 123, Action = "login", Timestamp = DateTime.UtcNow } },
            new object[] { 2u, new Product { Name = "Widget", Price = 29.99m, InStock = true, Tags = ["electronics", "gadget"] } },
        };

        await bulkCopy.WriteToServerAsync(rows);
        Console.WriteLine($"   Inserted {bulkCopy.RowsWritten} rows (no type registration needed!)");

        using var reader = await stringModeConnection.ExecuteReaderAsync($"SELECT id, data FROM {tableName} ORDER BY id");
        while (reader.Read())
        {
            var id = reader.GetFieldValue<uint>(0);
            var data = (JsonObject)reader.GetValue(1);
            Console.WriteLine($"     ID {id}: {data.ToJsonString()}");
        }

        // APPROACH 2: Binary mode - type registration with custom path mappings
        Console.WriteLine("\n   Approach 2: Binary mode with type registration");

        // Must explicitly set JsonWriteMode=Binary since default is now String
        using var binaryModeConnection = new ClickHouseConnection("Host=localhost;JsonWriteMode=Binary");
        await binaryModeConnection.OpenAsync();
        binaryModeConnection.CustomSettings["allow_experimental_json_type"] = 1;

        // Register types - enables custom path mappings via attributes
        binaryModeConnection.RegisterJsonSerializationType<EventWithCustomPaths>();

        var tableName2 = "example_json_poco_binary";
        await binaryModeConnection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName2}");
        await binaryModeConnection.ExecuteStatementAsync($"CREATE TABLE {tableName2} (data Json) ENGINE = Memory");

        using var bulkCopy2 = new ClickHouseBulkCopy(binaryModeConnection)
        {
            DestinationTableName = tableName2,
        };
        await bulkCopy2.InitAsync();

        var eventData = new EventWithCustomPaths
        {
            EventType = "click",
            XCoordinate = 150,
            YCoordinate = 300,
            InternalId = "should-be-ignored"
        };

        await bulkCopy2.WriteToServerAsync(new[] { new object[] { eventData } });

        using var reader2 = await binaryModeConnection.ExecuteReaderAsync($"SELECT data FROM {tableName2}");
        if (reader2.Read())
        {
            var data = (JsonObject)reader2.GetValue(0);
            Console.WriteLine($"     Custom paths result: {data.ToJsonString()}");
            Console.WriteLine("     Note: 'type' and 'position.x/y' paths, 'InternalId' ignored");
        }

        await stringModeConnection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await binaryModeConnection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName2}");
    }

    // POCO classes for Example 5

    /// <summary>
    /// Simple POCO with common property types.
    /// </summary>
    private class UserEvent
    {
        public int UserId { get; set; }
        public string Action { get; set; }
        public DateTime Timestamp { get; set; }
    }

    /// <summary>
    /// POCO with decimal and array types.
    /// </summary>
    private class Product
    {
        public string Name { get; set; }
        public decimal Price { get; set; }
        public bool InStock { get; set; }
        public string[] Tags { get; set; }
    }

    /// <summary>
    /// POCO demonstrating attribute-based customization:
    /// - ClickHouseJsonPath: Maps property to custom JSON path
    /// - ClickHouseJsonIgnore: Excludes property from serialization
    /// </summary>
    private class EventWithCustomPaths
    {
        [ClickHouseJsonPath("type")]
        public string EventType { get; set; }

        [ClickHouseJsonPath("position.x")]
        public int XCoordinate { get; set; }

        [ClickHouseJsonPath("position.y")]
        public int YCoordinate { get; set; }

        [ClickHouseJsonIgnore]
        public string InternalId { get; set; }
    }
}
