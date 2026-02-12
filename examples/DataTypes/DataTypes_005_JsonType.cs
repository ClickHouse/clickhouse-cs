using System.Text.Json;
using System.Text.Json.Nodes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates working with ClickHouse JSON type.
///
/// Examples are organized by operation:
/// 1. Insert with String Mode (default) - strings, JsonObject, JsonNode, POCOs
/// 2. Insert with Binary Mode - registered POCOs with custom attributes
/// 3. Read with Binary Mode (default) - JsonObject with nested access
/// 4. Read with String Mode - raw JSON strings
/// 5. Querying JSON Paths - SQL path access, typed schemas
/// </summary>
public static class JsonType
{
    public static async Task Run()
    {
        Console.WriteLine("JSON Type Examples");
        Console.WriteLine("=".PadRight(60, '='));

        await Example1_InsertStringMode();
        await Example2_InsertBinaryMode();
        await Example3_ReadBinaryMode();
        await Example4_ReadStringMode();
        await Example5_QueryingJsonPaths();

        Console.WriteLine("\n" + "=".PadRight(60, '='));
        Console.WriteLine("All JSON type examples completed!");
    }

    /// <summary>
    /// Writing in String mode (default) serializes all inputs to JSON strings.
    /// The server parses these strings via input_format_binary_read_json_as_string=1.
    ///
    /// Supported input types:
    /// - Raw JSON strings (passed through directly)
    /// - JsonObject/JsonNode (serialized via ToJsonString())
    /// - POCOs (serialized via System.Text.Json.JsonSerializer)
    /// </summary>
    private static async Task Example1_InsertStringMode()
    {
        Console.WriteLine("\n1. INSERT WITH STRING MODE (Default)");
        Console.WriteLine("-".PadRight(50, '-'));

        // JsonWriteMode.String is the default, so we can omit it
        using var client = new ClickHouseClient("Host=localhost;set_allow_experimental_json_type=1");

        var tableName = "example_insert_string_mode";
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (id UInt32, data Json) ENGINE = Memory");

        // All these input types work with String mode
        var columns = new[] { "id", "data" };
        var rows = new List<object[]>
        {
            // 1. Raw JSON string - passed through directly
            new object[] { 1u, """{"source": "raw_string", "value": 100}""" },

            // 2. JsonObject - serialized via ToJsonString()
            new object[] { 2u, new JsonObject { ["source"] = "JsonObject", ["value"] = 200 } },

            // 3. JsonNode (parsed) - serialized via ToJsonString()
            new object[] { 3u, JsonNode.Parse("""{"source": "JsonNode", "value": 300}""")! },

            // 4. Anonymous POCO - serialized via JsonSerializer.Serialize()
            new object[] { 4u, new { source = "anonymous_poco", value = 400 } },

            // 5. Typed POCO - also serialized via JsonSerializer.Serialize()
            new object[] { 5u, new SimpleEvent { Source = "typed_poco", Value = 500 } },
        };

        await client.InsertBinaryAsync(tableName, columns, rows);
        Console.WriteLine($"   Inserted {rows.Count} rows with various input types\n");

        // Verify the data
        using var reader = await client.ExecuteReaderAsync(
            $"SELECT id, data FROM {tableName} ORDER BY id");

        Console.WriteLine("   Results:");
        while (reader.Read())
        {
            var id = reader.GetFieldValue<uint>(0);
            var data = (JsonObject)reader.GetValue(1);
            Console.WriteLine($"     ID {id}: source={data["source"]}, value={data["value"]}");
        }

        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Writing in Binary mode writes POCOs using ClickHouse's native binary format.
    /// This preserves type information and supports custom path mappings.
    ///
    /// For paths that have types defined in the column, the type information will be used for appropriate serialization.
    /// For paths that are not typed, the client will use type inference to pick a suitable ClickHouse type depending on the .NET type.
    ///
    /// Requirements:
    /// - Must set JsonWriteMode=Binary in connection string
    /// - Must register POCO types via RegisterJsonSerializationType<T>()
    /// - Cannot write strings or JsonNode
    ///
    /// Controling the parsing via attributes:
    /// - [ClickHouseJsonPath] - map properties to custom JSON paths
    /// - [ClickHouseJsonIgnore] - exclude properties from serialization
    /// </summary>
    private static async Task Example2_InsertBinaryMode()
    {
        Console.WriteLine("\n2. INSERT WITH BINARY MODE");
        Console.WriteLine("-".PadRight(50, '-'));

        // Must explicitly set Binary mode
        using var client = new ClickHouseClient("Host=localhost;JsonWriteMode=Binary;set_allow_experimental_json_type=1");

        // Register POCO types before using them
        client.RegisterJsonSerializationType<EventWithCustomPaths>();
        Console.WriteLine("   Registered POCO types for binary serialization\n");

        var tableName = "example_insert_binary_mode";
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (id UInt32, data Json) ENGINE = Memory");

        var columns = new[] { "id", "data" };
        var rows = new List<object[]>
        {
            // Custom path mapping: EventType -> "type", XCoordinate -> "position.x"
            new object[] { 1u, new EventWithCustomPaths
            {
                EventType = "click",
                XCoordinate = 150,
                YCoordinate = 300,
                InternalId = "ignored-field"  // Will be excluded via [ClickHouseJsonIgnore]
            }},
        };

        await client.InsertBinaryAsync(tableName, columns, rows);
        Console.WriteLine($"   Inserted {rows.Count} rows with binary serialization\n");

        // Verify the data
        using var reader = await client.ExecuteReaderAsync(
            $"SELECT id, data FROM {tableName} ORDER BY id");

        Console.WriteLine("   Results:");
        while (reader.Read())
        {
            var id = reader.GetFieldValue<uint>(0);
            var data = (JsonObject)reader.GetValue(1);
            Console.WriteLine($"     ID {id}: {data.ToJsonString()}");
        }

        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Binary read mode (default) returns JSON as System.Text.Json.Nodes.JsonObject.
    /// This allows programmatic access to JSON properties with full type support.
    /// </summary>
    private static async Task Example3_ReadBinaryMode()
    {
        Console.WriteLine("\n3. READ WITH BINARY MODE (Default)");
        Console.WriteLine("-".PadRight(50, '-'));

        // JsonReadMode.Binary is the default, so we can omit it
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();
        connection.CustomSettings["allow_experimental_json_type"] = 1;

        // Read and print
        Console.WriteLine("\n   Property access:");
        using (var reader = await connection.ExecuteReaderAsync(
            """SELECT '{"user": {"profile": {"email": "bob@example.com", "verified": true}}}'::Json"""))
        {
            if (reader.Read())
            {
                var json = (JsonObject)reader.GetValue(0);
                var email = json["user"]!["profile"]!["email"]!.GetValue<string>();
                var verified = json["user"]!["profile"]!["verified"]!.GetValue<bool>();
                Console.WriteLine($"      Email: {email}");
                Console.WriteLine($"      Verified: {verified}");
            }
        }
    }

    /// <summary>
    /// String read mode returns JSON as raw strings.
    /// Useful for:
    /// - Pass-through scenarios (avoiding parse overhead)
    /// - When you need the exact server representation
    /// - Integration with other JSON libraries
    /// </summary>
    private static async Task Example4_ReadStringMode()
    {
        Console.WriteLine("\n4. READ WITH STRING MODE");
        Console.WriteLine("-".PadRight(50, '-'));

        using var connection = new ClickHouseConnection("Host=localhost;JsonReadMode=String");
        await connection.OpenAsync();
        connection.CustomSettings["allow_experimental_json_type"] = 1;

        // Read as raw string
        Console.WriteLine("   a) Raw string output:");
        using (var reader = await connection.ExecuteReaderAsync(
            """SELECT '{"event": "click", "x": 100, "y": 200}'::Json"""))
        {
            if (reader.Read())
            {
                var jsonString = (string)reader.GetValue(0);
                Console.WriteLine($"      Type returned: {jsonString.GetType().Name}");
                Console.WriteLine($"      Raw JSON: {jsonString}");
            }
        }

        // Parse manually if needed
        Console.WriteLine("\n   b) Manual parsing when needed:");
        using (var reader = await connection.ExecuteReaderAsync(
            """SELECT '{"user": "Alice", "action": "login"}'::Json"""))
        {
            if (reader.Read())
            {
                var jsonString = (string)reader.GetValue(0);

                // Parse with System.Text.Json if you need structured access
                var parsed = JsonNode.Parse(jsonString);
                Console.WriteLine($"      Parsed user: {parsed!["user"]}");
                Console.WriteLine($"      Parsed action: {parsed["action"]}");
            }
        }
    }

    /// <summary>
    /// ClickHouse supports querying JSON paths directly in SQL.
    /// This example covers:
    /// - Path access syntax (data.field)
    /// - Typed JSON schemas with hints
    /// - Combining SQL queries with .NET reading
    /// </summary>
    private static async Task Example5_QueryingJsonPaths()
    {
        Console.WriteLine("\n5. QUERYING JSON PATHS");
        Console.WriteLine("-".PadRight(50, '-'));

        using var connection = new ClickHouseConnection("Host=localhost;");
        await connection.OpenAsync();
        connection.CustomSettings["allow_experimental_json_type"] = 1;

        // Setup: Create table with JSON and insert data, this is partially typed and partially dynamic
        Console.WriteLine("\n   Typed JSON schema with hints:");
        var typedTable = "example_typed_json";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {typedTable}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {typedTable} (
                id UInt32,
                data Json(
                    age Int32,
                    tags Array(String),
                    `metadata.created` DateTime
                )
            ) ENGINE = Memory");

        Console.WriteLine("      Created table with typed JSON schema:");
        Console.WriteLine("        - age: Int23");
        Console.WriteLine("        - tags: Array(String)");
        Console.WriteLine("        - metadata.created: DateTime");

        // Insert data - hints ensure proper type handling
        using var client = new ClickHouseClient("Host=localhost;set_allow_experimental_json_type=1;set_date_time_input_format=best_effort");

        var columns = new[] { "id", "data" };
        var rows = new[]
        {
            new object[] { 1u, """{"name": "Alice", "age": 30, "tags": ["admin", "active"], "metadata": {"created": "2024-01-15T10:30:00Z"}}""" },
            new object[] { 2u, """{"name": "Bob", "age": 25, "tags": ["user"], "metadata": {"created": "2024-02-20T14:45:00Z"}}""" },
        };

        await client.InsertBinaryAsync(typedTable, columns, rows);

        // Query paths
        Console.WriteLine("\n   Querying typed paths:");
        using (var reader = await connection.ExecuteReaderAsync(
            $"SELECT data.name, data.age, data.tags, data.`metadata.created` FROM {typedTable} ORDER BY data.age"))
        {
            while (reader.Read())
            {
                var name = reader.GetString(0);
                var age = reader.GetInt32(1);
                var tags = reader.GetFieldValue<string[]>(2);
                var created = reader.GetDateTime(3);
                Console.WriteLine($"      {name} (age {age}): tags=[{string.Join(", ", tags)}], created={created:yyyy-MM-dd}");
            }
        }

        // Aggregations on JSON paths
        Console.WriteLine("\n   Aggregations on JSON paths:");
        using (var reader = await connection.ExecuteReaderAsync(
            $"SELECT avg(data.age), max(data.age), min(data.age) FROM {typedTable}"))
        {
            if (reader.Read())
            {
                Console.WriteLine($"      Average age: {reader.GetDouble(0):F1}");
                Console.WriteLine($"      Max age: {reader.GetInt32(1)}");
                Console.WriteLine($"      Min age: {reader.GetInt32(2)}");
            }
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {typedTable}");
    }

    /// <summary>Simple POCO for String mode serialization.</summary>
    private class SimpleEvent
    {
        [System.Text.Json.Serialization.JsonPropertyName("source")]
        public string Source { get; set; } = "";

        [System.Text.Json.Serialization.JsonPropertyName("value")]
        public int Value { get; set; }
    }

    /// <summary>
    /// POCO with custom path mappings for Binary mode.
    /// - [ClickHouseJsonPath] maps properties to custom JSON paths
    /// - [ClickHouseJsonIgnore] excludes properties from serialization
    /// </summary>
    private class EventWithCustomPaths
    {
        [ClickHouseJsonPath("type")]
        public required string EventType { get; set; }

        [ClickHouseJsonPath("position.x")]
        public int XCoordinate { get; set; }

        [ClickHouseJsonPath("position.y")]
        public int YCoordinate { get; set; }

        [ClickHouseJsonIgnore]
        public required string InternalId { get; set; }
    }
}
