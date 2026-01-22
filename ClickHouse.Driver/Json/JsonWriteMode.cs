namespace ClickHouse.Driver;

/// <summary>
/// Specifies how JSON data should be sent when writing to ClickHouse.
/// </summary>
public enum JsonWriteMode
{
    /// <summary>
    /// JSON is written in binary format. Only registered POCO types are supported.
    /// Use RegisterJsonSerializationType to register types with custom path mappings.
    /// </summary>
    Binary = 0,

    /// <summary>
    /// JSON is sent as a string (default). Accepts JsonObject, JsonNode, strings, and POCOs.
    /// POCOs are serialized via System.Text.Json.JsonSerializer.
    /// The string is passed directly to ClickHouse and parsed on the server side.
    /// </summary>
    String = 1,
}
