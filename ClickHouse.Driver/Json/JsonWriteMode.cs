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
#pragma warning disable CA1720 // Identifier contains type name
    String = 1,
#pragma warning restore CA1720 // Identifier contains type name

    /// <summary>
    /// No server settings are automatically applied. Use this for readonly connections
    /// where setting server parameters is not allowed. Behavior is same as Binary.
    /// Does not set input_format_binary_read_json_as_string.
    /// </summary>
    None = 2,
}
