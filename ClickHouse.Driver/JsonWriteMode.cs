namespace ClickHouse.Driver;

/// <summary>
/// Specifies how JSON data should be sent when writing to ClickHouse.
/// </summary>
public enum JsonWriteMode
{
    /// <summary>
    /// JSON is serialized from System.Text.Json.Nodes.JsonObject or other objects (default).
    /// </summary>
    JsonNode = 0,

    /// <summary>
    /// JSON is sent as a raw string.
    /// The string is passed directly to ClickHouse without parsing.
    /// </summary>
    String = 1,
}
