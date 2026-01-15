namespace ClickHouse.Driver;

/// <summary>
/// Specifies how JSON data should be returned when reading from ClickHouse.
/// </summary>
public enum JsonReadMode
{
    /// <summary>
    /// JSON is returned as System.Text.Json.Nodes.JsonObject (default).
    /// Provides structured access to JSON data but may lose type fidelity for specialized types.
    /// </summary>
    JsonNode = 0,

    /// <summary>
    /// JSON is returned as a raw string.
    /// Preserves the exact JSON representation from ClickHouse.
    /// </summary>
    String = 1,
}
