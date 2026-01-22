namespace ClickHouse.Driver;

/// <summary>
/// Specifies how JSON data should be returned when reading from ClickHouse.
/// </summary>
public enum JsonReadMode
{
    /// <summary>
    /// JSON is returned in binary format from the server and parsed into JsonObject.
    /// </summary>
    Binary = 0,

    /// <summary>
    /// Sets output_format_binary_write_json_as_string=1. JSON is returned as a raw string.
    /// Preserves the exact JSON string representation from ClickHouse.
    /// </summary>
    String = 1,
}
