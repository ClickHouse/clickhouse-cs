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
#pragma warning disable CA1720 // Identifier contains type name
    String = 1,
#pragma warning restore CA1720 // Identifier contains type name

    /// <summary>
    /// No server settings are automatically applied. Use this for readonly connections
    /// where setting server parameters is not allowed. Behavior is same as Binary.
    /// Does not set output_format_binary_write_json_as_string.
    /// </summary>
    None = 2,
}
