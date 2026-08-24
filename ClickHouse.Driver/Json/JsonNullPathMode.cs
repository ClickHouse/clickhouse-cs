namespace ClickHouse.Driver;

/// <summary>
/// Specifies whether a typed JSON path whose value is NULL appears in the returned JsonObject.
/// Applies to <see cref="JsonReadMode.Binary"/> and <see cref="JsonReadMode.None"/>;
/// <see cref="JsonReadMode.String"/> returns the server's own JSON text and is not affected.
/// </summary>
public enum JsonNullPathMode
{
    /// <summary>
    /// The path is present, holding a JSON null. This matches the server's own JSON rendering,
    /// and keeps "the path is not in this row" distinguishable from "the path is null".
    /// </summary>
    Include = 0,

    /// <summary>
    /// The path is left out, as a dynamic path whose value is null already is. A nested path
    /// takes its parent object with it, so JSON(a.b Nullable(Int64)) holding a null reads as an
    /// empty object rather than {"a":{"b":null}}.
    /// </summary>
    Omit = 1,
}
