namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Escapes parameter text for the native protocol's two server-side parsing stages.
/// </summary>
/// <remarks>
/// The formatter first escapes the SQL value for placeholder substitution, then escapes and quotes it for the
/// settings reader.
/// </remarks>
internal static class ParameterText
{
    /// <summary>Escapes the characters a server text reader treats as an escape sequence or a terminator.</summary>
    /// <param name="value">The text to escape.</param>
    /// <returns>The escaped text.</returns>
    /// <remarks>
    /// Tabs and newlines must be escaped because they terminate the server's text reader.
    /// </remarks>
    public static string Escape(this string value)
        => value.Replace("\\", "\\\\").Replace("'", "\\'").Replace("\n", "\\n").Replace("\t", "\\t");

    /// <summary>Wraps the text in single quotes.</summary>
    /// <param name="value">The text to quote.</param>
    /// <returns>The quoted text.</returns>
    public static string QuoteSingle(this string value) => $"'{value}'";
}
