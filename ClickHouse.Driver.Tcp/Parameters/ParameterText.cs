namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// The text escaping the native protocol's parameter list needs.
/// </summary>
/// <remarks>
/// A parameter value passes two server-side text stages. The settings reader restores a Field from the text,
/// which requires a quoted SQL literal, and the <c>{name:Type}</c> substitution then parses what that Field
/// holds. Each stage removes one round of backslash escapes, so the formatter escapes the value text once for
/// the substitution stage and escapes and quotes the whole parameter again for the Field stage.
/// </remarks>
internal static class ParameterText
{
    /// <summary>Escapes the characters a server text reader treats as an escape sequence or a terminator.</summary>
    /// <param name="value">The text to escape.</param>
    /// <returns>The escaped text.</returns>
    /// <remarks>
    /// A tab or a newline terminates the substitution stage's reader, so both must arrive as escape sequences
    /// rather than as themselves. This matches the HTTP transport's escaping — see the note on
    /// <see cref="TcpParameterFormatter"/>.
    /// </remarks>
    public static string Escape(this string value)
        => value.Replace("\\", "\\\\").Replace("'", "\\'").Replace("\n", "\\n").Replace("\t", "\\t");

    /// <summary>Wraps the text in single quotes.</summary>
    /// <param name="value">The text to quote.</param>
    /// <returns>The quoted text.</returns>
    /// <remarks>
    /// Unconditional, unlike the HTTP helper of the same name, which leaves already-quoted text alone. Every
    /// caller here quotes text that <see cref="Escape"/> has just produced, and an escaped string cannot start
    /// with a bare quote, so the HTTP check could never fire.
    /// </remarks>
    public static string QuoteSingle(this string value) => $"'{value}'";
}
