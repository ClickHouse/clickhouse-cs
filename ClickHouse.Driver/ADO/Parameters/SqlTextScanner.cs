using System;

namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Low-level helpers for walking SQL text and skipping over regions where parameter placeholders
/// must not be interpreted: quoted strings and identifiers, heredocs and comments.
/// </summary>
internal static class SqlTextScanner
{
    /// <summary>
    /// Skips a quoted region starting at <paramref name="startIndex"/>, which must hold the opening
    /// delimiter (<c>'</c>, <c>"</c> or <c>`</c>). Both backslash escapes and a doubled delimiter
    /// are treated as escapes.
    /// Returns the index of the first character after the closing delimiter,
    /// or sql.Length if the region is unterminated.
    /// </summary>
    public static int SkipQuoted(string sql, int startIndex)
    {
        var delimiter = sql[startIndex];
        var i = startIndex + 1;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (c == '\\')
            {
                i += 2;
                continue;
            }

            if (c == delimiter)
            {
                if (i + 1 < sql.Length && sql[i + 1] == delimiter)
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        return sql.Length;
    }

    /// <summary>
    /// Skips a heredoc (<c>$$...$$</c> or <c>$tag$...$tag$</c>) starting at
    /// <paramref name="startIndex"/>, which must hold the leading <c>$</c>.
    /// Returns the index of the first character after the closing tag, or -1 if there is no
    /// heredoc at this position.
    /// </summary>
    public static int TrySkipHeredoc(string sql, int startIndex)
    {
        var i = startIndex + 1;
        while (i < sql.Length && IsTagChar(sql[i]))
            i++;

        if (i >= sql.Length || sql[i] != '$')
            return -1;

        var tag = sql.Substring(startIndex, i - startIndex + 1);
        var endIndex = sql.IndexOf(tag, i + 1, StringComparison.Ordinal);
        return endIndex < 0 ? -1 : endIndex + tag.Length;
    }

    /// <summary>
    /// Skips to the end of a line
    /// Returns the index of the first character after the newline, or sql.Length if no newline found.
    /// </summary>
    public static int SkipToEndOfLine(string sql, int startIndex)
    {
        var newlineIndex = sql.IndexOf('\n', startIndex);
        return newlineIndex < 0 ? sql.Length : newlineIndex + 1;
    }

    /// <summary>
    /// Skips a C-style block comment (after /*).
    /// Returns the index of the first character after */, or sql.Length if not found.
    /// </summary>
    public static int SkipBlockComment(string sql, int startIndex)
    {
        var endIndex = sql.IndexOf("*/", startIndex, StringComparison.Ordinal);
        return endIndex < 0 ? sql.Length : endIndex + 2;
    }

    private static bool IsTagChar(char c) => c is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '_';
}
