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
    /// Returns the index of the first character after the closing tag, or -1 if there is no heredoc
    /// at this position.
    /// An opening tag that is never closed does not start a heredoc: the server falls back to lexing
    /// the whole run of word characters and dollar signs as one ordinary token and keeps parsing the
    /// rest of the query, so the returned index is the one just past that token. Skipping only the
    /// opening tag instead would let its trailing $ be mistaken for the start of a later heredoc.
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
        if (endIndex >= 0)
            return endIndex + tag.Length;

        // Unterminated: skip the ordinary token the server lexes instead
        i++;
        while (i < sql.Length && (IsTagChar(sql[i]) || sql[i] == '$'))
            i++;

        return i;
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
    /// Skips a nestable C-style block comment (after the opening /*), matching the server lexer:
    /// an inner /* opens a new level, so the comment ends only at the */ that closes the outermost one.
    /// Returns the index of the first character after the matching */, or sql.Length if not found.
    /// </summary>
    public static int SkipBlockComment(string sql, int startIndex)
    {
        var depth = 1;
        var i = startIndex;

        while (i + 1 < sql.Length)
        {
            if (sql[i] == '/' && sql[i + 1] == '*')
            {
                depth++;
                i += 2;
            }
            else if (sql[i] == '*' && sql[i + 1] == '/')
            {
                i += 2;
                if (--depth == 0)
                    return i;
            }
            else
            {
                i++;
            }
        }

        return sql.Length;
    }

    private static bool IsTagChar(char c) => c is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '_';
}
