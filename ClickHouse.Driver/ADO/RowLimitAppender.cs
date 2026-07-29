using System;

namespace ClickHouse.Driver.ADO;

/// <summary>
/// Appends a row-limiting clause (e.g. <c>LIMIT 0</c> / <c>LIMIT 1</c>) to a command's SQL text
/// for <see cref="System.Data.CommandBehavior.SchemaOnly"/> / <see cref="System.Data.CommandBehavior.SingleRow"/>.
/// A naive verbatim append breaks two ways: a trailing single-line comment (<c>--</c> / <c>#</c>)
/// swallows the appended clause, and a trailing statement terminator (<c>;</c>) turns the query into
/// a rejected multi-statement. This helper strips a trailing terminator and puts the clause on its own
/// line, scanning the text in a string/comment-aware manner (mirroring <see cref="Parameters.SqlParameterTypeExtractor"/>)
/// so that semicolons and comment markers inside string literals or comments are left untouched.
/// </summary>
internal static class RowLimitAppender
{
    /// <summary>
    /// Returns <paramref name="commandText"/> with <paramref name="limitClause"/> applied so it always
    /// takes effect: any trailing statement-terminating <c>;</c> is removed, and the clause is appended
    /// on a new line so a trailing single-line comment cannot swallow it.
    /// </summary>
    public static string Append(string commandText, string limitClause)
    {
        if (string.IsNullOrEmpty(commandText))
            return "\n" + limitClause;

        var terminator = FindTrailingSemicolon(commandText);
        var body = terminator >= 0 ? commandText.Remove(terminator, 1) : commandText;
        return body + "\n" + limitClause;
    }

    /// <summary>
    /// Returns the index of a top-level <c>;</c> that is followed only by whitespace and/or comments
    /// (a trailing statement terminator), or <c>-1</c> if there is none. Semicolons inside string
    /// literals or comments, and semicolons followed by further SQL code, are not reported.
    /// </summary>
    private static int FindTrailingSemicolon(string sql)
    {
        var trailingSemicolon = -1;
        var i = 0;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (c == '\'')
            {
                // String literal is code: it cancels any pending trailing terminator.
                trailingSemicolon = -1;
                i = SkipString(sql, i + 1);
            }
            else if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                i = SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '#')
            {
                i = SkipToEndOfLine(sql, i + 1);
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                i = SkipBlockComment(sql, i + 2);
            }
            else if (c == ';')
            {
                trailingSemicolon = i;
                i++;
            }
            else if (char.IsWhiteSpace(c))
            {
                // Whitespace and comments preserve a pending trailing terminator.
                i++;
            }
            else
            {
                // Any other SQL code cancels a pending trailing terminator.
                trailingSemicolon = -1;
                i++;
            }
        }

        return trailingSemicolon;
    }

    /// <summary>
    /// Skips a single-quoted string literal. <paramref name="i"/> is the index of the first character
    /// after the opening quote. A doubled quote (<c>''</c>) is an escaped quote, matching how
    /// <see cref="Parameters.SqlParameterTypeExtractor"/> scans string literals. Returns the index of the
    /// first character after the closing quote, or <c>sql.Length</c> if the string is unterminated.
    /// </summary>
    private static int SkipString(string sql, int i)
    {
        while (i < sql.Length)
        {
            if (sql[i] == '\'')
            {
                // Doubled quote ('') is an escaped quote, not a terminator.
                if (i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        return i;
    }

    /// <summary>
    /// Skips to the first character after the next newline, or <c>sql.Length</c> if none.
    /// </summary>
    private static int SkipToEndOfLine(string sql, int startIndex)
    {
        var newlineIndex = sql.IndexOf('\n', startIndex);
        return newlineIndex < 0 ? sql.Length : newlineIndex + 1;
    }

    /// <summary>
    /// Skips a C-style block comment (after <c>/*</c>). Returns the index after <c>*/</c>, or
    /// <c>sql.Length</c> if unterminated.
    /// </summary>
    private static int SkipBlockComment(string sql, int startIndex)
    {
        var endIndex = sql.IndexOf("*/", startIndex, StringComparison.Ordinal);
        return endIndex < 0 ? sql.Length : endIndex + 2;
    }
}
