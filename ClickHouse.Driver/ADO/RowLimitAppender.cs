using ClickHouse.Driver.ADO.Parameters;

namespace ClickHouse.Driver.ADO;

/// <summary>
/// Appends a row-limiting clause (e.g. <c>LIMIT 0</c> / <c>LIMIT 1</c>) to a command's SQL text
/// for <see cref="System.Data.CommandBehavior.SchemaOnly"/> / <see cref="System.Data.CommandBehavior.SingleRow"/>.
/// A naive verbatim append breaks two ways: a trailing single-line comment (<c>--</c> / <c>#</c>)
/// swallows the appended clause, and a trailing statement terminator (<c>;</c>) turns the query into
/// a rejected multi-statement. This helper strips a trailing terminator and puts the clause on its own
/// line, scanning the text with <see cref="SqlTextScanner"/> so that semicolons and comment markers
/// inside quoted strings and identifiers, heredocs or comments are left untouched.
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
    /// (a trailing statement terminator), or <c>-1</c> if there is none. Semicolons inside quoted
    /// strings and identifiers, heredocs or comments, and semicolons followed by further SQL code,
    /// are not reported. The token rules are the server's, shared with
    /// <see cref="SqlPlaceholderRewriter"/> through <see cref="SqlTextScanner"/>: diverging from
    /// them would make this scanner disagree with the server about where a statement ends.
    /// </summary>
    private static int FindTrailingSemicolon(string sql)
    {
        var trailingSemicolon = -1;
        var i = 0;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (c == '\'' || c == '"' || c == '`')
            {
                // String literal or quoted identifier is code: it cancels a pending terminator.
                trailingSemicolon = -1;
                i = SqlTextScanner.SkipQuoted(sql, i);
            }
            else if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                // SQL-style line comment: -- (skip to end of line)
                i = SqlTextScanner.SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '/')
            {
                // C++-style line comment: // (skip to end of line)
                i = SqlTextScanner.SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '#' && i + 1 < sql.Length && (sql[i + 1] == ' ' || sql[i + 1] == '!'))
            {
                // MySQL-style line comment: only "# " and "#!" start one, a bare "#x" does not
                i = SqlTextScanner.SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                // C-style block comment: /* ... */, nestable (skip to the matching */)
                i = SqlTextScanner.SkipBlockComment(sql, i + 2);
            }
            else if (c == '$')
            {
                // Heredoc: $$ ... $$ or $tag$ ... $tag$ (-1 when this $ does not open one). Either
                // way what is skipped is code, so a pending terminator is cancelled.
                var afterHeredoc = SqlTextScanner.TrySkipHeredoc(sql, i);
                trailingSemicolon = -1;
                i = afterHeredoc < 0 ? i + 1 : afterHeredoc;
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
}
