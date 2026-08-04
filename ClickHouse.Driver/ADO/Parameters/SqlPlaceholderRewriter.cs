using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Rewrites ADO.NET-style <c>@name</c> placeholders into ClickHouse-native <c>{name:Type}</c> syntax.
/// Only occurrences in code positions are rewritten: quoted strings and identifiers, heredocs and
/// comments are left untouched, since the server does not substitute parameters there either.
/// </summary>
internal static class SqlPlaceholderRewriter
{
    /// <summary>
    /// Replaces every <c>@name</c> placeholder found in a code position with its replacement.
    /// </summary>
    /// <param name="sql">The SQL query with @-style placeholders.</param>
    /// <param name="replacements">Replacement text keyed by placeholder, including the leading <c>@</c>.</param>
    public static string ReplacePlaceholders(string sql, Dictionary<string, string> replacements)
    {
        if (string.IsNullOrEmpty(sql) || replacements == null || replacements.Count == 0)
            return sql;

        // Longest first, so that @id does not shadow @id_2
        var placeholders = replacements.Keys.OrderByDescending(k => k.Length).ToArray();

        StringBuilder builder = null;
        var copiedUpTo = 0;
        var i = 0;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (c == '\'' || c == '"' || c == '`')
            {
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
                // C-style block comment: /* ... */ (skip to closing */)
                i = SqlTextScanner.SkipBlockComment(sql, i + 2);
            }
            else if (c == '$')
            {
                // Heredoc: $$ ... $$ or $tag$ ... $tag$ (-1 when this $ does not open one)
                var afterHeredoc = SqlTextScanner.TrySkipHeredoc(sql, i);
                i = afterHeredoc < 0 ? i + 1 : afterHeredoc;
            }
            else if (c == '@' && MatchPlaceholder(sql, i, placeholders) is string placeholder)
            {
                builder ??= new StringBuilder(sql.Length);
                builder.Append(sql, copiedUpTo, i - copiedUpTo);
                builder.Append(replacements[placeholder]);
                i += placeholder.Length;
                copiedUpTo = i;
            }
            else
            {
                i++;
            }
        }

        if (builder == null)
            return sql;

        builder.Append(sql, copiedUpTo, sql.Length - copiedUpTo);
        return builder.ToString();
    }

    private static string MatchPlaceholder(string sql, int startIndex, string[] placeholders)
    {
        foreach (var placeholder in placeholders)
        {
            if (startIndex + placeholder.Length > sql.Length)
                continue;
            if (string.CompareOrdinal(sql, startIndex, placeholder, 0, placeholder.Length) != 0)
                continue;

            var endIndex = startIndex + placeholder.Length;
            if (endIndex < sql.Length && IsWordChar(sql[endIndex]))
                continue; // @id must not match inside @identifier

            return placeholder;
        }

        return null;
    }

    // Matches the word characters of the regex this rewriter replaced, so that a placeholder is
    // still not recognized inside a longer identifier.
    private static bool IsWordChar(char c) => char.IsLetterOrDigit(c) || c == '_';
}
