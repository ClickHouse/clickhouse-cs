using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Extracts parameter type hints from SQL queries using ClickHouse's native parameter syntax, {parameter_name:type}
/// </summary>
internal static class SqlParameterTypeExtractor
{
    /// <summary>
    /// Extracts type hints from a SQL query string.
    /// </summary>
    /// <param name="sql">The SQL query containing parameter placeholders.</param>
    /// <returns>
    /// A dictionary mapping parameter names to their type definitions.
    /// Parameters without type hints (e.g., <c>{name}</c>) are not included.
    /// </returns>
    public static Dictionary<string, string> ExtractTypeHints(string sql)
    {
        var result = new Dictionary<string, string>();

        if (string.IsNullOrEmpty(sql))
            return result;

        var i = 0;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (c == '\'' || c == '`' || c == '"')
            {
                // String literal or quoted identifier
                i = SkipQuotedToken(sql, i);
            }
            else if (c == '$' && TrySkipHeredoc(sql, i, out var afterHeredoc))
            {
                // Heredoc: $tag$ ... $tag$
                i = afterHeredoc;
            }
            else if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                // SQL-style line comment: -- (skip to end of line)
                i = SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '/')
            {
                // C++-style line comment: // (skip to end of line)
                i = SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '#' && i + 1 < sql.Length && (sql[i + 1] == ' ' || sql[i + 1] == '!'))
            {
                // MySQL-style line comment: only "# " and "#!" start one, a bare "#x" does not
                i = SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                // C-style block comment: /* ... */, nestable (skip to the matching */)
                i = SkipBlockComment(sql, i + 2);
            }
            else if (c == '{')
            {
                // Potential parameter start - try to extract {name:Type}
                var (paramName, paramType, endIndex) = TryExtractParameter(sql, i);
                if (paramName != null && paramType != null)
                {
                    if (result.TryGetValue(paramName, out var existingType) && existingType != paramType)
                    {
                        throw new ArgumentException(
                            $"Parameter '{paramName}' has conflicting type hints: '{existingType}' and '{paramType}'");
                    }
                    result[paramName] = paramType;
                    i = endIndex;
                }
                else
                {
                    i++;
                }
            }
            else
            {
                i++;
            }
        }

        return result;
    }

    /// <summary>
    /// Tries to extract a parameter from the given position.
    /// Returns (name, type, endIndex) if successful, or (null, null, 0) if not a valid parameter.
    /// </summary>
    private static (string name, string type, int endIndex) TryExtractParameter(string sql, int startIndex)
    {
        // Must start with {
        if (sql[startIndex] != '{')
            return (null, null, 0);

        // Find the colon that separates name from type, searching only within this parameter's own
        // name: it must be a single run of parameter name characters, optionally surrounded by
        // whitespace. Otherwise a brace that is not a type hint, such as one inside a backtick-quoted
        // alias, would consume the colon of a later parameter and silently drop its hint.
        var colonIndex = -1;
        var nameLength = 0;
        var afterName = false;

        for (var j = startIndex + 1; j < sql.Length; j++)
        {
            var nameChar = sql[j];
            if (nameChar == ':')
            {
                colonIndex = j;
                break;
            }

            if (char.IsWhiteSpace(nameChar))
            {
                afterName = nameLength > 0;
                continue;
            }

            if (afterName || !IsParameterNameChar(nameChar))
                return (null, null, 0);

            nameLength++;
        }

        if (colonIndex < 0)
            return (null, null, 0);

        var paramName = sql.Substring(startIndex + 1, colonIndex - startIndex - 1).Trim();
        if (string.IsNullOrEmpty(paramName))
            return (null, null, 0);

        var i = colonIndex + 1;

        // Extract type definition
        var typeStart = i;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (c == '\'' || c == '`' || c == '"')
            {
                // Quoted token within the type, e.g. an Enum value or a named tuple element
                i = SkipQuotedToken(sql, i);
            }
            else if (c == '{')
            {
                // A type definition never contains an opening brace, so this parameter is
                // unterminated and the brace starts a new one
                return (null, null, 0);
            }
            else if (c == '}')
            {
                // End of parameter
                var paramType = sql.Substring(typeStart, i - typeStart).Trim();
                if (!string.IsNullOrEmpty(paramType))
                {
                    return (paramName, paramType, i + 1);
                }
                return (null, null, 0);
            }
            else
            {
                i++;
            }
        }

        // Unterminated parameter
        return (null, null, 0);
    }

    /// <summary>
    /// Determines whether the character can appear in a ClickHouse query parameter name. The server
    /// parses the name as a bare word, which is narrower than an identifier: a quoted identifier such
    /// as {`a`:Int32} or {"a":Int32} is rejected as a syntax error. Only ASCII word characters and $.
    /// </summary>
    private static bool IsParameterNameChar(char c) =>
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '$';

    /// <summary>
    /// Skips to the end of a line
    /// Returns the index of the first character after the newline, or sql.Length if no newline found.
    /// </summary>
    private static int SkipToEndOfLine(string sql, int startIndex)
    {
        var newlineIndex = sql.IndexOf('\n', startIndex);
        return newlineIndex < 0 ? sql.Length : newlineIndex + 1;
    }

    /// <summary>
    /// Skips a nestable C-style block comment (after the opening /*).
    /// Returns the index of the first character after the matching */, or sql.Length if not found.
    /// </summary>
    private static int SkipBlockComment(string sql, int startIndex)
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

    /// <summary>
    /// Skips a single-quoted string literal or a backtick/double-quote quoted identifier,
    /// starting at the opening quote. Both doubling ('') and backslash (\') escapes are honored.
    /// Returns the index of the first character after the closing quote, or sql.Length if unterminated.
    /// </summary>
    private static int SkipQuotedToken(string sql, int startIndex)
    {
        var quote = sql[startIndex];
        var i = startIndex + 1;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (c == '\\')
            {
                i += 2;
            }
            else if (c == quote)
            {
                if (i + 1 < sql.Length && sql[i + 1] == quote)
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }
            else
            {
                i++;
            }
        }

        return sql.Length;
    }

    /// <summary>
    /// Tries to skip a heredoc starting at a $ sign: $tag$ ... $tag$, where the tag is empty or
    /// consists of ASCII word characters. Returns false if this $ does not open a terminated heredoc,
    /// in which case it is an ordinary character. A heredoc can only begin at a token boundary, see
    /// <see cref="IsTokenChar"/>.
    /// </summary>
    private static bool TrySkipHeredoc(string sql, int startIndex, out int endIndex)
    {
        endIndex = 0;

        // A $ that continues a token cannot open a heredoc: the server lexes b$c$ as one identifier
        if (startIndex > 0 && IsTokenChar(sql[startIndex - 1]))
            return false;

        var i = startIndex + 1;
        while (i < sql.Length && IsHeredocTagChar(sql[i]))
            i++;

        if (i >= sql.Length || sql[i] != '$')
            return false;

        var tag = sql.Substring(startIndex, i - startIndex + 1);
        var closeIndex = sql.IndexOf(tag, i + 1, StringComparison.Ordinal);
        if (closeIndex < 0)
            return false;

        endIndex = closeIndex + tag.Length;
        return true;
    }

    private static bool IsHeredocTagChar(char c) =>
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';

    /// <summary>
    /// Determines whether the character can continue an ordinary token, so that a $ following it is
    /// part of that token rather than the start of a heredoc. The server lexes a word token as a run
    /// of ASCII word characters and dollar signs, so b$c$ is one identifier and not a heredoc opener.
    /// Looking only at the preceding character misses the case where it ends a literal instead of a
    /// word, as in 1$tag$...$tag$ or $$a$$$tag$...$tag$, where the server does open a heredoc. Both
    /// shapes place two literals next to each other, which the server rejects as a syntax error, so
    /// no query it accepts is affected.
    /// </summary>
    private static bool IsTokenChar(char c) => IsHeredocTagChar(c) || c == '$';
}
