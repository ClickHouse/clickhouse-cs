using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Extracts ClickHouse-native <c>{name:Type}</c> hints while ignoring SQL strings, quoted identifiers,
/// heredocs and comments.
/// </summary>
/// <remarks>
/// Ported from the HTTP scanner because the TCP assembly cannot reference it. Keep the two aligned.
/// </remarks>
internal static class SqlParameterTypeExtractor
{
    /// <summary>Extracts parameter names and types from a SQL query.</summary>
    /// <param name="sql">The SQL query containing parameter placeholders.</param>
    /// <returns>
    /// Parameter names mapped to their types; placeholders without types are omitted.
    /// </returns>
    /// <exception cref="ArgumentException">The same parameter name has two different type hints.</exception>
    public static Dictionary<string, string> ExtractTypeHints(string sql)
    {
        var result = new Dictionary<string, string>();

        if (string.IsNullOrEmpty(sql))
        {
            return result;
        }

        int i = 0;

        while (i < sql.Length)
        {
            char c = sql[i];

            if (c is '\'' or '`' or '"')
            {
                // String literal or quoted identifier.
                i = SkipQuotedToken(sql, i);
            }
            else if (c == '$' && TrySkipHeredoc(sql, i, out int afterHeredoc))
            {
                // Heredoc: $tag$ ... $tag$.
                i = afterHeredoc;
            }
            else if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                // SQL-style line comment: -- (skip to end of line).
                i = SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '/')
            {
                // C++-style line comment: // (skip to end of line).
                i = SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '#' && i + 1 < sql.Length && (sql[i + 1] == ' ' || sql[i + 1] == '!'))
            {
                // MySQL-style line comment: only "# " and "#!" start one, a bare "#x" does not.
                i = SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                // C-style block comment: /* ... */, nestable (skip to the matching */).
                i = SkipBlockComment(sql, i + 2);
            }
            else if (c == '{')
            {
                (string paramName, string paramType, int endIndex) = TryExtractParameter(sql, i);
                if (paramName != null && paramType != null)
                {
                    if (result.TryGetValue(paramName, out string existingType) && existingType != paramType)
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

    /// <summary>Extracts a parameter at the given position, or returns null values when it is invalid.</summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="startIndex">The index of the opening brace.</param>
    /// <returns>The parameter name, its type, and the index after the closing brace.</returns>
    private static (string Name, string Type, int EndIndex) TryExtractParameter(string sql, int startIndex)
    {
        if (sql[startIndex] != '{')
        {
            return (null, null, 0);
        }

        // Find the colon that separates name from type, searching only within this parameter's own
        // name: it must be a single run of parameter name characters, optionally surrounded by
        // whitespace. Otherwise a brace that is not a type hint, such as one inside a backtick-quoted
        // alias, would consume the colon of a later parameter and silently drop its hint.
        int colonIndex = -1;
        int nameLength = 0;
        bool afterName = false;

        for (int j = startIndex + 1; j < sql.Length; j++)
        {
            char nameChar = sql[j];
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
            {
                return (null, null, 0);
            }

            nameLength++;
        }

        if (colonIndex < 0)
        {
            return (null, null, 0);
        }

        string paramName = sql[(startIndex + 1)..colonIndex].Trim();
        if (string.IsNullOrEmpty(paramName))
        {
            return (null, null, 0);
        }

        int i = colonIndex + 1;
        int typeStart = i;

        while (i < sql.Length)
        {
            char c = sql[i];

            if (c is '\'' or '`' or '"')
            {
                // Quoted token within the type, e.g. an Enum value or a named tuple element.
                i = SkipQuotedToken(sql, i);
            }
            else if (c == '{')
            {
                // A type definition never contains an opening brace, so this parameter is
                // unterminated and the brace starts a new one.
                return (null, null, 0);
            }
            else if (c == '}')
            {
                string paramType = sql[typeStart..i].Trim();
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

        // Unterminated parameter.
        return (null, null, 0);
    }

    /// <summary>
    /// Reports whether the character can appear in a ClickHouse query parameter name. The server parses
    /// the name as a bare word, which is narrower than an identifier: a quoted identifier such as
    /// <c>{`a`:Int32}</c> or <c>{"a":Int32}</c> is rejected as a syntax error. Only ASCII word characters and $.
    /// </summary>
    /// <param name="c">The character.</param>
    /// <returns>True when the character can appear in a parameter name.</returns>
    private static bool IsParameterNameChar(char c) =>
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '$';

    /// <summary>Returns the index after the next newline, or the end of the SQL.</summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="startIndex">The index to scan from.</param>
    /// <returns>The index after the newline.</returns>
    private static int SkipToEndOfLine(string sql, int startIndex)
    {
        int newlineIndex = sql.IndexOf('\n', startIndex);
        return newlineIndex < 0 ? sql.Length : newlineIndex + 1;
    }

    /// <summary>Skips a nestable C-style block comment, starting after the opening marker.</summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="startIndex">The index to scan from.</param>
    /// <returns>The index after the matching closing marker, or the end of the SQL.</returns>
    private static int SkipBlockComment(string sql, int startIndex)
    {
        int depth = 1;
        int i = startIndex;

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
                {
                    return i;
                }
            }
            else
            {
                i++;
            }
        }

        return sql.Length;
    }

    /// <summary>
    /// Skips a single-quoted string literal or a backtick/double-quote quoted identifier, starting at the
    /// opening quote. Both doubling (<c>''</c>) and backslash (<c>\'</c>) escapes are honored.
    /// </summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="startIndex">The index of the opening quote.</param>
    /// <returns>The index after the closing quote, or the end of the SQL when unterminated.</returns>
    private static int SkipQuotedToken(string sql, int startIndex)
    {
        char quote = sql[startIndex];
        int i = startIndex + 1;

        while (i < sql.Length)
        {
            char c = sql[i];

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
    /// Tries to skip a heredoc starting at a <c>$</c>: <c>$tag$ ... $tag$</c>, where the tag is empty or
    /// ASCII word characters. Returns false when this <c>$</c> does not open a terminated heredoc, in which
    /// case it is an ordinary character. A heredoc can only begin at a token boundary, see <see cref="IsTokenChar"/>.
    /// </summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="startIndex">The index of the dollar sign.</param>
    /// <param name="endIndex">The index after the closing tag.</param>
    /// <returns>True when a terminated heredoc starts here.</returns>
    private static bool TrySkipHeredoc(string sql, int startIndex, out int endIndex)
    {
        endIndex = 0;

        // A $ that continues a token cannot open a heredoc: the server lexes b$c$ as one identifier.
        if (startIndex > 0 && IsTokenChar(sql[startIndex - 1]))
        {
            return false;
        }

        int i = startIndex + 1;
        while (i < sql.Length && IsHeredocTagChar(sql[i]))
        {
            i++;
        }

        if (i >= sql.Length || sql[i] != '$')
        {
            return false;
        }

        string tag = sql[startIndex..(i + 1)];
        int closeIndex = sql.IndexOf(tag, i + 1, StringComparison.Ordinal);
        if (closeIndex < 0)
        {
            return false;
        }

        endIndex = closeIndex + tag.Length;
        return true;
    }

    /// <summary>Reports whether the character can appear in a heredoc tag.</summary>
    /// <param name="c">The character.</param>
    /// <returns>True when the character can appear in a heredoc tag.</returns>
    private static bool IsHeredocTagChar(char c) =>
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';

    /// <summary>
    /// Reports whether the character can continue an ordinary token, so that a <c>$</c> following it is part
    /// of that token rather than the start of a heredoc. The server lexes a word token as a run of ASCII word
    /// characters and dollar signs, so <c>b$c$</c> is one identifier and not a heredoc opener. Looking only at
    /// the preceding character misses the case where it ends a literal instead of a word, as in
    /// <c>1$tag$...$tag$</c>, where the server does open a heredoc. That shape places two literals next to each
    /// other, which the server rejects as a syntax error, so no query it accepts is affected.
    /// </summary>
    /// <param name="c">The character.</param>
    /// <returns>True when a dollar sign after this character continues a token.</returns>
    private static bool IsTokenChar(char c) => IsHeredocTagChar(c) || c == '$';
}
