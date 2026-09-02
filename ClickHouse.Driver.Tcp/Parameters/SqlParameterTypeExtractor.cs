using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Extracts ClickHouse-native <c>{name:Type}</c> hints while ignoring SQL strings and comments.
/// </summary>
/// <remarks>
/// Ported from the HTTP scanner because the TCP assembly cannot reference it. Keep shared behavior aligned;
/// this copy additionally handles backslash-escaped quotes.
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
        bool inSqlString = false;

        while (i < sql.Length)
        {
            char c = sql[i];

            if (inSqlString)
            {
                // A backslash escapes the next character, including a quote.
                if (c == '\\' && i + 1 < sql.Length)
                {
                    i += 2;
                    continue;
                }

                // An escaped quote ('') stays inside the string.
                if (c == '\'' && i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    i += 2;
                    continue;
                }

                if (c == '\'')
                {
                    inSqlString = false;
                }

                i++;
                continue;
            }

            if (c == '\'')
            {
                inSqlString = true;
                i++;
            }
            else if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                // SQL-style line comment: -- (skip to end of line).
                i = SkipToEndOfLine(sql, i + 2);
            }
            else if (c == '#')
            {
                // SQL-style line comment: # or #! (skip to end of line).
                i = SkipToEndOfLine(sql, i + 1);
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                // C-style block comment: /* ... */ (skip to the closing */).
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

        // The colon separates the name from the type.
        int colonIndex = sql.IndexOf(':', startIndex + 1);
        if (colonIndex < 0)
        {
            return (null, null, 0);
        }

        int nameStart = startIndex + 1;
        string paramName = sql[nameStart..colonIndex].Trim();
        if (string.IsNullOrEmpty(paramName))
        {
            return (null, null, 0);
        }

        int i = colonIndex + 1;
        int typeStart = i;
        bool inQuote = false;

        while (i < sql.Length)
        {
            char c = sql[i];

            if (inQuote)
            {
                // A backslash escapes the next character inside the type argument.
                if (c == '\\' && i + 1 < sql.Length)
                {
                    i += 2;
                    continue;
                }

                // An escaped quote ('') stays inside the string.
                if (c == '\'' && i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    i += 2;
                    continue;
                }

                if (c == '\'')
                {
                    inQuote = false;
                }

                i++;
                continue;
            }

            if (c == '\'')
            {
                inQuote = true;
                i++;
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

    /// <summary>Returns the index after the next newline, or the end of the SQL.</summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="startIndex">The index to scan from.</param>
    /// <returns>The index after the newline.</returns>
    private static int SkipToEndOfLine(string sql, int startIndex)
    {
        int newlineIndex = sql.IndexOf('\n', startIndex);
        return newlineIndex < 0 ? sql.Length : newlineIndex + 1;
    }

    /// <summary>Returns the index after the block comment, or the end of the SQL.</summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="startIndex">The index to scan from.</param>
    /// <returns>The index after the closing marker.</returns>
    private static int SkipBlockComment(string sql, int startIndex)
    {
        int endIndex = sql.IndexOf("*/", startIndex, StringComparison.Ordinal);
        return endIndex < 0 ? sql.Length : endIndex + 2;
    }
}
