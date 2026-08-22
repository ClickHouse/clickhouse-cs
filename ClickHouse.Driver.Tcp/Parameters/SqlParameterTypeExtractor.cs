using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Parameters;

/// <summary>
/// Extracts parameter type hints from SQL queries using ClickHouse's native parameter syntax, {parameter_name:type}.
/// This is a port of the HTTP scanner <c>ClickHouse.Driver.ADO.Parameters.SqlParameterTypeExtractor</c>. The TCP
/// assembly cannot reference <c>ClickHouse.Driver</c>, because the project reference runs the other way. Keep the
/// two copies in step: a change to one must be applied to the other.
/// <para>
/// The two have already diverged in one place. This copy treats a backslash as an escape inside a string
/// literal, which the HTTP copy does not, so a query holding <c>\'</c> keeps its type hints here and loses
/// them there. That is a defect in the HTTP copy; fixing it changes a shipped client, so it is filed rather
/// than done — see the parity-check entry in the TCP TODO.
/// </para>
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
                // A backslash escapes whatever follows, so \' stays inside the string. Without this the
                // string appears to end early and the real placeholder after it is never seen.
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

    /// <summary>
    /// Tries to extract a parameter from the given position.
    /// Returns (name, type, endIndex) if successful, or (null, null, 0) if not a valid parameter.
    /// </summary>
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
                // A backslash escapes whatever follows, so \' stays inside the quoted argument. Without this
                // an enum label such as 'it\'s' ends early and the type is truncated.
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

    /// <summary>
    /// Skips to the end of a line.
    /// Returns the index of the first character after the newline, or sql.Length if no newline found.
    /// </summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="startIndex">The index to scan from.</param>
    /// <returns>The index after the newline.</returns>
    private static int SkipToEndOfLine(string sql, int startIndex)
    {
        int newlineIndex = sql.IndexOf('\n', startIndex);
        return newlineIndex < 0 ? sql.Length : newlineIndex + 1;
    }

    /// <summary>
    /// Skips a C-style block comment (after /*).
    /// Returns the index of the first character after */, or sql.Length if not found.
    /// </summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="startIndex">The index to scan from.</param>
    /// <returns>The index after the closing marker.</returns>
    private static int SkipBlockComment(string sql, int startIndex)
    {
        int endIndex = sql.IndexOf("*/", startIndex, StringComparison.Ordinal);
        return endIndex < 0 ? sql.Length : endIndex + 2;
    }
}
