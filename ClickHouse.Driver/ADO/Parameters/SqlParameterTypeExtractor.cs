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
        var inSqlString = false;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (inSqlString)
            {
                // Check for escaped quote ('')
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

            // Not in a SQL string
            if (c == '\'')
            {
                inSqlString = true;
                i++;
            }
            else if (c == '{')
            {
                // Potential parameter start - try to extract {name:Type}
                var (paramName, paramType, endIndex) = TryExtractParameter(sql, i);
                if (paramName != null && paramType != null)
                {
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

        // Find the colon that separates name from type
        var colonIndex = sql.IndexOf(':', startIndex + 1);
        if (colonIndex < 0)
            return (null, null, 0);

        var paramName = sql.Substring(startIndex + 1, colonIndex - startIndex - 1).Trim();
        if (string.IsNullOrEmpty(paramName))
            return (null, null, 0);

        var i = colonIndex + 1;

        // Extract type definition
        var typeStart = i;
        var inQuote = false;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (inQuote)
            {
                // Check for escaped quote ('')
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

            // Not in a quoted string within the type
            if (c == '\'')
            {
                inQuote = true;
                i++;
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
}
