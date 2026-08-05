using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace ClickHouse.Driver.Utility;

internal static class StringExtensions
{
    public static string Escape(this string str) => str.Replace("\\", "\\\\").Replace("\'", "\\\'").Replace("\n", "\\n").Replace("\t", "\\t");

    [SuppressMessage("Performance", "CA1865:Use char overload", Justification = "Not available in net462")]
    public static string QuoteSingle(this string str) => str.StartsWith("'", StringComparison.InvariantCulture) && str.EndsWith("'", StringComparison.InvariantCulture) ? str : $"'{str}'";

    [SuppressMessage("Performance", "CA1865:Use char overload", Justification = "Not available in net462")]
    public static string QuoteDouble(this string str) => str.StartsWith("\"", StringComparison.InvariantCulture) && str.EndsWith("\"", StringComparison.InvariantCulture) ? str : $"\"{str}\"";

    /// <summary>
    /// Encloses column name in backticks (`). Escapes ` symbol if met inside name
    /// Does nothing if column is already enclosed
    /// </summary>
    /// <param name="str">Column name</param>
    /// <returns>Backticked column name</returns>
    public static string EncloseColumnName(this string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;
        if (str[0] == '`' && str[str.Length - 1] == '`')
            return str; // Early return if already enclosed

        var builder = new StringBuilder();
        builder.Append('`');
        builder.Append(str.Replace("`", "\\`"));
        builder.Append('`');
        return builder.ToString();
    }

    /// <summary>
    /// Removes the enclosing backticks (`) from an identifier and unescapes the escape sequences
    /// ClickHouse uses inside a quoted identifier. Does nothing if the identifier is not enclosed.
    /// </summary>
    /// <param name="str">Possibly backtick-enclosed identifier</param>
    /// <returns>Bare identifier</returns>
    public static string DiscloseColumnName(this string str)
    {
        if (str == null || str.Length < 2 || str[0] != '`' || str[str.Length - 1] != '`')
            return str;

        var builder = new StringBuilder(str.Length - 2);
        for (var i = 1; i < str.Length - 1; i++)
        {
            var c = str[i];
            if (c != '\\' || i == str.Length - 2)
            {
                builder.Append(c);
                continue;
            }

            c = str[++i];
            builder.Append(c switch
            {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                '0' => '\0',
                'a' => '\a',
                'b' => '\b',
                'f' => '\f',
                'v' => '\v',
                _ => c, // \\, \`, \' and anything else stand for the character itself
            });
        }

        return builder.ToString();
    }

    public static string ToSnakeCase(this string str)
    {
        var result = new StringBuilder();
        for (int i = 0; i < str.Length; i++)
        {
            if (char.IsUpper(str[i]) && i > 0)
            {
                result.Append('_');
            }
            result.Append(char.ToLower(str[i], System.Globalization.CultureInfo.InvariantCulture));
        }

        return result.ToString();
    }
}
