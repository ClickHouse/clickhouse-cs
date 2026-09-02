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
    /// Appends a value prefixed with its length, so that a concatenation of appended values maps back to
    /// exactly one sequence — whatever characters the values themselves hold.
    /// </summary>
    /// <param name="builder">Builder to append to</param>
    /// <param name="value">Value to append</param>
    /// <returns>The same builder, for chaining</returns>
    public static StringBuilder AppendLengthPrefixed(this StringBuilder builder, string value) =>
        builder.Append(value.Length).Append(':').Append(value);

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
    /// Encloses a possibly database-qualified name (<c>database.table</c>) in backticks, one part at a
    /// time, so that a part which is not a legal unquoted identifier still reaches the server as a
    /// single identifier. Parts that are already enclosed are left as they are, which keeps a name
    /// that a caller pre-quoted from being quoted twice.
    /// </summary>
    /// <param name="str">Possibly qualified, possibly already enclosed name</param>
    /// <returns>Name with every part enclosed</returns>
    public static string EncloseQualifiedName(this string str)
    {
        if (string.IsNullOrEmpty(str) || str.IndexOf('.') < 0)
            return EnclosePart(str);

        var builder = new StringBuilder(str.Length + 4);
        var position = 0;
        while (true)
        {
            var end = FindPartEnd(str, position);
            if (builder.Length > 0)
                builder.Append('.');
            builder.Append(EnclosePart(str[position..end]));
            if (end >= str.Length)
                return builder.ToString();
            position = end + 1; // Skip the separator
        }
    }

    /// <summary>
    /// Encloses one part of a qualified name. A part the caller already enclosed is left as it is —
    /// in backticks, or in the double quotes ClickHouse equally accepts around an identifier, which
    /// is how a caller can have worked around the name not being enclosed at all.
    /// </summary>
    private static string EnclosePart(string part) =>
        part != null && part.Length >= 2 && part[0] == '"' && part[part.Length - 1] == '"'
            ? part
            : part.EncloseColumnName();

    /// <summary>
    /// Returns the index of the separator that ends the part starting at <paramref name="start"/>, or
    /// the length of <paramref name="str"/> for the last part. A separator inside an enclosed part
    /// belongs to the name, so it does not end the part.
    /// </summary>
    private static int FindPartEnd(string str, int start)
    {
        var i = start;
        if (i < str.Length && (str[i] == '`' || str[i] == '"'))
        {
            var quote = str[i];
            for (i++; i < str.Length; i++)
            {
                if (str[i] == '\\')
                {
                    i++; // The escaped character cannot close the part
                    continue;
                }

                if (str[i] == quote)
                {
                    i++;
                    break;
                }
            }
        }

        while (i < str.Length && str[i] != '.')
            i++;

        return i;
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

    /// <summary>
    /// Finds the space separating a leading identifier from the type which follows it, as used by
    /// named type elements (for example <c>`a b` Int64</c> in a named tuple, a Nested element or a
    /// JSON typed path). An identifier which needs quoting is enclosed in backticks by the server
    /// and may itself contain spaces, so the enclosed identifier is skipped before looking for the
    /// separator.
    /// </summary>
    /// <param name="str">Element declaration, optionally prefixed with an identifier</param>
    /// <returns>Index of the separator, or -1 if there is none (including an unterminated identifier).</returns>
    public static int IndexOfNameTypeSeparator(this string str)
    {
        if (string.IsNullOrEmpty(str))
            return -1;

        var i = 0;

        if (str[0] == '`')
        {
            for (i++; i < str.Length; i++)
            {
                // The server escapes a backtick inside a quoted identifier as \` ; a hand-written
                // type name may double it instead, the way SQL does.
                if (str[i] == '\\' || (str[i] == '`' && i + 1 < str.Length && str[i + 1] == '`'))
                {
                    i++;
                }
                else if (str[i] == '`')
                {
                    i++;
                    break;
                }
            }

            if (i >= str.Length)
                return -1; // unterminated identifier
        }

        return str.IndexOf(' ', i);
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
