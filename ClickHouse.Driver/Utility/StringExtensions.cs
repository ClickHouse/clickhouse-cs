﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.RegularExpressions;

namespace ClickHouse.Driver.Utility;

public static class StringExtensions
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

    public static string ReplaceMultipleWords(this string input, Dictionary<string, string> replacements)
    {
        if (replacements == null || replacements.Count == 0)
            return input;
        var regex = "(" + string.Join("\\b|", replacements.Keys) + "\\b)";
        return Regex.Replace(input, regex, (Match m) => { return replacements[m.Value]; });
    }
}
