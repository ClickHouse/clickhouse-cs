using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Splits a ClickHouse type string into tokens for <see cref="TypeParser"/>: the structural characters
/// <c>(</c>, <c>)</c>, <c>,</c> each as their own token, and the (trimmed) runs between them as identifier
/// tokens. Single-quoted spans (enum labels) are treated opaquely — commas and parentheses inside them, and
/// backslash escapes, do not split — so <c>Enum8('a,b' = 1)</c> tokenizes as one argument, not two.
/// </summary>
internal static class TypeTokenizer
{
    private static readonly char[] Breaks = [',', '(', ')'];

    /// <summary>Tokenizes a type string.</summary>
    /// <param name="input">The type string.</param>
    /// <returns>The token sequence, in source order.</returns>
    public static IEnumerable<string> Tokenize(string input)
    {
        int start = 0;
        bool inQuotes = false;
        bool escaped = false;

        for (int i = 0; i < input.Length; i++)
        {
            char c = input[i];
            if (inQuotes)
            {
                if (escaped)
                {
                    escaped = false;
                }
                else if (c == '\\')
                {
                    escaped = true;
                }
                else if (c == '\'')
                {
                    inQuotes = false;
                }

                continue;
            }

            if (c == '\'')
            {
                inQuotes = true;
                continue;
            }

            if (Array.IndexOf(Breaks, c) >= 0)
            {
                if (i > start)
                {
                    yield return input.Substring(start, i - start).Trim();
                }

                yield return input.Substring(i, 1);
                start = i + 1;
            }
        }

        if (inQuotes)
        {
            throw new FormatException(
                $"Malformed type string '{input}': unterminated quoted span (a single quote was never closed).");
        }

        if (start < input.Length)
        {
            yield return input.Substring(start).Trim();
        }
    }
}
