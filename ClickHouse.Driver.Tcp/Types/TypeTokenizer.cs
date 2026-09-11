using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Splits a ClickHouse type string into tokens for <see cref="TypeParser"/>: the structural characters
/// <c>(</c>, <c>)</c>, <c>,</c> each as their own token, and the (trimmed) runs between them as identifier
/// tokens. Quoted spans are opaque — single-quoted enum labels and backtick-quoted identifiers alike, so
/// <c>Enum8('a,b' = 1)</c> and <c>Tuple(`a,b` Int64)</c> each tokenize as one argument, not two. A run that is
/// only whitespace yields no token, so a pretty-printed <c>Array( Array(Int32) )</c> tokenizes like its compact
/// spelling.
/// </summary>
internal static class TypeTokenizer
{
    private static readonly char[] Breaks = [',', '(', ')'];

    /// <summary>Tokenizes a type string.</summary>
    /// <param name="input">The type string.</param>
    /// <returns>The token sequence, in source order.</returns>
    /// <exception cref="FormatException">A quoted span is never closed.</exception>
    public static IEnumerable<string> Tokenize(string input)
    {
        int start = 0;

        for (int i = 0; i < input.Length; i++)
        {
            char c = input[i];
            if (c is '\'' or '`')
            {
                int close = QuotedText.EndOfSpan(input, i);
                if (close < 0)
                {
                    throw new FormatException(
                        $"Malformed type string '{input}': unterminated quoted span (a {c} was never closed).");
                }

                i = close;
                continue;
            }

            if (Array.IndexOf(Breaks, c) >= 0)
            {
                string token = input.Substring(start, i - start).Trim();
                if (token.Length > 0)
                {
                    yield return token;
                }

                yield return input.Substring(i, 1);
                start = i + 1;
            }
        }

        if (start < input.Length)
        {
            string tail = input.Substring(start).Trim();
            if (tail.Length > 0)
            {
                yield return tail;
            }
        }
    }
}
