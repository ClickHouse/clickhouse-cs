using System.Collections.Generic;

namespace ClickHouse.Driver.Types.Grammar;

public static class Tokenizer
{
    private static readonly char[] Breaks = [',', '(', ')'];

    /// <summary>
    /// Quote characters ClickHouse uses when formatting a type name: single quotes for string
    /// literals (enum values, timezones) and backticks for identifiers which need quoting
    /// (such as a JSON typed path containing a space or a comma).
    /// </summary>
    private static readonly char[] Quotes = ['\'', '`'];

    public static IEnumerable<string> GetTokens(string input)
    {
        var start = 0;
        var quote = '\0';
        var escaped = false;

        for (var i = 0; i < input.Length; i++)
        {
            var c = input[i];
            if (quote != '\0')
            {
                if (escaped)
                {
                    escaped = false;
                    continue;
                }

                if (c == '\\')
                {
                    escaped = true;
                    continue;
                }

                if (c == quote)
                {
                    quote = '\0';
                }

                continue;
            }

            if (System.Array.IndexOf(Quotes, c) >= 0)
            {
                quote = c;
                continue;
            }

            if (System.Array.IndexOf(Breaks, c) >= 0)
            {
                if (i > start)
                {
                    yield return input.Substring(start, i - start).Trim();
                }

                yield return input.Substring(i, 1);
                start = i + 1;
            }
        }

        if (start < input.Length)
        {
            yield return input.Substring(start).Trim();
        }
    }
}
