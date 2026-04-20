using System.Collections.Generic;

namespace ClickHouse.Driver.Types.Grammar;

public static class Tokenizer
{
    private static readonly char[] Breaks = [',', '(', ')'];

    public static IEnumerable<string> GetTokens(string input)
    {
        var start = 0;
        var inQuotes = false;
        var escaped = false;

        for (var i = 0; i < input.Length; i++)
        {
            var c = input[i];
            if (inQuotes)
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

                if (c == '\'')
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
