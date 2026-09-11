using System.Text;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Scans the quoted spans a ClickHouse type string carries: single-quoted enum labels and backtick-quoted
/// identifiers (a tuple or nested field name, a JSON typed path). Both spellings use one escaping, checked
/// against a 26.6 server: <c>\a \b \e \f \n \r \t \v \0</c> and <c>\xHH</c> decode to that character, a
/// backslash before a quote, a backslash, a double quote or a slash yields that character, and any other
/// backslash pair keeps both characters. A doubled quote (<c>''</c> or <c>``</c>) is one literal quote, which
/// the server accepts on input although it always prints the backslash form.
/// </summary>
internal static class QuotedText
{
    /// <summary>Finds the end of the quoted span opening at <paramref name="openIndex"/>, without decoding it.</summary>
    /// <param name="input">The text to scan.</param>
    /// <param name="openIndex">The index of the opening quote character, which also selects the closing one.</param>
    /// <returns>The index of the closing quote, or -1 when the span is never closed.</returns>
    public static int EndOfSpan(string input, int openIndex) => Scan(input, openIndex, decoded: null);

    /// <summary>Reads the quoted span opening at <paramref name="openIndex"/> and decodes its escapes.</summary>
    /// <param name="input">The text to scan.</param>
    /// <param name="openIndex">The index of the opening quote character, which also selects the closing one.</param>
    /// <param name="text">The decoded text without its quotes, or null when the span is never closed.</param>
    /// <param name="end">The index just past the closing quote, or the input length when the span is never closed.</param>
    /// <returns>True when the span is closed.</returns>
    public static bool TryRead(string input, int openIndex, out string text, out int end)
    {
        var decoded = new StringBuilder();
        int close = Scan(input, openIndex, decoded);
        if (close < 0)
        {
            text = null;
            end = input.Length;
            return false;
        }

        text = decoded.ToString();
        end = close + 1;
        return true;
    }

    /// <summary>Scans one span, decoding into <paramref name="decoded"/> when a builder is supplied.</summary>
    /// <param name="input">The text to scan.</param>
    /// <param name="openIndex">The index of the opening quote character.</param>
    /// <param name="decoded">Receives the decoded text, or null to only find the end.</param>
    /// <returns>The index of the closing quote, or -1 when the span is never closed.</returns>
    private static int Scan(string input, int openIndex, StringBuilder decoded)
    {
        char quote = input[openIndex];
        for (int i = openIndex + 1; i < input.Length; i++)
        {
            char c = input[i];
            if (c == quote)
            {
                if (i + 1 < input.Length && input[i + 1] == quote)
                {
                    decoded?.Append(quote);
                    i++;
                    continue;
                }

                return i;
            }

            if (c == '\\' && i + 1 < input.Length)
            {
                i = AppendEscape(input, i, decoded);
                continue;
            }

            decoded?.Append(c);
        }

        return -1;
    }

    /// <summary>Decodes the escape sequence opening at <paramref name="backslash"/>.</summary>
    /// <param name="input">The text to scan.</param>
    /// <param name="backslash">The index of the backslash.</param>
    /// <param name="decoded">Receives the decoded character(s), or null to only measure the sequence.</param>
    /// <returns>The index of the sequence's last character.</returns>
    private static int AppendEscape(string input, int backslash, StringBuilder decoded)
    {
        char c = input[backslash + 1];
        if (c == 'x'
            && backslash + 3 < input.Length
            && TryHexDigit(input[backslash + 2], out int high)
            && TryHexDigit(input[backslash + 3], out int low))
        {
            decoded?.Append((char)((high << 4) | low));
            return backslash + 3;
        }

        switch (c)
        {
            case 'a': decoded?.Append('\a'); break;
            case 'b': decoded?.Append('\b'); break;
            case 'e': decoded?.Append('\u001B'); break;
            case 'f': decoded?.Append('\f'); break;
            case 'n': decoded?.Append('\n'); break;
            case 'r': decoded?.Append('\r'); break;
            case 't': decoded?.Append('\t'); break;
            case 'v': decoded?.Append('\v'); break;
            case '0': decoded?.Append('\0'); break;
            case '\\' or '\'' or '"' or '`' or '/': decoded?.Append(c); break;

            // An escape the server does not define keeps both characters, as the server's own lexer does.
            default: decoded?.Append('\\').Append(c); break;
        }

        return backslash + 1;
    }

    /// <summary>Reads one hexadecimal digit.</summary>
    /// <param name="c">The character.</param>
    /// <param name="value">The digit's value.</param>
    /// <returns>True when <paramref name="c"/> is a hexadecimal digit.</returns>
    private static bool TryHexDigit(char c, out int value)
    {
        value = c switch
        {
            >= '0' and <= '9' => c - '0',
            >= 'a' and <= 'f' => c - 'a' + 10,
            >= 'A' and <= 'F' => c - 'A' + 10,
            _ => -1,
        };

        return value >= 0;
    }
}
