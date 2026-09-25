using System.Text;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Scans and decodes quoted identifiers and enum labels in ClickHouse type strings.
/// Supports ClickHouse character, hexadecimal, backslash, and doubled-quote escapes.
/// </summary>
internal static class QuotedText
{
    /// <summary>Finds the closing quote without decoding the span.</summary>
    /// <param name="input">The text to scan.</param>
    /// <param name="openIndex">The opening quote index.</param>
    /// <returns>The index of the closing quote, or -1 when the span is never closed.</returns>
    public static int EndOfSpan(string input, int openIndex) => Scan(input, openIndex, decoded: null);

    /// <summary>Reads a quoted span and decodes its escapes.</summary>
    /// <param name="input">The text to scan.</param>
    /// <param name="openIndex">The opening quote index.</param>
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

    /// <summary>Scans one quoted span.</summary>
    /// <param name="input">The text to scan.</param>
    /// <param name="openIndex">The index of the opening quote character.</param>
    /// <param name="decoded">Receives decoded text, or null to scan only.</param>
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

    /// <summary>Decodes one escape sequence.</summary>
    /// <param name="input">The text to scan.</param>
    /// <param name="backslash">The index of the backslash.</param>
    /// <param name="decoded">Receives decoded characters, or null to scan only.</param>
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

            // Preserve unknown escapes, matching the server lexer.
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
