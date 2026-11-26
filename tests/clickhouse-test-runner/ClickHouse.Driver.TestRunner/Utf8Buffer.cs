using System.Text;

namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// A growable byte buffer for the shim's output. The output is bytes, not text: a <c>FixedString</c> or a
/// byte array can hold bytes that are not UTF-8, and the server writes them as they are.
/// </summary>
internal sealed class Utf8Buffer
{
    private byte[] buffer = new byte[4096];

    public int Length { get; private set; }

    public ReadOnlySpan<byte> Span => buffer.AsSpan(0, Length);

    public void Clear() => Length = 0;

    public void Append(byte b)
    {
        Ensure(1);
        buffer[Length++] = b;
    }

    public void Append(char c)
    {
        if (c < 0x80)
        {
            Append((byte)c);
            return;
        }
        Append(c.ToString());
    }

    public void Append(ReadOnlySpan<byte> bytes)
    {
        Ensure(bytes.Length);
        bytes.CopyTo(buffer.AsSpan(Length));
        Length += bytes.Length;
    }

    /// <summary>Appends <paramref name="s"/> as UTF-8.</summary>
    public void Append(string s)
    {
        Ensure(Encoding.UTF8.GetMaxByteCount(s.Length));
        Length += Encoding.UTF8.GetBytes(s, buffer.AsSpan(Length));
    }

    public void Append(Utf8Buffer other) => Append(other.Span);

    /// <summary>
    /// Appends <paramref name="bytes"/> with the escapes of ClickHouse's <c>writeAnyEscapedString</c> (with
    /// the single quote as the quote character), quoted when <paramref name="quote"/> is set.
    /// </summary>
    public void AppendEscaped(ReadOnlySpan<byte> bytes, bool quote)
    {
        if (quote)
        {
            Append((byte)'\'');
        }
        foreach (var c in bytes)
        {
            switch (c)
            {
                case (byte)'\\':
                    Append("\\\\"u8);
                    break;
                case (byte)'\'':
                    Append("\\'"u8);
                    break;
                case (byte)'\b':
                    Append("\\b"u8);
                    break;
                case (byte)'\f':
                    Append("\\f"u8);
                    break;
                case (byte)'\n':
                    Append("\\n"u8);
                    break;
                case (byte)'\r':
                    Append("\\r"u8);
                    break;
                case (byte)'\t':
                    Append("\\t"u8);
                    break;
                case 0:
                    Append("\\0"u8);
                    break;
                default:
                    Append(c);
                    break;
            }
        }
        if (quote)
        {
            Append((byte)'\'');
        }
    }

    /// <summary>Appends the UTF-8 bytes of <paramref name="s"/> with escapes; see <see cref="AppendEscaped(ReadOnlySpan{byte}, bool)"/>.</summary>
    public void AppendEscaped(string s, bool quote) => AppendEscaped(Encoding.UTF8.GetBytes(s), quote);

    /// <summary>Appends text that has no characters to escape (dates, UUIDs, addresses), quoted when nested.</summary>
    public void AppendQuotedIf(string s, bool quote)
    {
        if (quote)
        {
            Append((byte)'\'');
        }
        Append(s);
        if (quote)
        {
            Append((byte)'\'');
        }
    }

    public void WriteTo(Stream stream) => stream.Write(buffer, 0, Length);

    public override string ToString() => Encoding.UTF8.GetString(Span);

    private void Ensure(int extra)
    {
        if (Length + extra <= buffer.Length)
        {
            return;
        }
        var size = buffer.Length * 2;
        while (size < Length + extra)
        {
            size *= 2;
        }
        Array.Resize(ref buffer, size);
    }
}
