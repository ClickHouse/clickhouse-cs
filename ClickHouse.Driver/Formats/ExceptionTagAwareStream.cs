using System;
using System.IO;
using System.Text;

namespace ClickHouse.Driver.Formats;

/// <summary>
/// Stream wrapper that records recently-read bytes in a ring buffer and can detect
/// ClickHouse mid-stream exception markers when parsing failures occur.
/// </summary>
internal sealed class ExceptionTagAwareStream : Stream
{
    private const string ExceptionPrefix = "__exception__";
    private const int BufferCapacity = 4096; // 4KB ring buffer

    private readonly Stream innerStream;
    private readonly byte[] exceptionPrefixBytes; // "__exception__"
    private readonly byte[] tagBytes;             // exception tag/token

    // Ring buffer for recent bytes
    private readonly byte[] recentBytes = new byte[BufferCapacity];
    private int writePosition;
    private int bytesRecorded;

    public ExceptionTagAwareStream(Stream innerStream, string exceptionTag)
    {
        this.innerStream = innerStream ?? throw new ArgumentNullException(nameof(innerStream));

        if (string.IsNullOrEmpty(exceptionTag))
            throw new ArgumentException("Exception tag cannot be null or empty", nameof(exceptionTag));

        exceptionPrefixBytes = Encoding.UTF8.GetBytes(ExceptionPrefix);
        tagBytes = Encoding.UTF8.GetBytes(exceptionTag);
    }

    public override bool CanRead => innerStream.CanRead;

    public override bool CanSeek => innerStream.CanSeek;

    public override bool CanWrite => innerStream.CanWrite;

    public override long Length => innerStream.Length;

    public override long Position
    {
        get => innerStream.Position;
        set => innerStream.Position = value;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        int bytesRead = innerStream.Read(buffer, offset, count);

        if (bytesRead > 0)
            RecordBytes(buffer, offset, bytesRead);

        return bytesRead;
    }

    public override int ReadByte()
    {
        int b = innerStream.ReadByte();
        if (b >= 0)
        {
            recentBytes[writePosition] = (byte)b;
            writePosition = (writePosition + 1) % BufferCapacity;
            if (bytesRecorded < BufferCapacity)
                bytesRecorded++;
        }
        return b;
    }

    private void RecordBytes(byte[] buffer, int offset, int count)
    {
        // If count >= buffer capacity, only keep last BufferCapacity bytes
        if (count >= BufferCapacity)
        {
            Array.Copy(buffer, offset + count - BufferCapacity, recentBytes, 0, BufferCapacity);
            writePosition = 0;
            bytesRecorded = BufferCapacity;
            return;
        }

        // Copy into circular buffer, wrapping as needed
        int firstPart = Math.Min(count, BufferCapacity - writePosition);
        Array.Copy(buffer, offset, recentBytes, writePosition, firstPart);

        if (firstPart < count)
            Array.Copy(buffer, offset + firstPart, recentBytes, 0, count - firstPart);

        writePosition = (writePosition + count) % BufferCapacity;
        bytesRecorded = Math.Min(bytesRecorded + count, BufferCapacity);
    }

    /// <summary>
    /// Scans the ring buffer for a mid-stream exception marker and returns a
    /// ClickHouseServerException if found.
    /// </summary>
    /// <returns>ClickHouseServerException if marker found, null otherwise</returns>
    public ClickHouseServerException TryExtractMidStreamException()
    {
        if (bytesRecorded < exceptionPrefixBytes.Length + tagBytes.Length)
            return null;

        byte[] buffer = GetLinearBuffer();

        // Opening marker: "__exception__" <optional CR/LF> "<tag>".
        int markerIndex = FindDelimitedMarker(buffer, exceptionPrefixBytes, tagBytes, 0, out int messageStart);
        if (markerIndex < 0)
            return null;

        return ParseExceptionFormat(buffer, messageStart);
    }

    private byte[] GetLinearBuffer()
    {
        var result = new byte[bytesRecorded];

        if (bytesRecorded < BufferCapacity)
        {
            // Buffer hasn't wrapped - data starts at 0
            Array.Copy(recentBytes, 0, result, 0, bytesRecorded);
        }
        else
        {
            // Buffer has wrapped - writePosition is where oldest data starts
            int firstPart = BufferCapacity - writePosition;
            Array.Copy(recentBytes, writePosition, result, 0, firstPart);
            Array.Copy(recentBytes, 0, result, firstPart, writePosition);
        }

        return result;
    }

    private ClickHouseServerException ParseExceptionFormat(byte[] buffer, int messageStart)
    {
        // Full block: __exception__<sep><tag><sep><message>\n<size> <tag><sep>__exception__
        // where <sep> is the CR/LF run the server writes. messageStart points just past the
        // opening "<tag>"; skip the separator before the message text. We ignore <size>.
        while (messageStart < buffer.Length && (buffer[messageStart] == '\n' || buffer[messageStart] == '\r'))
            messageStart++;

        // Closing marker: "<tag>" <optional CR/LF> "__exception__".
        int closingIndex = FindDelimitedMarker(buffer, tagBytes, exceptionPrefixBytes, messageStart, out _);

        // Determine where message ends
        int messageEnd = closingIndex >= 0 ? closingIndex : buffer.Length;

        // Trim trailing whitespace/newlines (includes the size number line if present)
        while (messageEnd > messageStart && char.IsWhiteSpace((char)buffer[messageEnd - 1]))
            messageEnd--;

        // The "<size> " token sits between the message and the closing marker, so only strip a
        // trailing number when a closing marker was actually found. Otherwise a message captured
        // without its closing marker that legitimately ends in a digit would be mangled.
        if (closingIndex >= 0)
        {
            while (messageEnd > messageStart && char.IsDigit((char)buffer[messageEnd - 1]))
                messageEnd--;
            while (messageEnd > messageStart && char.IsWhiteSpace((char)buffer[messageEnd - 1]))
                messageEnd--;
        }

        if (messageEnd <= messageStart)
            return ClickHouseServerException.FromMidStreamException("Unknown error (could not parse exception message)");

        var errorMessage = Encoding.UTF8.GetString(buffer, messageStart, messageEnd - messageStart);
        return ClickHouseServerException.FromMidStreamException(errorMessage);
    }

    /// <summary>
    /// Finds <paramref name="first"/> followed by <paramref name="second"/>, allowing an optional
    /// run of CR/LF bytes between them. The ClickHouse server frames the in-band exception block as
    /// "__exception__\r\n&lt;tag&gt;" (open) and "&lt;tag&gt;\r\n__exception__" (close); tolerating the
    /// separator — and its absence — keeps detection robust across server framings.
    /// </summary>
    /// <param name="afterSecond">Index immediately past <paramref name="second"/> when found; -1 otherwise.</param>
    /// <returns>Index of <paramref name="first"/> when the delimited pair is found; -1 otherwise.</returns>
    private static int FindDelimitedMarker(byte[] buffer, byte[] first, byte[] second, int startIndex, out int afterSecond)
    {
        afterSecond = -1;
        int searchFrom = startIndex;

        while (true)
        {
            int firstIndex = FindPattern(buffer, first, searchFrom);
            if (firstIndex < 0)
                return -1;

            int pos = firstIndex + first.Length;

            // Skip an optional CR/LF separator run between the two parts.
            while (pos < buffer.Length && (buffer[pos] == (byte)'\r' || buffer[pos] == (byte)'\n'))
                pos++;

            if (pos + second.Length <= buffer.Length &&
                buffer.AsSpan(pos, second.Length).SequenceEqual(second))
            {
                afterSecond = pos + second.Length;
                return firstIndex;
            }

            // This occurrence of `first` is not followed by `second`; keep searching.
            searchFrom = firstIndex + 1;
        }
    }

    private static int FindPattern(byte[] buffer, byte[] pattern, int startIndex = 0)
    {
        if (pattern.Length == 0 || buffer.Length < pattern.Length + startIndex)
            return -1;

        var span = buffer.AsSpan(startIndex);
        byte firstByte = pattern[0];
        int patternLength = pattern.Length;

        int offset = 0;
        while (offset <= span.Length - patternLength)
        {
            // Use SIMD-optimized IndexOf to find first byte
            int pos = span.Slice(offset).IndexOf(firstByte);
            if (pos < 0)
                return -1;

            offset += pos;

            // Check if we have enough room for full pattern
            if (offset > span.Length - patternLength)
                return -1;

            // Verify the rest of the pattern
            if (span.Slice(offset, patternLength).SequenceEqual(pattern))
                return startIndex + offset;

            offset++;
        }

        return -1;
    }

    public override void Flush() => innerStream.Flush();

    public override long Seek(long offset, SeekOrigin origin) => innerStream.Seek(offset, origin);

    public override void SetLength(long value) => innerStream.SetLength(value);

    public override void Write(byte[] buffer, int offset, int count) => innerStream.Write(buffer, offset, count);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            innerStream.Dispose();

        base.Dispose(disposing);
    }
}
