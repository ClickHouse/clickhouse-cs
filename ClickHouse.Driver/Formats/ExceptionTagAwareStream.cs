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
    private readonly string exceptionToken;
    private readonly byte[] exceptionMarker; // "__exception__" + token
    private readonly byte[] closingMarker;   // token + "__exception__"

    // Ring buffer for recent bytes
    private readonly byte[] recentBytes = new byte[BufferCapacity];
    private int writePosition;
    private int bytesRecorded;

    public ExceptionTagAwareStream(Stream innerStream, string exceptionTag)
    {
        this.innerStream = innerStream ?? throw new ArgumentNullException(nameof(innerStream));

        if (string.IsNullOrEmpty(exceptionTag))
            throw new ArgumentException("Exception tag cannot be null or empty", nameof(exceptionTag));

        exceptionToken = exceptionTag;
        exceptionMarker = Encoding.UTF8.GetBytes(ExceptionPrefix + exceptionTag);
        closingMarker = Encoding.UTF8.GetBytes(exceptionTag + ExceptionPrefix);
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
        if (bytesRecorded < exceptionMarker.Length)
            return null;

        byte[] buffer = GetLinearBuffer();
        int markerIndex = FindPattern(buffer, exceptionMarker);

        if (markerIndex < 0)
            return null;

        return ParseExceptionFormat(buffer, markerIndex);
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

    private ClickHouseServerException ParseExceptionFormat(byte[] buffer, int markerIndex)
    {
        // Format: __exception__TOKEN\n<message>\n<size> TOKEN__exception__
        int messageStart = markerIndex + exceptionMarker.Length;

        // Skip newlines/whitespace after opening marker
        while (messageStart < buffer.Length &&
               (buffer[messageStart] == '\n' || buffer[messageStart] == '\r'))
        {
            messageStart++;
        }

        // Find closing marker: TOKEN__exception__
        int closingIndex = FindPattern(buffer, closingMarker, messageStart);

        string errorMessage;
        if (closingIndex >= 0)
        {
            // Find the size number before closing marker (format: "<size> TOKEN__exception__")
            int sizeEnd = closingIndex;

            // Skip space before token
            while (sizeEnd > messageStart && buffer[sizeEnd - 1] == ' ')
                sizeEnd--;

            // Find start of size number
            int sizeStart = sizeEnd;
            while (sizeStart > messageStart && char.IsDigit((char)buffer[sizeStart - 1]))
                sizeStart--;

            // Message ends before the size number (and any preceding newline)
            int messageEnd = sizeStart;
            while (messageEnd > messageStart &&
                   (buffer[messageEnd - 1] == '\n' || buffer[messageEnd - 1] == '\r' || buffer[messageEnd - 1] == ' '))
            {
                messageEnd--;
            }

            if (messageEnd > messageStart)
                errorMessage = Encoding.UTF8.GetString(buffer, messageStart, messageEnd - messageStart);
            else
                errorMessage = "Unknown error (could not parse exception message)";
        }
        else
        {
            // Closing marker not found - extract available text
            int messageEnd = buffer.Length;
            while (messageEnd > messageStart &&
                   (buffer[messageEnd - 1] == '\n' || buffer[messageEnd - 1] == '\r'))
            {
                messageEnd--;
            }

            if (messageEnd > messageStart)
                errorMessage = Encoding.UTF8.GetString(buffer, messageStart, messageEnd - messageStart);
            else
                errorMessage = "Unknown error (exception marker found but message incomplete)";
        }

        return ClickHouseServerException.FromMidStreamException(errorMessage.Trim());
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
